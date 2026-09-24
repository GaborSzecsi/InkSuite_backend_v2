"""Bounded EPUB ingestion and passive, allowlisted resource delivery.
The original archive is never a reader resource. Scripts, embedded documents,
remote requests, forms and SVG are intentionally unsupported in ARC v1.
"""

from io import BytesIO
from pathlib import PurePosixPath
from urllib.parse import unquote, urlsplit
import hashlib
import posixpath
import re
import zipfile
from defusedxml import ElementTree as SafeET
from xml.etree import ElementTree as ET
from fastapi import HTTPException

ET.register_namespace("", "http://www.w3.org/1999/xhtml")
ET.register_namespace("epub", "http://www.idpf.org/2007/ops")

MAX_EXPANDED = 160 * 1024 * 1024
MAX_RESOURCE = 12 * 1024 * 1024
ALLOWED = {
    "application/xhtml+xml",
    "text/html",
    "text/css",
    "image/jpeg",
    "image/png",
    "image/gif",
    "image/webp",
    "font/woff",
    "font/woff2",
    "font/ttf",
    "font/otf",
    "font/collection",
    "application/x-font-ttf",
    "application/x-font-opentype",
    "application/vnd.ms-opentype",
    "application/x-dtbncx+xml",
}
BLOCKED_TAGS = {
    "script",
    "iframe",
    "object",
    "embed",
    "form",
    "input",
    "button",
    "textarea",
    "select",
    "meta",
    "base",
    "svg",
    "math",
    "audio",
    "video",
    "source",
}


def fail(message="Choose a valid, unencrypted EPUB containing readable chapters."):
    raise HTTPException(422, message)


def local_path(value, parent=""):
    u = urlsplit(value)
    if u.scheme or u.netloc or value.startswith(("/", "\\")) or "\\" in value:
        return None
    decoded = unquote(u.path)
    if "\\" in decoded or "\x00" in decoded:
        return None
    path = posixpath.normpath(posixpath.join(parent, decoded))
    if path == ".." or path.startswith("../"):
        return None
    return path


def xml(data):
    try:
        return SafeET.fromstring(data)
    except Exception:
        fail("The EPUB contains unsafe or invalid XML.")


def inspect_epub(data):
    if not data:
        fail("Choose a non-empty EPUB file.")
    try:
        archive = zipfile.ZipFile(BytesIO(data))
        infos = archive.infolist()
        if len(infos) > 2500 or sum(i.file_size for i in infos) > MAX_EXPANDED:
            fail("The EPUB expands beyond the supported size.")
        names = set()
        for i in infos:
            name = i.filename
            if (
                name in names
                or local_path(name) != name.rstrip("/")
                or i.flag_bits & 1
                or (i.external_attr >> 16) & 0o170000 == 0o120000
            ):
                fail("The EPUB contains unsafe archive entries.")
            if (
                i.file_size > MAX_RESOURCE
                or i.file_size > max(i.compress_size, 1) * 250
            ):
                fail("The EPUB contains an oversized resource.")
            names.add(name)
        if archive.read("mimetype").strip() != b"application/epub+zip":
            fail()
        container = xml(archive.read("META-INF/container.xml"))
        roots = container.findall(".//{*}rootfile")
        if not roots:
            fail()
        opf = roots[0].get("full-path", "")
        if local_path(opf) != opf or opf not in names:
            fail()
        package = xml(archive.read(opf))
        manifest = {}
        ids = {}
        for item in package.findall(".//{*}manifest/{*}item"):
            path = local_path(item.get("href", ""), posixpath.dirname(opf))
            mime = item.get("media-type", "")
            if path in names and mime in ALLOWED:
                manifest[path] = mime
                ids[item.get("id")] = path
        obfuscated_fonts = {}
        if "META-INF/encryption.xml" in names:
            encryption = xml(archive.read("META-INF/encryption.xml"))
            entries = encryption.findall(
                "{http://www.w3.org/2001/04/xmlenc#}EncryptedData"
            )
            if not entries:
                fail("The EPUB has invalid font protection metadata.")
            identifier_id = package.get("unique-identifier")
            identifier = next(
                (
                    x.text or ""
                    for x in package.findall(".//{*}identifier")
                    if identifier_id and x.get("id") == identifier_id
                ),
                "",
            )
            identifier = re.sub(r"[ \t\r\n]", "", identifier)
            if not identifier:
                fail(
                    "The EPUB is missing the identifier needed to read its embedded fonts."
                )
            key = hashlib.sha1(identifier.encode("utf-8")).hexdigest()
            for entry in entries:
                method = entry.find("{*}EncryptionMethod")
                reference = entry.find("{*}CipherData/{*}CipherReference")
                if (
                    method is None
                    or method.get("Algorithm") != "http://www.idpf.org/2008/embedding"
                ):
                    fail(
                        "This EPUB uses unsupported encryption. Export a DRM-free EPUB."
                    )
                font_path = (
                    local_path(reference.get("URI", ""))
                    if reference is not None
                    else None
                )
                font_mime = manifest.get(font_path, "")
                if not font_path or not (
                    font_mime.startswith("font/")
                    or font_mime
                    in {
                        "application/vnd.ms-opentype",
                        "application/x-font-ttf",
                        "application/x-font-opentype",
                    }
                ):
                    fail(
                        "The EPUB font protection metadata points to a missing or unsupported font."
                    )
                obfuscated_fonts[font_path] = key
        spine = [
            ids.get(x.get("idref")) for x in package.findall(".//{*}spine/{*}itemref")
        ]
        if not spine or any(
            not x or manifest[x] not in ("application/xhtml+xml", "text/html")
            for x in spine
        ):
            fail(
                "This EPUB uses an unsupported or missing chapter format (SVG chapters are not supported)."
            )
        # Parse every XML resource during upload, not on a reader's first visit.
        for path, mime in manifest.items():
            if mime in (
                "application/xhtml+xml",
                "text/html",
                "application/x-dtbncx+xml",
            ):
                xml(archive.read(path))
        return {
            "opf": opf,
            "obfuscated_fonts": obfuscated_fonts,
            "resources": manifest,
            "spine": spine,
            "sha256": hashlib.sha256(data).hexdigest(),
        }
    except HTTPException:
        raise
    except (KeyError, ValueError, zipfile.BadZipFile, RuntimeError):
        fail()


def clean_declarations(content, path="", allowed=()):
    import tinycss2

    def safe_tokens(tokens):
        for token in tokens:
            if token.type in {"error", "bad-url", "bad-string"}:
                return False
            if token.type == "url":
                if local_path(token.value, posixpath.dirname(path)) not in allowed:
                    return False
            elif token.type == "function":
                if token.lower_name not in {
                    "url",
                    "format",
                    "rgb",
                    "rgba",
                    "hsl",
                    "hsla",
                    "calc",
                    "min",
                    "max",
                    "clamp",
                    "translate",
                    "translatex",
                    "translatey",
                    "translatez",
                    "translate3d",
                    "scale",
                    "scalex",
                    "scaley",
                    "scale3d",
                    "rotate",
                    "rotatex",
                    "rotatey",
                    "rotatez",
                    "rotate3d",
                    "skew",
                    "skewx",
                    "skewy",
                    "matrix",
                    "matrix3d",
                    "perspective",
                    "linear-gradient",
                    "radial-gradient",
                }:
                    return False
                if token.lower_name == "url":
                    args = [
                        x
                        for x in token.arguments
                        if x.type not in {"whitespace", "comment"}
                    ]
                    if (
                        len(args) != 1
                        or args[0].type != "string"
                        or local_path(args[0].value, posixpath.dirname(path))
                        not in allowed
                    ):
                        return False
                elif not safe_tokens(token.arguments):
                    return False
            elif hasattr(token, "content") and not safe_tokens(token.content):
                return False
        return True

    kept = []
    for declaration in tinycss2.parse_declaration_list(
        content, skip_comments=True, skip_whitespace=True
    ):
        if declaration.type != "declaration" or declaration.lower_name.startswith("--"):
            continue
        if declaration.lower_name in {"behavior", "-moz-binding"}:
            continue
        value = tinycss2.serialize(declaration.value)
        if declaration.lower_name == "position" and value.strip().lower() not in {
            "absolute",
            "relative",
            "static",
        }:
            continue
        if re.search(r"expression|javascript|[<>]", value, re.I) or not safe_tokens(
            declaration.value
        ):
            continue
        kept.append(
            declaration.lower_name
            + ":"
            + value
            + (" !important" if declaration.important else "")
        )
    return ";".join(kept)


def clean_css(text, path="", allowed=()):
    import tinycss2

    kept = []
    for rule in tinycss2.parse_stylesheet(
        text, skip_comments=True, skip_whitespace=True
    ):
        if rule.type == "qualified-rule":
            selector = tinycss2.serialize(rule.prelude)
            if any(x in selector for x in ("@", "\\", "<")):
                continue
            kept.append(
                selector + "{" + clean_declarations(rule.content, path, allowed) + "}"
            )
        elif (
            rule.type == "at-rule"
            and rule.lower_at_keyword == "font-face"
            and rule.content is not None
        ):
            kept.append(
                "@font-face{" + clean_declarations(rule.content, path, allowed) + "}"
            )
    return "\n".join(kept)


def resource(data, package, path):
    mime = (
        "application/oebps-package+xml"
        if path == package["opf"]
        else package["resources"].get(path)
    )
    if not mime:
        raise HTTPException(404, "Reader resource not found.")
    with zipfile.ZipFile(BytesIO(data)) as archive:
        raw = archive.read(path)
    font_key = package.get("obfuscated_fonts", {}).get(path)
    if font_key:
        key = bytes.fromhex(font_key)
        length = min(len(raw), 1040)
        raw = (
            bytes(
                value ^ key[index % len(key)]
                for index, value in enumerate(raw[:length])
            )
            + raw[length:]
        )
    if mime == "text/css":
        return (
            clean_css(
                raw.decode("utf-8", "replace"), path, package["resources"]
            ).encode(),
            mime,
        )
    if mime not in (
        "application/oebps-package+xml",
        "application/xhtml+xml",
        "text/html",
        "application/x-dtbncx+xml",
    ):
        return raw, mime
    root = xml(raw)
    allowed = set(package["resources"]) | {package["opf"]}
    for parent in list(root.iter()):
        for child in list(parent):
            if child.tag.split("}")[-1].lower() in BLOCKED_TAGS and not (
                child.tag.split("}")[-1].lower() == "meta"
                and (
                    mime == "application/oebps-package+xml"
                    or (
                        child.get("name", "").lower() == "viewport"
                        and not child.get("http-equiv")
                    )
                )
            ):
                parent.remove(child)
        for key, value in list(parent.attrib.items()):
            attr = key.split("}")[-1].lower()
            if attr.startswith("on") or attr in {
                "srcdoc",
                "srcset",
                "formaction",
                "action",
                "target",
                "download",
                "base",
            }:
                del parent.attrib[key]
            elif attr == "style":
                parent.set(key, clean_declarations(value, path, allowed))
            elif attr in {"href", "src", "poster", "data"}:
                target = local_path(value, posixpath.dirname(path))
                if target not in allowed or urlsplit(value).scheme:
                    del parent.attrib[key]
        if parent.tag.split("}")[-1].lower() == "style":
            parent.text = clean_css(parent.text or "", path, allowed)
    # Remove unsupported OPF items; the reader must never fetch arbitrary files.
    if path == package["opf"]:
        for manifest in root.findall(".//{*}manifest"):
            for item in list(manifest):
                if (
                    local_path(item.get("href", ""), posixpath.dirname(path))
                    not in allowed
                ):
                    manifest.remove(item)
    if mime in ("application/xhtml+xml", "text/html"):
        head = root.find("{*}head")
        if head is not None:
            ns = "{http://www.w3.org/1999/xhtml}"
            meta = ET.Element(
                ns + "meta",
                {
                    "http-equiv": "Content-Security-Policy",
                    "content": "default-src 'none'; img-src 'self' blob:; style-src 'self' 'unsafe-inline'; font-src 'self'; script-src 'none'; connect-src 'none'; frame-src 'none'; object-src 'none'; base-uri 'self'; form-action 'none'",
                },
            )
            head.insert(0, meta)
    return ET.tostring(root, encoding="utf-8", xml_declaration=True), mime
