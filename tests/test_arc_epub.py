import io
import zipfile
import pytest
from fastapi import HTTPException
from app.marketplace.arc_epub import inspect_epub, resource, local_path, clean_css


def make_epub(extra=None, chapter=None):
    files = {
        "mimetype": b"application/epub+zip",
        "META-INF/container.xml": b'<container><rootfiles><rootfile full-path="book.opf"/></rootfiles></container>',
        "book.opf": b'<package><manifest><item id="c" href="chapter.xhtml" media-type="application/xhtml+xml"/></manifest><spine><itemref idref="c"/></spine></package>',
        "chapter.xhtml": (
            chapter
            or '<html xmlns="http://www.w3.org/1999/xhtml"><head><title>Book</title></head><body><p>Chapter</p></body></html>'
        ).encode(),
    }
    files.update(extra or {})
    data = io.BytesIO()
    with zipfile.ZipFile(data, "w") as z:
        for name, content in files.items():
            z.writestr(name, content)
    return data.getvalue()


@pytest.mark.parametrize(
    "path",
    [
        "../secret",
        "/secret",
        "https://evil.invalid/x",
        "//evil.invalid/x",
        "a/../../secret",
        "%2e%2e/secret",
        "a\\secret",
    ],
)
def test_paths_cannot_escape(path):
    assert local_path(path) is None


@pytest.mark.parametrize(
    "extra",
    [
        {"../secret": b"x"},
        {"META-INF/encryption.xml": b"<x/>"},
        {"mimetype": b"application/zip"},
        {"executable.exe": b"x" * 13000000},
    ],
)
def test_rejects_invalid_archives(extra):
    with pytest.raises(HTTPException):
        inspect_epub(make_epub(extra))


def test_entities_are_rejected():
    with pytest.raises(HTTPException):
        inspect_epub(
            make_epub(
                chapter='<!DOCTYPE x [<!ENTITY bad SYSTEM "file:///etc/passwd">]><x>&bad;</x>'
            )
        )


def test_scripts_remote_links_and_styles_are_removed():
    data = make_epub(
        chapter='<html xmlns="http://www.w3.org/1999/xhtml"><head><style>@import "https://evil.invalid";p{color:red;background:url(https://evil.invalid)}</style></head><body><script>evil()</script><iframe src="https://evil.invalid"/><p onclick="evil()" style="background:red">Text</p><img src="https://evil.invalid"/></body></html>'
    )
    package = inspect_epub(data)
    content, mime = resource(data, package, "chapter.xhtml")
    text = content.decode()
    assert mime == "application/xhtml+xml"
    assert "evil" not in text and "onclick" not in text and "<script" not in text
    assert "Content-Security-Policy" in text and "<html xmlns=" in text
    assert "color:red" in text


def test_original_archive_and_unlisted_resources_never_delivered():
    data = make_epub({"private.txt": b"secret"})
    package = inspect_epub(data)
    for path in [
        "private.txt",
        "mimetype",
        "META-INF/container.xml",
        "book.epub",
        "../chapter.xhtml",
    ]:
        with pytest.raises(HTTPException):
            resource(data, package, path)


def test_css_escaped_urls_and_at_rules_blocked():
    css = clean_css('@import "x";p{background:u\\72l(x);color:blue;position:fixed}')
    assert "background" not in css and "@import" not in css and "fixed" not in css
    assert "color:blue" in css


def test_legacy_presigner_cannot_export_arc():
    from routers.uploads import _s3_url_for_key

    with pytest.raises(HTTPException) as error:
        _s3_url_for_key("tenants/test/data/uploads/title/arc/asset/version.epub")
    assert error.value.status_code == 403


def test_legacy_book_listing_omits_arc_objects(monkeypatch):
    from routers import uploads

    class S3:
        def list_objects_v2(self, **kwargs):
            return {
                "Contents": [
                    {
                        "Key": "tenants/test/data/uploads/title/arc/asset/version.epub",
                        "Size": 300,
                    }
                ]
            }

    monkeypatch.setattr(uploads, "USE_UPLOADS_S3", True)
    monkeypatch.setattr(uploads, "S3_BUCKET", "fixture")
    monkeypatch.setattr(uploads, "UPLOADS_S3_PREFIX", "tenants/test/data/uploads")
    monkeypatch.setattr(uploads, "_s3_client", lambda: S3())
    assert uploads.list_book_assets("title")["files"] == []


def test_accepts_archive_over_previous_upload_limit():
    data = make_epub({f"image-{i}.png": b"x" * (9 * 1024 * 1024) for i in range(5)})
    assert len(data) > 40 * 1024 * 1024
    assert inspect_epub(data)


def font_epub(algorithm="http://www.idpf.org/2008/embedding", target="font.ttf"):
    import hashlib
    original = b"\x00\x01\x00\x00" + bytes(range(256)) * 5
    key = hashlib.sha1(b"urn:test:book").digest()
    encoded = bytes(value ^ key[i % 20] for i, value in enumerate(original[:1040])) + original[1040:]
    data = make_epub({
        "book.opf": b'<package unique-identifier="uid"><metadata><identifier id="uid"> urn:test:book </identifier></metadata><manifest><item id="c" href="chapter.xhtml" media-type="application/xhtml+xml"/><item id="f" href="font.ttf" media-type="application/x-font-ttf"/></manifest><spine><itemref idref="c"/></spine></package>',
        "font.ttf": encoded,
        "META-INF/encryption.xml": f'<encryption xmlns:e="http://www.w3.org/2001/04/xmlenc#"><e:EncryptedData><e:EncryptionMethod Algorithm="{algorithm}"/><e:CipherData><e:CipherReference URI="{target}"/></e:CipherData></e:EncryptedData></encryption>'.encode(),
    })
    return data, original


def test_standard_font_obfuscation_is_accepted_and_decoded():
    data, original = font_epub()
    package = inspect_epub(data)
    decoded, _ = resource(data, package, "font.ttf")
    assert decoded == original


@pytest.mark.parametrize("algorithm,target", [
    ("http://www.w3.org/2001/04/xmlenc#aes256-cbc", "font.ttf"),
    ("http://www.idpf.org/2008/embedding", "chapter.xhtml"),
    ("http://www.idpf.org/2008/embedding", "../font.ttf"),
])
def test_encryption_exemption_only_allows_standard_embedded_fonts(algorithm, target):
    data, _ = font_epub(algorithm, target)
    with pytest.raises(HTTPException):
        inspect_epub(data)
