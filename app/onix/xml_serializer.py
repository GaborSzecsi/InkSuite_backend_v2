# app/onix/xml_serializer.py
# Canonical InkSuite ONIX payload -> ONIX 3.0 reference XML.
#
# No product metadata defaults are created here.
# Values are emitted only when supplied by assembly/database.

from __future__ import annotations

from datetime import datetime, timezone
from math import isfinite
import xml.etree.ElementTree as ET
from typing import Any, Dict, Iterable, List, Optional
from xml.dom import minidom

ONIX_NS = "http://ns.editeur.org/onix/3.0/reference"


def _s(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _attrs(**kwargs: Any) -> Dict[str, str]:
    return {
        key: _s(value)
        for key, value in kwargs.items()
        if _s(value)
    }


def _elem(
    parent: ET.Element,
    tag: str,
    value: Any = None,
    *,
    attrib: Optional[Dict[str, str]] = None,
    allow_empty: bool = False,
) -> Optional[ET.Element]:
    text = _s(value)
    if not text and not allow_empty:
        return None
    element = ET.SubElement(parent, tag, attrib or {})
    if text:
        element.text = text
    return element


def _has_values(mapping: Optional[Dict[str, Any]], *keys: str) -> bool:
    if not mapping:
        return False
    return any(_s(mapping.get(key)) for key in keys)


def _date_text(row: Dict[str, Any], text_key: str = "date_text", value_key: str = "date_value") -> str:
    text = _s(row.get(text_key)) or _s(row.get(value_key))
    return text.replace("-", "")

def _onix_number(value: Any) -> str:
    text = _s(value)
    if not text:
        return ""

    try:
        number = float(text)
    except (TypeError, ValueError):
        return text

    if number.is_integer():
        return str(int(number))

    return text


_FILE_FORMAT_TO_ONIX_DETAIL = {
    "EPUB": "E101",
    "EPUB2": "E101",
    "EPUB3": "E101",
    "PDF": "E107",
    "HTML": "E105",
    "MOBI": "E127",
    "AZW": "E116",
    "MP3": "A103",
    "WAV": "A104",
    "OTHER": "E100",
}

_EPUB_VERSION_TO_ONIX_CODE = {
    "2": "101A", "2.0": "101A", "2.0.1": "101A",
    "3": "101B", "3.0": "101B", "3.0.1": "101C",
    "3.1": "101D", "3.2": "101E", "3.3": "101F",
}

_COLOR_CONTENT_LABELS = {
    "00": "Color content unspecified",
    "01": "Black and white",
    "02": "Two color",
    "03": "Full color",
    "04": "Black and white with color sections",
    "05": "Mixed color",
}

def _file_format_detail(file_format: Any) -> str:
    return _FILE_FORMAT_TO_ONIX_DETAIL.get(_s(file_format).upper(), "")

def _digital_detail_is_compatible(product_form: Any, detail_code: str) -> bool:
    form = _s(product_form).upper()
    if not detail_code:
        return False
    if detail_code.startswith("E"):
        return form.startswith("E")
    if detail_code.startswith("A"):
        return form.startswith("A")
    if detail_code.startswith("V"):
        return form.startswith("V")
    return False

def _color_note(color_content: Any, color_pages: Any) -> str:
    code = _s(color_content)
    pages = _onix_number(color_pages)
    parts: List[str] = []
    if code:
        parts.append(_COLOR_CONTENT_LABELS.get(code, f"Color content {code}"))
    if pages:
        parts.append(f"{pages} color pages")
    return "; ".join(parts)


def _write_identifier(parent: ET.Element, tag: str, row: Dict[str, Any], *, type_tag: str, name_tag: str, value_tag: str) -> None:
    id_type = _s(
        row.get("id_type")
        or row.get("product_id_type")
        or row.get("work_id_type")
        or row.get("name_id_type")
        or row.get("supplier_id_type")
    )
    id_name = _s(
        row.get("id_type_name")
        or row.get("supplier_id_type_name")
    )
    id_value = _s(row.get("id_value") or row.get("identifier_value"))
    if not (id_type or id_name or id_value):
        return

    node = ET.SubElement(parent, tag)
    _elem(node, type_tag, id_type)
    _elem(node, name_tag, id_name)
    _elem(node, value_tag, id_value)


def _write_title_detail(parent: ET.Element, title: Dict[str, Any]) -> None:
    if not any(
        _s(title.get(key))
        for key in (
            "title_type",
            "title_element_level",
            "title_prefix",
            "title_without_prefix",
            "title_text",
            "subtitle",
            "part_number",
            "year_of_annual",
        )
    ):
        return

    detail = ET.SubElement(parent, "TitleDetail")
    _elem(detail, "TitleType", title.get("title_type"))

    element = ET.SubElement(detail, "TitleElement")
    _elem(element, "TitleElementLevel", title.get("title_element_level"))

    title_text = _s(title.get("title_text"))
    title_prefix = _s(title.get("title_prefix"))
    language_code = _s(title.get("language_code"))
    title_without_prefix = _s(
        title.get("title_without_prefix")
    )

    # ONIX TitleElement rule: TitleText is mutually exclusive
    # with TitlePrefix / TitleWithoutPrefix. Prefer the
    # structured form whenever it exists.
    if title_prefix or title_without_prefix:
        if title.get("no_prefix") is True:
            _elem(
                element,
                "NoPrefix",
                allow_empty=True,
            )
        else:
            _elem(
                element,
                "TitlePrefix",
                title_prefix,
                attrib=_attrs(language=language_code),
            )

        _elem(
            element,
            "TitleWithoutPrefix",
            title_without_prefix,
            attrib=_attrs(language=language_code),
        )
    else:
        _elem(
            element,
            "TitleText",
            title_text,
            attrib=_attrs(language=language_code),
        )

    _elem(element, "Subtitle", title.get("subtitle"), attrib=_attrs(language=language_code))
    _elem(element, "PartNumber", title.get("part_number"), attrib=_attrs(language=language_code))
    _elem(element, "YearOfAnnual", title.get("year_of_annual"))

def _write_collection(parent: ET.Element, row: Dict[str, Any]) -> None:
    if not any(
        _s(row.get(key))
        for key in (
            "collection_type",
            "collection_title",
            "collection_subtitle",
            "collection_number",
            "volume_number",
            "part_number",
        )
    ):
        return

    collection = ET.SubElement(parent, "Collection")
    _elem(collection, "CollectionType", row.get("collection_type"))

    for ident in row.get("identifiers") or []:
        _write_identifier(
            collection,
            "CollectionIdentifier",
            ident,
            type_tag="CollectionIDType",
            name_tag="IDTypeName",
            value_tag="IDValue",
        )

    if any(
        _s(row.get(key))
        for key in (
            "title_type",
            "collection_title",
            "collection_subtitle",
            "part_number",
        )
    ):
        detail = ET.SubElement(collection, "TitleDetail")
        _elem(detail, "TitleType", row.get("title_type"))

        element = ET.SubElement(detail, "TitleElement")
        _elem(element, "TitleElementLevel", row.get("title_element_level"))
        if row.get("no_prefix") is True:
            _elem(element, "NoPrefix", allow_empty=True)
        _elem(element, "TitleWithoutPrefix", row.get("collection_title"))
        _elem(element, "Subtitle", row.get("collection_subtitle"))
        _elem(element, "PartNumber", row.get("part_number"))

    # CollectionSequence is a composite in ONIX 3.x. Preserve InkSuite's
    # distinct collection-number and volume-number fields losslessly using
    # proprietary sequence labels, rather than assigning an unsupported
    # standard semantic. Existing explicit sequence_type/sequence_number is
    # also honored when present.
    sequences: List[Dict[str, Any]] = []

    explicit_type = _s(row.get("sequence_type"))
    explicit_number = _s(row.get("sequence_number"))
    if explicit_type and explicit_number:
        sequences.append({
            "type": explicit_type,
            "name": _s(row.get("sequence_type_name")),
            "number": explicit_number,
        })

    collection_number = _s(row.get("collection_number"))
    if collection_number:
        sequences.append({
            "type": "01",
            "name": "Collection number",
            "number": collection_number,
        })

    volume_number = _s(row.get("volume_number"))
    if volume_number:
        sequences.append({
            "type": "01",
            "name": "Volume number",
            "number": volume_number,
        })

    seen_sequences = set()
    for sequence in sequences:
        key = (sequence["type"], sequence["name"], sequence["number"])
        if key in seen_sequences:
            continue
        seen_sequences.add(key)

        seq = ET.SubElement(collection, "CollectionSequence")
        _elem(seq, "CollectionSequenceType", sequence["type"])
        if sequence["type"] == "01":
            _elem(seq, "CollectionSequenceTypeName", sequence["name"])
        _elem(seq, "CollectionSequenceNumber", sequence["number"])


def _write_contributor(parent: ET.Element, contributor: Dict[str, Any]) -> None:
    assignment = contributor.get("assignment") or {}
    party = contributor.get("party") or {}

    if not (
        _s(contributor.get("role"))
        or _s(party.get("display_name"))
        or _s(party.get("corporate_name"))
    ):
        return

    node = ET.SubElement(parent, "Contributor")
    _elem(node, "SequenceNumber", contributor.get("sequence_number"))
    _elem(node, "ContributorRole", contributor.get("role"))

    display_name = _s(party.get("display_name"))
    corporate_name = _s(party.get("corporate_name"))

    if display_name:
        _elem(node, "PersonName", display_name)
        _elem(node, "PersonNameInverted", party.get("person_name_inverted"))
        _elem(node, "NamesBeforeKey", party.get("names_before_key"))
        _elem(node, "KeyNames", party.get("key_names"))
    elif corporate_name:
        _elem(node, "CorporateName", corporate_name)

    for ident in contributor.get("name_identifiers") or []:
        # ONIX List 44: IDTypeName is only applicable to proprietary
        # name-identifier schemes. Suppress stale type names for standard
        # schemes such as ISNI (16), ORCID (21), VIAF (31), etc.
        ident_to_write = dict(ident)
        name_id_type = _s(
            ident_to_write.get("name_id_type")
            or ident_to_write.get("id_type")
        )
        if name_id_type not in {"01", "02"}:
            ident_to_write["id_type_name"] = ""

        _write_identifier(
            node,
            "NameIdentifier",
            ident_to_write,
            type_tag="NameIDType",
            name_tag="IDTypeName",
            value_tag="IDValue",
        )

    alternative_names = contributor.get("alternative_names") or []
    for alt in alternative_names:
        alt_node = ET.SubElement(node, "AlternativeName")
        _elem(alt_node, "NameType", alt.get("name_type"))
        _elem(alt_node, "PersonName", alt.get("display_name"))
        _elem(alt_node, "PersonNameInverted", alt.get("person_name_inverted"))
        _elem(alt_node, "NamesBeforeKey", alt.get("names_before_key"))
        _elem(alt_node, "KeyNames", alt.get("key_names"))
        _elem(alt_node, "CorporateName", alt.get("corporate_name"))

    # parties.pen_name is InkSuite's simple pseudonym field. Export it as an
    # ONIX AlternativeName / NameType 01 only when the richer repeatable
    # alternative-name data does not already contain the same pseudonym. This
    # preserves master-party alternative names instead of overwriting them.
    pen_name = _s(party.get("pen_name") or party.get("pseudonym"))
    if pen_name:
        existing_pseudonyms = {
            _s(alt.get("display_name")).casefold()
            for alt in alternative_names
            if _s(alt.get("name_type")) == "01" and _s(alt.get("display_name"))
        }
        if pen_name.casefold() not in existing_pseudonyms:
            pen_node = ET.SubElement(node, "AlternativeName")
            _elem(pen_node, "NameType", "01")
            _elem(pen_node, "PersonName", pen_name)

    for website in contributor.get("websites") or []:
        if not _s(website.get("website_link")):
            continue
        web = ET.SubElement(node, "Website")
        _elem(web, "WebsiteRole", website.get("website_role"))
        _elem(web, "WebsiteDescription", website.get("website_description"))
        _elem(web, "WebsiteLink", website.get("website_link"))

    for place in contributor.get("places") or []:
        if not any(
            _s(place.get(k))
            for k in ("contributor_place_relator", "country_code", "region_code", "location_name")
        ):
            continue
        p = ET.SubElement(node, "ContributorPlace")
        _elem(p, "ContributorPlaceRelator", place.get("contributor_place_relator"))
        _elem(p, "CountryCode", place.get("country_code"))
        _elem(p, "RegionCode", place.get("region_code"))
        _elem(p, "LocationName", place.get("location_name"))

    for code in contributor.get("from_language_codes") or []:
        lang = ET.SubElement(node, "FromLanguage")
        _elem(lang, "LanguageCode", code)

    for code in contributor.get("to_language_codes") or []:
        lang = ET.SubElement(node, "ToLanguage")
        _elem(lang, "LanguageCode", code)

    _elem(node, "ContributorDescription", contributor.get("contributor_description"))

    for d in contributor.get("dates") or []:
        dnode = ET.SubElement(node, "ContributorDate")
        _elem(dnode, "ContributorDateRole", d.get("contributor_date_role"))
        _elem(dnode, "Date", _date_text(d))

    for aff in contributor.get("professional_affiliations") or []:
        anode = ET.SubElement(node, "ProfessionalAffiliation")
        _elem(anode, "ProfessionalPosition", aff.get("professional_position"))
        _elem(anode, "Affiliation", aff.get("affiliation"))
        if _has_values(
            aff,
            "affiliation_id_type",
            "affiliation_id_type_name",
            "affiliation_id_value",
        ):
            aid = ET.SubElement(anode, "ProfessionalAffiliationIdentifier")
            _elem(aid, "ProfessionalAffiliationIDType", aff.get("affiliation_id_type"))
            _elem(aid, "IDTypeName", aff.get("affiliation_id_type_name"))
            _elem(aid, "IDValue", aff.get("affiliation_id_value"))

    _elem(
        node,
        "BiographicalNote",
        contributor.get("short_bio")
        or contributor.get("biographical_note")
        or contributor.get("biography")
        or party.get("short_bio")
        or party.get("biographical_note")
        or party.get("biography"),
        attrib=_attrs(textformat="05"),
    )


def _write_subject(parent: ET.Element, subject: Dict[str, Any]) -> None:
    if not any(
        _s(subject.get(k))
        for k in ("scheme_id", "scheme_name", "scheme_version", "subject_code", "heading_text", "keywords")
    ):
        return

    node = ET.SubElement(parent, "Subject")
    _elem(node, "MainSubject", allow_empty=True) if subject.get("is_main") is True else None
    scheme_id = _s(subject.get("scheme_id"))
    _elem(node, "SubjectSchemeIdentifier", scheme_id)

    # SubjectSchemeName is only appropriate for a proprietary subject
    # scheme (ONIX List 27 code 24). Standard schemes such as BISAC (10),
    # Keywords (20), and Thema (93) are identified by their scheme code
    # and must not emit a proprietary scheme name.
    if scheme_id == "24":
        _elem(node, "SubjectSchemeName", subject.get("scheme_name"))

    _elem(node, "SubjectSchemeVersion", subject.get("scheme_version"))
    _elem(node, "SubjectCode", subject.get("subject_code"))
    _elem(node, "SubjectHeadingText", subject.get("heading_text") or subject.get("keywords"))
    _elem(node, "RegionCode", subject.get("region_code"))


def _write_audience(parent: ET.Element, row: Dict[str, Any]) -> None:
    """
    ONIX Audience and AudienceRange are sibling composites under
    DescriptiveDetail. AudienceCodeValue belongs inside Audience.
    """
    code = _s(row.get("onix_audience_code"))
    code_type = _s(row.get("audience_code_type"))
    code_type_name = _s(row.get("audience_code_type_name"))

    # Audience is valid only when there is an actual AudienceCodeValue.
    # Range-only DB rows must serialize solely as AudienceRange.
    if code:
        aud = ET.SubElement(parent, "Audience")
        _elem(aud, "AudienceCodeType", code_type)
        _elem(aud, "AudienceCodeTypeName", code_type_name)
        _elem(aud, "AudienceCodeValue", code)

    qualifier = _s(row.get("audience_range_qualifier"))
    if qualifier:
        rng = ET.SubElement(parent, "AudienceRange")
        _elem(rng, "AudienceRangeQualifier", qualifier)

        if _s(row.get("range_value_1")):
            _elem(rng, "AudienceRangePrecision", row.get("range_precision_1"))
            _elem(rng, "AudienceRangeValue", row.get("range_value_1"))

        if _s(row.get("range_value_2")):
            _elem(rng, "AudienceRangePrecision", row.get("range_precision_2"))
            _elem(rng, "AudienceRangeValue", row.get("range_value_2"))



_ONIX_XHTML_TAGS = {"p", "br", "strong", "em", "b", "i", "cite", "ul", "ol", "li", "sub", "sup", "dl", "dt", "dd"}

def _append_onix_xhtml(parent: ET.Element, fragment: Any) -> None:
    raw = str(fragment or "").strip()
    if not raw: return
    try: wrapper = ET.fromstring(f"<wrapper>{raw}</wrapper>")
    except ET.ParseError as exc: raise ValueError(f"Invalid XHTML in ONIX text field: {exc}") from exc
    def clone(node: ET.Element) -> ET.Element:
        tag = str(node.tag).split("}")[-1].lower()
        if tag not in _ONIX_XHTML_TAGS: raise ValueError(f"Unsupported XHTML tag in ONIX text field: <{tag}>")
        attrs: Dict[str, str] = {}
        if tag == "ol":
            if node.get("type") in {"1", "a", "A", "i", "I"}: attrs["type"] = node.get("type") or ""
            start = node.get("start") or ""
            if start.isdigit(): attrs["start"] = start
        out = ET.Element(tag, attrs); out.text = node.text
        for child in list(node):
            cloned = clone(child); cloned.tail = child.tail; out.append(cloned)
        return out
    parent.text = wrapper.text
    for child in list(wrapper):
        cloned = clone(child); cloned.tail = child.tail; parent.append(cloned)

def _write_text_content(parent: ET.Element, row: Dict[str, Any]) -> None:
    if not _s(row.get("text_value")):
        return
    node = ET.SubElement(parent, "TextContent")
    _elem(node, "TextType", row.get("text_type"))
    _elem(node, "ContentAudience", row.get("content_audience") or row.get("audience"))
    text_format = _s(row.get("text_format")) or "06"
    if text_format == "05":
        text_node = ET.SubElement(node, "Text", _attrs(textformat="05"))
        _append_onix_xhtml(text_node, row.get("text_value"))
    else:
        _elem(node, "Text", row.get("text_value"), attrib=_attrs(textformat=text_format))
    _elem(node, "TextAuthor", row.get("author"))
    _elem(node, "TextSourceCorporate", row.get("source_corporate"))
    _elem(node, "SourceTitle", row.get("source_title"), attrib=_attrs(sourcetype=row.get("source_title_type")))
    _elem(node, "TextSourceLink", row.get("source_url"))

    for d in row.get("content_dates") or []:
        dn = ET.SubElement(node, "ContentDate")
        _elem(dn, "ContentDateRole", d.get("content_date_role"))
        _elem(dn, "Date", _date_text(d), attrib=_attrs(dateformat=d.get("date_format")))


def _write_cited_content(parent: ET.Element, row: Dict[str, Any]) -> None:
    if not any(_s(row.get(k)) for k in ("cited_content_type", "citation_note", "source_title", "resource_link")):
        return
    node = ET.SubElement(parent, "CitedContent")
    _elem(node, "CitedContentType", row.get("cited_content_type"))
    _elem(node, "ContentAudience", row.get("content_audience"))
    _elem(node, "SourceType", row.get("source_type"))
    _elem(node, "SourceTitle", row.get("source_title"))
    _elem(
        node,
        "CitationNote",
        row.get("citation_note"),
        attrib=_attrs(textformat=row.get("citation_note_text_format")),
    )
    _elem(node, "ResourceLink", row.get("resource_link"))
    _elem(node, "ListName", row.get("list_name"))
    _elem(node, "PositionOnList", row.get("position_on_list"))
    for d in row.get("content_dates") or []:
        dn = ET.SubElement(node, "ContentDate")
        _elem(dn, "ContentDateRole", d.get("content_date_role"))
        _elem(dn, "Date", _date_text(d), attrib=_attrs(dateformat=d.get("date_format")))


def _write_feature(parent: ET.Element, tag: str, type_tag: str, row: Dict[str, Any]) -> None:
    if not any(_s(row.get(k)) for k in ("feature_type", "feature_value", "feature_note")):
        return
    node = ET.SubElement(parent, tag)
    _elem(node, type_tag, row.get("feature_type"))
    _elem(node, "FeatureValue", row.get("feature_value"))
    _elem(node, "FeatureNote", row.get("feature_note"))


def _write_supporting_resource(parent: ET.Element, row: Dict[str, Any]) -> None:
    if not any(
        _s(row.get(k))
        for k in ("resource_content_type", "resource_mode", "resource_description", "caption")
    ) and not row.get("versions"):
        return

    node = ET.SubElement(parent, "SupportingResource")
    _elem(node, "ResourceContentType", row.get("resource_content_type"))
    _elem(node, "ContentAudience", row.get("content_audience"))
    _elem(node, "ResourceMode", row.get("resource_mode"))
    _elem(node, "ResourceDescription", row.get("resource_description"))
    _elem(node, "Caption", row.get("caption"))
    _elem(node, "Credit", row.get("credit"))

    territory_countries = _s(row.get("territory_countries"))
    if territory_countries:
        terr = ET.SubElement(node, "Territory")
        _elem(terr, "CountriesIncluded", territory_countries)

    resource_features = row.get("resource_features") or []
    for feature in resource_features:
        _write_feature(
            node,
            "ResourceFeature",
            "ResourceFeatureType",
            feature,
        )

    # ONIX List 160 code 04 carries the approximate duration of an
    # audio/video supporting resource in whole minutes at SupportingResource
    # level. InkSuite stores the editable value as seconds on the version row.
    # Do not manufacture a duplicate if an explicit type-04 ResourceFeature
    # already exists.
    has_duration_feature = any(
        _s(feature.get("feature_type")) == "04"
        for feature in resource_features
        if isinstance(feature, dict)
    )
    # List 160 code 04 is only semantically applicable to audio/video
    # supporting resources. Do not derive it from duration_seconds for images
    # or other resource modes merely because a stale/accidental value exists.
    resource_mode = _s(row.get("resource_mode"))
    supports_duration = resource_mode in {"01", "02"}

    if supports_duration and not has_duration_feature:
        duration_seconds = ""
        for version in row.get("versions") or []:
            candidate = _s(version.get("duration_seconds"))
            if candidate:
                duration_seconds = candidate
                break
        if duration_seconds:
            try:
                seconds = float(duration_seconds)
                if isfinite(seconds) and seconds > 0:
                    # Implementation choice: conventional nearest whole minute,
                    # with a minimum of 1 minute for any positive duration.
                    minutes = max(1, int((seconds / 60.0) + 0.5))
                    duration_feature = ET.SubElement(node, "ResourceFeature")
                    _elem(duration_feature, "ResourceFeatureType", "04")
                    _elem(duration_feature, "FeatureValue", str(minutes))
            except (TypeError, ValueError):
                pass

    for version in row.get("versions") or []:
        if not any(
            _s(version.get(k))
            for k in ("resource_form", "resource_link", "filename", "storage_key")
        ):
            continue
        v = ET.SubElement(node, "ResourceVersion")
        _elem(v, "ResourceForm", version.get("resource_form"))

        for feature in version.get("resource_version_features") or []:
            _write_feature(
                v,
                "ResourceVersionFeature",
                "ResourceVersionFeatureType",
                feature,
            )

        # ONIX permits ResourceLink to carry a language attribute. This is the
        # correct place for the persisted language of a language-specific
        # supporting-resource link.
        _elem(
            v,
            "ResourceLink",
            version.get("resource_link"),
            attrib=_attrs(language=version.get("language_code")),
        )
        if _s(version.get("content_date_role")) or _s(version.get("content_date_text")) or _s(version.get("content_date")):
            d = ET.SubElement(v, "ContentDate")
            _elem(d, "ContentDateRole", version.get("content_date_role"))
            _elem(
                d,
                "Date",
                _s(version.get("content_date_text")) or _s(version.get("content_date")).replace("-", ""),
                attrib=_attrs(dateformat=version.get("content_date_format")),
            )


def _write_prize(parent: ET.Element, row: Dict[str, Any]) -> None:
    prize_code = _s(row.get("award_status") or row.get("prize_code"))
    if not any(
        _s(row.get(k))
        for k in (
            "prize_name",
            "prize_year",
            "prize_country",
            "award_status",
            "prize_code",
            "prize_jury",
            "award_note",
            "sequence_number",
        )
    ):
        return

    node = ET.SubElement(parent, "Prize")
    _elem(node, "SequenceNumber", row.get("sequence_number"))
    _elem(
        node,
        "PrizeName",
        row.get("prize_name"),
        attrib=_attrs(language=row.get("language_code")),
    )
    _elem(node, "PrizeYear", row.get("prize_year"))
    _elem(node, "PrizeCountry", row.get("prize_country"))
    _elem(node, "PrizeCode", prize_code)
    _elem(
        node,
        "PrizeStatement",
        row.get("award_note"),
        attrib=_attrs(language=row.get("language_code")),
    )
    _elem(node, "PrizeJury", row.get("prize_jury"))


def _write_territory(parent: ET.Element, row: Dict[str, Any]) -> None:
    countries_included = _s(
        row.get("countries_included")
        or row.get("country_included")
        or row.get("territory_country_included")
    )
    regions_included = _s(
        row.get("regions_included")
        or row.get("region_included")
        or row.get("territory_region_included")
    )

    # Explicit country coverage and WORLD are alternative territory
    # representations. Never serialize both from legacy/imported data.
    if countries_included:
        region_tokens = [
            token
            for token in regions_included.replace(",", " ").split()
            if token.upper() != "WORLD"
        ]
        regions_included = " ".join(region_tokens)

    values = {
        "CountriesIncluded": countries_included,
        "RegionsIncluded": regions_included,
        "CountriesExcluded": row.get("countries_excluded") or row.get("country_excluded"),
        "RegionsExcluded": row.get("regions_excluded") or row.get("region_excluded"),
    }
    if not any(_s(v) for v in values.values()):
        return
    terr = ET.SubElement(parent, "Territory")
    for tag, value in values.items():
        _elem(terr, tag, value)


def _write_publishing_detail(root: ET.Element, payload: Dict[str, Any]) -> None:
    if not payload:
        return
    node = ET.SubElement(root, "PublishingDetail")

    imprints = payload.get("imprints") or []
    if imprints:
        for row in imprints:
            if not (_s(row.get("imprint_name")) or row.get("identifiers")):
                continue
            imprint = ET.SubElement(node, "Imprint")
            for ident in row.get("identifiers") or []:
                inode = ET.SubElement(imprint, "ImprintIdentifier")
                _elem(inode, "ImprintIDType", ident.get("imprint_id_type") or ident.get("id_type"))
                _elem(inode, "IDTypeName", ident.get("id_type_name"))
                _elem(inode, "IDValue", ident.get("id_value"))
            _elem(imprint, "ImprintName", row.get("imprint_name"))
    else:
        imprint_name = _s(payload.get("imprint_name"))
        if imprint_name:
            imprint = ET.SubElement(node, "Imprint")
            _elem(imprint, "ImprintName", imprint_name)

    publishers = payload.get("publishers") or []
    websites = payload.get("publisher_websites") or []

    if publishers:
        for index, row in enumerate(publishers):
            if not (
                _s(row.get("publishing_role"))
                or _s(row.get("publisher_name"))
            ):
                continue

            publisher = ET.SubElement(node, "Publisher")
            _elem(publisher, "PublishingRole", row.get("publishing_role"))
            for ident in row.get("identifiers") or []:
                inode = ET.SubElement(publisher, "PublisherIdentifier")
                _elem(inode, "PublisherIDType", ident.get("publisher_id_type") or ident.get("id_type"))
                _elem(inode, "IDTypeName", ident.get("id_type_name"))
                _elem(inode, "IDValue", ident.get("id_value"))
            _elem(publisher, "PublisherName", row.get("publisher_name"))

            # Existing website storage is edition-level. Attach it only to the
            # first Publisher composite until publisher-specific website FK
            # storage is introduced.
            if index == 0:
                for website in websites:
                    if not _s(website.get("website_link")):
                        continue
                    web = ET.SubElement(publisher, "Website")
                    _elem(web, "WebsiteRole", website.get("website_role"))
                    _elem(web, "WebsiteDescription", website.get("website_description"))
                    _elem(web, "WebsiteLink", website.get("website_link"))
    else:
        # Backwards compatibility for records not yet re-imported.
        publisher_name = _s(payload.get("publisher_name"))
        if publisher_name or websites or _s(payload.get("publishing_role")):
            publisher = ET.SubElement(node, "Publisher")
            _elem(publisher, "PublishingRole", payload.get("publishing_role"))
            _elem(publisher, "PublisherName", publisher_name)
            for website in websites:
                if not _s(website.get("website_link")):
                    continue
                web = ET.SubElement(publisher, "Website")
                _elem(web, "WebsiteRole", website.get("website_role"))
                _elem(web, "WebsiteDescription", website.get("website_description"))
                _elem(web, "WebsiteLink", website.get("website_link"))

    _elem(node, "CityOfPublication", payload.get("city_of_publication"))
    _elem(node, "CountryOfPublication", payload.get("country_of_publication"))
    _elem(node, "PublishingStatus", payload.get("publishing_status"))

    for d in payload.get("publishing_dates") or []:
        if not (_s(d.get("date_role")) or _date_text(d)):
            continue
        dn = ET.SubElement(node, "PublishingDate")
        _elem(dn, "PublishingDateRole", d.get("date_role"))
        _elem(
            dn,
            "Date",
            _date_text(d),
            attrib=_attrs(dateformat=d.get("date_format")),
        )

    rights_rows = payload.get("rights") or []
    copyright_year = payload.get("copyright_year")

    for rights in rights_rows:
        if any(
            _s(rights.get(k))
            for k in ("copyright_type", "copyright_holder", "copyright_notice")
        ) or copyright_year:
            copyright = ET.SubElement(node, "CopyrightStatement")
            _elem(copyright, "CopyrightType", rights.get("copyright_type"))
            _elem(copyright, "CopyrightYear", copyright_year)
            if _s(rights.get("copyright_holder")):
                holder = ET.SubElement(copyright, "CopyrightOwner")
                _elem(holder, "PersonName", rights.get("copyright_holder"))
            _elem(copyright, "CopyrightStatement", rights.get("copyright_notice"))

    sales_rights_rows = payload.get("sales_rights") or rights_rows
    for rights in sales_rights_rows:
        if any(
            _s(rights.get(k))
            for k in (
                "sales_rights_type",
                "countries_included",
                "countries_excluded",
                "regions_included",
                "regions_excluded",
            )
        ):
            sales = ET.SubElement(node, "SalesRights")
            _elem(sales, "SalesRightsType", rights.get("sales_rights_type"))
            _write_territory(sales, rights)

    _elem(node, "ROWSalesRightsType", payload.get("row_sales_rights_type"))

    # A SalesRestriction belongs inside a complete SalesRights composite.
    # The restriction's territory scopes the parent SalesRights composite;
    # SalesRestriction itself has no Territory child in ONIX 3.x.
    restrictions = payload.get("sales_restrictions") or []
    default_sales_rights = sales_rights_rows[0] if sales_rights_rows else {}
    default_sales_type = _s(default_sales_rights.get("sales_rights_type"))

    for restriction in restrictions:
        sales = ET.SubElement(node, "SalesRights")
        _elem(
            sales,
            "SalesRightsType",
            restriction.get("sales_rights_type") or default_sales_type,
        )

        has_restriction_territory = any(
            _s(restriction.get(k))
            for k in (
                "countries_included",
                "countries_excluded",
                "regions_included",
                "regions_excluded",
            )
        )
        _write_territory(
            sales,
            restriction if has_restriction_territory else default_sales_rights,
        )

        sr = ET.SubElement(sales, "SalesRestriction")
        _elem(sr, "SalesRestrictionType", restriction.get("restriction_type"))
        _elem(
            sr,
            "SalesRestrictionNote",
            restriction.get("restriction_note")
            or restriction.get("restriction_detail")
            or restriction.get("note"),
        )
        _elem(
            sr,
            "StartDate",
            _s(restriction.get("start_date") or restriction.get("effective_from")).replace("-", ""),
            attrib=_attrs(dateformat="00"),
        )
        _elem(
            sr,
            "EndDate",
            _s(restriction.get("end_date") or restriction.get("effective_until")).replace("-", ""),
            attrib=_attrs(dateformat="00"),
        )

    for contact in payload.get("product_contacts") or []:
        if not any(
            _s(contact.get(k))
            for k in ("product_contact_role", "product_contact_name", "contact_name", "email_address")
        ):
            continue
        c = ET.SubElement(node, "ProductContact")
        _elem(c, "ProductContactRole", contact.get("product_contact_role"))
        _elem(c, "ProductContactName", contact.get("product_contact_name"))
        _elem(c, "ContactName", contact.get("contact_name"))
        _elem(c, "EmailAddress", contact.get("email_address"))


def _write_related_material(root: ET.Element, payload: Dict[str, Any]) -> None:
    works = payload.get("related_works") or []
    products = payload.get("related_products") or []
    if not works and not products:
        return
    node = ET.SubElement(root, "RelatedMaterial")

    for row in works:
        rw = ET.SubElement(node, "RelatedWork")
        _elem(rw, "WorkRelationCode", row.get("work_relation_code"))
        if any(_s(row.get(k)) for k in ("work_id_type", "id_type_name", "id_value")):
            ident = ET.SubElement(rw, "WorkIdentifier")
            _elem(ident, "WorkIDType", row.get("work_id_type"))
            _elem(ident, "IDTypeName", row.get("id_type_name"))
            _elem(ident, "IDValue", row.get("id_value"))

    for row in products:
        rp = ET.SubElement(node, "RelatedProduct")
        _elem(rp, "ProductRelationCode", row.get("relation_code"))
        for ident in row.get("product_identifiers") or []:
            _write_identifier(
                rp,
                "ProductIdentifier",
                ident,
                type_tag="ProductIDType",
                name_tag="IDTypeName",
                value_tag="IDValue",
            )

        # ONIX 3.0/3.1 deliberately keeps RelatedProduct lean: identify the
        # related product and optionally state its form/detail. Title,
        # publisher, publication date and website belong in that product's
        # own ONIX record and are therefore not repeated here.
        _elem(rp, "ProductForm", row.get("related_product_form"))
        _elem(rp, "ProductFormDetail", row.get("related_product_form_detail"))


def _write_price(parent: ET.Element, row: Dict[str, Any]) -> None:
    if not any(
        _s(row.get(k))
        for k in ("price_type_code", "price_amount", "currency_code")
    ):
        return

    price = ET.SubElement(parent, "Price")
    price_type = _s(row.get("price_type_code"))
    _elem(price, "PriceType", price_type)
    _elem(price, "PriceQualifier", row.get("price_qualifier"))
    _elem(price, "PriceStatus", row.get("price_status"))

    if _has_values(row, "discount_code_type", "discount_type_name", "discount_code"):
        discount = ET.SubElement(price, "DiscountCoded")
        _elem(discount, "DiscountCodeType", row.get("discount_code_type"))
        _elem(discount, "DiscountCodeTypeName", row.get("discount_type_name"))
        _elem(discount, "DiscountCode", row.get("discount_code"))

    # ONIX sequence requires PriceAmount -> Tax -> CurrencyCode -> Territory.
    _elem(price, "PriceAmount", row.get("price_amount"))

    tax_included_price_types = {
        "02", "04", "07", "09", "12", "14", "17", "22", "24", "27", "42"
    }
    tax_rate_percent = _s(row.get("tax_rate_percent"))
    taxable_amount = _s(row.get("taxable_amount"))
    tax_amount = _s(row.get("tax_amount"))
    tax_rate_code = _s(row.get("tax_rate_code")).upper()

    # InkSuite requires the full tax detail set for tax-inclusive prices.
    # Excluding-tax prices never emit a Tax composite.
    if (
        price_type in tax_included_price_types
        and tax_rate_code in {"H", "P", "R", "S", "T", "Z"}
        and tax_rate_percent
        and taxable_amount
        and tax_amount
    ):
        tax = ET.SubElement(price, "Tax")
        _elem(tax, "TaxRateCode", tax_rate_code)
        _elem(tax, "TaxRatePercent", tax_rate_percent)
        _elem(tax, "TaxableAmount", taxable_amount)
        _elem(tax, "TaxAmount", tax_amount)

    _elem(price, "CurrencyCode", row.get("currency_code"))
    _write_territory(price, row)

    if _s(row.get("price_effective_from")):
        d = ET.SubElement(price, "PriceDate")
        _elem(d, "PriceDateRole", "14")
        _elem(d, "Date", _s(row.get("price_effective_from")).replace("-", ""))
    if _s(row.get("price_effective_until")):
        d = ET.SubElement(price, "PriceDate")
        _elem(d, "PriceDateRole", "15")
        _elem(d, "Date", _s(row.get("price_effective_until")).replace("-", ""))

    _elem(price, "MinimumOrderQuantity", row.get("minimum_order_quantity"))
    _elem(price, "PriceNote", row.get("price_note"))

def _write_product_supply(root: ET.Element, product: Dict[str, Any]) -> None:
    supply_rows = product.get("product_supply") or []
    pub = product.get("publishing_detail") or {}

    if not supply_rows and not any(
        _s(pub.get(k))
        for k in (
            "market_publishing_status",
            "promotion_contact",
            "initial_print_run",
            "promotion_campaign",
        )
    ):
        return

    # One ProductSupply per normalized SupplyDetail row preserves each row's
    # market/territory without merging distinct suppliers.
    rows = supply_rows or [{}]
    for row in rows:
        ps = ET.SubElement(root, "ProductSupply")

        market_rows = product.get("markets") or []
        if market_rows:
            for market_row in market_rows:
                market = ET.SubElement(ps, "Market")
                _write_territory(market, market_row)
        else:
            market_values = {
                "countries_included": row.get("country_included"),
                "regions_included": row.get("region_included"),
            }
            if any(_s(v) for v in market_values.values()):
                market = ET.SubElement(ps, "Market")
                _write_territory(market, market_values)

        if any(
            _s(pub.get(k))
            for k in (
                "market_publishing_status",
                "promotion_contact",
                "initial_print_run",
                "promotion_campaign",
            )
        ) or _has_values(pub.get("market_date") or {}, "market_date_role", "market_date_text"):
            mpd = ET.SubElement(ps, "MarketPublishingDetail")
            _elem(mpd, "MarketPublishingStatus", pub.get("market_publishing_status"))

            md = pub.get("market_date") or {}
            if _s(md.get("market_date_role")) or _s(md.get("market_date_text")):
                market_date = ET.SubElement(mpd, "MarketDate")
                _elem(market_date, "MarketDateRole", md.get("market_date_role"))
                _elem(
                    market_date,
                    "Date",
                    md.get("market_date_text"),
                    attrib=_attrs(dateformat=md.get("market_date_format")),
                )

            _elem(
                mpd,
                "PromotionContact",
                pub.get("promotion_contact"),
                attrib=_attrs(textformat=pub.get("promotion_contact_text_format")),
            )
            _elem(
                mpd,
                "InitialPrintRun",
                pub.get("initial_print_run"),
                attrib=_attrs(textformat=pub.get("initial_print_run_text_format")),
            )
            _elem(
                mpd,
                "PromotionCampaign",
                pub.get("promotion_campaign"),
                attrib=_attrs(textformat=pub.get("promotion_campaign_text_format")),
            )

        if not row:
            continue

        sd = ET.SubElement(ps, "SupplyDetail")

        if any(
            _s(row.get(k))
            for k in (
                "supplier_role",
                "supplier_name",
                "supplier_email",
                "supplier_telephone",
                "supplier_fax",
            )
        ) or row.get("supplier_identifiers"):
            supplier = ET.SubElement(sd, "Supplier")
            _elem(supplier, "SupplierRole", row.get("supplier_role"))
            for ident in row.get("supplier_identifiers") or []:
                _write_identifier(
                    supplier,
                    "SupplierIdentifier",
                    ident,
                    type_tag="SupplierIDType",
                    name_tag="IDTypeName",
                    value_tag="IDValue",
                )
            _elem(supplier, "SupplierName", row.get("supplier_name"))
            _elem(supplier, "TelephoneNumber", row.get("supplier_telephone"))
            _elem(supplier, "FaxNumber", row.get("supplier_fax"))
            _elem(supplier, "EmailAddress", row.get("supplier_email"))

        if any(_s(row.get(k)) for k in ("returns_code_type", "returns_code", "returns_note")):
            returns = ET.SubElement(sd, "ReturnsConditions")
            _elem(returns, "ReturnsCodeType", row.get("returns_code_type"))
            _elem(returns, "ReturnsCode", row.get("returns_code"))
            _elem(returns, "ReturnsNote", row.get("returns_note"))

        _elem(sd, "ProductAvailability", row.get("product_availability"))

        # ONIX has one PackQuantity element for the number of copies in a
        # supplier pack/carton. InkSuite historically stores both names.
        _elem(sd, "PackQuantity", row.get("carton_quantity") or row.get("pack_quantity"))

        stock_on_hand = _s(row.get("stock_on_hand") or row.get("stock_quantity"))
        if stock_on_hand:
            stock = ET.SubElement(sd, "Stock")
            _elem(stock, "OnHand", stock_on_hand)

        _elem(sd, "OrderTime", row.get("order_time_days"))

        supply_dates = row.get("supply_dates") or []
        if supply_dates:
            for d in supply_dates:
                if not (_s(d.get("date_role")) or _date_text(d)):
                    continue
                supply_date = ET.SubElement(sd, "SupplyDate")
                _elem(supply_date, "SupplyDateRole", d.get("date_role"))
                _elem(
                    supply_date, "Date", _date_text(d),
                    attrib=_attrs(dateformat=d.get("date_format")),
                )
        elif _s(row.get("expected_ship_date")):
            supply_date = ET.SubElement(sd, "SupplyDate")
            _elem(supply_date, "Date", _s(row.get("expected_ship_date")).replace("-", ""))

        _elem(sd, "SupplyDetailNote", row.get("supply_note"))

        for price in row.get("prices") or []:
            _write_price(sd, price)


def _product_to_xml(product: Dict[str, Any]) -> ET.Element:
    root = ET.Element("Product")

    _elem(root, "RecordReference", product.get("record_reference"))
    _elem(root, "NotificationType", product.get("notification_type"))
    _elem(root, "RecordSourceType", product.get("record_source_type"))
    _elem(root, "RecordSourceName", product.get("record_source_name"))

    for ident in product.get("identifiers") or []:
        _write_identifier(
            root,
            "ProductIdentifier",
            ident,
            type_tag="ProductIDType",
            name_tag="IDTypeName",
            value_tag="IDValue",
        )

    barcode = product.get("barcode") or {}
    if _has_values(barcode, "barcode_type", "position_on_product", "barcode_value"):
        node = ET.SubElement(root, "Barcode")
        barcode_type = _s(barcode.get("barcode_type"))
        _elem(node, "BarcodeType", barcode_type)

        # ONIX Best Practice: PositionOnProduct must be present if and only if
        # BarcodeType is different from 00 (no barcode / not barcoded).
        if barcode_type and barcode_type != "00":
            _elem(node, "PositionOnProduct", barcode.get("position_on_product"))

    dd = product.get("descriptive_detail") or {}
    if dd:
        desc = ET.SubElement(root, "DescriptiveDetail")
        _elem(desc, "ProductComposition", dd.get("product_composition"))
        _elem(desc, "ProductForm", dd.get("product_form"))

        detail_codes = []
        for row in dd.get("product_form_details") or []:
            code = _s(row.get("form_detail_code"))
            if code:
                detail_codes.append(code)
        scalar_detail = _s(dd.get("product_form_detail_scalar"))
        if scalar_detail and scalar_detail not in detail_codes:
            detail_codes.insert(0, scalar_detail)

        # Map the card's friendly File Format value to ONIX List 175.
        file_detail = _file_format_detail(dd.get("file_format"))
        if (
            _digital_detail_is_compatible(dd.get("product_form"), file_detail)
            and file_detail not in detail_codes
        ):
            detail_codes.append(file_detail)

        for code in detail_codes:
            _elem(desc, "ProductFormDetail", code)

        for feature in dd.get("product_form_features") or []:
            if not any(_s(feature.get(k)) for k in ("feature_type", "feature_value", "feature_description")):
                continue
            f = ET.SubElement(desc, "ProductFormFeature")
            _elem(f, "ProductFormFeatureType", feature.get("feature_type"))
            _elem(f, "ProductFormFeatureValue", feature.get("feature_value"))
            _elem(f, "ProductFormFeatureDescription", feature.get("feature_description"))

        # EPUB Version is a ProductFormFeature in ONIX 3.x. Prefer List 220.
        product_form = _s(dd.get("product_form")).upper()
        file_format = _s(dd.get("file_format")).upper()
        epub_version = _s(dd.get("epub_version"))
        if product_form.startswith("E") and file_format in {"EPUB", "EPUB2", "EPUB3"} and epub_version:
            vf = ET.SubElement(desc, "ProductFormFeature")
            version_code = _EPUB_VERSION_TO_ONIX_CODE.get(epub_version)
            if version_code:
                _elem(vf, "ProductFormFeatureType", "15")
                _elem(vf, "ProductFormFeatureValue", version_code)
            else:
                _elem(vf, "ProductFormFeatureType", "10")
                _elem(vf, "ProductFormFeatureValue", epub_version)

        _elem(desc, "ProductPackaging", dd.get("product_packaging"))

        content_type_rows = dd.get("product_content_types") or []
        primary_content_type = _s(dd.get("primary_content_type"))
        if not primary_content_type:
            for row in content_type_rows:
                candidate = _s(
                    row.get("content_type_code")
                    or row.get("product_content_type")
                    or row.get("content_type")
                )
                if candidate:
                    primary_content_type = candidate
                    break

        _elem(desc, "PrimaryContentType", primary_content_type)

        for row in content_type_rows:
            code = _s(
                row.get("content_type_code")
                or row.get("product_content_type")
                or row.get("content_type")
            )
            # Do not repeat the primary content type as an additional
            # ProductContentType unless a genuinely different code exists.
            if code and code != primary_content_type:
                _elem(desc, "ProductContentType", code)

        _elem(desc, "TradeCategory", dd.get("trade_category"))
        _elem(desc, "ProductFormDescription", dd.get("product_form_description"))
        _elem(desc, "CountryOfManufacture", dd.get("country_of_manufacture"))

        if _s(dd.get("product_form")).upper().startswith("E"):
            _elem(desc, "EpubTechnicalProtection", dd.get("technical_protection"))

        for measure in dd.get("measurements") or []:
            if not any(_s(measure.get(k)) for k in ("measure_type", "measurement", "measure_unit_code")):
                continue
            m = ET.SubElement(desc, "Measure")
            _elem(m, "MeasureType", measure.get("measure_type"))
            _elem(m, "Measurement", measure.get("measurement"))
            _elem(m, "MeasureUnitCode", measure.get("measure_unit_code"))

        # ONIX 3.x has no top-level NumberOfPieces. Preserve InkSuite's
        # scalar through ProductPart/NumberOfItemsOfThisForm.
        pieces = _onix_number(dd.get("number_of_pieces"))
        if pieces and _s(dd.get("product_form")):
            part = ET.SubElement(desc, "ProductPart")
            _elem(part, "ProductForm", dd.get("product_form"))
            _elem(part, "NumberOfItemsOfThisForm", pieces)

        collections = dd.get("collections") or []
        if collections:
            for collection in collections:
                _write_collection(desc, collection)
        elif dd.get("no_collection") is True:
            _elem(desc, "NoCollection", allow_empty=True)

        for title in dd.get("titles") or []:
            _write_title_detail(desc, title)

        for contributor in dd.get("contributors") or []:
            _write_contributor(desc, contributor)

        if _s(dd.get("edition_number")) or _s(dd.get("edition_statement")):
            _elem(desc, "EditionNumber", dd.get("edition_number"))
            _elem(desc, "EditionStatement", dd.get("edition_statement"))
        elif dd.get("no_edition") is True:
            _elem(desc, "NoEdition", allow_empty=True)

        for lang in dd.get("languages") or []:
            if not _s(lang.get("language_code")):
                continue
            l = ET.SubElement(desc, "Language")
            _elem(l, "LanguageRole", lang.get("language_role"))
            _elem(l, "LanguageCode", lang.get("language_code"))

        for extent in dd.get("extents") or []:
            if not any(_s(extent.get(k)) for k in ("extent_type", "extent_value", "extent_unit")):
                continue
            e = ET.SubElement(desc, "Extent")
            _elem(e, "ExtentType", extent.get("extent_type"))
            _elem(e, "ExtentValue", _onix_number(extent.get("extent_value")))
            _elem(e, "ExtentUnit", extent.get("extent_unit"))

        _elem(desc, "NumberOfIllustrations", _onix_number(dd.get("number_of_illustrations")))
        _elem(
            desc,
            "IllustrationsNote",
            dd.get("illustrations_note"),
            attrib=_attrs(textformat="05"),
        )

        # ONIX has no dedicated ColorContent / ColorPages elements. Preserve
        # both structured card values in the standard IllustrationsNote field.
        color_note = _color_note(dd.get("color_content"), dd.get("color_pages"))
        if color_note:
            _elem(desc, "IllustrationsNote", color_note)

        for anc in dd.get("ancillary_content") or []:
            if not any(_s(anc.get(k)) for k in ("ancillary_content_type", "description", "number")):
                continue
            a = ET.SubElement(desc, "AncillaryContent")
            _elem(a, "AncillaryContentType", anc.get("ancillary_content_type"))
            _elem(
                a,
                "AncillaryContentDescription",
                anc.get("description"),
                attrib=_attrs(textformat=anc.get("description_text_format")),
            )
            _elem(a, "Number", anc.get("number"))

        seen_main_subject_schemes = set()
        for subject in dd.get("subjects") or []:
            subject_to_write = subject
            scheme = _s(
                subject.get("scheme_id")
                or subject.get("subject_scheme_identifier")
                or subject.get("scheme_identifier")
            )

            if subject.get("is_main") is True and scheme:
                if scheme in seen_main_subject_schemes:
                    subject_to_write = {
                        **subject,
                        "is_main": False,
                    }
                else:
                    seen_main_subject_schemes.add(scheme)

            _write_subject(desc, subject_to_write)

        for audience in dd.get("audiences") or []:
            _write_audience(desc, audience)

        _elem(desc, "AudienceDescription", dd.get("audience_description"))

        for usage in dd.get("usage_constraints") or []:
            if not any(_s(usage.get(k)) for k in ("usage_type", "usage_status", "quantity", "unit_code", "usage_note")):
                continue
            uc = ET.SubElement(desc, "EpubUsageConstraint")
            _elem(uc, "EpubUsageType", usage.get("usage_type"))
            _elem(uc, "EpubUsageStatus", usage.get("usage_status"))
            if _s(usage.get("quantity")) or _s(usage.get("unit_code")):
                limit = ET.SubElement(uc, "EpubUsageLimit")
                _elem(limit, "Quantity", usage.get("quantity"))
                _elem(limit, "EpubUsageUnit", usage.get("unit_code"))
            _elem(uc, "EpubUsageNote", usage.get("usage_note"))

    collateral = product.get("collateral_detail") or {}
    if any(collateral.get(k) for k in ("texts", "cited_content", "supporting_resources", "prizes")):
        cd = ET.SubElement(root, "CollateralDetail")
        for row in collateral.get("texts") or []:
            _write_text_content(cd, row)
        for row in collateral.get("cited_content") or []:
            _write_cited_content(cd, row)
        for row in collateral.get("supporting_resources") or []:
            _write_supporting_resource(cd, row)
        for row in collateral.get("prizes") or []:
            _write_prize(cd, row)

    _write_publishing_detail(root, product.get("publishing_detail") or {})
    _write_related_material(root, product.get("related_material") or {})
    _write_product_supply(root, product)

    return root


def _write_header(root: ET.Element, header: Optional[Dict[str, Any]]) -> None:
    if not header:
        return

    if not any(_s(value) for value in header.values()):
        return

    node = ET.SubElement(root, "Header")

    if any(
        _s(header.get(k))
        for k in (
            "sender_name",
            "sender_identifier_type",
            "sender_identifier_value",
            "contact_name",
            "email_address",
        )
    ):
        sender = ET.SubElement(node, "Sender")
        if _s(header.get("sender_identifier_type")) or _s(header.get("sender_identifier_value")):
            ident = ET.SubElement(sender, "SenderIdentifier")
            _elem(ident, "SenderIDType", header.get("sender_identifier_type"))
            _elem(ident, "IDValue", header.get("sender_identifier_value"))
        _elem(sender, "SenderName", header.get("sender_name"))
        _elem(sender, "ContactName", header.get("contact_name"))
        _elem(sender, "EmailAddress", header.get("email_address"))

    if any(
        _s(header.get(k))
        for k in (
            "addressee_name",
            "addressee_identifier_type",
            "addressee_identifier_value",
        )
    ):
        addressee = ET.SubElement(node, "Addressee")
        if _s(header.get("addressee_identifier_type")) or _s(header.get("addressee_identifier_value")):
            ident = ET.SubElement(addressee, "AddresseeIdentifier")
            _elem(ident, "AddresseeIDType", header.get("addressee_identifier_type"))
            _elem(ident, "IDValue", header.get("addressee_identifier_value"))
        _elem(addressee, "AddresseeName", header.get("addressee_name"))

    # SentDateTime is transport metadata, not product metadata. It is generated at
    # serialization time only when a Header exists.
    _elem(
        node,
        "SentDateTime",
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
    )


def product_to_xml_string(product: Dict[str, Any], pretty: bool = True) -> str:
    root = _product_to_xml(product)
    rough = ET.tostring(root, encoding="unicode", method="xml")
    if pretty:
        try:
            return minidom.parseString(rough).toprettyxml(
                indent="  ",
                encoding="utf-8",
            ).decode("utf-8")
        except Exception:
            pass
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + rough


def message_to_xml(message: Dict[str, Any], pretty: bool = True) -> str:
    # ONIX release and namespace are protocol settings, not book metadata.
    attrs = {
        "release": _s(message.get("release")) or "3.0",
        "xmlns": ONIX_NS,
    }
    root = ET.Element("ONIXMessage", attrib=attrs)
    _write_header(root, message.get("header"))

    for product in message.get("products") or []:
        root.append(_product_to_xml(product))

    rough = ET.tostring(root, encoding="unicode", method="xml")
    if pretty:
        try:
            return minidom.parseString(rough).toprettyxml(
                indent="  ",
                encoding="utf-8",
            ).decode("utf-8")
        except Exception:
            pass
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + rough
