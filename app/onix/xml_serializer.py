# app/onix/xml_serializer.py
# Canonical dict -> ONIX 3.0 XML. UTF-8, deterministic ordering where practical.

from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any, Dict
from xml.dom import minidom

ONIX_NS = "http://ns.editeur.org/onix/3.0/reference"

_LANGUAGE_NAME_TO_CODE: Dict[str, str] = {
    "english": "eng",
    "en": "eng",
    "spanish": "spa",
    "spa": "spa",
    "french": "fre",
    "fre": "fre",
    "german": "ger",
    "ger": "ger",
    "italian": "ita",
    "ita": "ita",
    "dutch": "dut",
    "dut": "dut",
    "portuguese": "por",
    "por": "por",
    "chinese": "chi",
    "chi": "chi",
    "japanese": "jpn",
    "jpn": "jpn",
}

_PUBLISHING_STATUS_TO_CODE: Dict[str, str] = {
    "active": "04",
    "withdrawn": "02",
    "not yet available": "05",
    "out of stock": "10",
    "replaced": "07",
}


def _elem(parent: ET.Element, tag: str, text: str = "", attrib: Dict[str, str] | None = None) -> ET.Element:
    el = ET.SubElement(parent, tag, attrib or {})
    if text:
        el.text = text
    return el


def _person_name_safe(raw: Any) -> str:
    if raw is None:
        return ""
    s = str(raw).strip()
    if not s or s.startswith("{") or s.startswith("[") or "'name'" in s or '"name"' in s:
        return ""
    if len(s) > 200:
        return s[:200].strip()
    return s


def _language_code(value: str) -> str:
    v = (value or "").strip().lower()
    if not v:
        return "eng"
    if len(v) == 3 and v.isalpha():
        return v
    return _LANGUAGE_NAME_TO_CODE.get(v) or "eng"


def _onix_date(raw: str) -> tuple[str, str]:
    """
    ONIX List 55:
    00 = YYYYMMDD
    01 = YYYYMM
    05 = YYYY
    """
    s = (raw or "").strip().replace("-", "").replace("/", "")
    if len(s) == 8:
        return "00", s
    if len(s) == 6:
        return "01", s
    if len(s) == 4:
        return "05", s
    return "", s


def _text_type_code(text_type: str) -> str:
    """
    Keep this conservative:
    - Main Description -> 03
    - everything else -> 01
    """
    t = (text_type or "").strip().lower()
    if t.startswith("main"):
        return "03"
    return "01"


def _product_to_xml(product: Dict[str, Any]) -> ET.Element:
    root = ET.Element("Product")

    record_reference = (product.get("record_reference") or product.get("edition_id") or "").strip()
    if record_reference:
        _elem(root, "RecordReference", record_reference)

    notification_type = (product.get("notification_type") or "03").strip()
    _elem(root, "NotificationType", notification_type)

    # Identifiers: ISBN + proprietary/inventory where supplied by assembly
    for ident in product.get("identifiers") or []:
        id_value = (ident.get("id_value") or "").strip()
        if not id_value:
            continue
        pid = ET.SubElement(root, "ProductIdentifier")
        _elem(pid, "ProductIDType", ident.get("id_type") or "15")
        _elem(pid, "IDValue", id_value)

    desc = ET.SubElement(root, "DescriptiveDetail")
    _elem(desc, "ProductComposition", "00")
    _elem(desc, "ProductForm", product.get("product_form") or "BC")

    if product.get("product_form_detail"):
        _elem(desc, "ProductFormDetail", product.get("product_form_detail"))

    titl = ET.SubElement(desc, "TitleDetail")
    _elem(titl, "TitleType", "01")
    titl_c = ET.SubElement(titl, "TitleElement")
    _elem(titl_c, "TitleElementLevel", "01")
    _elem(titl_c, "TitleText", product.get("title") or "")
    if product.get("subtitle"):
        _elem(titl_c, "Subtitle", product.get("subtitle"))

    for c in product.get("contributors") or []:
        name = _person_name_safe(c.get("name"))
        if not name:
            continue
        contrib = ET.SubElement(desc, "Contributor")
        _elem(contrib, "ContributorRole", c.get("role") or "A01")
        _elem(contrib, "SequenceNumber", str(c.get("sequence_number") or 1))
        pname = ET.SubElement(contrib, "PersonName")
        pname.text = name

    lang = ET.SubElement(desc, "Language")
    _elem(lang, "LanguageRole", "01")
    _elem(lang, "LanguageCode", _language_code(product.get("language") or "eng"))

    if product.get("number_of_pages"):
        extent = ET.SubElement(desc, "Extent")
        _elem(extent, "ExtentType", "11")
        _elem(extent, "ExtentValue", str(product["number_of_pages"]))
        _elem(extent, "ExtentUnit", "03")

    pub_detail = ET.SubElement(root, "PublishingDetail")
    pub = ET.SubElement(pub_detail, "Publisher")
    _elem(pub, "PublisherName", product.get("publisher_name") or "")

    if product.get("publication_date"):
        fmt, clean = _onix_date(str(product.get("publication_date")))
        pub_date = ET.SubElement(pub_detail, "PublishingDate")
        _elem(pub_date, "PublishingDateRole", "01")
        if fmt:
            _elem(pub_date, "DateFormat", fmt)
        _elem(pub_date, "Date", clean)

    ps_raw = (product.get("publishing_status") or "").strip()
    ps = ps_raw.lower()
    code = _PUBLISHING_STATUS_TO_CODE.get(ps) or (ps_raw if len(ps_raw) <= 4 else "04")
    _elem(pub_detail, "PublishingStatus", code or "04")

    if product.get("series_title"):
        ser = ET.SubElement(root, "Series")
        _elem(ser, "TitleOfSeries", product.get("series_title"))
        if product.get("series_number"):
            _elem(ser, "NumberWithinSeries", str(product.get("series_number")))

    for s in product.get("subjects") or []:
        heading_text = s.get("heading_text") or ""
        subject_code = s.get("subject_code") or ""
        if heading_text or subject_code:
            subj = ET.SubElement(root, "Subject")
            _elem(subj, "SubjectSchemeIdentifier", s.get("scheme_id") or "24")
            if subject_code:
                _elem(subj, "SubjectCode", subject_code)
            if heading_text:
                _elem(subj, "SubjectHeadingText", heading_text)

    texts = product.get("texts") or []
    cover = product.get("cover_image_link") or ""
    collateral = None

    if any(t.get("text_value") for t in texts) or cover:
        collateral = ET.SubElement(root, "CollateralDetail")

    if collateral is not None:
        for t in texts:
            text_value = t.get("text_value") or ""
            if text_value:
                txt = ET.SubElement(collateral, "TextContent")
                _elem(txt, "TextType", _text_type_code(t.get("text_type") or ""))
                _elem(txt, "Text", text_value[:5000])

        if cover:
            supp = ET.SubElement(collateral, "SupportingResource")
            _elem(supp, "ResourceContentType", "01")
            _elem(supp, "ContentAudience", "00")
            _elem(supp, "ResourceMode", "03")
            version = ET.SubElement(supp, "ResourceVersion")
            _elem(version, "ResourceForm", "02")
            res_link = ET.SubElement(version, "ResourceLink")
            res_link.text = cover

    for sd in product.get("supply_details") or []:
        supply = ET.SubElement(root, "ProductSupply")
        ET.SubElement(supply, "Market")
        supply_detail = ET.SubElement(supply, "SupplyDetail")

        if sd.get("supplier_name"):
            _elem(supply_detail, "SupplierName", sd.get("supplier_name"))
        if sd.get("product_availability"):
            _elem(supply_detail, "ProductAvailability", sd.get("product_availability"))

        if sd.get("on_sale_date"):
            fmt, clean = _onix_date(sd.get("on_sale_date"))
            sale_date = ET.SubElement(supply_detail, "OnSaleDate")
            if fmt:
                _elem(sale_date, "DateFormat", fmt)
            sale_date.text = clean

        for pr in sd.get("prices") or []:
            if pr.get("price_amount") is not None:
                price_el = ET.SubElement(supply_detail, "Price")
                _elem(price_el, "PriceTypeCode", pr.get("price_type_code") or "01")
                _elem(price_el, "PriceAmount", f"{float(pr['price_amount']):.2f}")
                _elem(price_el, "CurrencyCode", pr.get("currency_code") or "USD")

    return root


def product_to_xml_string(product: Dict[str, Any], pretty: bool = True) -> str:
    root = _product_to_xml(product)
    rough = ET.tostring(root, encoding="unicode", method="xml")
    if pretty:
        try:
            reparsed = minidom.parseString(rough)
            return reparsed.toprettyxml(indent="  ", encoding="utf-8").decode("utf-8")
        except Exception:
            pass
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + rough


def message_to_xml(message: Dict[str, Any], pretty: bool = True) -> str:
    attrs = {
        "release": message.get("release") or "3.0",
        "xmlns": ONIX_NS,
    }
    root = ET.Element("ONIXMessage", attrib=attrs)
    for p in message.get("products") or []:
        root.append(_product_to_xml(p))
    rough = ET.tostring(root, encoding="unicode", method="xml")
    if pretty:
        try:
            reparsed = minidom.parseString(rough)
            return reparsed.toprettyxml(indent="  ", encoding="utf-8").decode("utf-8")
        except Exception:
            pass
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + rough