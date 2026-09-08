
# app/onix/importer.py — ONIX XML/ZIP parser and normalized InkSuite importer.
from __future__ import annotations

import io
import json
import re
import unicodedata
import uuid
import zipfile
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from xml.etree import ElementTree as ET

from psycopg.rows import dict_row

from app.core.db import db_conn
from routers.catalog_write import (
    add_work_contributor,
    update_edition_descriptive_content,
    update_edition_product_details,
    update_edition_product_identity,
    update_edition_publishing_dates,
    update_edition_related_products,
    update_edition_rights_restrictions,
    update_edition_subjects_audience,
    update_edition_supply_pricing,
    update_work_titles_collections,
)



def _clean(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _local(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


def _children(node: Optional[ET.Element], name: str) -> List[ET.Element]:
    if node is None:
        return []
    return [c for c in list(node) if _local(c.tag) == name]


def _child(node: Optional[ET.Element], name: str) -> Optional[ET.Element]:
    rows = _children(node, name)
    return rows[0] if rows else None


def _text(node: Optional[ET.Element], name: str, default: str = "") -> str:
    c = _child(node, name)
    return _clean(c.text) if c is not None else default


def _deep_text(node: Optional[ET.Element], name: str) -> str:
    if node is None:
        return ""
    for c in node.iter():
        if _local(c.tag) == name:
            return _clean(c.text)
    return ""


def _inner_xml(node: Optional[ET.Element]) -> str:
    if node is None:
        return ""
    if not list(node):
        return _clean(node.text)
    parts: List[str] = []
    if node.text:
        parts.append(node.text)
    for child in list(node):
        parts.append(ET.tostring(child, encoding="unicode", method="xml"))
    return "".join(parts).strip()


def _date_parts(date_node: Optional[ET.Element]) -> Dict[str, str]:
    if date_node is None:
        return {"date_format": "00", "date_text": ""}
    return {
        "date_format": _clean(date_node.attrib.get("dateformat")) or "00",
        "date_text": _clean(date_node.text),
    }


def _parse_date_compat(value: str) -> Optional[str]:
    value = _clean(value)
    if re.fullmatch(r"\d{8}", value):
        return f"{value[:4]}-{value[4:6]}-{value[6:8]}"
    if re.fullmatch(r"\d{6}", value):
        return f"{value[:4]}-{value[4:6]}-01"
    if re.fullmatch(r"\d{4}", value):
        return f"{value}-01-01"
    if re.fullmatch(r"\d{8}T\d{4,6}", value):
        return f"{value[:4]}-{value[4:6]}-{value[6:8]}"
    return None


def _read_upload(raw: bytes, filename: str) -> bytes:
    name = (filename or "").lower()
    if name.endswith(".zip") or raw[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            xml_names = [n for n in zf.namelist() if n.lower().endswith(".xml")]
            if not xml_names:
                raise ValueError("ZIP does not contain an XML file.")
            return zf.read(xml_names[0])
    return raw


def _products(xml_bytes: bytes) -> Tuple[ET.Element, List[ET.Element]]:
    root = ET.fromstring(xml_bytes)
    products = [n for n in root.iter() if _local(n.tag) == "Product"]
    if not products:
        raise ValueError("No ONIX Product records found.")
    return root, products


def _id_values(product: ET.Element) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for ident in _children(product, "ProductIdentifier"):
        rows.append({
            "id_type": _text(ident, "ProductIDType"),
            "id_type_name": _text(ident, "IDTypeName"),
            "id_value": _text(ident, "IDValue"),
        })
    return rows


def _isbn13(product: ET.Element) -> str:
    for row in _id_values(product):
        if row["id_type"] == "15" and re.fullmatch(r"\d{13}", row["id_value"]):
            return row["id_value"]
    return ""


def _title_elements(parent: Optional[ET.Element]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if parent is None:
        return out
    for td in _children(parent, "TitleDetail"):
        title_type = _text(td, "TitleType")
        for te in _children(td, "TitleElement"):
            out.append({
                "title_type": title_type,
                "title_element_level": _text(te, "TitleElementLevel"),
                "title_prefix": _text(te, "TitlePrefix"),
                "title_without_prefix": _text(te, "TitleWithoutPrefix"),
                "title_text": _text(te, "TitleText"),
                "subtitle": _text(te, "Subtitle"),
                "part_number": _text(te, "PartNumber"),
                "year_of_annual": _text(te, "YearOfAnnual"),
                "language_code": _text(te, "LanguageCode"),
                "no_prefix": _child(te, "NoPrefix") is not None,
            })
    return out


def _title_row_display(row: Dict[str, Any]) -> str:
    title_text = _clean(row.get("title_text"))
    if title_text:
        return title_text
    prefix = _clean(row.get("title_prefix"))
    without_prefix = _clean(row.get("title_without_prefix"))
    if prefix and without_prefix:
        return f"{prefix} {without_prefix}".strip()
    return without_prefix or prefix


def _summary(product: ET.Element) -> Dict[str, Any]:
    dd = _child(product, "DescriptiveDetail")
    title_rows = _title_elements(dd)
    product_title = next(
        (
            r for r in title_rows
            if r["title_type"] == "01" and r["title_element_level"] == "01"
            and (r["title_without_prefix"] or r["title_text"])
        ),
        None,
    )
    if not product_title:
        product_title = next((r for r in title_rows if r["title_without_prefix"] or r["title_text"]), {})
    title = _title_row_display(product_title or {})
    subtitle = _clean((product_title or {}).get("subtitle"))
    return {
        "record_reference": _text(product, "RecordReference"),
        "isbn13": _isbn13(product),
        "title": title,
        "subtitle": subtitle,
        "product_form": _text(dd, "ProductForm") if dd is not None else "",
        "product_form_detail": _text(dd, "ProductFormDetail") if dd is not None else "",
    }


def _product_by_key(products: List[ET.Element], record_reference: str, isbn13: str) -> ET.Element:
    for product in products:
        if record_reference and _text(product, "RecordReference") == record_reference:
            return product
        if isbn13 and _isbn13(product) == isbn13:
            return product
    raise ValueError("Selected ONIX Product was not found in the uploaded file.")


def _product_form_label(code: str) -> str:
    return {
        "BB": "Hardcover",
        "BC": "Paperback",
        "EA": "E-book",
        "AJ": "Audiobook",
        "BA": "Book",
    }.get(_clean(code).upper(), _clean(code))


def _collections(dd: Optional[ET.Element]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for index, coll in enumerate(_children(dd, "Collection"), start=1):
        title_rows = _title_elements(coll)
        title_row = next(
            (
                r for r in title_rows
                if r["title_without_prefix"] or r["title_text"]
            ),
            {},
        )

        part_number = _clean(title_row.get("part_number"))
        if not part_number:
            part_number = next(
                (
                    _clean(r.get("part_number"))
                    for r in title_rows
                    if _clean(r.get("part_number"))
                ),
                "",
            )

        rows.append({
            "collection_type": _text(coll, "CollectionType"),
            "title_type": _clean(title_row.get("title_type")) or "01",
            "title_element_level": _clean(title_row.get("title_element_level")) or "02",
            "title": _title_row_display(title_row),
            "collection_number": part_number,
            "part_number": part_number,
            "no_prefix": bool(title_row.get("no_prefix")),
            "item_order": index,
        })
    return rows


def _contributors(dd: Optional[ET.Element]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for index, c in enumerate(_children(dd, "Contributor"), start=1):
        websites = []
        for wi, website in enumerate(_children(c, "Website"), start=1):
            websites.append({
                "website_role": _text(website, "WebsiteRole"),
                "website_description": _text(website, "WebsiteDescription"),
                "website_link": _text(website, "WebsiteLink"),
                "item_order": wi,
            })
        places = []
        for pi, place in enumerate(_children(c, "ContributorPlace"), start=1):
            places.append({
                "contributor_place_relator": _text(place, "ContributorPlaceRelator"),
                "country_code": _text(place, "CountryCode"),
                "region_code": _text(place, "RegionCode"),
                "location_name": _text(place, "LocationName"),
                "item_order": pi,
            })

        rows.append({
            "party_type": "person",
            "name": _text(c, "PersonName") or _text(c, "PersonNameInverted"),
            "display_name": _text(c, "PersonName") or _text(c, "PersonNameInverted"),
            "names_before_key": _text(c, "NamesBeforeKey"),
            "key_names": _text(c, "KeyNames"),
            "person_name_inverted": _text(c, "PersonNameInverted"),
            "contributor_role_code": _text(c, "ContributorRole"),
            "sequence_number": _text(c, "SequenceNumber") or str(index),
            "contributor_description": _text(c, "ContributorDescription"),
            "biographical_note": _deep_text(c, "BiographicalNote"),
            "websites": websites,
            "contributor_places": places,
        })
    return rows


def _texts(collateral: Optional[ET.Element]) -> List[Dict[str, Any]]:
    rows = []
    for index, t in enumerate(_children(collateral, "TextContent"), start=1):
        text_node = _child(t, "Text")
        dates = []
        for di, cd in enumerate(_children(t, "ContentDate"), start=1):
            dn = _child(cd, "Date")
            dp = _date_parts(dn)
            dates.append({
                "content_date_role": _text(cd, "ContentDateRole"),
                "date_format": dp["date_format"],
                "date_text": dp["date_text"],
                "item_order": di,
            })
        rows.append({
            "text_type": _text(t, "TextType"),
            "content_audience": _text(t, "ContentAudience"),
            "text_format": _clean(text_node.attrib.get("textformat")) if text_node is not None else "",
            "text_value": _inner_xml(text_node),
            "source_corporate": _text(t, "SourceCorporate"),
            "source_title": _text(t, "SourceTitle"),
            "source_title_type": (_child(t, "SourceTitle").attrib.get("sourcetype") if _child(t, "SourceTitle") is not None else ""),
            "source_url": _text(t, "TextSourceLink"),
            "author": _text(t, "TextAuthor"),
            "language_code": _text(t, "LanguageCode"),
            "content_dates": dates,
            "item_order": index,
        })
    return rows


def _subjects_audience(dd: Optional[ET.Element]) -> Dict[str, Any]:
    subjects = []
    for index, s in enumerate(_children(dd, "Subject"), start=1):
        subjects.append({
            "subject_scheme_identifier": _text(s, "SubjectSchemeIdentifier"),
            "subject_scheme_name": _text(s, "SubjectSchemeName"),
            "subject_scheme_version": _text(s, "SubjectSchemeVersion"),
            "subject_code": _text(s, "SubjectCode"),
            "subject_heading_text": _text(s, "SubjectHeadingText"),
            "main_subject": _child(s, "MainSubject") is not None,
            "sequence_number": str(index),
        })

    audience_codes = []
    for index, a in enumerate(_children(dd, "Audience"), start=1):
        audience_codes.append({
            "audience_code_type": _text(a, "AudienceCodeType"),
            "audience_code_type_name": _text(a, "AudienceCodeTypeName"),
            "audience_code_value": _text(a, "AudienceCodeValue"),
            "audience_code": _text(a, "AudienceCodeValue"),
            "item_order": index,
        })

    ranges = []
    for index, a in enumerate(_children(dd, "AudienceRange"), start=1):
        precisions = _children(a, "AudienceRangePrecision")
        values = _children(a, "AudienceRangeValue")
        ranges.append({
            "audience_range_qualifier": _text(a, "AudienceRangeQualifier"),
            "audience_range_precision": _clean(precisions[0].text) if precisions else "",
            "audience_range_value": _clean(values[0].text) if values else "",
            "audience_range_precision_2": _clean(precisions[1].text) if len(precisions) > 1 else "",
            "audience_range_value_2": _clean(values[1].text) if len(values) > 1 else "",
            "item_order": index,
        })

    return {
        "subjects": subjects,
        "audience_codes": audience_codes,
        "audience_ranges": ranges,
    }


def _product_details(dd: Optional[ET.Element]) -> Dict[str, Any]:
    extents = []
    for index, e in enumerate(_children(dd, "Extent"), start=1):
        extents.append({
            "extent_type": _text(e, "ExtentType"),
            "extent_value": _text(e, "ExtentValue"),
            "extent_unit": _text(e, "ExtentUnit"),
            "item_order": index,
        })

    measurements = []
    for index, m in enumerate(_children(dd, "Measure"), start=1):
        measurements.append({
            "measurement_type": _text(m, "MeasureType"),
            "measurement": _text(m, "Measurement"),
            "measure_unit_code": _text(m, "MeasureUnitCode"),
            "item_order": index,
        })

    form_features = []
    for index, f in enumerate(_children(dd, "ProductFormFeature"), start=1):
        descriptions = _children(f, "ProductFormFeatureDescription")
        form_features.append({
            "feature_type": _text(f, "ProductFormFeatureType"),
            "feature_value": _text(f, "ProductFormFeatureValue"),
            "feature_description": _inner_xml(descriptions[0]) if descriptions else "",
            "item_order": index,
        })

    ancillary = []
    for index, a in enumerate(_children(dd, "AncillaryContent"), start=1):
        desc = _child(a, "AncillaryContentDescription")
        ancillary.append({
            "ancillary_content_type": _text(a, "AncillaryContentType"),
            "description": _inner_xml(desc),
            "description_text_format": _clean(desc.attrib.get("textformat")) if desc is not None else "",
            "number": _text(a, "Number"),
            "item_order": index,
        })

    product_form_details = [
        _clean(node.text)
        for node in _children(dd, "ProductFormDetail")
        if _clean(node.text)
    ]
    product_content_types = [
        _clean(node.text)
        for node in _children(dd, "ProductContentType")
        if _clean(node.text)
    ]

    return {
        "onix_product_form": _text(dd, "ProductForm"),
        "onix_product_form_detail": (
            product_form_details[0]
            if product_form_details
            else ""
        ),
        "primary_content_type": _text(dd, "PrimaryContentType"),
        "product_form_details": product_form_details,
        "product_content_types": product_content_types,
        "edition_number": _text(dd, "EditionNumber"),
        "edition_statement": _text(dd, "EditionStatement"),
        "country_of_manufacture": _text(dd, "CountryOfManufacture"),
        "product_form_description": _text(dd, "ProductFormDescription"),
        "extents": extents,
        "measurements": measurements,
        "product_form_features": form_features,
        "ancillary_content": ancillary,
    }


def _publishing(product: ET.Element) -> Dict[str, Any]:
    pd = _child(product, "PublishingDetail")
    dates = []
    for index, p in enumerate(_children(pd, "PublishingDate"), start=1):
        dn = _child(p, "Date")
        dp = _date_parts(dn)
        dates.append({
            "publishing_date_role": _text(p, "PublishingDateRole"),
            "date_format": dp["date_format"],
            "date_text": dp["date_text"],
            "date": dp["date_text"],
            "date_value": dp["date_text"],
            "display_date": dp["date_text"],
            "item_order": index,
        })

    publishers: List[Dict[str, Any]] = []
    for index, publisher in enumerate(_children(pd, "Publisher"), start=1):
        websites = []
        for wi, w in enumerate(_children(publisher, "Website"), start=1):
            websites.append({
                "website_role": _text(w, "WebsiteRole"),
                "website_description": _text(w, "WebsiteDescription"),
                "website_link": _text(w, "WebsiteLink"),
                "item_order": wi,
            })

        identifiers = []
        for ii, ident in enumerate(_children(publisher, "PublisherIdentifier"), start=1):
            identifiers.append({
                "publisher_id_type": _text(ident, "PublisherIDType"),
                "id_type_name": _text(ident, "IDTypeName"),
                "id_value": _text(ident, "IDValue"),
                "item_order": ii,
            })

        publishers.append({
            "publishing_role": _text(publisher, "PublishingRole"),
            "publisher_name": _text(publisher, "PublisherName"),
            "websites": websites,
            "identifiers": identifiers,
            "item_order": index,
        })

    imprints: List[Dict[str, Any]] = []
    for index, imprint in enumerate(_children(pd, "Imprint"), start=1):
        identifiers = []
        for ii, ident in enumerate(_children(imprint, "ImprintIdentifier"), start=1):
            identifiers.append({
                "imprint_id_type": _text(ident, "ImprintIDType"),
                "id_type_name": _text(ident, "IDTypeName"),
                "id_value": _text(ident, "IDValue"),
                "item_order": ii,
            })
        imprints.append({
            "imprint_name": _text(imprint, "ImprintName"),
            "identifiers": identifiers,
            "item_order": index,
        })

    primary_publisher = publishers[0] if publishers else {}
    primary_imprint = imprints[0] if imprints else {}

    return {
        "publishing_status": _text(pd, "PublishingStatus"),
        "publishing_dates": dates,

        # Existing InkSuite compatibility mirrors:
        "publisher": _clean(primary_publisher.get("publisher_name")),
        "publisher_role": _clean(primary_publisher.get("publishing_role")),
        "publisher_websites": primary_publisher.get("websites") or [],

        # Canonical repeatable ONIX Publisher composites:
        "publishers": publishers,
        "imprints": imprints,

        "imprint": _clean(primary_imprint.get("imprint_name")) or _deep_text(pd, "ImprintName"),
        "country_of_publication": _text(pd, "CountryOfPublication"),
        "city_of_publication": _text(pd, "CityOfPublication"),
    }


def _rights(product: ET.Element) -> Dict[str, Any]:
    pd = _child(product, "PublishingDetail")
    copyright_statements = []
    for index, c in enumerate(_children(pd, "CopyrightStatement"), start=1):
        copyright_statements.append({
            "copyright_type": _text(c, "CopyrightType") or "C",
            "copyright_year": _text(c, "CopyrightYear"),
            "item_order": index,
        })

    sales_rights = []
    for index, s in enumerate(_children(pd, "SalesRights"), start=1):
        territory = _child(s, "Territory")
        sales_rights.append({
            "sales_rights_type": _text(s, "SalesRightsType"),
            "countries_included": _text(territory, "CountriesIncluded"),
            "regions_included": _text(territory, "RegionsIncluded"),
            "countries_excluded": _text(territory, "CountriesExcluded"),
            "regions_excluded": _text(territory, "RegionsExcluded"),
            "item_order": index,
        })
    primary = copyright_statements[-1] if copyright_statements else {}
    primary_sales = sales_rights[0] if sales_rights else {}
    return {
        "copyright_type": primary.get("copyright_type", ""),
        "copyright_year": primary.get("copyright_year", ""),
        "copyright_statements": copyright_statements,
        "sales_rights": sales_rights,
        # Primary mirrors for the existing Rights card / edition_rights row.
        "sales_rights_type": primary_sales.get("sales_rights_type", ""),
        "countries_included": primary_sales.get("countries_included", ""),
        "regions_included": primary_sales.get("regions_included", ""),
        "countries_excluded": primary_sales.get("countries_excluded", ""),
        "regions_excluded": primary_sales.get("regions_excluded", ""),
        "row_sales_rights_type": _text(pd, "ROWSalesRightsType"),
    }


def _related(product: ET.Element) -> Dict[str, Any]:
    rm = _child(product, "RelatedMaterial")
    works = []
    for index, r in enumerate(_children(rm, "RelatedWork"), start=1):
        wi = _child(r, "WorkIdentifier")
        works.append({
            "work_relation_code": _text(r, "WorkRelationCode"),
            "work_id_type": _text(wi, "WorkIDType"),
            "id_type_name": _text(wi, "IDTypeName"),
            "id_value": _text(wi, "IDValue"),
            "item_order": index,
        })

    products = []
    for index, r in enumerate(_children(rm, "RelatedProduct"), start=1):
        identifiers = []
        for pi in _children(r, "ProductIdentifier"):
            identifiers.append({
                "product_id_type": _text(pi, "ProductIDType"),
                "id_type_name": _text(pi, "IDTypeName"),
                "id_value": _text(pi, "IDValue"),
            })
        isbn = next((i["id_value"] for i in identifiers if i["product_id_type"] == "15"), "")
        products.append({
            "product_relation_code": _text(r, "ProductRelationCode"),
            "related_isbn13": isbn,
            "related_product_form": _text(r, "ProductForm"),
            "related_product_form_detail": _text(r, "ProductFormDetail"),
            "product_identifiers": identifiers,
            "item_order": index,
        })
    return {"related_works": works, "related_products": products}


def _supply(product: ET.Element) -> Dict[str, Any]:
    ps = _child(product, "ProductSupply")
    mpd = _child(ps, "MarketPublishingDetail")

    markets = []
    for mi, market in enumerate(_children(ps, "Market"), start=1):
        territory = _child(market, "Territory")
        markets.append({
            "countries_included": _text(territory, "CountriesIncluded"),
            "regions_included": _text(territory, "RegionsIncluded"),
            "countries_excluded": _text(territory, "CountriesExcluded"),
            "regions_excluded": _text(territory, "RegionsExcluded"),
            "item_order": mi,
        })
    market_date = _child(mpd, "MarketDate")
    md = _date_parts(_child(market_date, "Date"))

    supplies = []
    for index, sd in enumerate(_children(ps, "SupplyDetail"), start=1):
        supplier = _child(sd, "Supplier")
        returns = _child(sd, "ReturnsConditions")
        prices = []
        for pi, p in enumerate(_children(sd, "Price"), start=1):
            territory = _child(p, "Territory")
            prices.append({
                "price_type": _text(p, "PriceType"),
                "price_amount": _text(p, "PriceAmount"),
                "currency_code": _text(p, "CurrencyCode"),
                "price_status": _text(p, "PriceStatus"),
                "country_code": _text(territory, "CountriesIncluded"),
                "territory": _text(territory, "RegionsIncluded"),
                "item_order": pi,
            })
        supply_dates = []
        for di, supply_date in enumerate(_children(sd, "SupplyDate"), start=1):
            dp = _date_parts(_child(supply_date, "Date"))
            supply_dates.append({
                "date_role": _text(supply_date, "SupplyDateRole"),
                "date_format": dp["date_format"],
                "date_text": dp["date_text"],
                "item_order": di,
            })

        supplies.append({
            "supplier_name": _text(supplier, "SupplierName"),
            "supplier_role": _text(supplier, "SupplierRole"),
            "supplier_email": _text(supplier, "EmailAddress"),
            "supplier_telephone": _text(supplier, "TelephoneNumber"),
            "supplier_fax": _text(supplier, "FaxNumber"),
            "product_availability": _text(sd, "ProductAvailability"),
            "pack_quantity": _text(sd, "PackQuantity"),
            "returns_code_type": _text(returns, "ReturnsCodeType"),
            "returns_code": _text(returns, "ReturnsCode"),
            "supply_dates": supply_dates,
            "prices": prices,
            "item_order": index,
        })

    promo_node = _child(mpd, "PromotionContact")
    return {
        "market_publishing_status": _text(mpd, "MarketPublishingStatus"),
        "market_date_role": _text(market_date, "MarketDateRole"),
        "market_date_format": md["date_format"],
        "market_date_text": md["date_text"],
        "promotion_contact": _inner_xml(promo_node),
        "promotion_contact_text_format": _clean(promo_node.attrib.get("textformat")) if promo_node is not None else "",
        "initial_print_run": _text(mpd, "InitialPrintRun"),
        "promotion_campaign": _text(mpd, "PromotionCampaign"),
        "markets": markets,
        "supply_details": supplies,
    }


def _resources(product: ET.Element) -> List[Dict[str, Any]]:
    collateral = _child(product, "CollateralDetail")
    resources = []
    for ri, r in enumerate(_children(collateral, "SupportingResource"), start=1):
        parent_features = []
        for fi, f in enumerate(_children(r, "ResourceFeature"), start=1):
            parent_features.append({
                "feature_type": _text(f, "ResourceFeatureType"),
                "feature_value": _text(f, "FeatureValue"),
                "feature_note": _inner_xml(_child(f, "FeatureNote")),
                "item_order": fi,
            })
        versions = []
        for vi, v in enumerate(_children(r, "ResourceVersion"), start=1):
            features = []
            filename = ""
            file_size_mb = ""
            width = ""
            height = ""
            file_format_code = ""
            for fi, f in enumerate(_children(v, "ResourceVersionFeature"), start=1):
                ft = _text(f, "ResourceVersionFeatureType")
                fv = _text(f, "FeatureValue")
                features.append({
                    "feature_type": ft,
                    "feature_value": fv,
                    "feature_note": _inner_xml(_child(f, "FeatureNote")),
                    "item_order": fi,
                })
                if ft == "01": file_format_code = fv
                if ft == "02": height = fv
                if ft == "03": width = fv
                if ft == "04": filename = fv
                if ft == "05": file_size_mb = fv

            cd = _child(v, "ContentDate")
            dn = _child(cd, "Date")
            dp = _date_parts(dn)
            versions.append({
                "resource_form": _text(v, "ResourceForm"),
                "resource_link": _text(v, "ResourceLink"),
                "content_date_role": _text(cd, "ContentDateRole"),
                "content_date_format": dp["date_format"],
                "content_date_text": dp["date_text"],
                "content_date": _parse_date_compat(dp["date_text"]),
                "filename": filename,
                "file_format_code": file_format_code,
                "file_size_mb": file_size_mb,
                "width_pixels": width,
                "height_pixels": height,
                "features": features,
                "item_order": vi,
            })
        resources.append({
            "resource_content_type": _text(r, "ResourceContentType"),
            "content_audience": _text(r, "ContentAudience"),
            "resource_mode": _text(r, "ResourceMode"),
            "territory_countries": _deep_text(r, "CountriesIncluded"),
            "resource_description": _text(r, "ResourceDescription"),
            "parent_features": parent_features,
            "versions": versions,
            "item_order": ri,
        })
    return resources


def _parse_product(product: ET.Element) -> Dict[str, Any]:
    dd = _child(product, "DescriptiveDetail")
    collateral = _child(product, "CollateralDetail")
    publishing = _publishing(product)

    identifiers = _id_values(product)
    barcode = _child(product, "Barcode")
    languages = _children(dd, "Language")
    primary_language = ""
    original_language = ""
    for lang in languages:
        role = _text(lang, "LanguageRole")
        code = _text(lang, "LanguageCode")
        if role == "01" and not primary_language:
            primary_language = code
        elif role == "02" and not original_language:
            original_language = code

    title_rows = _title_elements(dd)
    product_title = next(
        (
            r for r in title_rows
            if r["title_type"] == "01" and r["title_element_level"] == "01"
            and (r["title_without_prefix"] or r["title_text"])
        ),
        {},
    )
    # Helpers below are for matching/display only. Canonical ONIX title
    # fields are kept separately and persisted field-for-field.
    source_title = _title_row_display(product_title)
    source_subtitle = _clean(product_title.get("subtitle"))
    primary_title = dict(product_title) if product_title else {}

    identity = {
        "isbn13": _isbn13(product),
        "product_form": _product_form_label(_text(dd, "ProductForm")),
        "product_form_detail": _text(dd, "ProductFormDetail"),
        "onix_product_form": _text(dd, "ProductForm"),
        "onix_product_form_detail": _text(dd, "ProductFormDetail"),
        "notification_type": _text(product, "NotificationType"),
        "product_composition": _text(dd, "ProductComposition"),
        "primary_content_type": _text(dd, "PrimaryContentType"),
        "barcode_type": _text(barcode, "BarcodeType"),
        "barcode_position_on_product": _text(barcode, "PositionOnProduct"),
        "product_packaging": _text(dd, "ProductPackaging"),
        "record_reference": _text(product, "RecordReference"),
        "record_source_type": _text(product, "RecordSourceType"),
        "record_source_name": _text(product, "RecordSourceName"),
        "publishing_status": publishing["publishing_status"],
        "product_identifiers": identifiers,
        "publisher": publishing["publisher"],
        "imprint": publishing["imprint"],
        "country_of_publication": publishing["country_of_publication"],
        "language": primary_language,
        "original_language": original_language,
    }

    return {
        "summary": _summary(product),
        "source_title": source_title,
        "source_subtitle": source_subtitle,
        "primary_title": primary_title,
        "identity": identity,
        "collections": _collections(dd),
        "contributors": _contributors(dd),
        "descriptive_content": {"descriptive_texts": _texts(collateral)},
        "subjects_audience": _subjects_audience(dd),
        "product_details": _product_details(dd),
        "publishing": publishing,
        "rights": _rights(product),
        "related": _related(product),
        "supply": _supply(product),
        "supporting_resources": _resources(product),
    }



def _work_title_columns(cur) -> set[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'work_titles'
        """
    )
    return {
        _clean(row.get("column_name"))
        for row in (cur.fetchall() or [])
        if _clean(row.get("column_name"))
    }


def _upsert_primary_title_element_exact(
    cur,
    tenant_id: str,
    work_id: str,
    source: Dict[str, Any],
) -> None:
    """
    Persist the source ONIX primary TitleElement exactly.

    No title assembly, subtitle splitting, fallback, or interpretation occurs.
    Blank source fields overwrite stale canonical values with blank values.
    """
    if not isinstance(source, dict) or not source:
        return

    columns = _work_title_columns(cur)
    if not {"tenant_id", "work_id"}.issubset(columns):
        raise ValueError(
            "work_titles table does not contain tenant_id/work_id."
        )

    values = {
        "title_type": _clean(source.get("title_type")),
        "title_element_level": _clean(
            source.get("title_element_level")
        ),
        "title_prefix": _clean(source.get("title_prefix")),
        "title_without_prefix": _clean(
            source.get("title_without_prefix")
        ),
        "subtitle": _clean(source.get("subtitle")),
        "part_number": _clean(source.get("part_number")),
        "year_of_annual": _clean(
            source.get("year_of_annual")
        ),
        "language_code": _clean(
            source.get("language_code")
        ),
        "no_prefix": bool(source.get("no_prefix")),
        "is_primary": True,
        "item_order": 1,
    }

    cur.execute(
        """
        SELECT id
        FROM work_titles
        WHERE tenant_id = %s
          AND work_id = %s
          AND (
                is_primary = true
                OR (
                    title_type = %s
                    AND title_element_level = %s
                )
          )
        ORDER BY
            CASE WHEN is_primary = true THEN 0 ELSE 1 END,
            item_order NULLS LAST,
            created_at NULLS LAST,
            id
        LIMIT 1
        """,
        (
            tenant_id,
            work_id,
            values["title_type"],
            values["title_element_level"],
        ),
    )
    existing = cur.fetchone()

    writable = [
        name
        for name in (
            "title_type",
            "title_element_level",
            "title_prefix",
            "title_without_prefix",
            "subtitle",
            "part_number",
            "year_of_annual",
            "language_code",
            "no_prefix",
            "is_primary",
            "item_order",
        )
        if name in columns
    ]

    if existing:
        assignments = [f"{name} = %s" for name in writable]
        params = [values[name] for name in writable]

        if "updated_at" in columns:
            assignments.append("updated_at = now()")

        params.extend([tenant_id, work_id, existing["id"]])

        cur.execute(
            f"""
            UPDATE work_titles
            SET {", ".join(assignments)}
            WHERE tenant_id = %s
              AND work_id = %s
              AND id = %s
            """,
            tuple(params),
        )
        return

    insert_columns = ["tenant_id", "work_id", *writable]
    insert_values = [
        tenant_id,
        work_id,
        *[values[name] for name in writable],
    ]

    cur.execute(
        f"""
        INSERT INTO work_titles (
            {", ".join(insert_columns)}
        )
        VALUES (
            {", ".join(["%s"] * len(insert_columns))}
        )
        """,
        tuple(insert_values),
    )




def _sync_work_subtitle_from_primary_onix_title(
    cur,
    tenant_id: str,
    work_id: str,
    primary_title: Dict[str, Any],
) -> None:
    """Mirror the exact source ONIX Subtitle into works.subtitle."""
    if not isinstance(primary_title, dict) or not primary_title:
        return

    subtitle = _clean(primary_title.get("subtitle"))
    cur.execute(
        """
        UPDATE works
        SET subtitle = %s,
            updated_at = now()
        WHERE tenant_id = %s
          AND id = %s
        """,
        (subtitle, tenant_id, work_id),
    )


def _get_tenant_id(cur, tenant_slug: str) -> str:
    cur.execute(
        "SELECT id FROM tenants WHERE slug = %s LIMIT 1",
        (tenant_slug,),
    )
    row = cur.fetchone()
    if not row:
        raise ValueError("Tenant not found")
    return str(row["id"])


def _replace_publisher_websites(cur, tenant_id: str, edition_id: str, rows: List[Dict[str, Any]]) -> None:
    cur.execute(
        "DELETE FROM edition_publisher_websites WHERE tenant_id = %s AND edition_id = %s",
        (tenant_id, edition_id),
    )
    for index, row in enumerate(rows or [], start=1):
        role = _clean(row.get("website_role"))
        link = _clean(row.get("website_link"))
        if not link:
            continue
        cur.execute(
            """
            INSERT INTO edition_publisher_websites (
                tenant_id, edition_id, website_role, website_description,
                website_link, item_order
            ) VALUES (%s,%s,%s,%s,%s,%s)
            """,
            (
                tenant_id, edition_id, role,
                _clean(row.get("website_description")),
                link, index,
            ),
        )



def _replace_publishers(
    cur,
    tenant_id: str,
    edition_id: str,
    publishers: List[Dict[str, Any]],
) -> None:
    """Replace canonical repeatable Publisher composites and identifiers."""
    cur.execute(
        "SELECT id FROM edition_publishers WHERE tenant_id = %s AND edition_id = %s",
        (tenant_id, edition_id),
    )
    old_ids = [str(r["id"]) for r in (cur.fetchall() or [])]
    if old_ids:
        cur.execute(
            "DELETE FROM edition_publisher_identifiers WHERE tenant_id = %s AND publisher_id = ANY(%s::uuid[])",
            (tenant_id, old_ids),
        )
    cur.execute(
        "DELETE FROM edition_publishers WHERE tenant_id = %s AND edition_id = %s",
        (tenant_id, edition_id),
    )

    for index, row in enumerate(publishers or [], start=1):
        if not isinstance(row, dict):
            continue
        publishing_role = _clean(row.get("publishing_role"))
        publisher_name = _clean(row.get("publisher_name"))
        if not (publishing_role or publisher_name):
            continue
        publisher_id = str(uuid.uuid4())
        cur.execute(
            """
            INSERT INTO edition_publishers (
                id, tenant_id, edition_id, publishing_role, publisher_name, item_order
            ) VALUES (%s,%s,%s,%s,%s,%s)
            """,
            (publisher_id, tenant_id, edition_id, publishing_role, publisher_name, index),
        )
        for ii, ident in enumerate(row.get("identifiers") or [], start=1):
            if not isinstance(ident, dict):
                continue
            id_type = _clean(ident.get("publisher_id_type"))
            id_value = _clean(ident.get("id_value"))
            if not (id_type or id_value):
                continue
            cur.execute(
                """
                INSERT INTO edition_publisher_identifiers (
                    tenant_id, publisher_id, publisher_id_type,
                    id_type_name, id_value, item_order
                ) VALUES (%s,%s,%s,%s,%s,%s)
                """,
                (tenant_id, publisher_id, id_type,
                 _clean(ident.get("id_type_name")), id_value, ii),
            )


def _replace_imprints(
    cur,
    tenant_id: str,
    edition_id: str,
    imprints: List[Dict[str, Any]],
) -> None:
    """Replace canonical repeatable Imprint composites and identifiers."""
    cur.execute(
        "SELECT id FROM edition_imprints WHERE tenant_id = %s AND edition_id = %s",
        (tenant_id, edition_id),
    )
    old_ids = [str(r["id"]) for r in (cur.fetchall() or [])]
    if old_ids:
        cur.execute(
            "DELETE FROM edition_imprint_identifiers WHERE tenant_id = %s AND imprint_id = ANY(%s::uuid[])",
            (tenant_id, old_ids),
        )
    cur.execute(
        "DELETE FROM edition_imprints WHERE tenant_id = %s AND edition_id = %s",
        (tenant_id, edition_id),
    )

    for index, row in enumerate(imprints or [], start=1):
        if not isinstance(row, dict):
            continue
        imprint_name = _clean(row.get("imprint_name"))
        if not imprint_name and not (row.get("identifiers") or []):
            continue
        imprint_id = str(uuid.uuid4())
        cur.execute(
            """
            INSERT INTO edition_imprints (
                id, tenant_id, edition_id, imprint_name, item_order
            ) VALUES (%s,%s,%s,%s,%s)
            """,
            (imprint_id, tenant_id, edition_id, imprint_name, index),
        )
        for ii, ident in enumerate(row.get("identifiers") or [], start=1):
            if not isinstance(ident, dict):
                continue
            id_type = _clean(ident.get("imprint_id_type"))
            id_value = _clean(ident.get("id_value"))
            if not (id_type or id_value):
                continue
            cur.execute(
                """
                INSERT INTO edition_imprint_identifiers (
                    tenant_id, imprint_id, imprint_id_type,
                    id_type_name, id_value, item_order
                ) VALUES (%s,%s,%s,%s,%s,%s)
                """,
                (tenant_id, imprint_id, id_type,
                 _clean(ident.get("id_type_name")), id_value, ii),
            )

def _normalize_imported_audience_rows(
    cur,
    tenant_id: str,
    edition_id: str,
) -> None:
    """
    edition_audience may have a DB/default value of audience_code_type='01'.
    That must not leak onto rows which represent only AudienceRange.
    """
    cur.execute(
        """
        UPDATE edition_audience
        SET
            audience_code_type = '',
            audience_code_type_name = ''
        WHERE tenant_id = %s
          AND edition_id = %s
          AND COALESCE(onix_audience_code, '') = ''
          AND COALESCE(audience_range_qualifier, '') <> ''
        """,
        (tenant_id, edition_id),
    )



def _replace_supporting_resources(cur, tenant_id: str, edition_id: str, resources: List[Dict[str, Any]]) -> None:
    """
    Replace imported/external SupportingResource metadata without deleting
    InkSuite-managed public assets (versions with storage_key).

    If an InkSuite-managed front cover already exists, an imported external
    ResourceContentType 01 is not duplicated. Empty resources are never stored.
    """
    cur.execute(
        """
        SELECT DISTINCT r.id::text, r.resource_content_type
        FROM edition_supporting_resources r
        LEFT JOIN edition_supporting_resource_versions v
          ON v.tenant_id = r.tenant_id AND v.resource_id = r.id
        WHERE r.tenant_id = %s AND r.edition_id = %s
          AND NULLIF(trim(v.storage_key), '') IS NOT NULL
        """,
        (tenant_id, edition_id),
    )
    managed_rows = cur.fetchall() or []
    managed_ids = {str(r["id"]) for r in managed_rows}
    managed_types = {_clean(r.get("resource_content_type")) for r in managed_rows}

    cur.execute(
        "SELECT id::text FROM edition_supporting_resources WHERE tenant_id = %s AND edition_id = %s",
        (tenant_id, edition_id),
    )
    all_ids = {str(r["id"]) for r in (cur.fetchall() or [])}
    replace_ids = sorted(all_ids - managed_ids)

    if replace_ids:
        cur.execute(
            """
            DELETE FROM edition_supporting_resource_features
            WHERE tenant_id = %s AND resource_version_id IN (
                SELECT id FROM edition_supporting_resource_versions
                WHERE tenant_id = %s AND resource_id = ANY(%s::uuid[])
            )
            """,
            (tenant_id, tenant_id, replace_ids),
        )
        cur.execute(
            "DELETE FROM edition_supporting_resource_parent_features WHERE tenant_id = %s AND resource_id = ANY(%s::uuid[])",
            (tenant_id, replace_ids),
        )
        cur.execute(
            "DELETE FROM edition_supporting_resource_versions WHERE tenant_id = %s AND resource_id = ANY(%s::uuid[])",
            (tenant_id, replace_ids),
        )
        cur.execute(
            "DELETE FROM edition_supporting_resources WHERE tenant_id = %s AND id = ANY(%s::uuid[])",
            (tenant_id, replace_ids),
        )

    next_order = len(managed_ids) + 1
    for resource in resources or []:
        if not isinstance(resource, dict):
            continue
        content_type = _clean(resource.get("resource_content_type"))
        versions = [v for v in (resource.get("versions") or []) if isinstance(v, dict)]
        parent_features = [f for f in (resource.get("parent_features") or []) if isinstance(f, dict)]
        has_version_content = any(
            _clean(v.get("resource_link")) or _clean(v.get("resource_form")) or (v.get("features") or [])
            for v in versions
        )
        if not (content_type or parent_features or has_version_content):
            continue
        if content_type == "01" and "01" in managed_types:
            continue

        resource_id = str(uuid.uuid4())
        cur.execute(
            """
            INSERT INTO edition_supporting_resources (
                id, tenant_id, edition_id, resource_content_type,
                content_audience, resource_mode, resource_description,
                is_primary, item_order, territory_countries, caption, credit
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,false,%s,%s,'','')
            """,
            (resource_id, tenant_id, edition_id, content_type,
             _clean(resource.get("content_audience")),
             _clean(resource.get("resource_mode")),
             _clean(resource.get("resource_description")),
             next_order, _clean(resource.get("territory_countries"))),
        )
        next_order += 1

        for fi, feature in enumerate(parent_features, start=1):
            cur.execute(
                """
                INSERT INTO edition_supporting_resource_parent_features (
                    tenant_id, resource_id, feature_type, feature_value,
                    feature_note, item_order
                ) VALUES (%s,%s,%s,%s,%s,%s)
                """,
                (tenant_id, resource_id, _clean(feature.get("feature_type")),
                 _clean(feature.get("feature_value")), _clean(feature.get("feature_note")), fi),
            )

        for vi, version in enumerate(versions, start=1):
            if not (_clean(version.get("resource_link")) or _clean(version.get("resource_form")) or (version.get("features") or [])):
                continue
            version_id = str(uuid.uuid4())
            cur.execute(
                """
                INSERT INTO edition_supporting_resource_versions (
                    id, tenant_id, resource_id, resource_form, resource_link,
                    content_date_role, content_date, content_date_format,
                    content_date_text, file_format, file_size,
                    width_pixels, height_pixels, filename, item_order
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (version_id, tenant_id, resource_id,
                 _clean(version.get("resource_form")), _clean(version.get("resource_link")),
                 _clean(version.get("content_date_role")), version.get("content_date"),
                 _clean(version.get("content_date_format")), _clean(version.get("content_date_text")),
                 _clean(version.get("file_format_code")),
                 float(version["file_size_mb"]) if _clean(version.get("file_size_mb")) else None,
                 int(version["width_pixels"]) if _clean(version.get("width_pixels")).isdigit() else None,
                 int(version["height_pixels"]) if _clean(version.get("height_pixels")).isdigit() else None,
                 _clean(version.get("filename")), vi),
            )
            for fi, feature in enumerate(version.get("features") or [], start=1):
                cur.execute(
                    """
                    INSERT INTO edition_supporting_resource_features (
                        tenant_id, resource_version_id, feature_type,
                        feature_value, feature_note, item_order
                    ) VALUES (%s,%s,%s,%s,%s,%s)
                    """,
                    (tenant_id, version_id, _clean(feature.get("feature_type")),
                     _clean(feature.get("feature_value")), _clean(feature.get("feature_note")), fi),
                )

    # Remove legacy empty parents left by older UI/import behavior.
    cur.execute(
        """
        DELETE FROM edition_supporting_resources r
        WHERE r.tenant_id = %s AND r.edition_id = %s
          AND NOT EXISTS (
              SELECT 1 FROM edition_supporting_resource_versions v
              WHERE v.tenant_id = r.tenant_id AND v.resource_id = r.id
          )
          AND NOT EXISTS (
              SELECT 1 FROM edition_supporting_resource_parent_features f
              WHERE f.tenant_id = r.tenant_id AND f.resource_id = r.id
          )
        """,
        (tenant_id, edition_id),
    )

def _normalize_match_text(value: Any) -> str:
    s = _clean(value).lower()
    s = re.sub(r"\bvol(?:ume)?\.?\s*\d+\b", " ", s)
    s = re.sub(r"\([^)]*\)", " ", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return " ".join(s.split())


def _source_match_hints(product: ET.Element) -> List[str]:
    dd = _child(product, "DescriptiveDetail")
    hints: List[str] = []

    summary = _summary(product)
    title = _clean(summary.get("title"))
    subtitle = _clean(summary.get("subtitle"))
    if title:
        hints.append(title)
    if title and subtitle:
        hints.append(f"{title} {subtitle}")

    # Collection titles frequently carry the actual series/title identity more
    # usefully than the product-level TitleDetail in distributor feeds.
    for coll in _children(dd, "Collection"):
        for row in _title_elements(coll):
            value = _title_row_display(row)
            if value:
                hints.append(value)

    # Split "Series title: volume title" into a second useful hint.
    expanded: List[str] = []
    for value in hints:
        expanded.append(value)
        if ":" in value:
            left, right = value.split(":", 1)
            if _clean(left):
                expanded.append(_clean(left))
            if _clean(left) and _clean(right):
                expanded.append(
                    f"{_clean(left)} {_clean(right)}"
                )

    seen = set()
    out: List[str] = []
    for value in expanded:
        norm = _normalize_match_text(value)
        if norm and norm not in seen:
            seen.add(norm)
            out.append(value)
    return out


def _existing_product_by_isbn(
    cur,
    tenant_id: str,
    isbn13: str,
) -> Optional[Dict[str, Any]]:
    isbn13 = re.sub(r"[^0-9Xx]", "", _clean(isbn13))
    if not isbn13:
        return None

    cur.execute(
        """
        SELECT
            e.id AS edition_id,
            e.work_id,
            e.isbn13,
            e.product_form,
            e.product_form_detail,
            e.onix_product_form,
            e.onix_product_form_detail,
            w.title,
            w.subtitle,
            w.series_title
        FROM editions e
        JOIN works w
          ON w.tenant_id = e.tenant_id
         AND w.id = e.work_id
        WHERE e.tenant_id = %s
          AND regexp_replace(
                coalesce(e.isbn13, ''),
                '[^0-9Xx]',
                '',
                'g'
              ) = %s
        LIMIT 1
        """,
        (tenant_id, isbn13),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def _all_work_candidates(cur, tenant_id: str) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT
            w.id AS work_id,
            w.title,
            w.subtitle,
            w.series_title,
            count(e.id) AS edition_count
        FROM works w
        LEFT JOIN editions e
          ON e.tenant_id = w.tenant_id
         AND e.work_id = w.id
        WHERE w.tenant_id = %s
        GROUP BY
            w.id,
            w.title,
            w.subtitle,
            w.series_title
        ORDER BY w.title, w.subtitle
        """,
        (tenant_id,),
    )
    return [dict(r) for r in (cur.fetchall() or [])]


def _split_source_title_identity(
    product: ET.Element,
) -> Tuple[str, str]:
    """
    Return normalized source title/subtitle identity for work matching.

    A distributor may send:
      title = "Series: Volume title"
      subtitle = ""

    while InkSuite stores:
      title = "Series"
      subtitle = "Volume title"

    This helper makes those representations comparable.
    """
    summary = _summary(product)
    raw_title = _clean(summary.get("title"))
    raw_subtitle = _clean(summary.get("subtitle"))

    if raw_title and ":" in raw_title and not raw_subtitle:
        left, right = raw_title.split(":", 1)
        if _clean(left) and _clean(right):
            return (
                _normalize_match_text(left),
                _normalize_match_text(right),
            )

    return (
        _normalize_match_text(raw_title),
        _normalize_match_text(raw_subtitle),
    )


def _best_work_match(
    product: ET.Element,
    candidates: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """
    Match ONIX Product -> InkSuite work.

    Important safeguards:
    - exact ISBN is handled before this function
    - title + subtitle is preferred
    - same title with a conflicting subtitle is NOT considered a match
      (critical for series where every volume shares the same main title)
    - "Series: Volume" can match InkSuite title="Series", subtitle="Volume"
    """
    source_title, source_subtitle = (
        _split_source_title_identity(product)
    )

    hints = [
        _normalize_match_text(v)
        for v in _source_match_hints(product)
        if _normalize_match_text(v)
    ]

    if not source_title and not hints:
        return None

    best = None
    best_score = 0

    for row in candidates:
        title = _normalize_match_text(
            row.get("title")
        )
        subtitle = _normalize_match_text(
            row.get("subtitle")
        )
        combined = _normalize_match_text(
            f"{row.get('title') or ''} "
            f"{row.get('subtitle') or ''}"
        )

        score = 0

        # Strongest case: normalized title and subtitle both agree.
        if (
            source_title
            and title
            and source_title == title
        ):
            if source_subtitle and subtitle:
                if source_subtitle == subtitle:
                    score = 100
                elif (
                    source_subtitle in subtitle
                    or subtitle in source_subtitle
                ):
                    score = 96
                else:
                    # Same series/main title, but a different volume subtitle.
                    # Do not attach this ONIX Product to the wrong work.
                    score = 0
            elif source_subtitle and not subtitle:
                # Existing work is missing its subtitle. This is plausible but
                # should remain a confirmation-level title match.
                score = 90
            elif not source_subtitle and subtitle:
                # Source carries less identity than InkSuite. Avoid matching a
                # random volume solely because the shared main title matches.
                score = 0
            else:
                score = 96

        # Fall back to the richer source hints only if we did not already
        # identify a subtitle conflict.
        if score == 0 and not (
            source_title == title
            and source_subtitle
            and subtitle
            and source_subtitle != subtitle
        ):
            for hint in hints:
                if combined and hint == combined:
                    score = max(score, 100)

                if (
                    title
                    and subtitle
                    and title in hint
                    and subtitle in hint
                ):
                    score = max(score, 96)

                # Title-only fallback is safe only where InkSuite itself does
                # not have a subtitle distinguishing this work from siblings.
                if (
                    title
                    and not subtitle
                    and hint == title
                ):
                    score = max(score, 92)

                if (
                    title
                    and not subtitle
                    and len(title) >= 8
                    and title in hint
                ):
                    score = max(score, 88)

        if score > best_score:
            best_score = score
            best = dict(row)

    if best is None or best_score < 88:
        return None

    best["match_score"] = best_score
    return best


def _format_family(value: Any) -> str:
    raw = _clean(value)
    upper = raw.upper()

    onix_map = {
        "BB": "hardcover",
        "BC": "paperback",
        "BA": "book",
        "EA": "ebook",
        "EB": "ebook",
        "EC": "ebook",
        "ED": "ebook",
        "AJ": "audiobook",
        "AC": "audiobook",
    }
    if upper in onix_map:
        return onix_map[upper]

    normalized = _normalize_match_text(raw)

    if any(
        token in normalized
        for token in (
            "hardcover",
            "hardback",
            "casebound",
            "paper over board",
            "paper over boards",
        )
    ):
        return "hardcover"

    if any(
        token in normalized
        for token in (
            "paperback",
            "softcover",
            "soft cover",
        )
    ):
        return "paperback"

    if any(
        token in normalized
        for token in (
            "ebook",
            "e book",
            "digital book",
        )
    ):
        return "ebook"

    if any(
        token in normalized
        for token in (
            "audiobook",
            "audio book",
            "downloadable audio",
        )
    ):
        return "audiobook"

    return normalized


def _blank_isbn_edition_match(
    cur,
    tenant_id: str,
    work_id: str,
    product: ET.Element,
) -> Optional[Dict[str, Any]]:
    """
    If a title already exists in InkSuite, try to reuse an edition/format row
    whose ISBN is still blank instead of creating a duplicate edition.

    Matching priority:
      1. exact ONIX ProductForm + ProductFormDetail
      2. exact ONIX ProductForm
      3. friendly format-family match (Hardcover / Paperback / E-book / Audio)

    We deliberately do not consume a blank-ISBN edition whose format conflicts
    with the incoming ONIX Product.
    """
    summary = _summary(product)
    source_form = _clean(
        summary.get("product_form")
    ).upper()
    source_detail = _clean(
        summary.get("product_form_detail")
    ).upper()
    source_family = _format_family(source_form)

    cur.execute(
        """
        SELECT
            e.id AS edition_id,
            e.work_id,
            e.isbn13,
            e.product_form,
            e.product_form_detail,
            e.onix_product_form,
            e.onix_product_form_detail
        FROM editions e
        WHERE e.tenant_id = %s
          AND e.work_id = %s
          AND regexp_replace(
                coalesce(e.isbn13, ''),
                '[^0-9Xx]',
                '',
                'g'
              ) = ''
        ORDER BY e.created_at, e.id
        """,
        (tenant_id, work_id),
    )
    rows = [
        dict(r)
        for r in (cur.fetchall() or [])
    ]

    best = None
    best_score = 0
    tied = False

    for row in rows:
        candidate_form = _clean(
            row.get("onix_product_form")
        ).upper()
        candidate_detail = _clean(
            row.get("onix_product_form_detail")
        ).upper()

        candidate_family = _format_family(
            candidate_form
            or row.get("product_form")
        )

        score = 0

        if (
            source_form
            and candidate_form
            and source_form == candidate_form
        ):
            score = 80

            if (
                source_detail
                and candidate_detail
                and source_detail == candidate_detail
            ):
                score = 100

        elif (
            source_family
            and candidate_family
            and source_family == candidate_family
        ):
            score = 70

            # ProductFormDetail may be stored in the friendly/legacy field.
            friendly_detail = _clean(
                row.get("product_form_detail")
            ).upper()
            if (
                source_detail
                and (
                    source_detail == candidate_detail
                    or source_detail == friendly_detail
                )
            ):
                score = 88

        if score > best_score:
            best = row
            best_score = score
            tied = False
        elif score and score == best_score:
            tied = True

    # Ambiguous format placeholders should be confirmed manually rather than
    # silently choosing one.
    if (
        best is None
        or best_score < 70
        or tied
    ):
        return None

    best["format_match_score"] = best_score
    return best

def _target_descriptor(row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not row:
        return None
    return {
        "work_id": _clean(row.get("work_id")),
        "edition_id": _clean(row.get("edition_id")),
        "isbn13": _clean(row.get("isbn13")),
        "title": _clean(row.get("title")),
        "subtitle": _clean(row.get("subtitle")),
        "product_form": _clean(
            row.get("onix_product_form")
            or row.get("product_form")
        ),
        "product_form_detail": _clean(
            row.get("onix_product_form_detail")
            or row.get("product_form_detail")
        ),
    }


def _plan_product(
    cur,
    tenant_id: str,
    product: ET.Element,
    work_candidates: List[Dict[str, Any]],
) -> Dict[str, Any]:
    summary = _summary(product)

    # 1. Exact ISBN owns the decision.
    exact = _existing_product_by_isbn(
        cur,
        tenant_id,
        summary.get("isbn13") or "",
    )

    if exact:
        return {
            **summary,
            "suggested_action":
                "update_existing_edition",
            "match_reason":
                "Exact ISBN match",
            "match_confidence": "exact",
            "match_basis": ["isbn"],
            "requires_confirmation": True,
            "target": _target_descriptor(exact),
        }

    # 2. No ISBN match: identify the work by title/subtitle.
    work_match = _best_work_match(
        product,
        work_candidates,
    )

    if work_match:
        # 2a. If that work already has the right format with a blank ISBN,
        # update that placeholder edition and assign the source ISBN to it.
        blank_edition = _blank_isbn_edition_match(
            cur,
            tenant_id,
            _clean(work_match.get("work_id")),
            product,
        )

        if blank_edition:
            target = {
                **work_match,
                **blank_edition,
            }
            return {
                **summary,
                "suggested_action":
                    "update_existing_edition",
                "match_reason":
                    "No matching ISBN; title and format match an existing InkSuite edition whose ISBN is blank",
                "match_confidence": "probable",
                "match_basis": [
                    "title",
                    "format",
                    "blank_isbn",
                ],
                "requires_confirmation": True,
                "target":
                    _target_descriptor(target),
            }

        # 2b. Work exists, but this format does not. Add a new format/edition
        # under the existing title.
        return {
            **summary,
            "suggested_action":
                "create_new_edition",
            "match_reason":
                "No matching ISBN; existing title matched, but no blank-ISBN edition of this format exists",
            "match_confidence": "probable",
            "match_basis": ["title"],
            "requires_confirmation": True,
            "target": {
                "work_id": _clean(
                    work_match.get("work_id")
                ),
                "edition_id": "",
                "isbn13": "",
                "title": _clean(
                    work_match.get("title")
                ),
                "subtitle": _clean(
                    work_match.get("subtitle")
                ),
                "product_form":
                    _clean(
                        summary.get(
                            "product_form"
                        )
                    ),
                "product_form_detail":
                    _clean(
                        summary.get(
                            "product_form_detail"
                        )
                    ),
            },
        }

    # 3. No ISBN and no title match: this is a genuinely new work.
    return {
        **summary,
        "suggested_action":
            "create_new_work",
        "match_reason":
            "No ISBN or title match; create a new InkSuite title and its edition",
        "match_confidence": "new",
        "match_basis": [],
        "requires_confirmation": True,
        "target": None,
    }

def _create_edition_for_work(
    cur,
    tenant_id: str,
    work_id: str,
    parsed: Dict[str, Any],
) -> str:
    identity = parsed["identity"]
    cur.execute(
        """
        INSERT INTO editions (
            tenant_id,
            work_id,
            isbn13,
            product_form,
            product_form_detail,
            onix_product_form,
            onix_product_form_detail,
            publishing_status,
            notification_type,
            product_composition
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING id
        """,
        (
            tenant_id,
            work_id,
            _clean(identity.get("isbn13")) or None,
            _clean(identity.get("product_form")),
            _clean(identity.get("product_form_detail")),
            _clean(identity.get("onix_product_form")),
            _clean(identity.get("onix_product_form_detail")),
            _clean(identity.get("publishing_status")) or "02",
            _clean(identity.get("notification_type")) or "02",
            _clean(identity.get("product_composition")) or "00",
        ),
    )
    row = cur.fetchone() or {}
    edition_id = _clean(row.get("id"))
    if not edition_id:
        raise ValueError("Could not create edition.")
    return edition_id


def _create_work_and_edition(
    cur,
    tenant_id: str,
    parsed: Dict[str, Any],
) -> Tuple[str, str]:
    title = _clean(parsed.get("source_title"))
    subtitle = _clean(parsed.get("source_subtitle"))

    # If the product-level TitleDetail is not useful enough, use the first
    # meaningful collection title before falling back to Untitled.
    if not title:
        for collection in parsed.get("collections") or []:
            if _clean(collection.get("title")):
                title = _clean(collection.get("title"))
                break

    work_id = str(uuid.uuid4())
    uid = str(uuid.uuid4())

    cur.execute(
        """
        INSERT INTO works (
            id,
            tenant_id,
            uid,
            title,
            subtitle
        )
        VALUES (%s,%s,%s,%s,%s)
        """,
        (
            work_id,
            tenant_id,
            uid,
            title or "Untitled",
            subtitle,
        ),
    )

    edition_id = _create_edition_for_work(
        cur,
        tenant_id,
        work_id,
        parsed,
    )
    return work_id, edition_id



_ROLE_EQUIVALENTS = {
    "A01": {"A01", "AUTHOR"},
    "A12": {"A12", "ILLUSTRATOR"},
}


def _contributor_name_key(value: Any) -> str:
    text = unicodedata.normalize(
        "NFKD",
        _clean(value),
    )
    text = "".join(
        ch
        for ch in text
        if not unicodedata.combining(ch)
    )
    return re.sub(
        r"[^a-z0-9]+",
        "",
        text.casefold(),
    )


def _prepare_existing_contributor_assignment(
    cur,
    tenant_id: str,
    work_id: str,
    contributor: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Reuse existing InkSuite contributor assignments before ONIX import.

    AUTHOR is treated as equivalent to A01 and ILLUSTRATOR as equivalent to A12.
    Name matching is accent-insensitive. If an earlier import already created a
    duplicate equivalent assignment for the same person, that duplicate work
    assignment is removed before the original assignment is updated.
    """
    desired_role = _clean(
        contributor.get("contributor_role_code")
    ).upper()
    source_name_key = _contributor_name_key(
        contributor.get("name")
        or contributor.get("display_name")
    )

    equivalent_roles = _ROLE_EQUIVALENTS.get(
        desired_role,
        {desired_role},
    )

    if not desired_role or not source_name_key:
        return contributor

    cur.execute(
        """
        SELECT
            wc.id AS work_contributor_id,
            wc.party_id,
            wc.contributor_role,
            wc.sequence_number,
            p.display_name
        FROM work_contributors wc
        JOIN parties p
          ON p.tenant_id = wc.tenant_id
         AND p.id = wc.party_id
        WHERE wc.tenant_id = %s
          AND wc.work_id = %s
        ORDER BY wc.sequence_number, wc.id
        """,
        (tenant_id, work_id),
    )
    rows = cur.fetchall() or []

    candidates = []
    for row in rows:
        role = _clean(
            row.get("contributor_role")
        ).upper()
        if role not in equivalent_roles:
            continue
        if _contributor_name_key(
            row.get("display_name")
        ) != source_name_key:
            continue
        candidates.append(row)

    if not candidates:
        return contributor

    legacy_roles = equivalent_roles - {desired_role}
    chosen = next(
        (
            row
            for row in candidates
            if _clean(
                row.get("contributor_role")
            ).upper() in legacy_roles
        ),
        candidates[0],
    )

    chosen_id = _clean(
        chosen.get("work_contributor_id")
    )

    duplicate_ids = [
        _clean(row.get("work_contributor_id"))
        for row in candidates
        if _clean(row.get("work_contributor_id"))
        and _clean(row.get("work_contributor_id"))
        != chosen_id
    ]

    if duplicate_ids:
        cur.execute(
            """
            DELETE FROM work_contributors
            WHERE tenant_id = %s
              AND work_id = %s
              AND id = ANY(%s::uuid[])
            """,
            (
                tenant_id,
                work_id,
                duplicate_ids,
            ),
        )

    resolved = dict(contributor)
    resolved["party_id"] = _clean(
        chosen.get("party_id")
    )
    resolved["work_contributor_id"] = chosen_id
    return resolved


def _apply_parsed_to_target(
    cur,
    tenant_id: str,
    parsed: Dict[str, Any],
    target_work_id: str,
    target_edition_id: str,
    preserve_existing_title: bool,
) -> Dict[str, Any]:
    cur.execute(
        """
        SELECT w.title, w.subtitle
        FROM works w
        WHERE w.tenant_id = %s
          AND w.id = %s
        LIMIT 1
        """,
        (tenant_id, target_work_id),
    )
    target = cur.fetchone()
    if not target:
        raise ValueError("Target work was not found.")

    update_edition_product_identity(
        cur,
        tenant_id,
        target_work_id,
        target_edition_id,
        parsed["identity"],
    )

    if preserve_existing_title:
        title = _clean(target.get("title"))
        subtitle = _clean(target.get("subtitle"))
    else:
        title = (
            parsed["source_title"]
            or _clean(target.get("title"))
        )
        subtitle = parsed["source_subtitle"]

    update_work_titles_collections(
        cur,
        tenant_id,
        target_work_id,
        {
            "title": title,
            "subtitle": subtitle,
            "collections": parsed["collections"],
        },
    )

    # Canonical ONIX TitleElement wins over any display/helper transformation.
    _upsert_primary_title_element_exact(
        cur,
        tenant_id,
        target_work_id,
        parsed.get("primary_title") or {},
    )
    _sync_work_subtitle_from_primary_onix_title(
        cur,
        tenant_id,
        target_work_id,
        parsed.get("primary_title") or {},
    )

    for contributor in parsed["contributors"]:
        resolved_contributor = (
            _prepare_existing_contributor_assignment(
                cur,
                tenant_id,
                target_work_id,
                contributor,
            )
        )
        add_work_contributor(
            cur,
            tenant_id,
            target_work_id,
            {
                "contributor_role_code":
                    resolved_contributor.get(
                        "contributor_role_code"
                    ),
                "contributors": [
                    resolved_contributor
                ],
            },
        )

    update_edition_descriptive_content(
        cur,
        tenant_id,
        target_work_id,
        target_edition_id,
        parsed["descriptive_content"],
    )
    update_edition_subjects_audience(
        cur,
        tenant_id,
        target_work_id,
        target_edition_id,
        parsed["subjects_audience"],
    )
    _normalize_imported_audience_rows(
        cur,
        tenant_id,
        target_edition_id,
    )
    update_edition_product_details(
        cur,
        tenant_id,
        target_work_id,
        target_edition_id,
        parsed["product_details"],
    )
    update_edition_publishing_dates(
        cur,
        tenant_id,
        target_work_id,
        target_edition_id,
        {
            "publishing_dates":
                parsed["publishing"][
                    "publishing_dates"
                ]
        },
    )
    update_edition_rights_restrictions(
        cur,
        tenant_id,
        target_work_id,
        target_edition_id,
        parsed["rights"],
    )
    update_edition_related_products(
        cur,
        tenant_id,
        target_work_id,
        target_edition_id,
        parsed["related"],
    )
    update_edition_supply_pricing(
        cur,
        tenant_id,
        target_work_id,
        target_edition_id,
        parsed["supply"],
    )

    cur.execute(
        """
        UPDATE editions
        SET market_publishing_status = %s,
            market_date_role = %s,
            market_date_format = %s,
            market_date_text = %s,
            promotion_contact = %s,
            promotion_contact_text_format = %s,
            initial_print_run =
                COALESCE(
                    NULLIF(%s, ''),
                    initial_print_run
                ),
            promotion_campaign =
                COALESCE(
                    NULLIF(%s, ''),
                    promotion_campaign
                ),
            updated_at = now()
        WHERE tenant_id = %s
          AND work_id = %s
          AND id = %s
        """,
        (
            _clean(
                parsed["supply"].get(
                    "market_publishing_status"
                )
            ),
            _clean(
                parsed["supply"].get(
                    "market_date_role"
                )
            ),
            _clean(
                parsed["supply"].get(
                    "market_date_format"
                )
            ),
            _clean(
                parsed["supply"].get(
                    "market_date_text"
                )
            ),
            _clean(
                parsed["supply"].get(
                    "promotion_contact"
                )
            ),
            _clean(
                parsed["supply"].get(
                    "promotion_contact_text_format"
                )
            ),
            _clean(
                parsed["supply"].get(
                    "initial_print_run"
                )
            ),
            _clean(
                parsed["supply"].get(
                    "promotion_campaign"
                )
            ),
            tenant_id,
            target_work_id,
            target_edition_id,
        ),
    )

    _replace_publisher_websites(
        cur,
        tenant_id,
        target_edition_id,
        parsed["publishing"][
            "publisher_websites"
        ],
    )
    _replace_publishers(
        cur,
        tenant_id,
        target_edition_id,
        parsed["publishing"].get("publishers") or [],
    )
    _replace_imprints(
        cur,
        tenant_id,
        target_edition_id,
        parsed["publishing"].get("imprints") or [],
    )
    _replace_supporting_resources(
        cur,
        tenant_id,
        target_edition_id,
        parsed["supporting_resources"],
    )

    return {
        "work_id": target_work_id,
        "edition_id": target_edition_id,
    }


def preview_onix_file(
    raw: bytes,
    filename: str,
    tenant_slug: str,
) -> Dict[str, Any]:
    """
    Parse the entire feed and classify every Product against InkSuite.

    Matching order:
      1. exact ISBN -> update existing edition
      2. title + format match + blank ISBN -> update that placeholder edition
      3. title match but format missing -> create a new edition under that work
      4. no ISBN/title match -> create a new work + edition
    """
    xml_bytes = _read_upload(raw, filename or "")
    _, products = _products(xml_bytes)

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id(
                cur,
                tenant_slug,
            )
            work_candidates = _all_work_candidates(
                cur,
                tenant_id,
            )
            items = [
                _plan_product(
                    cur,
                    tenant_id,
                    product,
                    work_candidates,
                )
                for product in products
            ]

    counts = {
        "update_existing_edition": sum(
            1 for i in items
            if i["suggested_action"]
            == "update_existing_edition"
        ),
        "create_new_edition": sum(
            1 for i in items
            if i["suggested_action"]
            == "create_new_edition"
        ),
        "create_new_work": sum(
            1 for i in items
            if i["suggested_action"]
            == "create_new_work"
        ),
    }

    return {
        "ok": True,
        "filename": filename,
        "product_count": len(items),
        "items": items,
        "counts": counts,
    }


def apply_onix_import_batch(
    *,
    raw: bytes,
    filename: str,
    tenant_slug: str,
    plans: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Execute user-confirmed actions for every selected Product in one import.

    Supported actions:
      update_existing_edition
      create_new_edition
      create_new_work
      skip
    """
    xml_bytes = _read_upload(raw, filename or "")
    _, products = _products(xml_bytes)

    product_map: Dict[str, ET.Element] = {}
    for product in products:
        summary = _summary(product)
        key = (
            _clean(summary.get("record_reference"))
            or _clean(summary.get("isbn13"))
        )
        if key:
            product_map[key] = product

    results: List[Dict[str, Any]] = []

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id(
                cur,
                tenant_slug,
            )

            for plan in plans or []:
                action = _clean(
                    plan.get("action")
                )
                source_key = _clean(
                    plan.get("record_reference")
                    or plan.get("isbn13")
                )

                if action == "skip":
                    results.append({
                        "source_key": source_key,
                        "action": "skip",
                        "ok": True,
                    })
                    continue

                product = product_map.get(source_key)
                if product is None:
                    raise ValueError(
                        f"Source Product {source_key} was not found in the uploaded feed."
                    )

                parsed = _parse_product(product)
                preserve = bool(
                    plan.get(
                        "preserve_existing_title",
                        action
                        != "create_new_work",
                    )
                )

                if action == "update_existing_edition":
                    target_work_id = _clean(
                        plan.get("target_work_id")
                    )
                    target_edition_id = _clean(
                        plan.get("target_edition_id")
                    )

                    # Protect against accidental wrong-target updates:
                    # if the source ISBN exists, the confirmed target must be
                    # the edition that owns that ISBN.
                    exact = _existing_product_by_isbn(
                        cur,
                        tenant_id,
                        parsed["summary"].get(
                            "isbn13"
                        ) or "",
                    )
                    if exact:
                        exact_edition_id = _clean(
                            exact.get("edition_id")
                        )
                        if (
                            target_edition_id
                            and target_edition_id
                            != exact_edition_id
                        ):
                            raise ValueError(
                                f"ISBN {parsed['summary'].get('isbn13')} already belongs to a different InkSuite edition."
                            )
                        target_work_id = _clean(
                            exact.get("work_id")
                        )
                        target_edition_id = (
                            exact_edition_id
                        )

                    if (
                        not target_work_id
                        or not target_edition_id
                    ):
                        raise ValueError(
                            "Existing-edition update requires a target work and edition."
                        )

                elif action == "create_new_edition":
                    target_work_id = _clean(
                        plan.get("target_work_id")
                    )
                    if not target_work_id:
                        raise ValueError(
                            "Creating a new format requires an existing target work."
                        )

                    if _existing_product_by_isbn(
                        cur,
                        tenant_id,
                        parsed["summary"].get(
                            "isbn13"
                        ) or "",
                    ):
                        raise ValueError(
                            f"ISBN {parsed['summary'].get('isbn13')} already exists; it cannot be created as another edition."
                        )

                    target_edition_id = (
                        _create_edition_for_work(
                            cur,
                            tenant_id,
                            target_work_id,
                            parsed,
                        )
                    )

                elif action == "create_new_work":
                    if _existing_product_by_isbn(
                        cur,
                        tenant_id,
                        parsed["summary"].get(
                            "isbn13"
                        ) or "",
                    ):
                        raise ValueError(
                            f"ISBN {parsed['summary'].get('isbn13')} already exists; a new title cannot be created with the same ISBN."
                        )

                    # Re-check title matching inside the transaction. A previous
                    # Product from this same ONIX file may just have created the
                    # work. In that case this Product is another format, not a
                    # second copy of the title.
                    current_candidates = (
                        _all_work_candidates(
                            cur,
                            tenant_id,
                        )
                    )
                    batch_work_match = _best_work_match(
                        product,
                        current_candidates,
                    )

                    if batch_work_match:
                        target_work_id = _clean(
                            batch_work_match.get(
                                "work_id"
                            )
                        )

                        blank_edition = (
                            _blank_isbn_edition_match(
                                cur,
                                tenant_id,
                                target_work_id,
                                product,
                            )
                        )

                        if blank_edition:
                            target_edition_id = _clean(
                                blank_edition.get(
                                    "edition_id"
                                )
                            )
                        else:
                            target_edition_id = (
                                _create_edition_for_work(
                                    cur,
                                    tenant_id,
                                    target_work_id,
                                    parsed,
                                )
                            )

                        # Preserve the work title we just matched.
                        preserve = True
                    else:
                        (
                            target_work_id,
                            target_edition_id,
                        ) = _create_work_and_edition(
                            cur,
                            tenant_id,
                            parsed,
                        )
                        preserve = False

                else:
                    raise ValueError(
                        f"Unsupported import action: {action}"
                    )

                applied = _apply_parsed_to_target(
                    cur,
                    tenant_id,
                    parsed,
                    target_work_id,
                    target_edition_id,
                    preserve,
                )

                results.append({
                    "source_key": source_key,
                    "isbn13":
                        parsed["summary"].get(
                            "isbn13"
                        ),
                    "title":
                        parsed["summary"].get(
                            "title"
                        ),
                    "action": action,
                    "ok": True,
                    **applied,
                })

        conn.commit()

    return {
        "ok": True,
        "processed": len(results),
        "results": results,
    }


def apply_onix_import(
    *,
    raw: bytes,
    filename: str,
    tenant_slug: str,
    source_record_reference: str,
    source_isbn13: str,
    target_work_id: str,
    target_edition_id: str,
    preserve_existing_title: bool = True,
) -> Dict[str, Any]:
    """
    Import one selected ONIX Product into one existing InkSuite edition.

    The import is intentionally edition-targeted: it never creates another
    format/edition. For this Penguin & Panda test, preserve_existing_title=True
    keeps InkSuite's work title/subtitle while importing the source Product's
    richer edition metadata.
    """
    xml_bytes = _read_upload(raw, filename or "")
    _, products = _products(xml_bytes)
    source = _product_by_key(
        products,
        source_record_reference,
        source_isbn13,
    )
    parsed = _parse_product(source)

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id(cur, tenant_slug)

            cur.execute(
                """
                SELECT w.title, w.subtitle, e.id
                FROM works w
                JOIN editions e
                  ON e.tenant_id = w.tenant_id
                 AND e.work_id = w.id
                WHERE w.tenant_id = %s
                  AND w.id = %s
                  AND e.id = %s
                LIMIT 1
                """,
                (tenant_id, target_work_id, target_edition_id),
            )
            target = cur.fetchone()
            if not target:
                raise ValueError("Target work / edition was not found.")

            update_edition_product_identity(
                cur,
                tenant_id,
                target_work_id,
                target_edition_id,
                parsed["identity"],
            )

            if preserve_existing_title:
                title = _clean(target.get("title"))
                subtitle = _clean(target.get("subtitle"))
            else:
                title = (
                    parsed["source_title"]
                    or _clean(target.get("title"))
                )
                subtitle = parsed["source_subtitle"]

            update_work_titles_collections(
                cur,
                tenant_id,
                target_work_id,
                {
                    "title": title,
                    "subtitle": subtitle,
                    "collections": parsed["collections"],
                },
            )

            # Canonical ONIX TitleElement wins over any display/helper transformation.
            _upsert_primary_title_element_exact(
                cur,
                tenant_id,
                target_work_id,
                parsed.get("primary_title") or {},
            )
            _sync_work_subtitle_from_primary_onix_title(
                cur,
                tenant_id,
                target_work_id,
                parsed.get("primary_title") or {},
            )

            for contributor in parsed["contributors"]:
                add_work_contributor(
                    cur,
                    tenant_id,
                    target_work_id,
                    {
                        "contributor_role_code":
                            contributor.get(
                                "contributor_role_code"
                            ),
                        "contributors": [contributor],
                    },
                )

            update_edition_descriptive_content(
                cur,
                tenant_id,
                target_work_id,
                target_edition_id,
                parsed["descriptive_content"],
            )

            update_edition_subjects_audience(
                cur,
                tenant_id,
                target_work_id,
                target_edition_id,
                parsed["subjects_audience"],
            )
            _normalize_imported_audience_rows(
                cur,
                tenant_id,
                target_edition_id,
            )

            update_edition_product_details(
                cur,
                tenant_id,
                target_work_id,
                target_edition_id,
                parsed["product_details"],
            )

            update_edition_publishing_dates(
                cur,
                tenant_id,
                target_work_id,
                target_edition_id,
                {
                    "publishing_dates":
                        parsed["publishing"][
                            "publishing_dates"
                        ]
                },
            )

            update_edition_rights_restrictions(
                cur,
                tenant_id,
                target_work_id,
                target_edition_id,
                parsed["rights"],
            )

            update_edition_related_products(
                cur,
                tenant_id,
                target_work_id,
                target_edition_id,
                parsed["related"],
            )

            update_edition_supply_pricing(
                cur,
                tenant_id,
                target_work_id,
                target_edition_id,
                parsed["supply"],
            )

            # MarketPublishingDetail fields live on editions in the expanded
            # metadata schema. Update only columns introduced during that work.
            cur.execute(
                """
                UPDATE editions
                SET market_publishing_status = %s,
                    market_date_role = %s,
                    market_date_format = %s,
                    market_date_text = %s,
                    promotion_contact = %s,
                    promotion_contact_text_format = %s,
                    initial_print_run =
                        COALESCE(
                            NULLIF(%s, ''),
                            initial_print_run
                        ),
                    promotion_campaign =
                        COALESCE(
                            NULLIF(%s, ''),
                            promotion_campaign
                        ),
                    updated_at = now()
                WHERE tenant_id = %s
                  AND work_id = %s
                  AND id = %s
                """,
                (
                    _clean(
                        parsed["supply"].get(
                            "market_publishing_status"
                        )
                    ),
                    _clean(
                        parsed["supply"].get(
                            "market_date_role"
                        )
                    ),
                    _clean(
                        parsed["supply"].get(
                            "market_date_format"
                        )
                    ),
                    _clean(
                        parsed["supply"].get(
                            "market_date_text"
                        )
                    ),
                    _clean(
                        parsed["supply"].get(
                            "promotion_contact"
                        )
                    ),
                    _clean(
                        parsed["supply"].get(
                            "promotion_contact_text_format"
                        )
                    ),
                    _clean(
                        parsed["supply"].get(
                            "initial_print_run"
                        )
                    ),
                    _clean(
                        parsed["supply"].get(
                            "promotion_campaign"
                        )
                    ),
                    tenant_id,
                    target_work_id,
                    target_edition_id,
                ),
            )

            _replace_publisher_websites(
                cur,
                tenant_id,
                target_edition_id,
                parsed["publishing"][
                    "publisher_websites"
                ],
            )
            _replace_publishers(
                cur,
                tenant_id,
                target_edition_id,
                parsed["publishing"].get("publishers") or [],
            )

            _replace_supporting_resources(
                cur,
                tenant_id,
                target_edition_id,
                parsed["supporting_resources"],
            )

        conn.commit()

    return {
        "ok": True,
        "source": parsed["summary"],
        "target_work_id": target_work_id,
        "target_edition_id": target_edition_id,
        "preserved_title": preserve_existing_title,
        "imported": {
            "contributors": len(parsed["contributors"]),
            "collections": len(parsed["collections"]),
            "texts": len(
                parsed["descriptive_content"][
                    "descriptive_texts"
                ]
            ),
            "subjects": len(
                parsed["subjects_audience"]["subjects"]
            ),
            "audience_ranges": len(
                parsed["subjects_audience"][
                    "audience_ranges"
                ]
            ),
            "publishing_dates": len(
                parsed["publishing"][
                    "publishing_dates"
                ]
            ),
            "related_works": len(
                parsed["related"]["related_works"]
            ),
            "related_products": len(
                parsed["related"]["related_products"]
            ),
            "supply_details": len(
                parsed["supply"]["supply_details"]
            ),
            "supporting_resources": len(
                parsed["supporting_resources"]
            ),
            "publisher_websites": len(
                parsed["publishing"][
                    "publisher_websites"
                ]
            ),
        },
    }
