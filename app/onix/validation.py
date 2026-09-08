# app/onix/validation.py
# Export readiness validation for the canonical ONIX payload.
#
# Validation reports missing metadata; it never tells the exporter to invent
# product values or apply product-metadata defaults.

from __future__ import annotations

from typing import Any, Dict, List


def _s(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def validate_product(payload: Dict[str, Any]) -> Dict[str, Any]:
    errors: List[str] = []
    warnings: List[str] = []
    checks: Dict[str, str] = {}

    descriptive = payload.get("descriptive_detail") or {}
    publishing = payload.get("publishing_detail") or {}
    collateral = payload.get("collateral_detail") or {}

    record_reference = _s(payload.get("record_reference"))
    if not record_reference:
        errors.append("Missing RecordReference")
    checks["record_reference"] = "ok" if record_reference else "missing"

    notification_type = _s(payload.get("notification_type"))
    if not notification_type:
        errors.append("Missing NotificationType")
    checks["notification_type"] = "ok" if notification_type else "missing"

    identifiers = payload.get("identifiers") or []
    isbn13 = next(
        (
            _s(row.get("id_value"))
            for row in identifiers
            if _s(row.get("id_type")) == "15" and _s(row.get("id_value"))
        ),
        "",
    )
    if not isbn13:
        errors.append("Missing ISBN-13 ProductIdentifier")
    checks["isbn13"] = "ok" if isbn13 else "missing"

    product_form = _s(descriptive.get("product_form"))
    if not product_form:
        errors.append("Missing ProductForm")
    checks["product_form"] = "ok" if product_form else "missing"

    titles = descriptive.get("titles") or []
    has_title = any(
        _s(row.get("title_without_prefix") or row.get("title_text"))
        for row in titles
    )
    if not has_title:
        errors.append("Missing TitleDetail / TitleElement title")
    checks["title"] = "ok" if has_title else "missing"

    contributors = descriptive.get("contributors") or []
    if not contributors:
        warnings.append("No Contributor composite")
    checks["contributors"] = "ok" if contributors else "warning"

    languages = descriptive.get("languages") or []
    if not languages:
        warnings.append("No Language composite")
    checks["languages"] = "ok" if languages else "warning"

    publishing_status = _s(publishing.get("publishing_status"))
    if not publishing_status:
        warnings.append("Missing PublishingStatus")
    checks["publishing_status"] = "ok" if publishing_status else "warning"

    publishing_dates = publishing.get("publishing_dates") or []
    if not publishing_dates:
        warnings.append("No PublishingDate composite")
    checks["publishing_dates"] = "ok" if publishing_dates else "warning"

    supply = payload.get("product_supply") or []
    if not supply:
        warnings.append("No SupplyDetail")
        checks["supply"] = "warning"
    else:
        checks["supply"] = "ok"
        if not any(row.get("prices") for row in supply):
            warnings.append("No Price composite")
            checks["prices"] = "warning"
        else:
            checks["prices"] = "ok"

    if not any(collateral.get(k) for k in ("texts", "supporting_resources", "cited_content")):
        warnings.append("No CollateralDetail content")
        checks["collateral"] = "warning"
    else:
        checks["collateral"] = "ok"

    status = "blocked" if errors else ("warning" if warnings else "ready")
    return {
        "status": status,
        "errors": errors,
        "warnings": warnings,
        "checks": checks,
    }
