# app/onix/validation.py
# Export readiness validation for the canonical ONIX payload.
#
# Validation reports missing metadata; it never tells the exporter to invent
# product values or apply product-metadata defaults.

from __future__ import annotations

from typing import Any, Dict, List
from decimal import Decimal, InvalidOperation


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
    if isbn13:
        normalized = isbn13.replace("-", "").replace(" ", "")
        valid_isbn = len(normalized) == 13 and normalized.isdigit() and sum(int(d)*(1 if i%2==0 else 3) for i,d in enumerate(normalized))%10==0
        if not valid_isbn: errors.append("Invalid ISBN-13 checksum or length")
    checks["isbn13"] = "ok" if isbn13 and not any(e.startswith("Invalid ISBN") for e in errors) else "missing"

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

    for index, contributor in enumerate(contributors, start=1):
        if not _s(contributor.get("role")):
            errors.append(f"Contributor {index}: missing ContributorRole; source evidence required")
            checks["contributors"] = "missing"

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
        def has_positive_price(row):
            for price in row.get("prices") or []:
                try:
                    amount = Decimal(str(price.get("price_amount")))
                    if amount.is_finite() and amount > 0: return True
                except (InvalidOperation, TypeError): pass
            return False
        if not any(has_positive_price(row) for row in supply):
            warnings.append("No confirmed positive price; zero placeholders export as price to be announced")
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
