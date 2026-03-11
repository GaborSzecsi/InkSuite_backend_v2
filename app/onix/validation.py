# app/onix/validation.py
# Export readiness: ready / warning / blocked per product.
from __future__ import annotations

from typing import Any, Dict, List


def validate_product(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Classify product as ready, warning, or blocked. Return status, errors, warnings, checks."""
    errors: List[str] = []
    warnings: List[str] = []
    checks: Dict[str, str] = {}

    title = (payload.get("title") or "").strip()
    if not title:
        errors.append("Missing title")
    checks["title"] = "ok" if title else "missing"

    isbns = [i for i in (payload.get("identifiers") or []) if (i.get("id_type") == "15" or "ISBN-13" in (i.get("id_type_name") or ""))]
    isbn13 = (isbns[0].get("id_value") or "").strip() if isbns else ""
    if not isbn13:
        errors.append("Missing ISBN-13")
    checks["isbn13"] = "ok" if isbn13 else "missing"

    publisher = (payload.get("publisher_name") or "").strip()
    if not publisher:
        warnings.append("Missing publisher/imprint")
    checks["publisher"] = "ok" if publisher else "missing"

    contributors = payload.get("contributors") or []
    primary = any(
        (c.get("role") or "").upper() in ("A01", "AUTHOR") or "author" in (c.get("role") or "").lower()
        for c in contributors
    )
    if not primary and not contributors:
        warnings.append("No primary contributor (author)")
    checks["contributor"] = "ok" if (primary or contributors) else "warning"

    product_form = (payload.get("product_form") or "").strip()
    if not product_form:
        warnings.append("Product form not set (defaulting to BC)")
    checks["product_form"] = "ok" if product_form else "default"

    pub_date = (payload.get("publication_date") or "").strip()
    if not pub_date:
        warnings.append("Publication date missing")
    checks["publication_date"] = "ok" if pub_date else "missing"

    lang = (payload.get("language") or "").strip()
    if not lang:
        warnings.append("Language not set")
    checks["language"] = "ok" if lang else "missing"

    supply = payload.get("supply_details") or []
    has_price = any(
        (s.get("prices") or [])
        for s in supply
    )
    if not supply:
        warnings.append("No supply details")
    elif not has_price:
        warnings.append("No price information")
    checks["supply_price"] = "ok" if (supply and has_price) else "warning"

    texts = payload.get("texts") or []
    has_desc = any(
        (t.get("text_type") or "").lower() in ("main description", "description", "01")
        for t in texts
    ) or bool((payload.get("main_description") or "").strip())
    if not has_desc and not texts:
        warnings.append("No descriptive text")
    checks["description"] = "ok" if (has_desc or texts) else "warning"

    if errors:
        status = "blocked"
    elif warnings:
        status = "warning"
    else:
        status = "ready"

    return {
        "status": status,
        "errors": errors,
        "warnings": warnings,
        "checks": checks,
    }
