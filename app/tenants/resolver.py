# Resolve tenant from slug in path (DB lookup when RDS is ready).
from __future__ import annotations

from typing import Optional

# When RDS is ready: query tenants table by slug, return tenant_id and slug.
# For now: accept any slug and return a placeholder.


def resolve_tenant(slug: str) -> Optional[dict]:
    """
    Resolve tenant by slug. Return dict with id, slug, name or None.
    """
    if not slug or not slug.strip():
        return None
    slug = slug.strip().lower()
    # Placeholder: no DB yet.
    return {"id": slug, "slug": slug, "name": slug.replace("-", " ").title()}
