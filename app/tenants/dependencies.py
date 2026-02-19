# require_tenant_access(), require_role(roles=[...])
from __future__ import annotations

from typing import Annotated, List

from fastapi import Depends, HTTPException, Request, status

from app.auth.dependencies import get_current_user
from app.tenants.resolver import resolve_tenant


async def get_tenant_slug_from_path(request: Request) -> str:
    """Extract tenant_slug from path (e.g. /api/tenants/{tenant_slug}/books)."""
    return request.path_params.get("tenant_slug", "")


async def require_tenant_access(
    tenant_slug: Annotated[str, Depends(get_tenant_slug_from_path)],
    user: Annotated[dict, Depends(get_current_user)],
) -> dict:
    """Resolve tenant and ensure user has membership. Placeholder: no DB yet."""
    tenant = resolve_tenant(tenant_slug)
    if not tenant:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tenant not found")
    return tenant


def require_role(roles: List[str]):
    """Dependency factory: user's membership role must be in roles. Placeholder."""

    async def _require_role(
        user: Annotated[dict, Depends(get_current_user)],
        tenant: Annotated[dict, Depends(require_tenant_access)],
    ) -> dict:
        return tenant

    return _require_role
