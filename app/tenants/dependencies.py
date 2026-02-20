# app/tenants/dependencies.py
from __future__ import annotations

from typing import Annotated, List

from fastapi import Depends, HTTPException, Request, status

from app.auth.dependencies import get_current_user
from app.tenants.resolver import resolve_tenant

# Reuse your auth/service helpers (these are sync; ok to call inside async deps)
from app.auth.service import get_user_db_record_from_claims, is_superadmin


async def get_tenant_slug_from_path(request: Request) -> str:
    """Extract tenant_slug from path (e.g. /api/tenants/{tenant_slug}/...)."""
    return (request.path_params.get("tenant_slug") or "").strip()


async def require_tenant_access(
    tenant_slug: Annotated[str, Depends(get_tenant_slug_from_path)],
    user_claims: Annotated[dict, Depends(get_current_user)],
) -> dict:
    """
    Resolve tenant and ensure user has membership (or is superadmin).

    Returns a context dict:
      {
        "tenant": {...},
        "user": {...},
        "membership_role": "tenant_admin" | "tenant_editor" | "superadmin"
      }
    """
    tenant = resolve_tenant(tenant_slug)
    if not tenant:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tenant not found")

    user = get_user_db_record_from_claims(user_claims)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")

    if is_superadmin(user.get("platform_role")):
        return {"tenant": tenant, "user": user, "membership_role": "superadmin"}

    # membership check
    from app.core.db import db_conn  # local import avoids circulars during startup

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT role
            FROM memberships
            WHERE tenant_id = %s AND user_id = %s
            """,
            (tenant["id"], user["id"]),
        )
        row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")

    return {"tenant": tenant, "user": user, "membership_role": row[0]}


def require_role(roles: List[str]):
    """
    Dependency factory: membership_role must be in roles (or superadmin).
    """

    allowed = {r.strip().lower() for r in (roles or [])}

    async def _require_role(
        ctx: Annotated[dict, Depends(require_tenant_access)],
    ) -> dict:
        role = (ctx.get("membership_role") or "").strip().lower()
        if role == "superadmin":
            return ctx
        if role not in allowed:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")
        return ctx

    return _require_role