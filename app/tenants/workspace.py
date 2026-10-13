"""Request-local publisher boundary for legacy APIs.

No environment/default tenant and no implicit platform-admin bypass. Background
workers must pass their own tenant explicitly; they do not inherit HTTP context.
"""
from contextvars import ContextVar
from dataclasses import dataclass
import re
from pathlib import Path

from fastapi import HTTPException
from app.auth.service import get_user_db_record_from_claims, get_memberships_for_user


@dataclass(frozen=True)
class Workspace:
    tenant_id: str
    slug: str
    role: str
    user_id: str


current_workspace: ContextVar[Workspace | None] = ContextVar('publisher_workspace', default=None)
SLUG = re.compile(r'^[a-z0-9]+(?:-[a-z0-9]+)*$')
PREFIXES = (
    '/api/catalog', '/api/books', '/api/contracts', '/api/uploads',
    '/api/royalty', '/api/financials', '/api/salesdata', '/api/ingest',
    '/api/banking', '/api/payment-instructions', '/api/project-management',
    '/api/tenants', '/api/onix',
)


def guarded_path(path):
    return any(path == prefix or path.startswith(prefix + '/') for prefix in PREFIXES)


def workspace():
    value = current_workspace.get()
    if value is None:
        raise HTTPException(403, 'Publisher workspace is required.')
    return value


def tenant_slug():
    return workspace().slug


def tenant_id():
    return workspace().tenant_id


def storage_directory(legacy_root):
    """Keep existing Marble Press files in place; isolate every other publisher.

    Never infer a workspace from an environment variable or create directories
    at import time. Resolve the request context for each file operation.
    """
    selected = workspace()
    if not SLUG.fullmatch(selected.slug):
        raise HTTPException(403, 'Invalid publisher workspace.')
    root = Path(legacy_root)
    return root if selected.slug == 'marble-press' else root / 'tenants' / selected.slug


def select_workspace(request, claims):
    if not claims:
        raise HTTPException(401, 'Not authenticated')
    values = [request.cookies.get('workspace_company'), request.headers.get('x-workspace'),
              request.headers.get('x-tenant')]
    values.extend(request.query_params.getlist('tenant_slug'))
    if request.url.path.startswith('/api/tenants/'):
        values.append(request.url.path.split('/')[3])
    supplied = {str(v).strip().lower() for v in values if v}
    if len(supplied) != 1:
        raise HTTPException(403, 'Select one publisher workspace; conflicting tenant context is not allowed.')
    slug = next(iter(supplied))
    if not SLUG.fullmatch(slug):
        raise HTTPException(403, 'Invalid publisher workspace.')
    user = get_user_db_record_from_claims(claims)
    if not user:
        raise HTTPException(401, 'Not authenticated')
    membership = next((m for m in get_memberships_for_user(user['id'])
                       if m.get('tenant_slug') == slug), None)
    if not membership:
        raise HTTPException(403, 'You do not have access to this publisher workspace.')
    return Workspace(str(membership['tenant_id']), slug, membership['role'], str(user['id']))


def enforce_legacy_boundary(path, selected):
    # These handlers still use process-wide files/indexes. Do not expose those
    # files to a new tenant while their storage migration is being completed.
    shared = ('/api/books', '/api/contracts', '/api/ingest', '/api/royalty/statements', '/api/royalty/render')
    if selected.slug != 'marble-press' and (
        any(path == p or path.startswith(p + '/') for p in shared)
        or path in ('/api/financials/upload', '/api/financials/uploads')
    ):
        raise HTTPException(409, 'This module is awaiting publisher-specific storage setup. No other publisher data has been accessed.')
