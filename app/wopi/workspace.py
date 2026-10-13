"""Restore the publisher scope carried by a signed, document-specific token."""
from fastapi import HTTPException, Request
from app.tenants.workspace import Workspace, SLUG, current_workspace
from app.wopi.tokens import verify_wopi_token


def token_workspace(payload, *, file_id, file_kind, method):
    slug = payload.get('tenant_slug', '')
    if not isinstance(slug, str) or not SLUG.fullmatch(slug) or not payload.get('tenant_id'):
        raise HTTPException(401, 'Reopen the document to refresh its editor session.')
    if payload.get('file_id') != file_id or payload.get('file_kind') != file_kind:
        raise HTTPException(401, 'Invalid document editor token.')
    if payload.get('read_only') is not False and method not in ('GET', 'HEAD'):
        raise HTTPException(403, 'This document invitation is read-only.')
    return Workspace(str(payload['tenant_id']), slug, 'document_editor', str(payload.get('user_id', '')))


async def editor_workspace(request: Request):
    from app.core.config import get_settings
    token = request.query_params.get('access_token') or request.headers.get('X-WOPI-AccessToken')
    if not token:
        authorization = request.headers.get('Authorization', '')
        token = authorization[7:].strip() if authorization.startswith('Bearer ') else ''
    payload = verify_wopi_token(token, get_settings().wopi_access_token_secret)
    kind = 'template' if '/wopi/templates/' in request.url.path else 'draft'
    selected = token_workspace(payload, file_id=request.path_params.get('tid') or request.path_params.get('file_id'), file_kind=kind, method=request.method)
    marker = current_workspace.set(selected)
    try:
        yield
    finally:
        current_workspace.reset(marker)
