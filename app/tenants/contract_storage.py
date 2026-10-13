"""Resolve contract locations from authenticated request/editor context."""
from pathlib import Path
from fastapi import HTTPException
from app.tenants.workspace import storage_directory, tenant_slug


def template_prefix():
    return f'tenants/{tenant_slug()}/data/Templates/'


def draft_prefix():
    return f'tenants/{tenant_slug()}/data/TempDraftContracts/'


def template_directory():
    return storage_directory(Path.home() / 'Documents' / 'marble_app' / 'data') / 'Templates'


def draft_directory():
    return storage_directory(Path.home() / 'Documents' / 'marble_app' / 'data') / 'TempDraftContracts'


def checked_key(key, prefix):
    value = str(key or '')
    if not value.startswith(prefix) or '\\' in value or any(p in ('.', '..') for p in value.split('/')):
        raise HTTPException(404, 'Document not found in this publisher workspace.')
    return value


def checked_local_path(path, root):
    target, parent = Path(path).resolve(), Path(root).resolve()
    if target == parent or not target.is_relative_to(parent):
        raise HTTPException(404, 'Document not found in this publisher workspace.')
    return target


def filename(value):
    value = str(value or '')
    if not value or value in ('.', '..') or any(c in value for c in '/\\:*?[]'):
        raise HTTPException(400, 'Invalid document filename.')
    return value
