"""Public PDFs reuse publishing_assets and existing title/publisher storage roots."""
from .core import one, rows


def edition(cur, edition_id):
    return one(cur,"""SELECT e.id,w.id AS work_id,w.uid,w.tenant_id,t.slug,o.id AS organization_id
      FROM editions e JOIN works w ON w.id=e.work_id AND w.tenant_id=e.tenant_id
      JOIN tenants t ON t.id=w.tenant_id JOIN marketplace_organizations o ON o.tenant_id=t.id
      AND o.organization_type='publisher' WHERE e.id=%s""",(edition_id,))


def organization(cur, org_id):
    return one(cur,"SELECT o.*,t.slug AS tenant_slug FROM marketplace_organizations o JOIN tenants t ON t.id=o.tenant_id WHERE o.id=%s AND o.organization_type='publisher'",(org_id,))


def assets(cur, tenant_id, work_id=None):
    if work_id is not None:
        return rows(cur,"""SELECT id,asset_name,file_size_bytes,created_at FROM publishing_assets
          WHERE tenant_id=%s AND work_id=%s AND asset_type='marketing_material' AND is_active AND is_public
          ORDER BY created_at DESC,id""",(tenant_id,work_id))
    return rows(cur,"""SELECT id,asset_name,file_size_bytes,created_at FROM publishing_assets
      WHERE tenant_id=%s AND lower(asset_type) IN ('catalog','catalogue') AND is_active AND is_public
      ORDER BY created_at DESC,id""",(tenant_id,))


def save(cur, identity, tenant_id, work_id, kind, name, key, url, size):
    return one(cur,"""INSERT INTO publishing_assets(id,tenant_id,work_id,asset_type,asset_name,storage_key,public_url,file_format,file_size_bytes,is_public,is_active)
      VALUES(%s,%s,%s,%s,%s,%s,%s,'pdf',%s,true,true) RETURNING id,asset_name,file_size_bytes,created_at""",
      (identity,tenant_id,work_id,kind,name,key,url,size))


def public_book(cur, slug):
    return one(cur,"""SELECT w.id AS work_id,w.tenant_id FROM marketplace_books b JOIN works w ON w.id=b.work_id
      JOIN marketplace_organizations o ON o.id=b.publisher_organization_id AND o.tenant_id=w.tenant_id
      WHERE b.slug=%s AND b.marketplace_status='public' AND o.status='active' AND o.organization_type='publisher'""",(slug,))


def public_asset(cur, asset_id):
    return one(cur,"""SELECT a.*,t.slug FROM publishing_assets a JOIN tenants t ON t.id=a.tenant_id
      JOIN marketplace_organizations o ON o.tenant_id=a.tenant_id AND o.status='active' AND o.organization_type='publisher'
      WHERE a.id=%s AND a.is_active AND a.is_public AND lower(a.file_format)='pdf'
      AND (lower(a.asset_type) IN ('catalog','catalogue') OR (a.asset_type='marketing_material' AND EXISTS(
        SELECT 1 FROM marketplace_books b WHERE b.work_id=a.work_id AND b.publisher_organization_id=o.id AND b.marketplace_status='public')))""",(asset_id,))
