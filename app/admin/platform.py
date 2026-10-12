"""Platform operations with explicit tenant context and audited diagnostic support."""
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from psycopg import sql
from psycopg.errors import ForeignKeyViolation, UniqueViolation, LockNotAvailable, DeadlockDetected
from psycopg.rows import dict_row
from app.auth.dependencies import require_superadmin, get_current_user
from app.auth.service import get_user_db_record_from_claims, get_memberships_for_user
from app.core.db import db_conn

router = APIRouter(prefix="/platform-admin", tags=["Platform administration"])
MODULES = {"home", "dashboard", "book-info", "books", "project_management", "royalty", "contracts", "financials", "marketplace", "meetings", "settings", "team", "users", "platform"}

class TenantBody(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    slug: str = Field(pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$", max_length=80)

class DeleteBody(BaseModel):
    confirmation: str

class SupportBody(BaseModel):
    tenant_id: UUID
    reason: str = Field(min_length=10, max_length=500)

class VisitBody(BaseModel):
    module: str
    tenant_slug: str | None = Field(default=None,max_length=80)

def ready(cur):
    cur.execute("SELECT to_regclass('public.platform_activity') AS activity, to_regclass('public.platform_support_sessions') AS support")
    row=cur.fetchone()
    if not row['activity'] or not row['support']:
        raise HTTPException(503,'Platform administration requires migration 031_platform_admin.sql.')

def audit(cur, user, action, tenant=None, module='platform'):
    cur.execute('INSERT INTO platform_activity(actor_id,tenant_id,tenant_slug,action,module) VALUES(%s,%s,%s,%s,%s)',
                (user['id'],tenant['id'] if tenant else None,tenant['slug'] if tenant else None,action,module))

def tenant_row(cur, tenant_id, lock=False):
    cur.execute('SELECT id,name,slug,created_at FROM tenants WHERE id=%s'+(' FOR UPDATE' if lock else ''),(tenant_id,))
    row=cur.fetchone()
    if not row: raise HTTPException(404,'Tenant not found')
    return row

def record_counts(cur, tenant_id, lock=False):
    # Explicitly inspect tenant-owned data before deletion; never blindly cascade.
    cur.execute("""SELECT c.table_name,c.column_name FROM information_schema.columns c
      JOIN information_schema.tables t USING(table_schema,table_name)
      WHERE c.table_schema='public' AND c.column_name='tenant_id' AND t.table_type='BASE TABLE'
      AND c.table_name NOT IN ('memberships','platform_activity','platform_support_sessions')
      UNION
      SELECT k.table_name,k.column_name FROM information_schema.referential_constraints r
      JOIN information_schema.key_column_usage k ON k.constraint_schema=r.constraint_schema AND k.constraint_name=r.constraint_name
      JOIN information_schema.constraint_column_usage u ON u.constraint_schema=r.unique_constraint_schema AND u.constraint_name=r.unique_constraint_name
      WHERE k.table_schema='public' AND u.table_schema='public' AND u.table_name='tenants'
      AND u.column_name='id' AND k.table_name<>'memberships' ORDER BY 1,2""")
    tables=[(r['table_name'],r['column_name']) for r in cur.fetchall()]
    result=[]
    if lock:
        # Also protect legacy tables without a tenant FK against concurrent inserts.
        for table in sorted({t for t,_ in tables}):
            cur.execute(sql.SQL('LOCK TABLE {} IN SHARE ROW EXCLUSIVE MODE').format(sql.Identifier('public',table)))
    for table,column in tables:
        cur.execute(sql.SQL('SELECT count(*) AS count FROM {} WHERE {}::text=%s').format(sql.Identifier('public',table),sql.Identifier(column)),(str(tenant_id),))
        count=cur.fetchone()['count']
        if count: result.append({'table':table,'records':count})
    return result

@router.get('/tenants')
def tenants(_user: dict=Depends(require_superadmin)):
    with db_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        ready(cur)
        cur.execute("""SELECT t.id,t.name,t.slug,t.created_at,
          (SELECT count(*) FROM memberships m WHERE m.tenant_id=t.id) AS members,
          (SELECT count(DISTINCT a.actor_id) FROM platform_activity a WHERE a.tenant_id=t.id AND a.action='PAGE_VIEW' AND a.occurred_at>=now()-interval '30 days') AS active_users,
          (SELECT max(a.occurred_at) FROM platform_activity a WHERE a.tenant_id=t.id AND a.action='PAGE_VIEW') AS last_active
          FROM tenants t ORDER BY t.name,t.slug""")
        return {'tenants':cur.fetchall()}

@router.post('/tenants',status_code=201)
def create_tenant(body: TenantBody,user: dict=Depends(require_superadmin)):
    if not body.name.strip() or body.slug=='inksuite': raise HTTPException(400,'Enter a name and a slug other than the reserved name inksuite.')
    try:
        with db_conn() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
            ready(cur)
            cur.execute('INSERT INTO tenants(name,slug,s3_prefix) VALUES(%s,%s,%s) RETURNING id,name,slug,created_at',(body.name.strip(),body.slug,f'tenants/{body.slug}'))
            tenant=cur.fetchone()
            cur.execute("INSERT INTO memberships(tenant_id,user_id,role) VALUES(%s,%s,'tenant_admin')",(tenant['id'],user['id']))
            audit(cur,user,'TENANT_CREATED',tenant)
            return {'tenant':tenant}
    except UniqueViolation: raise HTTPException(409,'This tenant slug already exists.')

@router.get('/tenants/{tenant_id}/deletion-check')
def deletion_check(tenant_id: UUID,_user: dict=Depends(require_superadmin)):
    with db_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        ready(cur)
        tenant=tenant_row(cur,tenant_id)
        blockers=record_counts(cur,tenant_id)
        return {'tenant':tenant,'can_delete':not blockers and tenant['slug']!='inksuite','blockers':blockers}

@router.delete('/tenants/{tenant_id}')
def delete_tenant(tenant_id: UUID,body: DeleteBody,user: dict=Depends(require_superadmin)):
    try:
        with db_conn() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
            ready(cur)
            tenant=tenant_row(cur,tenant_id,True)
            if tenant['slug']=='inksuite' or body.confirmation!=tenant['slug']: raise HTTPException(400,'Type the exact tenant slug. The platform tenant cannot be deleted.')
            cur.execute("SET LOCAL lock_timeout='3s'")
            if record_counts(cur,tenant_id,True): raise HTTPException(409,'This tenant contains business data and cannot be deleted here.')
            audit(cur,user,'TENANT_DELETED',tenant)
            cur.execute('UPDATE platform_support_sessions SET ended_at=now() WHERE tenant_id=%s AND ended_at IS NULL',(tenant_id,))
            cur.execute('DELETE FROM memberships WHERE tenant_id=%s',(tenant_id,))
            cur.execute('DELETE FROM tenants WHERE id=%s',(tenant_id,))
            return {'ok':True}
    except ForeignKeyViolation: raise HTTPException(409,'Other records reference this tenant; nothing was deleted.')
    except (LockNotAvailable,DeadlockDetected): raise HTTPException(409,'The tenant is currently in use. Nothing was deleted; try again later.')

@router.post('/activity',status_code=202)
def visit(body: VisitBody,claims: dict=Depends(get_current_user)):
    if body.module not in MODULES: raise HTTPException(400,'Unknown module')
    user=get_user_db_record_from_claims(claims)
    if not user: raise HTTPException(401,'Not authenticated')
    tenant=None
    if body.tenant_slug:
        match=next((m for m in get_memberships_for_user(user['id']) if m['tenant_slug']==body.tenant_slug),None)
        if not match: raise HTTPException(403,'Tenant membership required')
        tenant={'id':match['tenant_id'],'slug':match['tenant_slug']}
    elif str(user.get('platform_role','')).lower()!='superadmin': raise HTTPException(403,'Select a tenant')
    with db_conn() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
        ready(cur)
        cur.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))',(str(user['id']),))
        cur.execute("""SELECT 1 FROM platform_activity WHERE actor_id=%s AND tenant_id IS NOT DISTINCT FROM %s
          AND module=%s AND action='PAGE_VIEW' AND occurred_at>now()-interval '30 seconds' LIMIT 1""",
          (user['id'],tenant['id'] if tenant else None,body.module))
        if not cur.fetchone(): audit(cur,user,'PAGE_VIEW',tenant,body.module)
    return {'ok':True}

@router.get('/activity')
def activity(days: int=Query(30,ge=1,le=90),tenant_id: UUID|None=None,offset: int=Query(0,ge=0,le=100000),_user: dict=Depends(require_superadmin)):
    with db_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        ready(cur)
        where="a.occurred_at>=now()-(%s * interval '1 day') AND (%s::uuid IS NULL OR a.tenant_id=%s::uuid)"
        params=(days,tenant_id,tenant_id)
        cur.execute('SELECT count(*) AS total FROM platform_activity a WHERE '+where,params)
        total=cur.fetchone()['total']
        cur.execute('SELECT a.*,u.email,u.full_name FROM platform_activity a LEFT JOIN users u ON u.id=a.actor_id WHERE '+where+' ORDER BY a.occurred_at DESC,a.id DESC LIMIT 50 OFFSET %s',params+(offset,))
        events=cur.fetchall()
        cur.execute("""SELECT (a.occurred_at AT TIME ZONE 'UTC')::date AS day,
          count(*) FILTER(WHERE a.action='PAGE_VIEW') AS page_views,
          count(DISTINCT a.actor_id) FILTER(WHERE a.action='PAGE_VIEW') AS active_users,
          count(DISTINCT a.tenant_id) FILTER(WHERE a.action='PAGE_VIEW') AS active_tenants
          FROM platform_activity a WHERE """+where+' GROUP BY 1 ORDER BY 1 DESC',params)
        daily=cur.fetchall()
        cur.execute('SELECT min(occurred_at) AS tracking_since FROM platform_activity')
        return {'events':events,'daily':daily,'total':total,'tracking_since':cur.fetchone()['tracking_since'],'timezone':'UTC'}

@router.get('/support-sessions')
def sessions(user: dict=Depends(require_superadmin)):
    with db_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        ready(cur)
        cur.execute('SELECT s.*,u.email AS admin_email,(s.ended_at IS NULL AND s.expires_at>now()) AS active FROM platform_support_sessions s LEFT JOIN users u ON u.id=s.actor_id ORDER BY s.started_at DESC LIMIT 100')
        return {'sessions':cur.fetchall(),'actor_id':user['id']}

@router.post('/support-sessions',status_code=201)
def start(body: SupportBody,user: dict=Depends(require_superadmin)):
    if len(body.reason.strip())<10: raise HTTPException(400,'Describe the support reason in at least 10 characters.')
    with db_conn() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
        ready(cur)
        tenant=tenant_row(cur,body.tenant_id)
        cur.execute('INSERT INTO platform_support_sessions(actor_id,tenant_id,tenant_slug,reason) VALUES(%s,%s,%s,%s) RETURNING *',(user['id'],tenant['id'],tenant['slug'],body.reason.strip()))
        session=cur.fetchone()
        audit(cur,user,'SUPPORT_STARTED',tenant)
        return {'session':session}

@router.get('/support-sessions/{session_id}')
def support(session_id: UUID,user: dict=Depends(require_superadmin)):
    with db_conn() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
        ready(cur)
        cur.execute('SELECT * FROM platform_support_sessions WHERE id=%s AND actor_id=%s AND ended_at IS NULL AND expires_at>now()',(session_id,user['id']))
        session=cur.fetchone()
        if not session: raise HTTPException(404,'Support session is expired, ended, or belongs to another administrator.')
        tenant=tenant_row(cur,session['tenant_id'])
        cur.execute('SELECT u.email,u.full_name,m.role,m.module_permissions FROM memberships m JOIN users u ON u.id=m.user_id WHERE m.tenant_id=%s ORDER BY u.email',(tenant['id'],))
        members=cur.fetchall()
        audit(cur,user,'SUPPORT_VIEWED',tenant)
        return {'session':session,'tenant':tenant,'members':members,'record_counts':record_counts(cur,tenant['id'])}

@router.post('/support-sessions/{session_id}/end')
def end(session_id: UUID,user: dict=Depends(require_superadmin)):
    with db_conn() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
        ready(cur)
        cur.execute('UPDATE platform_support_sessions SET ended_at=now() WHERE id=%s AND actor_id=%s AND ended_at IS NULL RETURNING tenant_id,tenant_slug',(session_id,user['id']))
        row=cur.fetchone()
        if not row: raise HTTPException(404,'Active support session not found')
        audit(cur,user,'SUPPORT_ENDED',{'id':row['tenant_id'],'slug':row['tenant_slug']})
        return {'ok':True}
