"""Authenticated payment API. Tenant selection is checked against membership."""
from decimal import Decimal
from typing import Literal
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pydantic import BaseModel, ConfigDict, Field
from app.auth.dependencies import get_current_user
from app.core.db import db_conn
from services.financial_payments import service as svc
from services.financial_payments.bank_connectors.registry import connector

router=APIRouter(prefix='/financials/payments',tags=['Financial payments'])


class Strict(BaseModel):
    model_config=ConfigDict(extra='forbid')


class Selection(Strict):
    statement_id: UUID
    amount: Decimal=Field(gt=0,max_digits=14,decimal_places=2,allow_inf_nan=False)


class Batch(Strict):
    funding_account_id: UUID
    idempotency_key: UUID
    items: list[Selection]=Field(min_length=1,max_length=500)


class Connection(Strict):
    display_name: str=Field(default='Mock Bank (simulation)',min_length=1,max_length=100)
    opening_balance: Decimal=Field(ge=0,max_digits=20,decimal_places=2,allow_inf_nan=False)
    idempotency_key: UUID


class Verify(Strict):
    statement_id: UUID
    party_id: UUID


class Decision(Strict):
    comment: str=Field(default='',max_length=1000)


class SimulatedEvent(Strict):
    status: Literal['processing','settled','failed','returned']
    event_id: UUID


class Policy(Strict):
    operating_reserve: Decimal=Field(ge=0,max_digits=20,decimal_places=2,allow_inf_nan=False)
    dual_approval_required: bool
    approval_threshold: Decimal=Field(ge=0,max_digits=20,decimal_places=2,allow_inf_nan=False)
    balance_max_age_minutes: int=Field(ge=1,le=1440)


def authorized(tenant_slug: str=Query(...,min_length=1,max_length=100),claims=Depends(get_current_user)):
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        cur.execute('''SELECT u.id AS user_id,t.id AS tenant_id,t.slug,m.role,m.module_permissions
            FROM public.users u JOIN public.memberships m ON m.user_id=u.id
            JOIN public.tenants t ON t.id=m.tenant_id WHERE u.cognito_sub=%s AND t.slug=%s''',(claims.get('sub'),tenant_slug))
        row=cur.fetchone()
    if not row or row['role'] in ('reader','librarian') or not (
        row['role']=='tenant_admin' or (row['module_permissions'] or {}).get('financials') is True):
        raise HTTPException(403,'Payments require publisher Financials access.')
    return row


def admin(user):
    if user['role']!='tenant_admin': raise HTTPException(403,'A tenant administrator must perform this action.')


def reply(data):
    return JSONResponse(svc.serial(data),headers={'Cache-Control':'private, no-store','Pragma':'no-cache'})


def mutate(user,fn):
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
        svc.lock(cur,user['tenant_id'])
        # Share the royalty engine's account locks to serialize against manual
        # payments and finalization, including obligations spanning periods.
        cur.execute('''SELECT DISTINCT work_id,party FROM public.royalty_statements WHERE tenant_id=%s
            ORDER BY work_id,party''',(user['tenant_id'],))
        from services.royalty_accounts import lock_account
        for row in cur.fetchall(): lock_account(cur,str(user['tenant_id']),str(row['work_id']),row['party'])
        result=fn(cur)
    return reply(result)


@router.get('/payables')
def payables(user=Depends(authorized),search: str='',period: str='',arrangement: str='',eligible: bool|None=None):
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
        items=svc.payables(cur,user)
        if search: items=[r for r in items if search.lower() in (r['title']+' '+r['contributor']).lower()]
        if period: items=[r for r in items if r['period']==period]
        if arrangement: items=[r for r in items if r['payee_mode']==arrangement]
        if eligible is not None: items=[r for r in items if r['eligible']==eligible]
    return reply(dict(currency='USD',items=items,simulated=True))


@router.get('/summary')
def summary(user=Depends(authorized),funding_account_id: UUID|None=None):
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
        items=svc.payables(cur,user); bank=svc.bank_summary(cur,user['tenant_id'],funding_account_id)
        cur.execute('''SELECT a.* FROM financial_payments.funding_accounts a WHERE tenant_id=%s AND status='active'
            ORDER BY is_default DESC,created_at''',(user['tenant_id'],))
        accounts=cur.fetchall()
    return reply(dict(total_outstanding=sum((r['outstanding'] for r in items),svc.ZERO),
        eligible_payables=sum((r['available_to_schedule'] for r in items if r['eligible']),svc.ZERO),
        bank=bank,funding_accounts=accounts,can_authorize=user['role']=='tenant_admin',live_payments_enabled=False,simulated=True))


@router.post('/bank-connections')
def connect(body: Connection,user=Depends(authorized)):
    admin(user)
    def run(cur):
        tenant=user['tenant_id']; provider=connector('mock')
        # Connection identity also makes browser retries safe.
        cur.execute('SELECT * FROM financial_payments.bank_connections WHERE tenant_id=%s AND id=%s',(tenant,body.idempotency_key))
        old=cur.fetchone()
        if old:
            account=svc.funding(cur,tenant)
            cur.execute('SELECT * FROM financial_payments.funding_accounts WHERE tenant_id=%s AND connection_id=%s',(tenant,old['id']))
            saved=cur.fetchone()
            if old['display_name']!=body.display_name or saved['mock_opening_balance']!=body.opening_balance:
                raise HTTPException(409,'Connection request ID was used for other details.')
            return dict(connection=old,account=saved)
        data=provider.connect(body.display_name)
        cur.execute('''INSERT INTO financial_payments.bank_connections
            (id,tenant_id,provider_code,display_name,external_connection_id,status,capabilities)
            VALUES(%s,%s,'mock',%s,%s,'connected',%s) RETURNING *''',
            (body.idempotency_key,tenant,body.display_name,data['external_connection_id'],Jsonb(data['capabilities'])))
        connection=cur.fetchone(); account_data=provider.list_accounts(connection)[0]
        cur.execute("UPDATE financial_payments.funding_accounts SET is_default=false WHERE tenant_id=%s AND currency='USD'",(tenant,))
        cur.execute('''INSERT INTO financial_payments.funding_accounts
            (tenant_id,connection_id,external_account_id,display_name,bank_name,account_last4,currency,is_default,mock_opening_balance)
            VALUES(%s,%s,%s,%s,%s,%s,'USD',true,%s) RETURNING *''',
            (tenant,connection['id'],account_data['external_account_id'],account_data['display_name'],account_data['bank_name'],account_data['account_last4'],body.opening_balance))
        account=cur.fetchone(); svc.refresh_balance(cur,tenant,account | {'provider_code':'mock'})
        return dict(connection=connection,account=account,simulated=True)
    return mutate(user,run)


@router.post('/funding-accounts/{account_id}/refresh-balance')
def refresh(account_id: UUID,user=Depends(authorized)):
    return mutate(user,lambda cur:svc.refresh_balance(cur,user['tenant_id'],svc.funding(cur,user['tenant_id'],account_id)))


@router.post('/recipients/verify-simulation')
def verify(body: Verify,user=Depends(authorized)):
    admin(user)
    return mutate(user,lambda cur:svc.verify_recipient(cur,user,body.statement_id,body.party_id))


@router.get('/settings')
def get_settings(user=Depends(authorized)):
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        svc.ready(cur); value=svc.settings(cur,user['tenant_id'])
    return reply(value)


@router.put('/settings')
def save_settings(body: Policy,user=Depends(authorized)):
    admin(user)
    def run(cur):
        # A policy change invalidates prepared authorizations, releasing reserves.
        cur.execute("SELECT id FROM financial_payments.payment_batches WHERE tenant_id=%s AND status IN ('draft','pending_approval','approved')",(user['tenant_id'],))
        for row in cur.fetchall(): svc.batch_action(cur,user,row['id'],'cancel')
        cur.execute('''INSERT INTO financial_payments.payment_settings
            (tenant_id,operating_reserve,dual_approval_required,approval_threshold,balance_max_age_minutes)
            VALUES(%s,%s,%s,%s,%s) ON CONFLICT(tenant_id) DO UPDATE SET
            operating_reserve=EXCLUDED.operating_reserve,dual_approval_required=EXCLUDED.dual_approval_required,
            approval_threshold=EXCLUDED.approval_threshold,balance_max_age_minutes=EXCLUDED.balance_max_age_minutes,updated_at=now()''',
            (user['tenant_id'],body.operating_reserve,body.dual_approval_required,body.approval_threshold,body.balance_max_age_minutes))
        return svc.settings(cur,user['tenant_id'])
    return mutate(user,run)


@router.post('/batches')
def create(body: Batch,user=Depends(authorized)):
    return mutate(user,lambda cur:svc.create_batch(cur,user,body))


@router.get('/batches')
def batches(user=Depends(authorized),offset: int=Query(0,ge=0),limit: int=Query(25,ge=1,le=100)):
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        svc.ready(cur)
        cur.execute('''SELECT b.*,u.full_name AS initiated_by FROM financial_payments.payment_batches b
            JOIN public.users u ON u.id=b.created_by WHERE b.tenant_id=%s ORDER BY b.created_at DESC,b.id LIMIT %s OFFSET %s''',
            (user['tenant_id'],limit+1,offset)); rows=cur.fetchall()
    return reply(dict(items=rows[:limit],has_more=len(rows)>limit))


@router.get('/batches/{batch_id}')
def detail(batch_id: UUID,user=Depends(authorized)):
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        svc.ready(cur); data=svc.batch_detail(cur,user['tenant_id'],batch_id)
    return reply(data)


@router.post('/batches/{batch_id}/{action}')
def action(batch_id: UUID,action: Literal['submit-for-approval','approve','reject','execute','cancel'],body: Decision,user=Depends(authorized)):
    return mutate(user,lambda cur:svc.batch_action(cur,user,batch_id,action,body.comment))


@router.post('/transfers/{transfer_id}/simulate-event')
def simulate(transfer_id: UUID,body: SimulatedEvent,user=Depends(authorized)):
    admin(user)
    return mutate(user,lambda cur:svc.simulate_event(cur,user,transfer_id,body.status,'mock:'+str(body.event_id)))


@router.get('/history')
def history(user=Depends(authorized),search: str='',status: str='',sort: Literal['newest','oldest','amount','recipient']='newest',offset: int=Query(0,ge=0),limit: int=Query(25,ge=1,le=100)):
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        svc.ready(cur)
        order={'newest':'date DESC,id','oldest':'date,id','amount':'amount DESC,date DESC,id','recipient':'recipient,date DESC,id'}[sort]
        cur.execute('''SELECT * FROM (
          SELECT t.id,t.created_at AS date,r.display_name AS recipient,t.amount,t.currency,t.status,
            t.provider_transfer_id AS bank_reference,t.batch_id,true AS simulated,'Simulated ACH' AS payment_method,
            u.full_name AS initiated_by,
            (SELECT string_agg(DISTINCT w.title,', ') FROM financial_payments.transfer_allocations m
             JOIN financial_payments.payment_allocations a ON a.tenant_id=m.tenant_id AND a.id=m.allocation_id
             JOIN financial_payments.payment_obligations o ON o.tenant_id=a.tenant_id AND o.id=a.obligation_id
             JOIN public.works w ON w.tenant_id=o.tenant_id AND w.id=o.work_id WHERE m.tenant_id=t.tenant_id AND m.transfer_id=t.id) AS titles,
            (SELECT string_agg(DISTINCT p.period_code,', ') FROM financial_payments.transfer_allocations m
             JOIN financial_payments.payment_allocations a ON a.tenant_id=m.tenant_id AND a.id=m.allocation_id
             JOIN financial_payments.payment_obligations o ON o.tenant_id=a.tenant_id AND o.id=a.obligation_id
             JOIN public.royalty_statements s ON s.tenant_id=o.tenant_id AND s.id=o.source_statement_id
             JOIN public.royalty_periods p ON p.tenant_id=s.tenant_id AND p.id=s.period_id WHERE m.tenant_id=t.tenant_id AND m.transfer_id=t.id) AS period
          FROM financial_payments.payment_transfers t JOIN financial_payments.payment_recipients r ON r.tenant_id=t.tenant_id AND r.id=t.recipient_id
          JOIN financial_payments.payment_batches b ON b.tenant_id=t.tenant_id AND b.id=t.batch_id JOIN public.users u ON u.id=b.created_by WHERE t.tenant_id=%s
          UNION ALL
          SELECT pay.id,pay.payment_date::timestamptz,COALESCE(pa.display_name,pay.payee_role),pay.amount,pay.currency,'recorded',
            pay.reference_number,NULL::uuid,false,pay.payment_method,'Manual record',w.title,p.period_code
          FROM public.royalty_payments pay JOIN public.royalty_statements s ON s.tenant_id=pay.tenant_id AND s.id=pay.statement_id
          JOIN public.works w ON w.tenant_id=s.tenant_id AND w.id=s.work_id JOIN public.royalty_periods p ON p.tenant_id=s.tenant_id AND p.id=s.period_id
          LEFT JOIN public.parties pa ON pa.tenant_id=pay.tenant_id AND pa.id=pay.payee_party_id WHERE pay.tenant_id=%s
        ) h WHERE (%s='' OR concat_ws(' ',recipient,titles,bank_reference,period) ILIKE %s)
          AND (%s='' OR status=%s) ORDER BY '''+order+''' LIMIT %s OFFSET %s''',
        (user['tenant_id'],user['tenant_id'],search,'%'+search+'%',status,status,limit+1,offset))
        rows=cur.fetchall()
    return reply(dict(items=rows[:limit],has_more=len(rows)>limit))
