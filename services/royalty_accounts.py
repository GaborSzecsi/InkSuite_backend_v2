"""Account policy and recorded payments; never initiates a bank transfer."""
from decimal import Decimal
from datetime import date
from typing import Literal
from uuid import UUID
from pydantic import BaseModel, Field
from fastapi import HTTPException
from psycopg.types.json import Jsonb

class AccountChange(BaseModel):
    request_id: UUID
    work_id: UUID
    royalty_set_id: UUID
    period_id: UUID
    party: Literal["author", "illustrator"]
    version: int = Field(ge=0)
    minimum_payout: Decimal = Field(ge=0, max_digits=14, decimal_places=2)
    reserve_percent: Decimal = Field(ge=0, le=100, max_digits=7, decimal_places=4)
    reserve_held: Decimal = Field(ge=0, max_digits=14, decimal_places=2)
    reason: str = Field(min_length=1, max_length=1000)

class PaymentRecord(BaseModel):
    request_id: UUID
    statement_id: UUID
    amount: Decimal = Field(gt=0, max_digits=14, decimal_places=2)
    payment_date: date
    reference_number: str = Field(min_length=1, max_length=200)
    payment_method: str = Field(min_length=1, max_length=80)
    payee_role: Literal["contributor", "agency"] = "contributor"
    payee_party_id: UUID | None = None


def tracking_ready(cur):
    cur.execute("SELECT to_regclass('public.royalty_account_settings') IS NOT NULL AND to_regclass('public.royalty_account_events') IS NOT NULL AS ready")
    return bool(cur.fetchone()["ready"])


def account_state(cur, tenant_id, work_id, party):
    cur.execute("SELECT * FROM royalty_account_settings WHERE tenant_id=%s::uuid AND work_id=%s::uuid AND party=%s", (tenant_id,work_id,party))
    row = cur.fetchone()
    return dict(row) if row else {"minimum_payout": Decimal('100'), "reserve_percent": Decimal('0'), "reserve_held": Decimal('0'), "currency": 'USD', "version": 0}


def require_tracking(cur):
    if not tracking_ready(cur):
        raise HTTPException(503, "Account tracking setup is pending. Apply migration 019_royalty_account_tracking.sql first.")


def lock_account(cur, tenant_id, work_id, party):
    cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f'royalty-account:{tenant_id}:{work_id}:{party}',))


def replay(cur, tenant_id, request_id, payload):
    cur.execute("SELECT tenant_id::text, request_payload, after_values FROM royalty_account_events WHERE id=%s::uuid", (str(request_id),))
    row = cur.fetchone()
    if not row:
        return None
    if row['tenant_id'] != str(tenant_id) or row['request_payload'] != payload:
        raise HTTPException(409, "This request ID was already used for different data.")
    return row['after_values']


def event(cur, tenant_id, work_id, party, period_id, event_type, actor, reason, body, before, after):
    cur.execute("""INSERT INTO royalty_account_events
        (id,tenant_id,work_id,party,period_id,event_type,actor,reason,request_payload,before_values,after_values)
        VALUES (%s::uuid,%s::uuid,%s::uuid,%s,%s::uuid,%s,%s,%s,%s,%s,%s)""",
        (str(body.request_id),tenant_id,str(work_id),party,str(period_id),event_type,actor,reason,
         Jsonb(body.model_dump(mode='json')), Jsonb(before), Jsonb(after)))


def save_settings(cur, tenant_id, actor, body):
    from services.royalty_statement_engine import assert_work, assert_royalty_set_for_work, load_period
    require_tracking(cur)
    assert_work(cur,tenant_id,str(body.work_id))
    assert_royalty_set_for_work(cur,tenant_id,str(body.royalty_set_id),str(body.work_id))
    load_period(cur,tenant_id,str(body.period_id))
    reason = body.reason.strip()
    if not reason:
        raise HTTPException(422, 'Enter a reason for this account change.')
    lock_account(cur,tenant_id,body.work_id,body.party)
    previous = replay(cur,tenant_id,body.request_id,body.model_dump(mode='json'))
    if previous is not None:
        return previous
    old = account_state(cur,tenant_id,str(body.work_id),body.party)
    if body.version != old['version']:
        raise HTTPException(409, 'This account changed in another session. Reload before saving.')
    before = {k: str(old[k]) for k in ('minimum_payout','reserve_percent','reserve_held')}
    after = {k: str(getattr(body,k)) for k in before}
    after.update(version=body.version+1, currency=old['currency'], reserve_change=str(body.reserve_held-old['reserve_held']))
    cur.execute("""INSERT INTO royalty_account_settings
        (tenant_id,work_id,party,minimum_payout,reserve_percent,reserve_held,version)
        VALUES (%s::uuid,%s::uuid,%s,%s,%s,%s,%s)
        ON CONFLICT (tenant_id,work_id,party) DO UPDATE SET
          minimum_payout=EXCLUDED.minimum_payout, reserve_percent=EXCLUDED.reserve_percent,
          reserve_held=EXCLUDED.reserve_held,version=EXCLUDED.version,updated_at=now()""",
        (tenant_id,str(body.work_id),body.party,body.minimum_payout,body.reserve_percent,body.reserve_held,body.version+1))
    event(cur,tenant_id,body.work_id,body.party,body.period_id,'settings',actor,reason,body,before,after)
    return after


def record_payment(cur, tenant_id, actor, body):
    require_tracking(cur)
    cur.execute("SELECT * FROM royalty_statements WHERE id=%s::uuid AND tenant_id=%s::uuid FOR UPDATE", (str(body.statement_id),tenant_id))
    statement = cur.fetchone()
    if not statement or statement['status'] != 'final':
        raise HTTPException(422, 'Select a finalized statement for this tenant.')
    lock_account(cur,tenant_id,statement['work_id'],statement['party'])
    previous = replay(cur,tenant_id,body.request_id,body.model_dump(mode='json'))
    if previous is not None:
        return previous
    if body.payment_date > date.today():
        raise HTTPException(422, 'Record an actual payment date, not a future payment.')
    if not body.reference_number.strip() or not body.payment_method.strip():
        raise HTTPException(422, 'Payment method and reference are required.')
    if body.payee_party_id:
        cur.execute("SELECT id FROM parties WHERE id=%s::uuid AND tenant_id=%s::uuid", (str(body.payee_party_id),tenant_id))
        if not cur.fetchone():
            raise HTTPException(422, 'The payee does not belong to this tenant.')
    cur.execute("SELECT COALESCE(SUM(amount),0) AS paid FROM royalty_payments WHERE tenant_id=%s::uuid AND statement_id=%s::uuid AND currency=%s", (tenant_id,str(body.statement_id),statement['currency']))
    paid = cur.fetchone()['paid']
    remaining = max(Decimal('0'), statement['payable_this_period']-paid)
    if body.amount > remaining:
        raise HTTPException(422, 'Payment exceeds the unpaid statement balance.')
    cur.execute("SELECT id FROM royalty_payments WHERE tenant_id=%s::uuid AND statement_id=%s::uuid AND reference_number=%s", (tenant_id,str(body.statement_id),body.reference_number.strip()))
    if cur.fetchone():
        raise HTTPException(409, 'A payment with this reference is already recorded for the statement.')
    cur.execute("""INSERT INTO royalty_payments
      (id,tenant_id,statement_id,payee_party_id,payee_role,payment_date,amount,currency,payment_method,reference_number,notes)
      VALUES (%s::uuid,%s::uuid,%s::uuid,%s::uuid,%s,%s,%s,%s,%s,%s,%s)""",
      (str(body.request_id),tenant_id,str(body.statement_id),str(body.payee_party_id) if body.payee_party_id else None,
       body.payee_role,body.payment_date,body.amount,statement['currency'],body.payment_method.strip(),body.reference_number.strip(),'Recorded actual payment; no transfer initiated.'))
    after = {'payment_id': str(body.request_id), 'amount': str(body.amount), 'unpaid_balance': str(remaining-body.amount),
             'payment_date': str(body.payment_date), 'reference_number': body.reference_number.strip(), 'currency': statement['currency']}
    event(cur,tenant_id,statement['work_id'],statement['party'],statement['period_id'],'payment',actor,
          body.reference_number.strip(),body,{'unpaid_balance':str(remaining)},after)
    return after
