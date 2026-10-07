"""Frozen settlement policy; moving reserves and threshold accrual, no payments."""
from decimal import Decimal, ROUND_HALF_UP
from services.royalty_accounts import account_state, require_tracking, lock_account

ZERO = Decimal('0')
def money(value):
    return Decimal(str(value or 0)).quantize(Decimal('.01'), rounding=ROUND_HALF_UP)


def calculate_settlement(gross, accrued, opening_reserve, average, percent, minimum):
    gross, accrued, opening_reserve = [max(ZERO, money(v)) for v in (gross, accrued, opening_reserve)]
    target = opening_reserve if average is None else max(ZERO, money(Decimal(str(average)) * Decimal(str(percent)) / 100))
    held = min(target, gross + accrued + opening_reserve)
    available = money(gross + accrued + opening_reserve - held)
    payable = available if available >= money(minimum) else ZERO
    result = {k: str(money(v)) for k,v in dict(
        gross_available=gross, accrued_brought_forward=accrued, opening_reserve=opening_reserve,
        reserve_average=average or ZERO, reserve_percent=percent, reserve_target=target,
        reserve_held=held, reserve_change=held-opening_reserve, available_after_reserve=available,
        minimum_payout=minimum, accrued_carried_forward=available if payable == ZERO else ZERO,
        actual_payable=payable).items()}
    result['reserve_percent']=str(Decimal(str(percent)))
    result['reserve_shortfall']=str(money(target-held))
    return result


def payment_balances(cur, tenant_id, work_id, party, through_date, include_current=False, currency='USD'):
    # Payment liability stays on its source statement. Sent/delivered is not paid.
    comparison = '<=' if include_current else '<'
    cur.execute(f"""SELECT s.id::text AS statement_id,p.id::text AS period_id,p.period_code,
        s.currency,s.payable_this_period,s.settlement,
        COALESCE((SELECT SUM(pay.amount) FROM royalty_payments pay
            WHERE pay.tenant_id=s.tenant_id AND pay.statement_id=s.id AND pay.currency=s.currency),0) AS paid_amount
        FROM royalty_statements s JOIN royalty_periods p ON p.id=s.period_id AND p.tenant_id=s.tenant_id
        WHERE s.tenant_id=%s::uuid AND s.work_id=%s::uuid AND s.party=%s AND s.status='final'
          AND p.period_start >= DATE '2025-07-01' AND p.period_end {comparison} %s AND s.currency=%s
        ORDER BY p.period_end,p.period_start,s.id""", (tenant_id,work_id,party,through_date,currency))
    balances=[]
    for row in cur.fetchall():
        snapshot=row.get('settlement') or {}
        payable,paid=money(row['payable_this_period']),money(row['paid_amount'])
        outstanding=max(ZERO,payable-paid)
        balances.append(dict(statement_id=row['statement_id'],period_id=row['period_id'],period_code=row['period_code'],
            currency=row['currency'],statement_payable=str(payable),paid_amount=str(paid),outstanding_amount=str(outstanding),
            settlement_version=snapshot.get('settlement_version',1),
            account_balance_carried_forward=snapshot.get('account_balance_carried_forward'),
            payments_recorded_total=snapshot.get('payments_recorded_total','0'),
            accrued_carried_forward=snapshot.get('accrued_carried_forward','0'),
            payment_status='No payment due' if payable <= ZERO else 'Paid' if outstanding == ZERO else 'Partially paid' if paid > ZERO else 'Unpaid'))
    if balances and balances[-1].get('settlement_version') == 2:
        account_remaining=unpaid_account_balance(balances)[0]
        for balance in balances:
            original=money(balance['outstanding_amount'])
            balance['outstanding_amount']=str(min(original,account_remaining))
            if original > ZERO and account_remaining == ZERO:
                balance['payment_status']='Settled through account payments'
    return balances


def unpaid_account_balance(balances):
    paid_total=sum((money(row['paid_amount']) for row in balances), ZERO)
    if balances and balances[-1].get('settlement_version') == 2:
        latest=balances[-1]
        # A new settlement includes the whole unpaid account. Subtract only payments
        # made since that snapshot; summing rolling statement payables would double debt.
        unpaid=max(ZERO, money(latest['account_balance_carried_forward']) -
                   (paid_total-money(latest['payments_recorded_total'])))
    else:
        unpaid=sum((money(row['outstanding_amount']) for row in balances), ZERO)
        if balances:
            unpaid += money(balances[-1].get('accrued_carried_forward',0))
    return money(unpaid), money(paid_total)


def account_payment_capacity(cur, tenant_id, work_id, party, currency):
    balances=payment_balances(cur,tenant_id,work_id,party,'9999-12-31',True,currency)
    if balances and balances[-1].get('settlement_version') == 2:
        return unpaid_account_balance(balances)[0]
    return None


def build_settlement(cur, tenant_id, work_id, party, period_start, gross):
    require_tracking(cur)
    cur.execute("""SELECT EXISTS (SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name='royalty_statements' AND column_name='settlement') AS ready""")
    if not cur.fetchone()['ready']:
        from services.royalty_statement_engine import StatementValidationError
        raise StatementValidationError('Settlement setup is pending. Apply migration 023_royalty_settlement.sql.')
    lock_account(cur, tenant_id, work_id, party)
    settings = account_state(cur, tenant_id, work_id, party)
    cur.execute("""SELECT s.id::text, s.earned_this_period, s.settlement
        FROM royalty_statements s JOIN royalty_periods p ON p.id=s.period_id AND p.tenant_id=s.tenant_id
        WHERE s.tenant_id=%s::uuid AND s.work_id=%s::uuid AND s.party=%s AND s.status='final'
          AND p.period_end < %s
        ORDER BY p.period_end DESC, p.period_start DESC, s.id DESC LIMIT 3""",
        (tenant_id, work_id, party, period_start))
    history=cur.fetchall()
    balances=payment_balances(cur,tenant_id,work_id,party,period_start)
    accrued, paid_total=unpaid_account_balance(balances)
    average=sum((money(r['earned_this_period']) for r in history), ZERO)/len(history) if history else None
    result=calculate_settlement(gross, accrued, settings['reserve_held'], average,
                                settings['reserve_percent'], settings['minimum_payout'])
    result.update(account_version=settings['version'], history_ids=[r['id'] for r in history],
                  history_count=len(history), settlement_version=2,
                  account_balance_carried_forward=result['available_after_reserve'],
                  payments_recorded_total=str(paid_total), payment_tracking_start='2025-07-01',
                  prior_payment_balances=balances, total_payment_due=result['actual_payable'])
    return result


def approve_settlement(cur, tenant_id, statement_id):
    from fastapi import HTTPException
    cur.execute("""SELECT s.*,p.period_start,p.period_end FROM royalty_statements s
        JOIN royalty_periods p ON p.id=s.period_id AND p.tenant_id=s.tenant_id
        WHERE s.id=%s::uuid AND s.tenant_id=%s::uuid""", (statement_id,tenant_id))
    head=cur.fetchone()
    snapshot=head.get('settlement')
    if not snapshot:
        raise HTTPException(409,'Rebuild this draft to calculate its minimum payment and reserve before approving.')
    current=build_settlement(cur,tenant_id,str(head['work_id']),head['party'],head['period_start'],snapshot['gross_available'])
    if current != snapshot:
        raise HTTPException(409,'Account settings, prior statements, or recorded payments changed. Rebuild and review this draft before approving.')
    cur.execute("""SELECT 1 FROM royalty_statements s JOIN royalty_periods p ON p.id=s.period_id
        WHERE s.tenant_id=%s::uuid AND s.work_id=%s::uuid AND s.party=%s AND s.status='final'
          AND p.period_end >= %s AND s.id <> %s::uuid LIMIT 1""",
        (tenant_id,str(head['work_id']),head['party'],head['period_end'],statement_id))
    if cur.fetchone():
        raise HTTPException(409,'A later or overlapping statement is already final. Use the audit workflow to revise historical settlements.')
    cur.execute("""INSERT INTO royalty_account_settings(tenant_id,work_id,party,minimum_payout,reserve_percent,reserve_held,version)
        VALUES (%s::uuid,%s::uuid,%s,%s,%s,%s,1)
        ON CONFLICT(tenant_id,work_id,party) DO UPDATE SET reserve_held=EXCLUDED.reserve_held,
          version=royalty_account_settings.version+1,updated_at=now()""",
        (tenant_id,str(head['work_id']),head['party'],snapshot['minimum_payout'],snapshot['reserve_percent'],snapshot['reserve_held']))
    from psycopg.types.json import Jsonb
    from uuid import uuid4
    before={k:snapshot[k] for k in ('minimum_payout','reserve_percent')}
    before['reserve_held']=snapshot['opening_reserve']
    after={k:snapshot[k] for k in ('minimum_payout','reserve_percent','reserve_held','reserve_change')}
    after['version']=snapshot['account_version']+1
    cur.execute("""INSERT INTO royalty_account_events
        (id,tenant_id,work_id,party,period_id,event_type,actor,reason,request_payload,before_values,after_values)
        VALUES (%s::uuid,%s::uuid,%s::uuid,%s,%s::uuid,'settings','statement approval',
          'Automatic reserve settlement on statement approval',%s,%s,%s)""",
        (str(uuid4()),tenant_id,str(head['work_id']),head['party'],str(head['period_id']),
         Jsonb({'statement_id':statement_id,'automatic':True}),Jsonb(before),Jsonb(after)))
