"""All writes run under a tenant transaction lock; amounts always use Decimal.

Mock reconciliation is deliberately isolated from public.royalty_payments.
There is no production execution path in this release.
"""
import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

from fastapi import HTTPException
from psycopg.types.json import Jsonb
from services.royalty_settlement import payment_balances, unpaid_account_balance
from .domain import ZERO, money, split_amount, next_status
from .bank_connectors.registry import connector


def serial(value):
    return json.loads(json.dumps(value, default=str))


def fingerprint(value):
    return hashlib.sha256(json.dumps(serial(value), sort_keys=True).encode()).hexdigest()


def ready(cur):
    cur.execute("SELECT to_regclass('financial_payments.payment_batches') AS ready")
    if not cur.fetchone()['ready']:
        raise HTTPException(503, 'Payments setup is pending. Apply migration 026_financial_payments.sql.')


def lock(cur, tenant):
    ready(cur)
    cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", ('financial-payments:'+str(tenant),))


def settings(cur, tenant):
    cur.execute('SELECT * FROM financial_payments.payment_settings WHERE tenant_id=%s', (tenant,))
    return cur.fetchone() or dict(operating_reserve=ZERO, dual_approval_required=True,
                                 approval_threshold=ZERO, balance_max_age_minutes=15, live_payments_enabled=False)


def funding(cur, tenant, account_id=None):
    cur.execute('''SELECT a.*,c.provider_code,c.status AS connection_status,c.capabilities
        FROM financial_payments.funding_accounts a JOIN financial_payments.bank_connections c
        ON c.tenant_id=a.tenant_id AND c.id=a.connection_id
        WHERE a.tenant_id=%s AND a.status='active' AND c.status='connected'
          AND (%s::uuid IS NULL OR a.id=%s::uuid)
        ORDER BY a.is_default DESC,a.created_at,a.id''', (tenant, account_id, account_id))
    accounts=cur.fetchall()
    return accounts[0] if accounts else None


def refresh_balance(cur, tenant, account):
    if not account:
        raise HTTPException(409, 'Connect a funding account first.')
    cur.execute('''SELECT COALESCE(sum(t.amount) FILTER(WHERE t.status IN ('submitted','processing','reconciliation_required')),0) AS pending,
        COALESCE(sum(t.amount) FILTER(WHERE t.status='settled'),0) AS settled
        FROM financial_payments.payment_transfers t JOIN financial_payments.payment_batches b
        ON b.tenant_id=t.tenant_id AND b.id=t.batch_id WHERE t.tenant_id=%s AND b.funding_account_id=%s''', (tenant,account['id']))
    amounts=cur.fetchone()
    balance=connector(account['provider_code']).balance(account,amounts['pending'],amounts['settled'])
    cur.execute('''INSERT INTO financial_payments.bank_balance_snapshots
        (tenant_id,funding_account_id,currency,available_balance,current_balance,balance_as_of,provider_reference,commitments_reflected)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *''',
        (tenant,account['id'],account['currency'],balance['available_balance'],balance['current_balance'],
         balance['balance_as_of'],balance['provider_reference'],balance['commitments_reflected']))
    return cur.fetchone()


def bank_summary(cur, tenant, account_id=None):
    account=funding(cur,tenant,account_id)
    policy=settings(cur,tenant)
    if not account:
        return dict(account=None, available_to_allocate=None, funding_verified=False, settings=policy,
                    pending_outgoing=ZERO, prepared_commitments=ZERO, simulated=True)
    cur.execute('''SELECT * FROM financial_payments.bank_balance_snapshots WHERE tenant_id=%s AND funding_account_id=%s
        ORDER BY retrieved_at DESC,id DESC LIMIT 1''',(tenant,account['id']))
    snapshot=cur.fetchone()
    cur.execute('''SELECT COALESCE(sum(t.amount) FILTER(WHERE t.status IN ('submitted','processing','reconciliation_required')),0) AS pending,
        COALESCE(sum(t.amount) FILTER(WHERE t.status='created' AND b.status IN ('draft','pending_approval','approved')),0) AS prepared
        FROM financial_payments.payment_transfers t JOIN financial_payments.payment_batches b
        ON b.tenant_id=t.tenant_id AND b.id=t.batch_id WHERE t.tenant_id=%s AND b.funding_account_id=%s''',(tenant,account['id']))
    sums=cur.fetchone()
    fresh=bool(snapshot and snapshot['available_balance'] is not None and snapshot['balance_as_of'] and
        0 <= (datetime.now(timezone.utc)-snapshot['balance_as_of']).total_seconds() <= policy['balance_max_age_minutes']*60)
    # Snapshot may predate submission: be conservative until refreshed. The mock
    # reports pending commitments in its available balance at refresh time.
    pending_unreflected=sums['pending']
    if fresh and snapshot['commitments_reflected']:
        cur.execute('''SELECT COALESCE(sum(t.amount),0) AS amount FROM financial_payments.payment_transfers t
            JOIN financial_payments.payment_batches b ON b.tenant_id=t.tenant_id AND b.id=t.batch_id
            WHERE t.tenant_id=%s AND b.funding_account_id=%s AND t.status IN ('submitted','processing','reconciliation_required')
            AND t.submitted_at>%s''',(tenant,account['id'],snapshot['balance_as_of']))
        pending_unreflected=cur.fetchone()['amount']
    allocate=money(snapshot['available_balance']-pending_unreflected-sums['prepared']-policy['operating_reserve']) if fresh else None
    return dict(account=account, snapshot=snapshot, available_to_allocate=allocate, funding_verified=fresh,
                pending_outgoing=sums['pending'], prepared_commitments=sums['prepared'], settings=policy, simulated=True)


def instruction(cur, tenant, work, party, date):
    cur.execute('''SELECT id,payee_mode,contributor_party_id,agency_party_id,contributor_percent,agency_percent,
        effective_start,effective_end,updated_at FROM public.royalty_payment_instructions
        WHERE tenant_id=%s AND work_id=%s AND party=%s
        AND (effective_start IS NULL OR effective_start<=%s) AND (effective_end IS NULL OR effective_end>=%s)
        ORDER BY id''', (tenant,work,party,date,date))
    rows=cur.fetchall()
    if len(rows)!=1:
        raise HTTPException(409,'Payment instructions are missing or overlap for this statement period.')
    return rows[0]


def secure_destination(cur, tenant, slug, party_id, role, currency):
    # Secure subsystem's tenant_id is a slug TEXT, not the public UUID.
    cur.execute('''SELECT a.id,a.account_last4,a.bank_name,a.currency,a.verification_status,a.updated_at,
        a.payment_method,a.country_code,p.status AS profile_status
        FROM secure_payments.payment_profiles p JOIN secure_payments.payment_accounts a ON a.payment_profile_id=p.id
        JOIN public.parties party ON party.id=%s AND party.tenant_id=%s
        WHERE p.tenant_id=%s AND p.recipient_id=%s AND p.recipient_type=%s AND a.is_active
        AND a.currency=%s AND a.payment_method='ACH' AND a.country_code='US'
        AND p.status='COMPLETE' ORDER BY a.created_at DESC''',
        (party_id,tenant,slug,str(party_id),'AGENCY' if role=='agency' else 'CONTRIBUTOR',currency))
    rows=cur.fetchall()
    return rows[0] if len(rows)==1 else None


def recipients(cur, user, head, amount):
    inst=instruction(cur,user['tenant_id'],head['work_id'],head['party'],head['period_end'])
    result=[]
    resolved=split_amount(amount,inst)
    # Keep destination identities visible even when cent rounding produces a
    # zero share for a very small remaining balance or a fully reserved payable.
    shares={role:value for role,_,_,value in resolved}
    identities=[('contributor',inst['contributor_party_id'],inst['contributor_percent']),
                ('agency',inst['agency_party_id'],inst['agency_percent'])]
    for role,pid,percent in identities:
        if Decimal(str(percent))<=0: continue
        value=shares.get(role,ZERO)
        cur.execute('SELECT display_name FROM public.parties WHERE tenant_id=%s AND id=%s',(user['tenant_id'],pid))
        party=cur.fetchone()
        if not party:
            raise HTTPException(409,'A recipient does not belong to this publisher.')
        if role=='contributor':
            cur.execute('SELECT 1 FROM public.work_contributors WHERE tenant_id=%s AND work_id=%s AND party_id=%s',
                        (user['tenant_id'],head['work_id'],pid))
            if not cur.fetchone():
                raise HTTPException(409,'Instruction contributor is not linked to this title.')
        bank=secure_destination(cur,user['tenant_id'],user['slug'],pid,role,head['currency'])
        # Simulated verification is stored separately, never edits the secure account.
        cur.execute('''SELECT a.* FROM financial_payments.recipient_bank_accounts a
            JOIN financial_payments.payment_recipients r ON r.tenant_id=a.tenant_id AND r.id=a.recipient_id
            WHERE a.tenant_id=%s AND r.party_id=%s AND r.status='active' AND a.secure_bank_account_ref=%s
              AND a.verification_status='verified' ''', (user['tenant_id'],pid,bank['id'] if bank else None))
        destination=cur.fetchone()
        result.append(dict(party_id=pid,role=role,display_name=party['display_name'],percent=percent,amount=value,
                           secure_account=bank, destination=destination,
                           bank_status='Simulation verified' if destination else 'Needs simulation verification' if bank else 'Banking missing or change pending'))
    return inst,result


def payables(cur, user):
    ready(cur)
    tenant=user['tenant_id']
    cur.execute('''SELECT s.*,w.title,p.period_code,p.period_start,p.period_end FROM public.royalty_statements s
        JOIN public.works w ON w.id=s.work_id AND w.tenant_id=s.tenant_id
        JOIN public.royalty_periods p ON p.id=s.period_id AND p.tenant_id=s.tenant_id
        WHERE s.tenant_id=%s AND s.status='final' AND s.currency='USD' AND p.period_start>=DATE '2025-07-01'
        ORDER BY w.title,s.work_id,s.party,p.period_end,p.period_start,s.id''',(tenant,))
    groups={}
    for row in cur.fetchall():
        groups.setdefault((row['work_id'],row['party'],row['currency']),[]).append(row)
    items=[]
    for (work,party,currency),heads in groups.items():
        balances=payment_balances(cur,str(tenant),str(work),party,'9999-12-31',True,currency)
        capacity,_=unpaid_account_balance(balances)
        v2=bool(balances and balances[-1].get('settlement_version')==2)
        # Rolling settlement snapshots include earlier debts. Only the latest
        # finalized statement owns the payable identity for a v2 account.
        selected=[heads[-1]] if v2 else heads
        for head in selected:
            cur.execute('''SELECT id,statement_id,amount,currency FROM public.royalty_payments
                WHERE tenant_id=%s AND statement_id=ANY(%s::uuid[]) ORDER BY id''',(tenant,[h['id'] for h in heads]))
            payments=cur.fetchall()
            paid=sum((money(p['amount']) for p in payments if p['statement_id']==head['id'] and p['currency']==currency),ZERO)
            if v2:
                snapshot=head['settlement'] or {}
                since=sum((money(p['amount']) for p in payments if p['currency']==currency),ZERO)-money(snapshot.get('payments_recorded_total','0'))
                outstanding=max(ZERO,min(capacity,money(head['payable_this_period'])-since))
                paid=max(ZERO,money(since))
            else:
                outstanding=max(ZERO,money(head['payable_this_period'])-paid)
            cur.execute('''SELECT
                COALESCE(sum(a.amount) FILTER(WHERE a.status IN ('reserved','submitted')),0) AS reserved,
                COALESCE(sum(a.amount) FILTER(WHERE a.status='settled'),0) AS mock_settled
                FROM financial_payments.payment_allocations a JOIN financial_payments.payment_obligations o
                ON o.tenant_id=a.tenant_id AND o.id=a.obligation_id
                WHERE a.tenant_id=%s AND o.work_id=%s AND o.currency=%s
                AND o.source_statement_id IN (SELECT id FROM public.royalty_statements WHERE tenant_id=%s AND work_id=%s AND party=%s)
                AND (%s OR o.source_statement_id=%s)''',(tenant,work,currency,tenant,work,party,v2,head['id']))
            reserved=cur.fetchone()
            available=max(ZERO,outstanding-reserved['reserved']-reserved['mock_settled'])
            reason=''; inst=None; rec=[]
            try:
                inst,rec=recipients(cur,user,head,available if available>ZERO else Decimal('.01'))
                if any(not r['destination'] for r in rec): reason='Verify all recipient destinations in simulation before preparing payment.'
            except HTTPException as exc: reason=str(exc.detail)
            if available<=ZERO: reason='No unreserved payable remains in this simulation.'
            source=fingerprint(dict(head=serial(head),payments=serial(payments)))
            contributor_name=party.title()
            if inst:
                cur.execute('SELECT display_name FROM public.parties WHERE tenant_id=%s AND id=%s',(tenant,inst['contributor_party_id']))
                contributor_party=cur.fetchone()
                if contributor_party: contributor_name=contributor_party['display_name']
            items.append(dict(statement_id=head['id'],work_id=work,party=party,title=head['title'],period=head['period_code'],
                period_end=head['period_end'],currency=currency,contributor=contributor_name,
                original_payable=money(head['payable_this_period']),net_settled=paid,outstanding=outstanding,
                active_reservations=reserved['reserved'],simulated_settled=reserved['mock_settled'],available_to_schedule=available,
                instruction=inst,recipients=rec,eligible=available>ZERO and not reason,reason=reason,source_fingerprint=source,
                payee_mode=inst['payee_mode'] if inst else 'Needs review',simulated=True))
    return items


def verify_recipient(cur,user,statement_id,party_id):
    row=next((r for r in payables(cur,user) if r['statement_id']==statement_id),None)
    if not row: raise HTTPException(404,'Payable not found.')
    rec=next((r for r in row['recipients'] if r['party_id']==party_id),None)
    if not rec or not rec['secure_account']: raise HTTPException(409,'No active US ACH banking account for this recipient.')
    tenant=user['tenant_id']; bank=rec['secure_account']
    cur.execute('''INSERT INTO financial_payments.payment_recipients(tenant_id,party_id,recipient_type,display_name)
        VALUES(%s,%s,%s,%s) ON CONFLICT(tenant_id,party_id) DO UPDATE SET display_name=EXCLUDED.display_name RETURNING id''',
        (tenant,party_id,rec['role'],rec['display_name']))
    rid=cur.fetchone()['id']; verified=connector('mock').verify_destination(bank)
    cur.execute('''INSERT INTO financial_payments.recipient_bank_accounts
        (tenant_id,recipient_id,secure_bank_account_ref,provider_destination_ref,account_last4,verification_status,verified_at)
        VALUES(%s,%s,%s,%s,%s,'verified',now()) ON CONFLICT(tenant_id,recipient_id,secure_bank_account_ref)
        DO NOTHING RETURNING id''',
        (tenant,rid,bank['id'],verified['provider_destination_ref'],bank['account_last4']))
    inserted=cur.fetchone()
    if not inserted:
        cur.execute('''SELECT id FROM financial_payments.recipient_bank_accounts
            WHERE tenant_id=%s AND recipient_id=%s AND secure_bank_account_ref=%s''',(tenant,rid,bank['id']))
        inserted=cur.fetchone()
    return dict(id=inserted['id'],simulated=True,message='Simulation verification only; no real bank verification performed.')


def create_batch(cur,user,body):
    tenant=user['tenant_id']; payload=body.model_dump(mode='json')
    cur.execute('SELECT * FROM financial_payments.payment_batches WHERE tenant_id=%s AND idempotency_key=%s',(tenant,body.idempotency_key))
    previous=cur.fetchone()
    if previous:
        if previous['request_payload']!=payload: raise HTTPException(409,'Idempotency key was used for different payment details.')
        return batch_detail(cur,tenant,previous['id'])
    account=funding(cur,tenant,body.funding_account_id)
    if not account: raise HTTPException(409,'Funding account is unavailable.')
    items=payables(cur,user); by_id={r['statement_id']:r for r in items}
    if len({i.statement_id for i in body.items})!=len(body.items): raise HTTPException(422,'Duplicate payable selection.')
    chosen=[]
    for item in body.items:
        row=by_id.get(item.statement_id)
        if not row or not row['eligible']: raise HTTPException(409,row['reason'] if row else 'Payable is no longer eligible.')
        if item.amount>row['available_to_schedule']: raise HTTPException(409,'Amount exceeds the unreserved outstanding balance.')
        chosen.append((item,row))
    total=sum((i.amount for i,_ in chosen),ZERO)
    refresh_balance(cur,tenant,account)
    bank=bank_summary(cur,tenant,account['id'])
    if not bank['funding_verified'] or total>bank['available_to_allocate']: raise HTTPException(409,'Insufficient verified funding after commitments and operating reserve.')
    bid=uuid4()
    cur.execute('''INSERT INTO financial_payments.payment_batches
        (id,tenant_id,funding_account_id,currency,total_amount,idempotency_key,request_payload,created_by)
        VALUES(%s,%s,%s,'USD',%s,%s,%s,%s)''',(bid,tenant,account['id'],total,body.idempotency_key,Jsonb(payload),user['user_id']))
    transfers={}
    for item,row in chosen:
        inst=row['instruction']
        cur.execute('''INSERT INTO financial_payments.payment_obligations
            (tenant_id,source_obligation_key,source_statement_id,work_id,contributor_party_id,currency,original_payable)
            VALUES(%s,%s,%s,%s,%s,'USD',%s) ON CONFLICT(tenant_id,source_system,source_obligation_key) DO NOTHING''',
            (tenant,str(row['statement_id']),row['statement_id'],row['work_id'],inst['contributor_party_id'],row['original_payable']))
        cur.execute('SELECT id FROM financial_payments.payment_obligations WHERE tenant_id=%s AND source_obligation_key=%s',(tenant,str(row['statement_id'])))
        oid=cur.fetchone()['id']
        for role,pid,percent,amount in split_amount(item.amount,inst):
            rec=next(r for r in row['recipients'] if r['party_id']==pid); dest=rec['destination']; aid=uuid4()
            cur.execute('''INSERT INTO financial_payments.payment_allocations
                (id,tenant_id,batch_id,obligation_id,recipient_id,amount,allocation_percent,instruction_snapshot,source_snapshot)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                (aid,tenant,bid,oid,dest['recipient_id'],amount,percent,
                 Jsonb(serial(dict(instruction=inst,role=role,party_id=pid,secure_account=rec['secure_account']))),
                 Jsonb(dict(fingerprint=row['source_fingerprint'],statement_id=str(row['statement_id'])))))
            key=(dest['recipient_id'],dest['id'])
            transfers.setdefault(key,[]).append((aid,amount))
    for (rid,dest),allocations in transfers.items():
        tid=uuid4(); amount=sum((a[1] for a in allocations),ZERO)
        cur.execute('''INSERT INTO financial_payments.payment_transfers
            (id,tenant_id,batch_id,recipient_id,destination_account_id,amount,currency,provider_code,idempotency_key)
            VALUES(%s,%s,%s,%s,%s,%s,'USD','mock',%s)''',(tid,tenant,bid,rid,dest,amount,uuid4()))
        for aid,amount in allocations:
            cur.execute('''INSERT INTO financial_payments.transfer_allocations
                (tenant_id,transfer_id,allocation_id,batch_id,recipient_id,amount) VALUES(%s,%s,%s,%s,%s,%s)''',(tenant,tid,aid,bid,rid,amount))
    return batch_detail(cur,tenant,bid)


def batch_detail(cur,tenant,bid):
    cur.execute('SELECT * FROM financial_payments.payment_batches WHERE tenant_id=%s AND id=%s',(tenant,bid))
    batch=cur.fetchone()
    if not batch: raise HTTPException(404,'Batch not found.')
    cur.execute('''SELECT a.*,o.source_statement_id,o.work_id,w.title,r.display_name FROM financial_payments.payment_allocations a
        JOIN financial_payments.payment_obligations o ON o.tenant_id=a.tenant_id AND o.id=a.obligation_id
        JOIN public.works w ON w.tenant_id=o.tenant_id AND w.id=o.work_id
        JOIN financial_payments.payment_recipients r ON r.tenant_id=a.tenant_id AND r.id=a.recipient_id
        WHERE a.tenant_id=%s AND a.batch_id=%s ORDER BY w.title,r.display_name''',(tenant,bid))
    allocations=cur.fetchall()
    cur.execute('''SELECT t.*,r.display_name,d.account_last4 FROM financial_payments.payment_transfers t
        JOIN financial_payments.payment_recipients r ON r.tenant_id=t.tenant_id AND r.id=t.recipient_id
        JOIN financial_payments.recipient_bank_accounts d ON d.tenant_id=t.tenant_id AND d.id=t.destination_account_id
        WHERE t.tenant_id=%s AND t.batch_id=%s ORDER BY r.display_name''',(tenant,bid))
    transfers=cur.fetchall()
    cur.execute('SELECT * FROM financial_payments.payment_approvals WHERE tenant_id=%s AND batch_id=%s ORDER BY decision_at',(tenant,bid))
    approvals=cur.fetchall()
    cur.execute('''SELECT e.* FROM financial_payments.payment_events e JOIN financial_payments.payment_transfers t
        ON t.tenant_id=e.tenant_id AND t.id=e.transfer_id WHERE e.tenant_id=%s AND t.batch_id=%s ORDER BY e.received_at''',(tenant,bid))
    events=cur.fetchall()
    cur.execute('''SELECT r.* FROM financial_payments.payment_reconciliations r JOIN financial_payments.payment_transfers t
        ON t.tenant_id=r.tenant_id AND t.id=r.transfer_id WHERE r.tenant_id=%s AND t.batch_id=%s ORDER BY r.created_at''',(tenant,bid))
    return dict(batch=batch,allocations=allocations,transfers=transfers,approvals=approvals,events=events,reconciliations=cur.fetchall())


def revalidate(cur,user,detail):
    rows={str(r['statement_id']):r for r in payables(cur,user)}
    totals={}
    for a in detail['allocations']:
        sid=str(a['source_statement_id']); row=rows.get(sid)
        if not row or row['source_fingerprint']!=a['source_snapshot']['fingerprint']:
            raise HTTPException(409,'Royalty source changed. Cancel this batch and prepare it again.')
        snap=a['instruction_snapshot']; rec=next((r for r in row['recipients'] if str(r['party_id'])==snap['party_id']),None)
        if (serial(row['instruction'])!=snap['instruction'] or not rec or not rec['destination'] or
            serial(rec['secure_account'])!=snap['secure_account']):
            raise HTTPException(409,'Instructions or banking destination changed. Cancel and prepare a new batch.')
        totals[sid]=totals.get(sid,ZERO)+a['amount']
    for sid,amount in totals.items():
        row=rows[sid]
        if amount>row['outstanding']-row['simulated_settled'] or row['active_reservations']>row['outstanding']-row['simulated_settled']:
            raise HTTPException(409,'The batch now exceeds the outstanding royalty account balance.')


def batch_action(cur,user,bid,action,comment=''):
    tenant=user['tenant_id']; detail=batch_detail(cur,tenant,bid); b=detail['batch']; policy=settings(cur,tenant)
    if action=='cancel':
        if b['status']=='canceled': return detail
        if b['status'] not in ('draft','pending_approval','approved'): raise HTTPException(409,'Submitted batches cannot be canceled here.')
        cur.execute("UPDATE financial_payments.payment_allocations SET status='released' WHERE tenant_id=%s AND batch_id=%s",(tenant,bid))
        cur.execute("UPDATE financial_payments.payment_transfers SET status='canceled' WHERE tenant_id=%s AND batch_id=%s",(tenant,bid))
        status='canceled'
    elif action=='submit-for-approval':
        if b['status']=='pending_approval': return detail
        if b['status']!='draft': raise HTTPException(409,'Only a draft can be submitted for approval.')
        revalidate(cur,user,detail); status='pending_approval'
    elif action in ('approve','reject'):
        if user['role']!='tenant_admin': raise HTTPException(403,'A tenant administrator must authorize payments.')
        if b['status']!='pending_approval': raise HTTPException(409,'This batch is not awaiting approval.')
        revalidate(cur,user,detail)
        dual=policy['dual_approval_required'] and b['total_amount']>=policy['approval_threshold']
        if action=='approve' and dual and b['created_by']==user['user_id']:
            raise HTTPException(403,'Dual approval requires approvers other than the batch creator.')
        cur.execute('''INSERT INTO financial_payments.payment_approvals(tenant_id,batch_id,actor_user_id,decision,comment)
            VALUES(%s,%s,%s,%s,%s) ON CONFLICT(tenant_id,batch_id,actor_user_id) DO NOTHING''',
            (tenant,bid,user['user_id'],'approved' if action=='approve' else 'rejected',comment))
        if action=='reject':
            return batch_action(cur,user,bid,'cancel')
        cur.execute("SELECT count(*) AS count FROM financial_payments.payment_approvals WHERE tenant_id=%s AND batch_id=%s AND decision='approved'",(tenant,bid))
        status='approved' if cur.fetchone()['count'] >= (2 if dual else 1) else 'pending_approval'
        if status=='approved':
            cur.execute('UPDATE financial_payments.payment_batches SET approved_at=now() WHERE tenant_id=%s AND id=%s',(tenant,bid))
    elif action=='execute':
        if user['role']!='tenant_admin': raise HTTPException(403,'A tenant administrator must execute payments.')
        if b['status']=='processing': return detail
        if b['status']!='approved': raise HTTPException(409,'Approve this batch before execution.')
        revalidate(cur,user,detail)
        account=funding(cur,tenant,b['funding_account_id'])
        if not account: raise HTTPException(409,'Funding account is unavailable.')
        provider=connector(account['provider_code'])
        if not provider.simulated: raise HTTPException(409,'Live execution is disabled.')
        refresh_balance(cur,tenant,account); bank=bank_summary(cur,tenant,account['id'])
        # Prepared commitments already include this batch: do not subtract it twice.
        if not bank['funding_verified'] or bank['available_to_allocate']<ZERO:
            raise HTTPException(409,'Insufficient funding after all commitments and operating reserve.')
        for transfer in detail['transfers']:
            result=provider.submit_credit(transfer)
            cur.execute("UPDATE financial_payments.payment_transfers SET status='submitted',provider_transfer_id=%s,submitted_at=now() WHERE tenant_id=%s AND id=%s",(result['provider_transfer_id'],tenant,transfer['id']))
            event(cur,tenant,transfer['id'],'submitted','submission:'+str(transfer['id']))
        cur.execute("UPDATE financial_payments.payment_allocations SET status='submitted' WHERE tenant_id=%s AND batch_id=%s",(tenant,bid))
        cur.execute('UPDATE financial_payments.payment_batches SET submitted_at=now() WHERE tenant_id=%s AND id=%s',(tenant,bid))
        status='processing'
    else: raise HTTPException(404,'Unknown batch action.')
    cur.execute('UPDATE financial_payments.payment_batches SET status=%s WHERE tenant_id=%s AND id=%s',(status,tenant,bid))
    return batch_detail(cur,tenant,bid)


def event(cur,tenant,tid,status,event_id):
    # Whitelist payload fields; provider secrets/account numbers cannot enter JSON.
    cur.execute('''INSERT INTO financial_payments.payment_events(tenant_id,transfer_id,event_type,provider_event_id,event_payload)
        VALUES(%s,%s,%s,%s,%s) ON CONFLICT(tenant_id,provider_event_id) DO NOTHING RETURNING id''',
        (tenant,tid,status,event_id,Jsonb(dict(status=status,simulated=True))))
    return bool(cur.fetchone())


def simulate_event(cur,user,tid,status,event_id):
    tenant=user['tenant_id']
    cur.execute('SELECT * FROM financial_payments.payment_transfers WHERE tenant_id=%s AND id=%s FOR UPDATE',(tenant,tid))
    t=cur.fetchone()
    if not t: raise HTTPException(404,'Transfer not found.')
    if t['status']=='created': raise HTTPException(409,'Submit the authorized batch before simulating bank events.')
    cur.execute('SELECT transfer_id,event_type FROM financial_payments.payment_events WHERE tenant_id=%s AND provider_event_id=%s',(tenant,event_id))
    old=cur.fetchone()
    if old:
        if old['transfer_id']!=tid or old['event_type']!=status: raise HTTPException(409,'Provider event identity conflicts with prior event.')
        return batch_detail(cur,tenant,t['batch_id'])
    connector(t['provider_code']).parse_event(t,status,event_id)
    event(cur,tenant,tid,status,event_id)
    target=next_status(t['status'],status)
    if target!=t['status']:
        cur.execute('''SELECT a.* FROM financial_payments.transfer_allocations m
            JOIN financial_payments.payment_allocations a ON a.tenant_id=m.tenant_id AND a.id=m.allocation_id
            WHERE m.tenant_id=%s AND m.transfer_id=%s''',(tenant,tid))
        allocations=cur.fetchall()
        if target in ('settled','returned'):
            # Append shadow postings only. Never touch the real royalty ledger.
            actions=['settlement'] if target=='settled' else ['settlement','reversal']
            for a in allocations:
                for action in actions:
                    cur.execute('''INSERT INTO financial_payments.payment_reconciliations
                        (tenant_id,allocation_id,transfer_id,action,amount,status,posted_at)
                        VALUES(%s,%s,%s,%s,%s,'posted',now()) ON CONFLICT(tenant_id,allocation_id,transfer_id,action) DO NOTHING''',
                        (tenant,a['id'],tid,action,a['amount']))
        allocation_status={'settled':'settled','returned':'returned','failed':'released','canceled':'released'}.get(target,'submitted')
        for a in allocations:
            cur.execute('UPDATE financial_payments.payment_allocations SET status=%s WHERE tenant_id=%s AND id=%s',(allocation_status,tenant,a['id']))
        cur.execute("UPDATE financial_payments.payment_transfers SET status=%s,settled_at=CASE WHEN %s='settled' THEN now() ELSE settled_at END WHERE tenant_id=%s AND id=%s",(target,target,tenant,tid))
    cur.execute('SELECT status FROM financial_payments.payment_transfers WHERE tenant_id=%s AND batch_id=%s',(tenant,t['batch_id']))
    states=[r['status'] for r in cur.fetchall()]
    terminal={'settled','failed','returned','canceled'}
    batch_status='processing'
    if all(s in terminal for s in states):
        batch_status='completed' if all(s=='settled' for s in states) else 'partially_failed' if any(s=='settled' for s in states) else 'failed'
    cur.execute('UPDATE financial_payments.payment_batches SET status=%s WHERE tenant_id=%s AND id=%s',(batch_status,tenant,t['batch_id']))
    account=funding(cur,tenant,batch_detail(cur,tenant,t['batch_id'])['batch']['funding_account_id'])
    if account: refresh_balance(cur,tenant,account)
    return batch_detail(cur,tenant,t['batch_id'])
