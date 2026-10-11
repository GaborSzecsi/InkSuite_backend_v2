-- Additive payment orchestration. No live bank connection or financial data is seeded.
BEGIN;
CREATE SCHEMA IF NOT EXISTS financial_payments;

CREATE TABLE financial_payments.payment_settings (
 tenant_id uuid PRIMARY KEY REFERENCES public.tenants(id),
 operating_reserve numeric(20,2) NOT NULL DEFAULT 0 CHECK(operating_reserve>=0),
 dual_approval_required boolean NOT NULL DEFAULT true,
 approval_threshold numeric(20,2) NOT NULL DEFAULT 0 CHECK(approval_threshold>=0),
 balance_max_age_minutes integer NOT NULL DEFAULT 15 CHECK(balance_max_age_minutes BETWEEN 1 AND 1440),
 live_payments_enabled boolean NOT NULL DEFAULT false CHECK(NOT live_payments_enabled),
 updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE financial_payments.bank_connections (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id uuid NOT NULL REFERENCES public.tenants(id),
 provider_code text NOT NULL CHECK(provider_code='mock'), display_name text NOT NULL,
 external_connection_id text NOT NULL, credential_secret_ref text,
 environment text NOT NULL DEFAULT 'sandbox' CHECK(environment='sandbox'),
 status text NOT NULL CHECK(status IN ('connected','disconnected','restricted','error','revoked')),
 capabilities jsonb NOT NULL, connected_at timestamptz NOT NULL DEFAULT now(),
 last_checked_at timestamptz, created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(tenant_id,id)
);
CREATE TABLE financial_payments.funding_accounts (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id uuid NOT NULL REFERENCES public.tenants(id),
 connection_id uuid NOT NULL, external_account_id text NOT NULL, display_name text NOT NULL,
 bank_name text, account_last4 varchar(4), currency text NOT NULL CHECK(currency='USD'),
 is_default boolean NOT NULL DEFAULT false, status text NOT NULL DEFAULT 'active' CHECK(status IN ('active','disabled')),
 mock_opening_balance numeric(20,2) NOT NULL CHECK(mock_opening_balance>=0),
 created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(tenant_id,id), UNIQUE(tenant_id,connection_id,external_account_id),
 FOREIGN KEY(tenant_id,connection_id) REFERENCES financial_payments.bank_connections(tenant_id,id)
);
CREATE UNIQUE INDEX financial_funding_default ON financial_payments.funding_accounts(tenant_id,currency) WHERE is_default AND status='active';
CREATE TABLE financial_payments.bank_balance_snapshots (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id uuid NOT NULL,
 funding_account_id uuid NOT NULL, currency text NOT NULL CHECK(currency='USD'),
 available_balance numeric(20,2), current_balance numeric(20,2),
 balance_as_of timestamptz, retrieved_at timestamptz NOT NULL DEFAULT now(), provider_reference text,
 commitments_reflected boolean NOT NULL DEFAULT false,
 FOREIGN KEY(tenant_id,funding_account_id) REFERENCES financial_payments.funding_accounts(tenant_id,id)
);
CREATE INDEX financial_balance_latest ON financial_payments.bank_balance_snapshots(tenant_id,funding_account_id,retrieved_at DESC);
CREATE TABLE financial_payments.payment_recipients (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id uuid NOT NULL REFERENCES public.tenants(id),
 party_id uuid NOT NULL REFERENCES public.parties(id), recipient_type text NOT NULL CHECK(recipient_type IN ('contributor','agency','other')),
 display_name text NOT NULL, status text NOT NULL DEFAULT 'active' CHECK(status IN ('active','blocked','inactive')),
 created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(tenant_id,id), UNIQUE(tenant_id,party_id)
);
CREATE TABLE financial_payments.recipient_bank_accounts (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id uuid NOT NULL, recipient_id uuid NOT NULL,
 secure_bank_account_ref uuid NOT NULL REFERENCES secure_payments.payment_accounts(id), provider_destination_ref text,
 account_last4 varchar(4), verification_status text NOT NULL CHECK(verification_status IN ('pending','verified','failed','revoked')),
 simulated boolean NOT NULL DEFAULT true CHECK(simulated), verified_at timestamptz,
 created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(tenant_id,id,recipient_id), UNIQUE(tenant_id,id),
 UNIQUE(tenant_id,recipient_id,secure_bank_account_ref),
 FOREIGN KEY(tenant_id,recipient_id) REFERENCES financial_payments.payment_recipients(tenant_id,id)
);
CREATE TABLE financial_payments.payment_obligations (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id uuid NOT NULL REFERENCES public.tenants(id),
 source_system text NOT NULL DEFAULT 'royalty', source_obligation_key text NOT NULL,
 source_statement_id uuid NOT NULL REFERENCES public.royalty_statements(id),
 work_id uuid NOT NULL REFERENCES public.works(id), contributor_party_id uuid REFERENCES public.parties(id),
 currency text NOT NULL CHECK(currency='USD'), original_payable numeric(20,2) NOT NULL CHECK(original_payable>=0),
 status text NOT NULL DEFAULT 'open' CHECK(status IN ('open','held','closed','void')),
 created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(tenant_id,id), UNIQUE(tenant_id,source_system,source_obligation_key)
);
CREATE TABLE financial_payments.payment_batches (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id uuid NOT NULL, funding_account_id uuid NOT NULL,
 currency text NOT NULL CHECK(currency='USD'), status text NOT NULL DEFAULT 'draft'
 CHECK(status IN ('draft','pending_approval','approved','processing','completed','partially_failed','failed','canceled')),
 total_amount numeric(20,2) NOT NULL CHECK(total_amount>0), idempotency_key uuid NOT NULL, request_payload jsonb NOT NULL,
 created_by uuid NOT NULL REFERENCES public.users(id), approved_at timestamptz, submitted_at timestamptz,
 simulated boolean NOT NULL DEFAULT true CHECK(simulated), created_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(tenant_id,id), UNIQUE(tenant_id,idempotency_key),
 FOREIGN KEY(tenant_id,funding_account_id) REFERENCES financial_payments.funding_accounts(tenant_id,id)
);
CREATE TABLE financial_payments.payment_allocations (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id uuid NOT NULL, batch_id uuid NOT NULL,
 obligation_id uuid NOT NULL, recipient_id uuid NOT NULL, amount numeric(20,2) NOT NULL CHECK(amount>0),
 allocation_percent numeric(9,6) NOT NULL CHECK(allocation_percent BETWEEN 0 AND 100), instruction_snapshot jsonb NOT NULL,
 source_snapshot jsonb NOT NULL, status text NOT NULL DEFAULT 'reserved'
 CHECK(status IN ('reserved','submitted','settled','released','returned')),
 created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(tenant_id,id), UNIQUE(tenant_id,id,batch_id,recipient_id),
 UNIQUE(tenant_id,batch_id,obligation_id,recipient_id),
 FOREIGN KEY(tenant_id,batch_id) REFERENCES financial_payments.payment_batches(tenant_id,id),
 FOREIGN KEY(tenant_id,obligation_id) REFERENCES financial_payments.payment_obligations(tenant_id,id),
 FOREIGN KEY(tenant_id,recipient_id) REFERENCES financial_payments.payment_recipients(tenant_id,id)
);
CREATE INDEX financial_active_reservations ON financial_payments.payment_allocations(tenant_id,obligation_id,status);
CREATE TABLE financial_payments.payment_transfers (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id uuid NOT NULL, batch_id uuid NOT NULL,
 recipient_id uuid NOT NULL, destination_account_id uuid NOT NULL, amount numeric(20,2) NOT NULL CHECK(amount>0),
 currency text NOT NULL CHECK(currency='USD'), provider_code text NOT NULL CHECK(provider_code='mock'),
 provider_transfer_id text, idempotency_key uuid NOT NULL,
 status text NOT NULL DEFAULT 'created' CHECK(status IN ('created','submitted','processing','settled','failed','returned','canceled','reconciliation_required')),
 submitted_at timestamptz, settled_at timestamptz, created_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(tenant_id,id), UNIQUE(tenant_id,id,batch_id,recipient_id), UNIQUE(tenant_id,idempotency_key),
 UNIQUE(tenant_id,provider_code,provider_transfer_id),
 FOREIGN KEY(tenant_id,batch_id) REFERENCES financial_payments.payment_batches(tenant_id,id),
 FOREIGN KEY(tenant_id,recipient_id) REFERENCES financial_payments.payment_recipients(tenant_id,id),
 FOREIGN KEY(tenant_id,destination_account_id,recipient_id) REFERENCES financial_payments.recipient_bank_accounts(tenant_id,id,recipient_id)
);
CREATE TABLE financial_payments.transfer_allocations (
 tenant_id uuid NOT NULL, transfer_id uuid NOT NULL, allocation_id uuid NOT NULL,
 batch_id uuid NOT NULL, recipient_id uuid NOT NULL, amount numeric(20,2) NOT NULL CHECK(amount>0),
 PRIMARY KEY(tenant_id,transfer_id,allocation_id), UNIQUE(tenant_id,allocation_id),
 FOREIGN KEY(tenant_id,transfer_id,batch_id,recipient_id) REFERENCES financial_payments.payment_transfers(tenant_id,id,batch_id,recipient_id),
 FOREIGN KEY(tenant_id,allocation_id,batch_id,recipient_id) REFERENCES financial_payments.payment_allocations(tenant_id,id,batch_id,recipient_id)
);
CREATE TABLE financial_payments.payment_events (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id uuid NOT NULL, transfer_id uuid NOT NULL,
 event_type text NOT NULL, provider_event_id text NOT NULL, event_payload jsonb NOT NULL,
 occurred_at timestamptz NOT NULL DEFAULT now(), received_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(tenant_id,provider_event_id),
 FOREIGN KEY(tenant_id,transfer_id) REFERENCES financial_payments.payment_transfers(tenant_id,id)
);
CREATE TABLE financial_payments.payment_approvals (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id uuid NOT NULL, batch_id uuid NOT NULL,
 actor_user_id uuid NOT NULL REFERENCES public.users(id), decision text NOT NULL CHECK(decision IN ('approved','rejected')),
 decision_at timestamptz NOT NULL DEFAULT now(), comment text, UNIQUE(tenant_id,batch_id,actor_user_id),
 FOREIGN KEY(tenant_id,batch_id) REFERENCES financial_payments.payment_batches(tenant_id,id)
);
CREATE TABLE financial_payments.payment_reconciliations (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id uuid NOT NULL, allocation_id uuid NOT NULL, transfer_id uuid NOT NULL,
 royalty_payment_id uuid REFERENCES public.royalty_payments(id), action text NOT NULL CHECK(action IN ('settlement','reversal')),
 amount numeric(20,2) NOT NULL CHECK(amount>0), status text NOT NULL CHECK(status IN ('pending','posted','failed')),
 simulated boolean NOT NULL DEFAULT true CHECK(simulated), posted_at timestamptz, error_detail text,
 created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(tenant_id,allocation_id,transfer_id,action),
 CHECK(royalty_payment_id IS NULL),
 FOREIGN KEY(tenant_id,allocation_id) REFERENCES financial_payments.payment_allocations(tenant_id,id),
 FOREIGN KEY(tenant_id,transfer_id) REFERENCES financial_payments.payment_transfers(tenant_id,id)
);

-- Check totals at commit, after all allocations and mappings have been inserted.
CREATE FUNCTION financial_payments.check_totals() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE tid uuid; bid uuid; expected numeric; actual numeric;
BEGIN
 tid:=NEW.tenant_id;
 IF TG_TABLE_NAME='payment_batches' THEN bid:=NEW.id; ELSE bid:=NEW.batch_id; END IF;
 SELECT total_amount INTO expected FROM financial_payments.payment_batches WHERE tenant_id=tid AND id=bid;
 SELECT COALESCE(sum(amount),0) INTO actual FROM financial_payments.payment_allocations WHERE tenant_id=tid AND batch_id=bid;
 IF actual<>expected THEN RAISE EXCEPTION 'Batch allocation total mismatch'; END IF;
 IF EXISTS(SELECT 1 FROM financial_payments.payment_transfers t
   WHERE t.tenant_id=tid AND t.batch_id=bid AND t.amount<>(SELECT COALESCE(sum(m.amount),0)
   FROM financial_payments.transfer_allocations m WHERE m.tenant_id=tid AND m.transfer_id=t.id))
 THEN RAISE EXCEPTION 'Transfer allocation total mismatch'; END IF;
 IF EXISTS(SELECT 1 FROM financial_payments.payment_allocations a WHERE a.tenant_id=tid AND a.batch_id=bid
   AND a.amount<>(SELECT COALESCE(sum(m.amount),0) FROM financial_payments.transfer_allocations m WHERE m.tenant_id=tid AND m.allocation_id=a.id))
 THEN RAISE EXCEPTION 'Allocation mapping mismatch'; END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER financial_batch_totals AFTER INSERT OR UPDATE ON financial_payments.payment_batches
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION financial_payments.check_totals();
CREATE CONSTRAINT TRIGGER financial_allocation_totals AFTER INSERT OR UPDATE ON financial_payments.payment_allocations
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION financial_payments.check_totals();
CREATE CONSTRAINT TRIGGER financial_transfer_totals AFTER INSERT OR UPDATE ON financial_payments.payment_transfers
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION financial_payments.check_totals();
CREATE CONSTRAINT TRIGGER financial_mapping_totals AFTER INSERT OR UPDATE ON financial_payments.transfer_allocations
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION financial_payments.check_totals();

CREATE FUNCTION financial_payments.protect_history() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'Payment history cannot be deleted'; END IF;
 IF TG_TABLE_NAME IN ('payment_events','payment_approvals','payment_reconciliations','bank_balance_snapshots','transfer_allocations','payment_obligations','recipient_bank_accounts') THEN
   RAISE EXCEPTION 'Financial history is append-only';
 ELSIF TG_TABLE_NAME='payment_allocations' AND (to_jsonb(OLD)-'status')<>(to_jsonb(NEW)-'status') THEN
   RAISE EXCEPTION 'Payment allocation snapshot is immutable';
 ELSIF TG_TABLE_NAME='payment_transfers' AND
   (to_jsonb(OLD)-ARRAY['status','provider_transfer_id','submitted_at','settled_at'])<>(to_jsonb(NEW)-ARRAY['status','provider_transfer_id','submitted_at','settled_at']) THEN
   RAISE EXCEPTION 'Transfer instructions are immutable';
 ELSIF TG_TABLE_NAME='payment_batches' AND
   (to_jsonb(OLD)-ARRAY['status','approved_at','submitted_at'])<>(to_jsonb(NEW)-ARRAY['status','approved_at','submitted_at']) THEN
   RAISE EXCEPTION 'Batch authorization contents are immutable; cancel and prepare a new batch';
 END IF;
 RETURN NEW;
END $$;
DO $$ DECLARE tab text; BEGIN
 FOREACH tab IN ARRAY ARRAY['payment_batches','payment_allocations','payment_transfers','transfer_allocations','payment_events','payment_approvals','payment_reconciliations','bank_balance_snapshots','payment_obligations','recipient_bank_accounts'] LOOP
  EXECUTE format('CREATE TRIGGER protect_financial_history BEFORE UPDATE OR DELETE ON financial_payments.%I FOR EACH ROW EXECUTE FUNCTION financial_payments.protect_history()',tab);
 END LOOP;
END $$;

CREATE FUNCTION financial_payments.check_source_owner() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_TABLE_NAME='payment_recipients' THEN
  IF NOT EXISTS(SELECT 1 FROM public.parties p WHERE p.id=NEW.party_id AND p.tenant_id=NEW.tenant_id) THEN
   RAISE EXCEPTION 'Recipient tenant mismatch';
  END IF;
 ELSIF TG_TABLE_NAME='payment_obligations' THEN
  IF NOT EXISTS(SELECT 1 FROM public.royalty_statements s JOIN public.works w ON w.id=s.work_id AND w.tenant_id=s.tenant_id
   WHERE s.id=NEW.source_statement_id AND s.tenant_id=NEW.tenant_id AND s.work_id=NEW.work_id AND s.currency=NEW.currency AND s.status='final')
  OR (NEW.contributor_party_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM public.parties p WHERE p.id=NEW.contributor_party_id AND p.tenant_id=NEW.tenant_id))
  THEN RAISE EXCEPTION 'Obligation source tenant mismatch'; END IF;
 ELSIF TG_TABLE_NAME='recipient_bank_accounts' THEN
  IF NOT EXISTS(SELECT 1 FROM secure_payments.payment_accounts a JOIN secure_payments.payment_profiles p ON p.id=a.payment_profile_id
    JOIN public.tenants t ON t.slug=p.tenant_id
    JOIN financial_payments.payment_recipients r ON r.tenant_id=t.id AND r.party_id::text=p.recipient_id
    WHERE a.id=NEW.secure_bank_account_ref AND t.id=NEW.tenant_id AND r.id=NEW.recipient_id AND a.is_active
    AND p.status='COMPLETE' AND a.payment_method='ACH' AND a.country_code='US' AND a.currency='USD'
    AND p.recipient_type=CASE WHEN r.recipient_type='agency' THEN 'AGENCY' ELSE 'CONTRIBUTOR' END)
  THEN RAISE EXCEPTION 'Secure banking identity mismatch'; END IF;
 ELSIF TG_TABLE_NAME='payment_batches' THEN
  IF NOT EXISTS(SELECT 1 FROM public.memberships m WHERE m.tenant_id=NEW.tenant_id AND m.user_id=NEW.created_by)
  THEN RAISE EXCEPTION 'Batch creator tenant mismatch'; END IF;
 ELSIF TG_TABLE_NAME='payment_approvals' THEN
  IF NOT EXISTS(SELECT 1 FROM public.memberships m WHERE m.tenant_id=NEW.tenant_id AND m.user_id=NEW.actor_user_id AND m.role='tenant_admin')
  THEN RAISE EXCEPTION 'Approver tenant mismatch'; END IF;
 END IF;
 RETURN NEW;
END $$;
DO $$ DECLARE tab text; BEGIN
 FOREACH tab IN ARRAY ARRAY['payment_recipients','payment_obligations','recipient_bank_accounts','payment_batches','payment_approvals'] LOOP
  EXECUTE format('CREATE TRIGGER financial_source_owner BEFORE INSERT OR UPDATE ON financial_payments.%I FOR EACH ROW EXECUTE FUNCTION financial_payments.check_source_owner()',tab);
 END LOOP;
END $$;
COMMIT;
