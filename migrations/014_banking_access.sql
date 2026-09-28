-- Additive: existing secure_payments profiles, accounts and encryption are unchanged.
BEGIN;
CREATE TABLE IF NOT EXISTS secure_payments.banking_authenticators (
 tenant_id uuid NOT NULL REFERENCES public.tenants(id),
 user_id uuid NOT NULL REFERENCES public.users(id),
 secret_ciphertext bytea NOT NULL,
 confirmed_at timestamptz,
 setup_expires_at timestamptz NOT NULL,
 last_step bigint NOT NULL DEFAULT -1,
 failed_attempts integer NOT NULL DEFAULT 0 CHECK (failed_attempts >= 0),
 locked_until timestamptz,
 recovery_hashes jsonb NOT NULL DEFAULT '[]'::jsonb,
 PRIMARY KEY (tenant_id,user_id)
);
CREATE TABLE IF NOT EXISTS secure_payments.banking_access_audit (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 tenant_id uuid NOT NULL REFERENCES public.tenants(id),
 user_id uuid NOT NULL REFERENCES public.users(id),
 account_id uuid,
 action text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS banking_access_audit_tenant_time
 ON secure_payments.banking_access_audit(tenant_id,created_at DESC);
COMMIT;
