-- Additive setup for recorded manual payments. No payments or paid statuses are inferred.
BEGIN;
ALTER TABLE royalty_statements ADD COLUMN IF NOT EXISTS currency text NOT NULL DEFAULT 'USD';
CREATE TABLE IF NOT EXISTS royalty_payments (
    id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL REFERENCES tenants(id),
    statement_id uuid NOT NULL REFERENCES royalty_statements(id),
    payee_party_id uuid REFERENCES parties(id),
    payee_role text NOT NULL CHECK (payee_role IN ('contributor','agency')),
    payment_date date NOT NULL,
    amount numeric(14,2) NOT NULL CHECK (amount > 0),
    currency text NOT NULL DEFAULT 'USD',
    payment_method text NOT NULL,
    reference_number text NOT NULL,
    notes text,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS royalty_payments_statement_tracking_idx
    ON royalty_payments(tenant_id,statement_id,currency);
COMMIT;
