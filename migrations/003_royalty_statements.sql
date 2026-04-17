-- Persisted royalty statement headers + optional line snapshot for carry-forward balances.
-- running_balance matches RoyaltyCalculator cumulative balance (feeds next period last_balance).

CREATE TABLE IF NOT EXISTS royalty_statements (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    work_id uuid NOT NULL REFERENCES works (id) ON DELETE CASCADE,
    party text NOT NULL CHECK (party IN ('author', 'illustrator')),
    period_start date NOT NULL,
    period_end date NOT NULL,
    opening_recoupment_balance numeric(14, 2) NOT NULL DEFAULT 0,
    earned_this_period numeric(14, 2) NOT NULL DEFAULT 0,
    recouped_this_period numeric(14, 2) NOT NULL DEFAULT 0,
    adjustments_this_period numeric(14, 2) NOT NULL DEFAULT 0,
    closing_recoupment_balance numeric(14, 2) NOT NULL DEFAULT 0,
    payable_this_period numeric(14, 2) NOT NULL DEFAULT 0,
    running_balance numeric(14, 2),
    status text NOT NULL DEFAULT 'draft',
    party_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT royalty_statements_period_unique UNIQUE (tenant_id, work_id, party, period_start, period_end)
);

CREATE INDEX IF NOT EXISTS idx_royalty_statements_work_party
    ON royalty_statements (tenant_id, work_id, party, period_end DESC);
