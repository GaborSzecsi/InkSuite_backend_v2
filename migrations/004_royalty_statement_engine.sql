-- Engine-ready royalty statements + frozen lines (first_rights).
-- Safe to run on existing DBs: uses IF NOT EXISTS / IF NOT EXISTS columns.

ALTER TABLE royalty_statements
    ADD COLUMN IF NOT EXISTS period_id uuid REFERENCES royalty_periods (id),
    ADD COLUMN IF NOT EXISTS royalty_set_id uuid REFERENCES royalty_sets (id) ON DELETE CASCADE;

CREATE UNIQUE INDEX IF NOT EXISTS ux_royalty_statements_engine_ctx
    ON royalty_statements (tenant_id, work_id, royalty_set_id, party, period_id)
    WHERE period_id IS NOT NULL AND royalty_set_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_royalty_statements_period_party
    ON royalty_statements (tenant_id, work_id, royalty_set_id, party, period_end DESC);

CREATE TABLE IF NOT EXISTS royalty_statement_lines (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid (),
    tenant_id uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    statement_id uuid NOT NULL REFERENCES royalty_statements (id) ON DELETE CASCADE,
    line_type text NOT NULL DEFAULT 'first_rights',
    category_label text NOT NULL,
    edition_id uuid REFERENCES editions (id) ON DELETE SET NULL,
    units_sold numeric(18, 6) NOT NULL DEFAULT 0,
    units_returned numeric(18, 6) NOT NULL DEFAULT 0,
    net_units numeric(18, 6) NOT NULL DEFAULT 0,
    basis_amount numeric(18, 6) NOT NULL DEFAULT 0,
    royalty_rate numeric(18, 8) NOT NULL DEFAULT 0,
    royalty_amount numeric(18, 6) NOT NULL DEFAULT 0,
    applied_rule_id uuid REFERENCES royalty_rules (id) ON DELETE SET NULL,
    applied_tier_id uuid REFERENCES royalty_tiers (id) ON DELETE SET NULL,
    frozen_list_price_usd numeric(18, 6),
    created_at timestamptz NOT NULL DEFAULT now ()
);

CREATE INDEX IF NOT EXISTS idx_royalty_statement_lines_stmt
    ON royalty_statement_lines (statement_id);

ALTER TABLE royalty_sales_lines
    ADD COLUMN IF NOT EXISTS period_id uuid REFERENCES royalty_periods (id);

CREATE INDEX IF NOT EXISTS idx_royalty_sales_lines_period_edition
    ON royalty_sales_lines (tenant_id, period_id, edition_id)
    WHERE period_id IS NOT NULL;
