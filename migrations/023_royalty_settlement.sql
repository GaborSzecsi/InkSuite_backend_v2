-- Add frozen settlement detail; existing final statements remain untouched.
BEGIN;
ALTER TABLE royalty_statements ADD COLUMN IF NOT EXISTS settlement jsonb;
ALTER TABLE royalty_account_settings ALTER COLUMN minimum_payout SET DEFAULT 50;
COMMIT;
