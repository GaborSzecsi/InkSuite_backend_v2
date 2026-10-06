-- Subrights always use publisher net receipts. Repair only basis values;
-- preserve rates, income records, and all frozen statement headers/lines.
BEGIN;

UPDATE royalty_rules
SET base = 'net_receipts'
WHERE rights_type = 'subrights' AND base IS DISTINCT FROM 'net_receipts';

UPDATE royalty_tiers t
SET base = 'net_receipts'
FROM royalty_rules r
WHERE r.id = t.rule_id AND r.rights_type = 'subrights'
  AND t.base IS DISTINCT FROM 'net_receipts';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'royalty_rules'::regclass
          AND conname = 'royalty_rules_subrights_net_receipts'
    ) THEN
        ALTER TABLE royalty_rules ADD CONSTRAINT royalty_rules_subrights_net_receipts
        CHECK (rights_type <> 'subrights' OR (base IS NOT NULL AND base = 'net_receipts'));
    END IF;
END
$$;

COMMIT;
