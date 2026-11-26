BEGIN;
ALTER TABLE distribution_shipment_feeds
    ADD COLUMN automation_enabled boolean NOT NULL DEFAULT false,
    ADD COLUMN automation_interval_minutes integer NOT NULL DEFAULT 15
        CHECK (automation_interval_minutes BETWEEN 5 AND 1440),
    ADD COLUMN automation_actor uuid,
    ADD COLUMN automation_location_id text,
    ADD COLUMN automation_due_at timestamptz,
    ADD COLUMN automation_lease uuid,
    ADD COLUMN automation_last_run_at timestamptz,
    ADD COLUMN automation_result jsonb NOT NULL DEFAULT '{}',
    ADD CHECK (NOT automation_enabled OR (automation_actor IS NOT NULL AND automation_location_id IS NOT NULL AND automation_due_at IS NOT NULL));
CREATE INDEX distribution_shipment_automation_due
    ON distribution_shipment_feeds (automation_due_at) WHERE automation_enabled;

CREATE TABLE distribution_shipment_automation_rows (
    tenant_id uuid NOT NULL,
    connection_id uuid NOT NULL,
    row_key text NOT NULL,
    retry_at timestamptz,
    PRIMARY KEY (tenant_id, connection_id, row_key),
    FOREIGN KEY (tenant_id, connection_id)
        REFERENCES distribution_shipment_feeds (tenant_id, connection_id) ON DELETE CASCADE
);
COMMIT;
