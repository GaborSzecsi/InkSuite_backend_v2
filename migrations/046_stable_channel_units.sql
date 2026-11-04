BEGIN;
ALTER TABLE royalty_sales_lines ADD COLUMN unit_adjustment numeric NOT NULL DEFAULT 0;
ALTER TABLE canada_sales_lines ADD COLUMN unit_adjustment numeric NOT NULL DEFAULT 0;
CREATE TABLE stable_channel_movements (
 tenant_id uuid NOT NULL, import_id uuid NOT NULL, edition_id uuid NOT NULL,
 territory text NOT NULL CHECK(territory IN ('US','CA')), buyer text NOT NULL,
 outbound numeric NOT NULL CHECK(outbound>=0), returned numeric NOT NULL CHECK(returned>=0),
 PRIMARY KEY(import_id,edition_id,territory,buyer),
 FOREIGN KEY(tenant_id,import_id) REFERENCES stable_imports(tenant_id,id),
 FOREIGN KEY(tenant_id,edition_id) REFERENCES editions(tenant_id,id)
);
CREATE INDEX stable_channel_lookup ON stable_channel_movements(tenant_id,territory,edition_id);
CREATE OR REPLACE VIEW fraser_ca_sales_lines WITH (security_invoker=true) AS SELECT * FROM canada_sales_lines;
-- Existing view grants are preserved.
-- Explicitly grant the worker only the new operations it requires.
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='inksuite_stable_worker') THEN
  GRANT SELECT,INSERT ON stable_channel_movements TO inksuite_stable_worker;
 END IF;
END $$;
COMMIT;
