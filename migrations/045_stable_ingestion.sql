-- Apply explicitly after backup and dry-run review. No automatic application startup DDL.
BEGIN;
DO $$ BEGIN
 IF to_regclass('public.canada_sales_lines') IS NULL THEN
   ALTER TABLE public.fraser_ca_sales_lines RENAME TO canada_sales_lines;
   CREATE VIEW public.fraser_ca_sales_lines WITH (security_invoker=true) AS SELECT * FROM public.canada_sales_lines;
 END IF;
END $$;
DO $$ DECLARE permission record; BEGIN
 FOR permission IN SELECT grantee,privilege_type,is_grantable FROM information_schema.role_table_grants
   WHERE table_schema='public' AND table_name='canada_sales_lines'
     AND privilege_type IN ('SELECT','INSERT','UPDATE','DELETE') LOOP
   EXECUTE format('GRANT %s ON public.fraser_ca_sales_lines TO %s%s',permission.privilege_type,
     CASE WHEN permission.grantee='PUBLIC' THEN 'PUBLIC' ELSE quote_ident(permission.grantee) END,
     CASE WHEN permission.is_grantable='YES' THEN ' WITH GRANT OPTION' ELSE '' END);
 END LOOP;
END $$;
CREATE TABLE stable_sources (
 tenant_id uuid PRIMARY KEY REFERENCES tenants(id), bucket text NOT NULL,
 key_prefix text NOT NULL, enabled boolean NOT NULL DEFAULT false,
 us_currency text NOT NULL CHECK(us_currency ~ '^[A-Z]{3}$'),
 ca_currency text NOT NULL CHECK(ca_currency ~ '^[A-Z]{3}$'),
 inventory_connection_id uuid,
 usable_inventory_confirmed boolean NOT NULL DEFAULT false,
 FOREIGN KEY(tenant_id,inventory_connection_id) REFERENCES distribution_connections(tenant_id,id)
);
CREATE TABLE stable_imports (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id uuid NOT NULL REFERENCES tenants(id),
 bucket text NOT NULL, object_key text NOT NULL, version_id text, content_hash text NOT NULL,
 parser_version text NOT NULL, report_type text NOT NULL,
 territory text, period_code text, as_of date,
 status text NOT NULL CHECK(status IN ('needs_review','validated','posted','failed','superseded')),
 issues jsonb NOT NULL DEFAULT '[]', payload jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(), posted_at timestamptz,
 UNIQUE NULLS NOT DISTINCT(tenant_id,content_hash,as_of), UNIQUE(tenant_id,id)
);
CREATE UNIQUE INDEX stable_one_posted_statement ON stable_imports(tenant_id,period_code,territory)
 WHERE status='posted' AND report_type='statement';
CREATE INDEX stable_import_queue ON stable_imports(tenant_id,status,created_at);
CREATE TABLE stable_receipt_adjustments (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id uuid NOT NULL, import_id uuid NOT NULL,
 edition_id uuid NOT NULL, territory text NOT NULL, currency text NOT NULL, period_id uuid NOT NULL REFERENCES royalty_periods(id),
 kind text NOT NULL, amount numeric NOT NULL, billed_inventory numeric,
 UNIQUE(import_id,edition_id,kind),
 FOREIGN KEY(tenant_id,import_id) REFERENCES stable_imports(tenant_id,id),
 FOREIGN KEY(tenant_id,edition_id) REFERENCES editions(tenant_id,id)
);
CREATE TABLE stable_inventory_snapshots (
 tenant_id uuid NOT NULL, import_id uuid NOT NULL, edition_id uuid NOT NULL,
 as_of date NOT NULL, available integer NOT NULL CHECK(available>=0), ending integer NOT NULL CHECK(ending>=0),
 PRIMARY KEY(import_id,edition_id),
 FOREIGN KEY(tenant_id,import_id) REFERENCES stable_imports(tenant_id,id),
 FOREIGN KEY(tenant_id,edition_id) REFERENCES editions(tenant_id,id)
);
-- All consumers enforce tenant_id. No public/anonymous grants are introduced.
COMMIT;
