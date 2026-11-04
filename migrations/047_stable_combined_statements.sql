BEGIN;
ALTER TABLE stable_imports DROP CONSTRAINT stable_imports_tenant_id_content_hash_as_of_key;
ALTER TABLE stable_imports ADD CONSTRAINT stable_imports_source_territory_key
 UNIQUE NULLS NOT DISTINCT(tenant_id,content_hash,as_of,territory);
COMMIT;
