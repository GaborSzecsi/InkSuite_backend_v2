-- Read-only preflight. Run against inksuite, not the default postgres database.
BEGIN READ ONLY;
SELECT current_database(),current_user,version();
SELECT table_name,column_name,data_type,is_nullable,column_default FROM information_schema.columns
 WHERE table_schema='public' AND table_name IN ('royalty_sales_lines','fraser_ca_sales_lines','canada_sales_lines',
 'royalty_periods','royalty_statements','distribution_inventory','distribution_connections','editions')
 ORDER BY table_name,ordinal_position;
SELECT c.relname,c.relrowsecurity,c.relforcerowsecurity,p.polname,pg_get_expr(p.polqual,p.polrelid) AS policy
 FROM pg_class c LEFT JOIN pg_policy p ON p.polrelid=c.oid
 WHERE c.relnamespace='public'::regnamespace AND c.relname IN ('royalty_sales_lines','fraser_ca_sales_lines','canada_sales_lines');
SELECT conrelid::regclass,conname,pg_get_constraintdef(oid) FROM pg_constraint
 WHERE conrelid IN (to_regclass('public.royalty_sales_lines'),to_regclass('public.fraser_ca_sales_lines'),to_regclass('public.canada_sales_lines'));
SELECT table_name,grantee,privilege_type FROM information_schema.role_table_grants
 WHERE table_schema='public' AND table_name IN ('royalty_sales_lines','fraser_ca_sales_lines','canada_sales_lines');
SELECT source_system,transaction_type,currency,count(*) FROM royalty_sales_lines GROUP BY 1,2,3 ORDER BY 1,2,3;
SELECT tenant_id,id,display_name,adapter,enabled,inventory_enabled,mode FROM distribution_connections ORDER BY tenant_id,display_name;
ROLLBACK;
