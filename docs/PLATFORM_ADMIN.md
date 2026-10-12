# Platform administration

Apply `migrations/031_platform_admin.sql` before deploying the new backend and frontend. The migration creates two independent audit/usage tables and does not create tenants or copy publisher data. Without it, the new API returns a specific 503 migration message.

The console is at `/app/platform`. Choose **Platform administrator** at sign-in (company `inksuite`). The backend verifies `users.platform_role = superadmin`; choosing an account type does not grant a role. The frontend hides publisher navigation in platform mode. To work in an authorized publisher, sign out and sign in as Publisher with that company's slug. The selected company is stored in an HTTP-only cookie and filtered against memberships returned by the backend, never treated as an authorization grant.

## Operations

- Create a tenant: valid lowercase slug, unique name identifier, and creator administrator membership in one transaction. No copying or invitations are performed. Create `InkSuite Demo` / `inksuite-demo` for reviewer content.
- Delete: only empty tenants; type the exact slug. Enumerates public tenant_id columns and direct foreign keys to tenants, blocks existing business data, locks relevant tables during the final check, and removes memberships plus tenant atomically. User identities and audit records remain. This is not a publisher-data purge facility. External assets and historical legacy data need a separate reviewed offboarding process.
- Support: explicit reason, 30-minute session, diagnostics scoped to one tenant and initiating administrator. Shows membership permissions and table counts, not document contents. Start, view and end actions are audited. This is diagnostic support, not user impersonation or remote control.
- Activity: authenticated frontend module navigation, deduplicated per actor/tenant/module over 30 seconds, plus platform administrative events. Does not store URL parameters, document identifiers, form content, tokens or buyer details. Navigation collection is best-effort and is not a complete security audit of every backend operation. API access requires superadmin for reading the logs; publishers may submit only their own membership-scoped activity.
- Daily use: UTC day, page views, unique users and unique tenants. No historical activity is invented. Current UI exposes at most 90 days with 50-row event pagination; this is a query window, not an automated retention policy. Support history returns the latest 100 sessions.

## Verification

`node tests/platform_admin_schema.cjs` uses a disposable PGlite database; it never loads DATABASE_URL. Verify backend HTTP authorization and create/delete/support flows in an isolated environment before deploying. Frontend TypeScript checks can be scoped to affected files while reporting unrelated existing errors separately.

No migration or tenant creation is performed by adding these files.
