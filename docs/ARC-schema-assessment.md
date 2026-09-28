# ARC schema assessment — live PostgreSQL inspection

Inspected information_schema columns, pg_constraint, pg_indexes, pg_trigger and marketplace helper functions before writing migration. Full read-only snapshot: arc-schema-snapshot.json.

## Existing relationships
- users.id UUID is the application identity; cognito_sub is the authentication identity. memberships joins users to tenants with role/module_permissions.
- marketplace_profiles.user_id PK/FK -> users.id. marketplace_actors.id UUID identifies either one profile via unique user_id or one organization via unique organization_id (exclusive check).
- marketplace_organizations.id -> tenants.id through unique nullable tenant_id; publisher organizations own Marketplace listings.
- marketplace_books.id -> works.id (unique work_id), publisher_organization_id -> marketplace_organizations.id. Existing owner trigger checks work and publisher tenants agree. These ownership fields are immutable.
- marketplace_book_editions PK (marketplace_book_id,edition_id), with edition_id unique -> editions.id. Owner trigger enforces edition/work/tenant consistency. editions has work_id and tenant_id; works has tenant_id and uid (private upload folder identity).
- marketplace_library_items PK (user_id,marketplace_book_id), FKs to marketplace_profiles.user_id and marketplace_books.id; status saved/want_to_read/reading/read. Existing updated_at trigger and recent/book indexes. There is no surrogate Library id and no actor_id ownership column.
- Existing notifications derive from domain records; no generic notification queue. ARC notices will extend that bell with request/access state, not create a second messaging service.
- No existing EPUB locations/bookmarks/sessions found. Existing work-contributor and edition metadata remain authoritative.

## Minimal extension and foreign keys
1. marketplace_arc_assets: edition_id unique FK -> editions.id identifies the canonical edition; tenant/work/publisher are derived. Availability/request/status/current revision fields. uploaded_by FK -> users.id is upload audit identity.
2. marketplace_arc_versions: (asset_id,revision) PK; asset_id FK -> assets; uploaded_by FK -> users. Immutable private S3 key, package manifest and file metadata per replacement. Old versions retained for audit; no per-reader EPUB copies.
3. marketplace_arc_requests: asset_id FK -> assets; requester_actor_id FK -> actors for request identity; requested_by FK -> profiles.user_id establishes the existing personal Library owner, including publisher employees. decided_by FK -> users for audit. Unique(asset_id,requested_by) avoids duplicate access requests under switched actors.
4. Extend marketplace_library_items with optional arc_asset_id FK -> assets, entitlement dates/random public identifier, versioned reading position and progress. No duplicated work_id/edition_id/tenant_id/actor_id. Trigger checks asset edition is explicitly listed for this Marketplace book. Existing reading-list statuses remain intact. Library removal will preserve ARC history.
5. marketplace_reader_sessions: random id, composite (user_id,marketplace_book_id) FK -> existing Library, revision and expiry. Composite fields are the actual parent key, not a second identity model. Every access rechecks current entitlement and current revision.
6. marketplace_reader_bookmarks: id, same composite Library FK, revision/location/chapter/label; no extra actor/book ownership copies. Old revision bookmarks are retained but not applied to revised content.
7. marketplace_arc_audit: asset FK and acting user FK for structured security events. No document content or secrets logged.

## Storage and reader
Existing private title files use tenants/{tenant_slug}/data/uploads/{works.uid}/, outside the public/ child. ARC canonical versions use that same private title root under arc/. Reuse routers.uploads S3 client and bucket; never expose keys or original URLs in responses. EPUB parser has bounded ZIP validation and sanitized, allowlisted resources. EPUB.js renders protected unpacked-package resources, with scripts/popups disabled. Reading-session URLs alone never authorize a request: application login and Library ownership remain required.

## Compatibility
No existing table is recreated or dropped; no catalog or identity records duplicated. All new IDs are UUIDs. Existing legacy Library endpoints remain supported. Schema preflight checks exact current relationships; migration is transactional with bounded lock timeout. The migration is delivered for review/application, not executed on production automatically.
