# Distribution & Inventory: inspection and build status

## Existing application inspected (2026-10-02)

Read-only SQL inspection was performed against the database reached through the user's AWS tunnel before preparing migration 020. No distribution migration has been applied there.

- `tenants.id`, `works.id`, `editions.id` are UUIDs. Memberships use `(tenant_id,user_id)` with role and JSON module permissions. New routes use `require_tenant_access` and enforce Financials permission; configuration changes require a tenant administrator.
- `works` owns title/description metadata. `editions` owns ISBN13, format and cover references. Format display labels (Hardcover, Paperback) and `onix_product_form` (BB, BC) are separate. New tables reference existing editions; no duplicate title metadata.
- `edition_identifiers`, `edition_supply_details`, and `edition_prices` already supply identifiers and pricing metadata.
- `marketplace_books` links a work to a publisher organization; `marketplace_book_editions` links existing editions. Marketplace is currently a catalog/social/ARC system, with no customer checkout, commerce order or payment tables. Stripe was selected by the user but explicitly deferred.
- `inventory_movements` is a financial movement/cost ledger, not current distributor stock. It is not used as a commerce snapshot or changed by this module.
- Existing royalty sales/import tables and all accounting routes remain untouched.
- Existing background workers run as systemd services from `app.*.worker`; the new distribution worker follows that deployment pattern.
- Existing AWS integrations use boto3/Secrets Manager, S3 and an EC2 runtime. New secrets use a separate `inksuite/distribution/{tenant_uuid}/{connection_uuid}` namespace. The supplied IAM role must receive narrowly scoped access at deployment.
- Existing audit patterns use tenant-scoped event tables; distribution events contain identifiers/action codes, not credentials or copied order payloads.
- Frontend calls go through an authenticated same-origin BFF. A requested tenant must be one of the user's memberships; no default Marble Press tenant.
- No existing Shopify, Exporteo or Hachette integration was found in the backend code search.

## Observed Exporteo configuration (read-only inspection)

The Shopify store is `110c13-2.myshopify.com`. Its active automation is “Daily Export to HBG”, every day at 23:30 UTC.

Filters: paid orders; line location HBG Indiana; created less than one day ago; updated less than one day ago; exclude nonmatching lines. Post-export tags/notes/metafields are unchecked.

Transport: SFTP, port 22, `sftp.ulyssespress.store`, directory `/web-orders/marble-press/queued`. This is an intermediary endpoint observed in Exporteo, not a presumed Hachette-owned endpoint. Authentication is password-based. Username and password were redacted in the browser's accessible view; neither was extracted or saved.

Filename pattern: `orders_{{ "now" | date: "%Y%m%d_%H%M%S" }}.csv`. Although the UI labels the output CSV, the actual Liquid template uses tabs and CRLF, UTF-8, no file encryption. The existing integration was not saved, run, paused or replaced.

HDR fields, in order (21): HDR, order name, order name, recipient name, address1, address2, blank, blank, city, province code, postal code, country code, blank, email, shipping code, blank, blank, blank, blank, blank, gift note.

DTL fields: DTL, order name, 1-based line index, SKU, quantity.

Observed legacy shipping rules: contains GROUND -> PG; contains 2ND/SECOND -> P2A; NEXT DAY/OVERNIGHT -> P1AP; 3 DAY/3-DAY/3 DAY SELECT -> P3GP; otherwise PG. The new adapter deliberately requires an explicit exact-label mapping instead of silently defaulting an unknown service. Codes are evidence from the current template, not independently certified distributor specifications.

Company and phone have no defined position in the observed template. The new renderer blocks those values pending confirmed placement rather than dropping them. Credentials, SSH host key, acknowledgements, inventory feed, tracking feed, and distributor duplicate-recognition behavior remain required.

## Implemented locally

- Financials -> Inventory subpage with Inventory, Orders, Distributor tabs. The supplied logo is reserved for external app branding, not displayed inside InkSuite.
- Read-only existing catalog preview even before migration; explicit unknown availability, never invented quantities.
- Additive migration 020 for connections, centralized availability, reservations, normalized orders/items, jobs, events, Shopify installations/mappings/inbox/OAuth state.
- Composite tenant foreign keys on new relationships; an additive unique index on existing `(editions.tenant_id,editions.id)` enables those references.
- Atomic all-item stock reservation using row locks, central safety buffer, stale stock gate, cancellation rules, deterministic source-order uniqueness, changed-payload conflict.
- Preview order intake and downloadable Hachette-format preview; no live submission endpoint.
- Adapter capability boundary, operator-approved HTTPS/SFTP/FTPS/FTP transport primitives, validated public IPs, pinned HTTPS socket, required SSH host-key matching, bounded responses, no redirects or blind upload overwrite.
- Secrets Manager credential write/replace; secret reference only in database, no password returned to browser.
- Worker for queued connection tests, Shopify catalog import and verified-event processing.
- Standalone Shopify OAuth with state/cookie/HMAC/expiry checks; expiring offline tokens and serialized refresh; supported GraphQL Admin API version 2026-07; read scopes only.
- Raw-body webhook HMAC verification, bounded request size, installation lookup, unique delivery IDs, minimal persisted identifiers. Customer payloads are fetched only for order preparation, not archived as raw webhooks.
- Canonical GraphQL paid-order/location checks, manual variant mapping and ISBN catalog matching, split previews by distributor, explicit review inbox and retries. Pagination beyond the bounded order read goes to review rather than dropping items.
- Marketplace edition preferences; checkout remains disabled per user instruction.

## Not operational / not yet complete

This is a preview build, not a production Exporteo replacement. Do not switch fulfillment to it.

- AWS migration 020 is prepared but not applied. No Shopify app is registered/installed, no worker is started, no credentials are extracted, and no live distribution traffic has been sent.
- Hachette inventory/acknowledgement/tracking schemas and snapshot reconciliation watermark are missing. Inventory sync fails closed until an adapter can implement them. Scheduling and channel inventory publication depend on that work.
- SFTP order submission must remain disabled until actual duplicate-recognition/reconciliation semantics and cutover are agreed. Atomic rename alone is not proof of exactly-once delivery after the receiver consumes a file.
- Fulfilled reservations intentionally remain counted until a confirmed snapshot watermark reconciles them; blind refresh must not release them. This is a conservative gate, not finished stock reconciliation.
- No Shopify inventory or fulfillment write scopes are requested; outbound quantity/tracking publication is not implemented or enabled.
- Privacy webhooks are retained as operator-review requests; automatic GDPR deletion/export and data retention policy must be implemented before public app distribution.
- Automatic order cancellation after transmission, partial shipments, and distributor acknowledgement processing await actual distributor specifications.
- Stripe checkout/payment, Marketplace checkout/order customer pages, and payment confirmation are explicitly deferred. No public buyer can place a paid order in this build.

## Deployment prerequisites (do not run automatically)

Review and apply migration 020 separately. Install `requirements-distribution.txt`. Use the existing backend environment and service conventions. `DISTRIBUTION_ALLOWED_HOSTS` must contain only independently approved distributor hosts. Plain FTP is off unless `DISTRIBUTION_ALLOW_PLAIN_FTP=1`; prefer SFTP/FTPS.

Create Shopify app credentials in Secrets Manager and set `DISTRIBUTION_SHOPIFY_APP_SECRET` to that secret reference; its JSON must have `client_id` and `client_secret`. Set `DISTRIBUTION_PUBLIC_ORIGIN` to the app's HTTPS origin. OAuth callback is `/api/distribution/shopify/callback`; webhooks are `/api/distribution/shopify/webhooks`. Register the paid/updated/cancelled order and uninstall events only for the preview app. Compliance processing must be completed before public distribution.

Systemd unit supplied but NOT installed. Preview mode is enforced in service order creation and the adapter; there is no UI switch to live mode. Keep Exporteo active throughout preview.

## Validation

Run `python -m unittest tests.test_distribution tests.test_distribution_sql`. SQL tests use an in-memory PGlite PostgreSQL engine, never DATABASE_URL. They exercise real migration SQL, tenant foreign keys, duplicate order handling, last-copy reservation, cancellation, stale inventory and route reads/writes. Multi-process PostgreSQL concurrency/load testing is still required before live rollout.

Frontend TypeScript checking found no diagnostics in the new distribution files; the repository has pre-existing errors elsewhere. Authenticated localhost browser review confirmed catalog loading, title search, preview gates and removal of the internal logo.

## Official Shopify references consulted

- https://shopify.dev/docs/apps/build/authentication-authorization/authenticate-standalone-apps
- https://shopify.dev/docs/api/webhooks/latest
- https://shopify.dev/docs/api/admin-graphql

## Additional Shopify inspection (2026-10-02, read-only)

- Installed apps include Exporteo and eWarehousing; no Hachette-named app appeared. Legacy custom apps list Lakeside Book Company.
- HBG Indiana is the active default merchant location, ID 89127190750, with online fulfillment enabled. It is not listed under app/custom fulfillment locations; eWarehousing has its own separate app-managed location. The Lakeside merchant location is inactive.
- Into the Deep Blue at HBG Indiana shows 41 available/on hand. Its displayed adjustment history contains a manual relocation of 41 copies by Susan Szecsi on August 25. This sample does not prove that other titles lack automation.
- Order #1100 is fulfilled from HBG Indiana. Its timeline says the user marked one item fulfilled, and the fulfillment panel offers Add tracking. This sample does not identify an automated tracking feed.
- The inspected configuration and examples do not reveal Hachette inventory, acknowledgement or shipment feed specifications. Shopify quantities must not be treated as independently verified warehouse stock.
- No Shopify configuration, quantities, fulfillment status or installed apps were changed in this additional inspection.

## Authorized AWS migration (2026-10-02)

After explicit deployment approval, migration 020 was applied through the existing AWS tunnel as inksuite_app. Preflight confirmed UUID keys and no existing distribution tables. All 12 tables were created in one transaction, and the availability view was queried successfully. No connections or orders were inserted. The earlier pending-migration notes describe the pre-deployment state.

The production-owned Shopify registration is now under Inksuite, Inc., organization 238735439, app 430942748673. The user confirmed the corrected application and callback URLs, standalone mode and legacy OAuth flow. Its webhook version is 2026-10; Admin GraphQL requests still use 2026-07. Store installation and webhook subscriptions remain pending, including payload validation against the configured webhook version. The separate Marble Press registration is not the production app.
