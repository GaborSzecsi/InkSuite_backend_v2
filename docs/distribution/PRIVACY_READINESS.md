# Shopify privacy readiness — implementation and outstanding evidence

## Scope and minimization

Shopify preview ingestion fetches order ID/date/status, fulfillment location, variant identifiers, remaining quantities and shipping service. It does not query recipient name, address, email, phone, company, order name or free-text notes. Shopify webhook bodies may contain personal data in transit; the receiver verifies HMAC and retains only order identifiers. Privacy requests retain the order IDs Shopify supplies, not customer contact details. IDs and purchases remain protected/pseudonymous data, even without contact information. Do not answer “no customer data.”

Customer fields are not persisted in the preview order table or shown in the UI. The service drops submitted Shopify recipient/notes values before hashing and insertion, and API reads hide legacy values. File previews of Shopify orders are disabled. Existing Marketplace behavior is unchanged; checkout remains deferred. InkSuite subscriber/account/billing information belongs to separate systems and policies.

There is no live distributor sender. A future sender must fetch delivery data immediately before transmission, hold it only in memory, never log payloads or create temporary local files, and refetch on a confirmed safe retry. Distributor-side copies have a separate retention responsibility. Do not present this future transport as already implemented.

## Retention implemented by the worker

- Preview Shopify order records: 30 days from local creation. Source orders older than 30 days are not imported.
- Order webhook inbox and finished catalog jobs: 30 days.
- Expired OAuth state: deleted after one day.
- Suppression fingerprints: 30 days, to prevent immediate reimport of deleted orders. These are pseudonymous and not proof of anonymization.
- Audit records and scrubbed completed privacy requests: 365 days. They contain actor/resource IDs and action codes, not customer payloads.
- Pending data-access requests protect matching store metadata from routine expiry for at most the request’s 30-day response window. Overdue requests remain visible for operator action.
- Worker must be installed, running and monitored. A code change alone does not activate retention.
- Backup expiry/deletion is not verified; do not claim immediate removal from backups.

## Privacy operations

All three Shopify compliance topics go to /api/distribution/shopify/webhooks, using 2026-10 subscriptions. HMAC failures return 401. Body shop_domain must match the header, and request IDs/order arrays are validated. Duplicate deliveries are idempotent. Unknown uninstalled shops return success because no installation data is retained there.

customers/data_request becomes READY. Tenant administrators can download retained order metadata, securely deliver it to the customer, and explicitly confirm completion. Download alone is not recorded as customer delivery. No second export file is persisted on the server. The merchant is responsible for copies downloaded to their device. No customer contact data is included in exports.

customers/redact deletes matching preview orders and dependent distribution items/events/jobs/reservations only within the tenant and store, and suppresses their reimport. Requests with insufficient order identifiers require operator review instead of deleting broadly. A response acknowledging receipt does not claim completion.

shop/redact requires an inactive installation. It schedules its token secret for AWS deletion with a seven-day recovery window, removes that store’s preview distribution records, mappings, jobs, inbox and OAuth state, and scrubs pending request payloads. The app-wide secret and distributor credentials are never deleted. Reinstalled stores, live orders, and token-deletion failures require explicit operator review. Live-order privacy fulfillment is not ready for rollout.

## Security evidence collected 2026-10-02

- Local AWS tunnel connection negotiated PostgreSQL TLSv1.3; this does not verify the production service's connection or TLS enforcement.
- Existing production Shopify order count was zero (read-only check).
- Tenant scoping and admin-only export/delete management are implemented; unit/SQL tests exercise cross-tenant denial.
- Order list/detail/file accesses and privacy downloads/actions are logged without payloads. This is distribution-module auditing, not proof that every platform or administrator data access is audited.
- Authentication uses Cognito; deployed password policy/MFA was not readable from this environment.
- app/core/db.py defaults to sslmode=prefer. Production must be separately verified and configured to require authenticated TLS as appropriate; no global database settings were changed.
- RDS encryption, backup encryption/retention, network exposure, and IAM settings could not be queried because local AWS credentials were unavailable.
- Local development still has a production database tunnel. All automated tests here use synthetic disposable PGlite data and no AWS DSN, but that alone does not establish environment separation.

## Deployment and operations requirements

Apply 021 before deploying this code and starting the distribution worker. No migration was run during this implementation. Ensure Secrets Manager permission includes GetSecretValue/CreateSecret/PutSecretValue for installation tokens and DescribeSecret/DeleteSecret for exact installation-token resources; do not grant blanket access to unrelated secrets.

Register the three mandatory Shopify compliance webhook subscriptions and test actual deliveries as well as synthetic HMAC tests. Install deploy/inksuite-distribution-worker.service using the existing service environment; monitor process health and the privacy queue daily, including overdue and REVIEW_REQUIRED requests. Complete each request within Shopify's 30-day limit. Choose an accountable operator and escalation contact before app submission.

Before answering Shopify's security questionnaire, verify RDS/Cognito/backup settings, implement isolated development credentials/data, adopt incident-response and data-loss-prevention procedures, and execute any required merchant data-processing agreement. No third-party audit or certification has been claimed.

Sources: https://shopify.dev/docs/apps/build/compliance/privacy-law-compliance and https://shopify.dev/docs/apps/launch/protected-customer-data
