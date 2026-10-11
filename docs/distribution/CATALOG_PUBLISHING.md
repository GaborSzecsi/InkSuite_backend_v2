# Shopify catalog publishing

Local implementation; not yet deployed or live-tested with write_products.

Apply migration 029 after 028, deploy backend/frontend, restart API and distribution worker. Release Shopify app configuration with write_products and approve the permission in the test store; re-link the publisher to refresh offline credentials if needed. Keep Exporteo and inventory sending unchanged.

The embedded catalog lists database editions, current unambiguous store-currency retail prices, descriptions, and public HTTPS front/back cover and jacket resources. All edition checkboxes start unchecked. New titles are DRAFT. Existing single Format/Binding/Edition-option products receive selected missing variants without overwriting existing content. Other option structures require review in Shopify. Existing active products may show new variants immediately; the confirmation explains this.

Before publishing, the worker rereads the entire Shopify variant catalog and checks ISBNs/title conflicts. Queued metadata is compared against current database metadata. A persisted RUNNING intent prevents automatic repeats after ambiguous API outcomes; REVIEW_REQUIRED entries must be reconciled by an operator, not blindly resubmitted. New image URLs are handed to Shopify; successful creation does not prove asynchronous image processing succeeded. Missing/private URLs are flagged; there is no server-side URL fetch.

Catalog synchronization marks absent variants inactive only after the final successful page. Failed/partial scans never hide old rows. Import progress and publication outcomes poll every five seconds. An import must be requested to reconcile external deletions (no product-delete webhook is subscribed).

No production schema changes or Shopify permission grants are performed by this implementation.
