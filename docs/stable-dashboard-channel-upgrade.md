# Dashboard and channel-unit upgrade

Apply `migrations/046_stable_channel_units.sql` to the InkSuite database before
deploying this backend release. Deploy the same `app/stable` version to the
Collabora worker, and publish the frontend changes together with the backend.
Migration 045 is already installed and must not be reapplied.

The new unit_adjustment fields preserve reported gross units and physical
returns. Signed Amazon adjustments change net units and royalty units. Reported
monetary net already includes the dollar adjustments; never deduct them again.
No extra handling fee is inferred from settlement adjustments.

Channel movements store only monthly ISBN/account outbound and returned units.
They do not create paid-sales or giveaway units. Gifting account names remain
source classifications, not confirmed giveaways. Stock is cumulative through
the selected period and uses a zero opening balance assumption.

Older posted imports do not have channel movements. They are deliberately not
silently rewritten, especially when royalties have already been issued. A
controlled source reparse/backfill is required before their implied stock can
be displayed. Pending/conflicting statements also make the stock unavailable;
the UI does not manufacture a zero balance. Validate the reconciliation against
the original S3 statements before accepting such a backfill.

The worker revalidates needs_review imports when the parser version changes.
It retains revision, overlap and issued-statement protections. Existing posted
imports and issued royalty statements are left intact.

Dashboard financial totals include posted Stable statement rows only. Historical
US publisher_receipts values that merely duplicate gross sales are not presented
as verified adjusted receipts. Filters support all periods, full year, year to
date (Pacific month), and a selected year/month. Stock does not reset in January.

Frontend session changes recover a missing access cookie using the server-side
refresh cookie before protected navigation. Active sessions renew silently;
30-minute inactivity logout has a two-minute warning and a working Continue
button. Renewal outages do not clear cookies. Reauthentication is shown without
discarding an already open page. No authentication tokens are exposed to JS.

Validation: Stable parser and disposable PostgreSQL tests, including original
statement acceptance tests; royalty rule/workflow tests; frontend session and
search tests; desktop/mobile dashboard interaction checks. Full frontend type
checking still includes unrelated pre-existing repository errors.
