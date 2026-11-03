# Stable ingestion implementation and activation

This implements original-workbook ingestion, not Excel-summary generation. Production activation is separate from local implementation. No production migration, financial posting, or server restart was performed during development.

## Accounting

`app/stable/parser.py` discovers sections using header signatures, preserves source sheet/row evidence, uses Decimal arithmetic, verifies account settlements and monthly controls, and resolves ISBN-10 checksums. Missing control caches are reconstructed only for the documented fee arithmetic. Missing transaction values are never replaced with zero.

US/Canada purchases reuse `royalty_sales_lines` and `canada_sales_lines`, including `customer_name`, gross units, positive returned units, gross source money, discount only where an active royalty tier requires it, and receipts after commission/handling/reserve. Amazon stock transfers have zero paid units and are represented as handling adjustments, not purchases. Monthly storage and $0.50-per-ISBN technology live in `stable_receipt_adjustments` exactly once per accepted statement. Only final posting inputs and monetary amounts are stored in PostgreSQL; the existing royalty engine rounds its aggregated basis. Commission/reserve percentages and original source rows remain in S3. Temporary payloads are cleared after posting, file totals are not duplicated, and deductions are grouped by ISBN/type. Source identity and the existing sales reference_number retain provenance without an additional per-sale evidence table.

The royalty engine adds the separate cost records to the matching edition/period and includes Stable Canadian sales through its existing Canada stream. Net-receipt rules see adjusted receipts. List-price rules still use units and list price; zero-unit costs do not reduce their basis. Finalized statements are not rebuilt. Closed periods and overlapping issued statements stop posting for review.

The Canadian table is renamed by migration 045; the old name remains an updatable, security-invoker compatibility view. Existing table privileges are copied to that view. Historical rows remain intact.

Currencies are configured explicitly per source territory. The dashboard filters currencies rather than adding USD and CAD. Non-USD Stable data is blocked from the USD royalty calculation until an exchange-rate policy is implemented. The Canadian Amazon sheets supply their own CAD-per-USD exchange-rate note. The parser uses that explicit rate to normalize Amazon monetary amounts to USD, matching the report's converted totals; it never infers an exchange rate from geography. A CAD section without adequate conversion evidence is held for review. May Amazon Canada reports 8%; June-August report 10%. Actual reported monetary fees/net due are authoritative; inconsistent percentage labels are not stored or used in place of available cash amounts.

## Inventory and schedule

The daily file's `Default Useable Balance Qty` is available stock; `Ending Balance Qty` is separately retained. The approved naming convention is MMDDYY-Marble Daily Inventory. Publication requires `usable_inventory_confirmed=true` after confirming that usable is available-to-sell and that the filename denotes the closing inventory day. The distribution timestamp uses that day's 23:59 Pacific closing convention; this is a date convention, not a measured upload timestamp.

All accepted daily snapshots retain history. Older snapshots never overwrite current stock. Missing rows never zero other titles. Conflicting reports for the same day require review. Byte-identical reports on different verified report dates retain separate history; retries for the same content/date are no-ops. Historical reports can be recorded without publishing stale inventory.

A configured connection uses the existing distribution reconciliation and PUBLISH queue, preserving reservation and Shopify opt-in behavior. This does not automatically create a live distributor order adapter or enable checkout/order forwarding. Use the dedicated stable_s3_inventory connection type. The existing distribution scheduler skips inventory polling for that adapter and continues PUBLISH jobs for opt-in and stale-stock handling. It cannot forward orders.

`deploy/inksuite-stable-sync.timer` runs at **00:05 America/Los_Angeles**, including daylight saving time, and catches up after downtime. The worker waits five minutes after S3 object completion, checks the ETag while reading, bounds workbook sizes, rejects external links/macros, and serializes tenant processing with a PostgreSQL advisory lock. Container/SFTP settings are unchanged.

## Validation performed

- Original March US: 274 gross units, no returns, $2,309.86 gross sales, $1,051.539975 adjusted receipts ($1,051.54 displayed).
- Original March Canada: 8 gross units, $43.67 gross sales, $24.511575 adjusted receipts ($24.51 displayed).
- The actual November 2 S3 daily workbook parses 398 ISBN rows.
- Disposable PostgreSQL tests exercise the real migration, sales posting, separate costs, idempotency, tenant isolation, Canada compatibility, dashboard totals and frozen-statement protection.
- New dashboard components pass focused TypeScript compilation and desktop/mobile browser rendering with no browser errors. Repository-wide TypeScript checking still reports pre-existing errors elsewhere, including BookDetails props and project-management types. The distribution execution suite has two existing simulator failures, reproduced using the unchanged HEAD execution code; the other 24 distribution checks pass.

Run:

```powershell
$env:STABLE_ACCEPTANCE_DIR='PATH_TO_ORIGINAL_STATEMENTS'
python -m unittest discover -s tests -p 'test_stable*.py' -v
python -m scripts.stable_dry_run --directory PATH_TO_S3_DOWNLOADS --output stable-audit.json
```

## Production preflight and activation

1. Run `scripts/stable_schema_audit.sql` against **inksuite** using the application role. Direct RDS access from this workstation timed out, but read-only access through the existing backend SSH connection succeeded. Confirmed PostgreSQL 16.13, application database inksuite, allowed transaction types sale/return/adjustment, and 398/398 daily inventory ISBNs uniquely matched in the 734-edition Marble catalog. Existing sales tables have no RLS policies. Forty-two works have active discount-based royalty conditions. Marble connections are currently preview/test connections. Review grants and deployment rollback, and back up before migration.
2. Verify the actual allowed `transaction_type` and existing currency conventions. Confirm all report ISBNs resolve to exactly one tenant edition. Review prior-distributor overlaps by ISBN/source, especially March/April. Do not replace existing months wholesale.
3. Review `migrations/045_stable_ingestion.sql`, then apply it explicitly with ON_ERROR_STOP. Install `requirements-stable.txt` in the backend virtual environment. Deploy backend and frontend together; the old Canadian name remains compatible during rollout.
4. Insert the publisher's `stable_sources` row using its existing tenant ID, bucket `inksuite-data`, prefix `tenants/marble-press/sftp/stablegroup/incoming/`, verified US/Canada currencies and verified transaction type. Start with **enabled=false** and no inventory connection. No password or AWS access key belongs in this configuration. The backend IAM role requires ListBucket on that prefix and GetObject on its contents; uploads retain the existing SFTPGo role.
5. Run `python -m app.stable.worker` once to stage/report only. The disabled source still validates files but posts nothing. Inspect the dashboard Import activity tab and `stable_imports.issues`. Enable posting only after reconciling the catalog/schema and source files. Do not simply clear review flags: unsupported source details need a parser/policy correction and a controlled reparse.
6. Enable a verified source and rerun. Matching content is a no-op. A second distinct statement for the same territory/period is held; the worker never silently replaces posted financial data. Controlled revision replacement/reparse requires an operator migration that preserves the prior evidence and reverses its exact provenance; a general-purpose review/replace button is intentionally not implemented yet.
7. Assign the reviewed inventory connection, confirm usable stock semantics, remove competing inventory polling for that connection, and verify Shopify opt-in/reservations/staleness before enabling publication. Compare one actual Shopify SKU to the accepted source before broad opt-in.
8. Install the two systemd units, run daemon-reload, then enable/start the timer. Verify `systemctl list-timers inksuite-stable-sync.timer` and a successful service run. Activation is incomplete until these server steps are executed.

## Source issues requiring review

The S3 audit covers 15 monthly workbooks. Five pass parsing/reconciliation: March US, and March/May/June/July Canada. Others are held, rather than guessed: January/February lack ISBN evidence; later account/detail quantities differ, Amazon unit adjustments lack a verified physical-return interpretation, and September changes HBG reporting from sales to throughput/exclusions. September must not be treated as a mere sheet-name change. Its excluded units have handling implications requiring ISBN evidence.

The full per-file findings are generated by `scripts/stable_dry_run.py`. Import review currently presents reasons; it does not override fees, currency conflicts, source revisions, or unknown ISBNs. These source/policy issues remain before complete financial activation. The current backend still has only 909 MiB RAM and roughly 132 MiB available, with no swap. Do not enable the ingestion timer there before increasing capacity or placing the worker on a suitably configured larger instance. The unit uses the existing /etc/inksuite-api.env, a 256 MiB worker cap and a high OOM score; it does not assume DATABASE_URL exists in the project .env. No capacity or production configuration was changed during this implementation.

## Dashboard and book pages

The dashboard uses SQL data through an authenticated workspace-bound proxy, with gross sales, adjusted receipts, net units, monthly history, buyer share/ranking, title performance, currency/territory/year/search filters, CSV export and import review notes. It retains blue/purple/teal/gold styling. Net receipts are not labeled profit. Buyer receipts exclude once-only ISBN storage/technology allocations and are labeled accordingly.

Book Information uses accepted current inventory when available, reports its date, and displays selected-period adjusted USD receipts. Royalty preparation shows sync/review status; its existing statement engine consumes the adjusted accounting inputs. No mock financial values are shipped in the new dashboard.
