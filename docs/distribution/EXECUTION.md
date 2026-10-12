# Orders and inventory

Order forwarding and inventory synchronization are core services, activated per
publisher connection. Product publishing is independent. A missing distributor
connection is a setup state, not a global prohibition on execution.

## Connection types

* `simulator` exercises the real queue, reservations, acknowledgements and inventory
  reconciliation using test stock. It never fetches buyer contact information,
  contacts a distributor, or writes Shopify inventory.
* `standard_json` uses the HTTPS contract below. A distributor or its integration
  gateway can implement it. New distributor-specific adapters translate their
  protocols into the same snapshot and receipt objects; the Shopify app workflow
  and publisher configuration model stay the same.
* `hachette_exporteo` transmits the previously observed order format over verified
  SFTP. Its inventory and acknowledgement capabilities remain unavailable until
  their formats are known. Do not substitute guesses for those formats. This does
  not disable inventory for the other connection types.

An arbitrary existing distributor API is not automatically compatible with the
standard contract. Its gateway/adapter must translate its data correctly.

## HTTPS distributor contract, version 1

Configure an HTTPS base URL without a query string, embedded credentials or fragment.
Its hostname must appear in `DISTRIBUTION_ALLOWED_HOSTS`. Credentials live in AWS
Secrets Manager. The current setup screen supports HTTP Basic credentials. The
transport also supports an operator-provisioned bearer token or API key. Redirects
are not followed. Public DNS, TLS certificate validation, timeouts and a 10 MiB
response limit apply.

`GET <base>/health` must return exactly:

```json
{"protocol":"inksuite-distributor","version":1,"idempotent_orders":true}
```

`GET <base>/inventory` returns:

```json
{
  "version": 1,
  "as_of": "2026-10-12T18:00:00Z",
  "complete": true,
  "items": [{"isbn":"9780744311570","on_hand":100}],
  "reflected_order_keys": []
}
```

Use a current timezone-aware snapshot time, not the last time an item changed.
Timestamps must advance, be no older than 24 hours and at most five minutes ahead.
Quantities are nonnegative integers. Duplicate ISBNs, ambiguous publisher ISBNs
and invalid snapshots are rejected before mutation. A complete snapshot makes
omitted previously known editions unavailable; a delta leaves omissions unchanged.

`on_hand` is stock allocated to this publisher/channel pool, including this
connection's accepted but unshipped allocations. It excludes copies shipped and
stock allocated outside that pool. It is NOT an available-to-sell quantity that
already deducts those same unshipped orders. Adapters must normalize the source
feed to these semantics. InkSuite subtracts its local outstanding reservations
and the configured safety buffer.

`reflected_order_keys` contains shipped InkSuite order UUIDs whose quantities have
already been deducted from this snapshot. Only those acknowledged shipments release
their local reservations. A partial snapshot must include every edition in each
reflected shipment. Unknown historical keys are ignored within the publisher scope;
known orders that have not reached SHIPPED are rejected. This avoids double
deduction and tolerates removal of historical records under privacy handling.

`PUT <base>/orders/<order_key>` receives:

```json
{
  "version":1,
  "order_key":"11111111-1111-4111-8111-111111111111",
  "reference":"11111111-1111-4111-8111-111111111111",
  "recipient":{"name":"Example recipient","company":"","address_1":"Example street","address_2":"","city":"Example city","state_region":"CA","postal_code":"00000","country":"US","phone":"","email":""},
  "shipping_method":"Ground",
  "items":[{"isbn":"9780744311570","quantity":1}]
}
```

The receiver MUST persist the order key atomically with order acceptance and must
never create a second fulfillment for that key, including after a timeout. A
repeated request returns the original result; conflicting contents must not create
a new order. The key must remain available for the full fulfillment/reconciliation
lifetime, not expire after a short retry window.

Both PUT and `GET <base>/orders/<order_key>` return:

```json
{"order_key":"11111111-1111-4111-8111-111111111111","distributor_order_id":"D-1001","status":"ACCEPTED"}
```

Statuses are SUBMITTED, ACCEPTED, SHIPPED, or REJECTED. Distributor IDs must contain
only letters, numbers, underscores and hyphens (maximum 100 characters), never
recipient details. No tracking/address/free-text response fields are accepted in v1.
For a definitively absent order, GET returns HTTP 200 with exactly:

```json
{"order_key":"11111111-1111-4111-8111-111111111111","status":"NOT_FOUND"}
```

An HTTP error or timeout is not proof of absence. Before retrying an uncertain
idempotent submission, InkSuite checks that same key. Hachette SFTP uploads are not
assumed idempotent: uncertain uploads require operator reconciliation and cannot
be replayed through the retry button.

## Activation and operation

1. Apply migration 030 separately, after running the regression tests. It does not
   activate an existing connection or enable writes to a store.
2. Deploy backend and frontend and restart the distribution worker. Do not run the
   worker locally against the AWS database tunnel.
3. Release the app configuration with `write_inventory` and approve it in the test
   store. Existing catalog functionality does not require this new permission;
   enabling inventory publication checks it explicitly.
4. In the embedded app select/add a distributor, save configuration/credentials,
   and run its connection test. After success, enable orders and/or inventory.
5. For real orders, explicitly confirm that the previous exporter has been stopped
   for those orders. The code does not modify or stop Exporteo.
6. Select a fulfillment location and maintain variant-to-edition/distributor mappings.
   To publish stock, enable Shopify stock updates separately and confirm the stock
   authority. Shopify inventory tracking and the selected stocked location are required.

Only newly created Shopify orders after the latest order-processing activation
are automatically forwarded. Old previews are not promoted. Pausing stops queued
processing; it does not undo a request already transmitted. Orders are revalidated
as paid, uncancelled, mapped and assigned to the selected location immediately
before delivery details are fetched and transmitted. Changed split routing goes
to review. Cancellations before submission release local holds; cancellations after
transmission require distributor reconciliation.

Inventory and status checks run every five minutes. Stock publication is a separate
job so a failed feed does not prevent already stale stock from being set to zero.
Shopify writes use compare-and-set and mutation idempotency keys. New Shopify
commitments continue to reduce availability; known local reservations are not
deducted twice. Distributor shipments awaiting Shopify fulfillment reconciliation
are accounted for separately. This version does not create Shopify fulfillments
or tracking updates: those remain in Shopify/the fulfillment service.

One edition's pool cannot be published in full to multiple managed variants/stores.
Such configurations require explicit stock allocation support and are held for
review. Pagination/batch limits, untracked inventory, stale data and ambiguous
results also appear in processing activity instead of silently succeeding.

Buyer contact details are fetched only for a real transmission, used in memory,
and never saved in order rows, jobs, previews or event details. Minimal order IDs
and quantities remain for idempotency and operational reconciliation. Existing
privacy-request handling still sends live-order retention cases to operator review;
do not represent an unresolved case as automatically erased or submit privacy
attestations without reviewing that operational procedure. Marketplace checkout
remains disabled.

## Simulation and verification

Create a simulated distributor, select a print edition and set starting stock to
10 with a buffer of 2. Save, test, enable both functions and synchronize inventory:
sellable should be 8. Create one simulated paid order: sellable becomes 7. Once
accepted, synchronize again: it stays 7. Simulate shipment, wait for its status
check, and synchronize: distributor stock becomes 9, the reservation clears, and
sellable remains 7. No real order or Shopify stock changes occur.

From the backend root, run:

```powershell
python -m unittest discover -s tests -p "test_distribution*.py"
python -m unittest discover -s tests -p "test_shopify*.py"
node tests/distribution_execution_schema.cjs
```

The SQL tests run against disposable PGlite and never use the configured AWS DSN.
Test real stock publication separately on a development store with a compatible
test gateway, including a concurrent sale and a simulated transport timeout, before
activating a production publisher. Do not claim end-to-end verification solely from
the simulator or schema tests.
