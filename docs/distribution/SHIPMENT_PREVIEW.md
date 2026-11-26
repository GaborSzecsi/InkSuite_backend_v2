# Shipment confirmation preview

The Shipment Confirmation tab in Financials → Inventory lets a publisher administrator configure and test a distributor's inbound feed. It uses the existing distributor connection. A publisher administrator can select an active Shopify store for this shipment feed without changing the distributor's inventory or outbound order setup. The shipment password goes to the existing distribution Secrets Manager namespace under a separate credential ID. Changing the host, port or verified key requires the administrator to re-enter shipment credentials. A superseded secret remains in Secrets Manager for operator cleanup. Shipment previews are synchronous and are not scheduled.

## Installation

Apply migration 048 through the existing deployment process after approval. Redeploy the backend API and frontend. The distribution worker does not require a restart for these endpoints. If rolling back the application code, retain the additive table so its credential references remain available.

After approval, on the API deployment host with the production database environment already set, apply the migration and restart the API after the backend release is in place.

```sh
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f migrations/048_distribution_shipment_feeds.sql
sudo systemctl restart inksuite-api
```

Publish the frontend using its existing Amplify release workflow.

Verify the migration with these read-only queries.

```sql
SELECT to_regclass('public.distribution_shipment_feeds') AS shipment_feeds;
SELECT conname, pg_get_constraintdef(oid)
FROM pg_constraint
WHERE conrelid = 'public.distribution_shipment_feeds'::regclass;
```

## Marble Press setup

1. Select Marble Press's existing distributor in the Shipment Confirmation tab.
2. Enter the feed name **Hachette Shipment Confirmation**, SFTP host **sftp.ulyssespress.store**, and port **22**. Confirm the account-relative shipment directory with Stable. The previous configuration refers to **web-orders/shipment-confirmations/**, and the path visible to this account may differ.
3. Use the independently verified server host key in base64 form. Leave the key blank when working with a local sample only. Enter the **shipmentconfirmations** username and separate shipment password after saving the feed.
4. Enter `SG_shipment_confirmation_{{MM}}{{DD}}{{YYYY}}-{{*}}.txt` as the pattern. The preview expands date tokens in UTC and includes today in its lookback.
5. Select Tab, UTF-8, and zero-based positions. Start with no header, order reference in column 0, tracking number in column 5, and UPS as the default carrier. Add a column 0 **Starts with** filter for **#99**. These are provisional settings. Verify them with a redacted shipment sample and leave unknown item and quantity mappings blank.
6. Test the connection, list files and preview a remote file or upload a local sample. The test reports directory and file readability separately.
7. Select the Marble Press Shopify store under Field mappings and save the shipment configuration. With a sample of up to 25 distinct Shopify order names, enable Shopify lookup to check exact names and distributor location assignments. Use **Check Shopify fulfillment permission** to read the linked installation's merchant-managed fulfillment write grant. Confirm separately that the distributor location is merchant-managed and the Shopify user has permission to fulfill and ship orders.

The preview identifies duplicate rows within a file and shows local order matches or Shopify lookup results. It does not establish that all items for an order shipped. Before scheduling live updates, confirm file format, item-level shipment semantics, location ownership and permissions. A live cutover also needs persistent file and shipment history, reconciliation of uncertain Shopify results, and a process that prevents EZ Fulfill and InkSuite from handling the same shipment.

## API

The existing publisher-scoped Distribution router exposes these paths under `connections/{connection_id}/shipment`. All require publisher administrator access.

| Method | Path | Purpose |
| --- | --- | --- |
| GET, PUT | Base path | Read or save feed configuration |
| PUT | `/credentials` | Save shipment-only credentials |
| POST | `/test` | Test directory and file readability |
| GET | `/files` | Discover matching files |
| POST | `/preview` | Preview one remote file or a base64-encoded local sample |
| GET | `/shopify-permissions` | Read the installed Shopify app's merchant-managed fulfillment write grant |
