# Embedded Shopify deployment — 2026-10-11

This change is local and not yet installed or approved by Shopify. No production migration or dashboard setting was changed.

## Architecture

App URL `/shopify` renders the order workspace inside Shopify Admin. It loads the current CDN App Bridge script with the public client ID in a meta tag. API requests use a fresh App Bridge ID token and no InkSuite cookies. The ordinary InkSuite session manager is disabled only on this surface.

The backend validates HS256, audience, issuer/destination, expiry, not-before and subject. Shopify online token exchange authenticates before account-linking UI. Offline expiring tokens are acquired and stored only when the publisher is linked; background processing uses the existing refresh flow.

An authenticated InkSuite publisher administrator generates a 192-bit, single-use link code, valid for 10 minutes. Its hash is stored. Entering it in the Shopify app grants that Shopify subject access to the publisher's distribution workspace. Each request checks current publisher administrator membership and active installation. Other Shopify subjects must be independently linked. Uninstall removes grants. Shop deletion cascades grants. No customer contacts are fetched by this flow.

The embedded workspace supports distributor preview creation, location selection, product import and edition matching, order review and event retries. It deliberately does not expose other stores' orders or Marketplace orders. Marketplace checkout and live distributor sending remain disabled.

## Deploy before changing the active Shopify version

1. Review and commit only the intended backend/frontend changes; preserve unrelated data/banking.json.
2. Apply migrations 020 and 021 if needed, then 027_shopify_embedded.sql once, using the existing controlled migration workflow. 027 adds link-code and user-grant tables; it does not modify commerce/accounting rows.
3. Install requirements-distribution.txt and restart the API. Verify distribution worker health.
4. Configure the FRONTEND server environment SHOPIFY_CLIENT_ID with this InkSuite app's public client ID. This is not the secret. Keep the client secret only in backend Secrets Manager. Rebuild/deploy the frontend.
5. Verify /shopify loads App Bridge before other scripts, sets the expected shopify-api-key, and sends CSP frame-ancestors limited to the validated shop plus https://admin.shopify.com. Check that infrastructure does not add a conflicting X-Frame-Options DENY/SAMEORIGIN header. Do not weaken framing for unrelated InkSuite pages.
6. Release a Shopify app version with:
   - Application URL: https://www.inksuite.io/shopify
   - Embedded: true
   - Shopify managed installation: enabled (use_legacy_install_flow=false)
   - Existing read-only scopes unchanged
   - All compliance and operational webhook subscriptions preserved
   The legacy callback is no longer used by the embedded flow. The manual domain connect endpoint returns 410.
7. Install in a development store through Shopify. Verify incognito/cookies-blocked authentication, publisher linking, location selection, catalog import, mapping, paid order import, retries, uninstall/reinstall and all three privacy deliveries.
8. Finish security evidence and required reviewer credentials before submission. Record actual tested UI only.

## Pricing

The integration is free for InkSuite users. State the InkSuite account requirement clearly. If access depends on a paid external InkSuite subscription, obtain Shopify's confirmation of the applicable billing treatment rather than declaring an exemption based on the word free.

## Validation

39 unit/SQL tests pass using disposable synthetic PGlite, not AWS. Full frontend tsc still has pre-existing errors outside the changed files; no changed-file diagnostics were reported. Live App Bridge, production CSP, managed installation and Shopify token exchange still require a configured development-store test; synthetic tests do not establish those results.

References:
https://shopify.dev/docs/apps/build/authentication-authorization/implement-token-exchange
https://shopify.dev/docs/apps/build/authentication-authorization/id-tokens
https://shopify.dev/docs/apps/launch/shopify-app-store/app-store-requirements
