# Marketplace V1 — local implementation

The approved schema is in `migrations/007_marketplace.sql`. Application startup does **not** run migrations. The implementation has not deployed to AWS, applied the migration, registered a real reader, initialized a real storefront, published content or uploaded test files to S3.

## Activation

1. Apply the approved migration manually to the existing database, as agreed. It is a one-time transactional migration; do not rerun it after successful application.
2. Restart the existing local FastAPI process so `main.py` loads `app.marketplace.routes`. The process on port 8000 returned 404 for the new routes during implementation, indicating that it had not loaded this module.
3. Open `http://localhost:3000/marketplace` or the existing Marketplace workspace entry. Sign in using the existing InkSuite identity.
4. Under **Manage storefront**, initialize the linked tenant's draft storefront. This resolves the tenant ID through current membership, including Marble Press; it does not create another tenant.
5. Select the publisher identity. Save the public description and optional logo/banner. Choose existing titles and the editions approved for public viewing. Set the selected listings to Public and discoverable; make the storefront Public when ready.
6. Verify reader registration/email confirmation with the configured Cognito pool. No pool configuration was changed. A read-only attempt to inspect signup settings could not obtain AWS credentials in the agent shell; this does not establish whether the running backend has credentials or whether pool self-signup is enabled.

The local frontend/backend still use your existing database and AWS assets. Actions performed after activation will persist there. No separate local database has been introduced.

## Implemented

- Public directory, publisher storefront with Books/Posts/About, book details and public reader profiles.
- Existing work/edition metadata projection, including approved edition covers, contributors, subjects, publication dates and qualified retail prices. Unknown/mismatched tenant ownership fails closed.
- Reader sign-in, signup and email confirmation through the existing Cognito client. Company sign-in is unchanged. Personal profiles require no tenant. A pending Add to Library action resumes after profile creation.
- Personal and publisher acting identities, checked server-side for each operation. Publisher operations reuse current memberships. Marketplace access and private-message permission are exposed in the existing Users permission editor. Administrators have both.
- Existing-title selection, explicit edition approval, draft/public/hidden listings, discoverable and featured flags. No catalog metadata editing in Marketplace.
- Personal library with Saved / Want to read / Reading / Read and removal.
- Chronological Following and Discover feeds, text posts, image attachments, existing-book cards, flat comments, likes/unlikes and own-identity deletion.
- Follows, bilateral requests and acceptance/decline/cancel/removal, identity blocks and report storage.
- Private direct conversations, inbox previews, unread state, read markers, send/reply, own message removal, active-page polling and sender identity labels. No groups, files or WebSockets.
- Image selection/upload for posts, reader avatars and publisher logos/banners. Images are decoded, size-checked, resized and re-encoded to JPEG to strip metadata. Publisher uploads reuse Marketing storage and quota. Personal images use the same S3 client/bucket under `marketplace/users/<user-id>/images/`, with a 1 GB limit, selection paging and unused-image deletion. The deployment's S3 IAM role must allow this new prefix; no IAM policy was changed. Selected image references are validated server-side; display URLs are short-lived signed URLs. No new asset catalog table.
- Responsive layouts using `globals.css` controls, panels and tokens; only Marketplace layout styles were added.

## Files

Backend additions:

- `app/marketplace/{__init__,core,schemas,identity,catalog,social,messages,media,routes}.py`
- `migrations/007_marketplace.sql` (unchanged approved migration)
- `tests/test_marketplace_security.py`
- `requirements-marketplace.txt`
- This document

Backend modification: `main.py` imports and mounts the Marketplace router.

Frontend additions/replacement:

- `src/features/marketplace/`: Marketplace shell/context, API/types, discovery, feed, account/profile, management, network, messages, media selection and scoped layout CSS.
- `src/app/marketplace/page.tsx` replaces the empty placeholder.
- `src/app/marketplace/[...path]/page.tsx` handles the shared reader routes.
- `src/app/(app)/app/marketplace/page.tsx` serves the existing workspace entry.
- `src/app/api/marketplace/[...path]/route.ts` forwards session tokens server-side, checks mutation origin, sets HttpOnly login cookies and avoids returning tokens to browser JavaScript.
- `src/lib/modulePermissions.ts`, `src/components/ModuleGuard.tsx` and the Users page add Marketplace permissions; the Marketplace guard handles multiple tenant memberships.

## Endpoint families

All are mounted under `/api/marketplace`:

| Family | Operations |
|---|---|
| auth/login, auth/register, auth/confirm, auth/resend | Same Cognito identity; tenant-free Marketplace entry and email confirmation |
| me, profile, users/{username} | Authorized identities, personal profile editing, safe public profile |
| publishers, publishers/{slug}, books, books/{slug} | Public catalog/directory projection |
| organizations/from-tenant/{id}, organizations/{id}/settings, organizations/{id}, organizations/{id}/catalog, organizations/{id}/listings | Authorized storefront setup and listing management |
| library, library/{book_id} | Private personal library and status/removal |
| feed, posts, posts/{id}, posts/{id}/comments, comments/{id}, posts/{id}/like | Social feed, publishing and engagement |
| network, follow, follow/{id}, connections, connections/{id}, block, block/{id}, reports | Relationships, blocks and reporting |
| conversations, conversations/{id}/messages, conversations/{id}/read/{message_id}, messages/{id}, unread | Private messaging and read state |
| media, images, images/{id}, identity-image | Existing publisher assets, personal images and branding |

## Security and limitations

Public results are explicitly constructed; full catalog payloads, emails, Cognito identifiers, private tenant settings and messages are never serialized through public endpoints. Private profiles suppress identity details in shared content. UI rendering treats user text as plain text.

Current membership is checked on each request. A cached UI actor is never proof of permission. Organization messaging requires tenant_admin, superadmin, or both `marketplace` and `marketplace_messages` permission flags. Blocks and message sends share a transaction advisory lock for the actor pair. Send limits apply to the underlying user across identities and API workers. Reader auth endpoints have a basic process-local attempt limiter; production edge-level abuse controls can complement it later.

S3 image uploads and Cognito signup are implemented but were not exercised against AWS. No real signup, message, post, upload, or public catalog seed was performed. Publisher assets remain managed through Marketing; deleting such an asset there can remove an image referenced by Marketplace content. The live permission/storage configuration must be verified during activation.

Network panels, books, feed, comments, library, inbox and image selection are paginated. Text-search and feed ordering are intentionally simple V1 queries. There is no moderation console, commerce, checkout, recommendation engine, bookstore management UI or external-social cross-posting.

## Validation

- Approved migration: 25 in-memory SQL/constraint checks passed against core table definitions obtained by read-only schema inspection.
- 22 focused backend security tests passed with no database or AWS calls.
- 59 real route/SQL assertions passed against a disposable in-memory PostgreSQL engine, plus a mocked-storage upload request. These exercise production route code and queries: catalog ownership, publication visibility, library upserts, actor impersonation, revoked memberships, private-message isolation, reporting authorization, blocking, connections, unread state and image reference/deletion safety. Fixture data is confined to the test engine, never the application UI or live database.
- Scoped TypeScript checking and Python compilation were performed. Next build is not treated as a type check because the existing project config ignores build errors.
- The actual localhost frontend rendered at desktop and 390px phone width, including reader sign-in and setup feedback. Authenticated browser flows cannot be verified until the migration is applied and the running local backend is restarted.

The disposable SQL test engine is a test harness, not a separate development database. The integration harness is retained in the Codex workspace under `work/test-marketplace-integration.py` and `work/pgtest/marketplace-rpc.cjs`; no DSN or cloud credentials are used by it.
