InkSuite public PDF uploads and chat changes

Implemented in the existing frontend and backend repositories. No production deployment, S3 upload, or database migration was performed by this implementation.

Title workspace
The existing Upload ARC tile now contains one Marketing materials uploader beneath the ARC controls. It accepts multiple PDFs and repeated uploads without a total file-count cap. Each PDF is limited to 50 MB and is uploaded sequentially; successful files remain listed if a later file fails. Marketing materials belong to the title/work and are shared across its editions. Original filenames are displayed; UUID storage names prevent files with the same name from overwriting each other.
Storage: tenants/{tenant-slug}/data/uploads/{work-uid}/public/marketing/{uuid}.pdf.
The ARC remains in its existing private arc/ folder. It is not exposed publicly.

Marketplace downloads
On a book page, Marketing materials appears immediately below the sidebar photo/name tile, listing the title's active public marketing PDFs. Downloads work for signed-out visitors while the book and publisher are public. Changing the book/storefront to draft/hidden prevents generation of new download links. Existing signed links expire after 15 minutes.
The storefront Catalog tile lists all uploaded catalogs. Storefront settings now include a multiple-PDF Catalogs uploader beside the logo/banner controls.
Catalog storage: tenants/{tenant-slug}/assets/marketing/public/catalogs/{uuid}.pdf.
Both types use the existing publishing_assets records and existing S3 client/bucket. No duplicate asset model or new bucket was created. Downloads use same-origin stable endpoints which verify public visibility and redirect to an attachment download, so the underlying bucket need not allow anonymous reads. Legacy catalog links remain supported.

Authorization and validation
Title uploads reuse the ARC publisher administrator permission. Catalog uploads require administrator permission for the exact publisher. All title/tenant paths resolve from SQL; clients cannot choose storage keys. Upload validation checks the PDF extension, header and EOF marker, with a bounded file read. This is a format-signature check, not a full PDF parser or malware scan. Upload writes application/pdf and attachment disposition. If SQL persistence fails, only the newly-created UUID object is cleaned up. Public download endpoints reject inactive/private assets, unpublished books/storefronts and keys outside the tenant's permitted public folder.

Database change: migrations/036_marketplace_public_documents.sql
The inspected publishing_assets.work_id is currently NOT NULL. Publisher catalogs are publisher-wide and have no title. Migration 036 permits NULL work_id only for catalog/catalogue asset types and adds a partial lookup index. All other asset types, including marketing materials, must still have a title. Existing rows and files are preserved. The migration was tested against the inspected schema in disposable PGlite and was NOT applied to the connected database.

Endpoints under /api/marketplace
GET/POST /editions/{edition_id}/marketing-materials — authorized title list/upload.
GET/POST /organizations/{org_id}/catalogs — authorized publisher list/upload.
GET /books/{slug}/marketing-materials — anonymous list for a public book.
GET /documents/{asset_id}/download — anonymous visibility-checked PDF attachment redirect.
GET /publishers/{slug}/catalogs — existing catalog tile, extended for new download endpoints.
The frontend proxy retains Origin checks, permits bounded 52 MB multipart requests for these PDF routes, allows a 120-second upstream upload timeout, and forwards the download redirects.

Chat follow-up
The previous requested chat changes are present: no per-message Remove/Report buttons or timestamps, no Sending as label or reconnect status line; recipient/presence header stays visible when minimized; minimize/expand and close use adjacent icons. Incoming messages open/expand the sender conversation for the active identity, with REST recovery for messages missed by the socket. Presence now has a page heartbeat in addition to WebSocket sessions and recognizes a person's active session while using an organization identity. These chat changes require the updated application code but no migration beyond already-applied 035.

Validation
PASS: python tests/marketplace_documents_integration.py — 55 REST/SQL requests, plus anonymous redirects, public visibility, tenant isolation, PDF signature/size checks, same-name uploads, storage cleanup on failed SQL, and lists exceeding 15 materials/catalogs. Uses the actual application endpoints and SQL with disposable PGlite and a fake S3 client. No AWS writes.
PASS: python tests/messaging_integration.py — 78 messaging REST/SQL checks, including active-page heartbeats, multi-session presence, ownership checks and expiry.
PASS: 38 focused backend tests; four existing media tests requiring inaccessible temporary filesystem paths remain excluded.
PASS: node tests/marketplace-messaging.test.cjs — six frontend tests covering simplified message rendering, reconnect protocol, PDF multiple-file input and sidebar download placement, plus existing Inbox/publisher regressions.
Python compilation and diff whitespace checks pass. TypeScript reports no errors in Marketplace or ARCUploadCard; unrelated project errors remain elsewhere.
Browser/live-account verification remains outstanding because the local Next.js preview cannot spawn its child process (EPERM). Tests do not constitute a real S3/browser end-to-end check.

Rollout
1. Review and explicitly approve migration 036 before running it on production; use the existing migration process. Do not rerun migration 035.
2. Deploy the frontend and backend changes together and restart their services.
3. Ensure the reverse proxy allows PDF multipart requests up to 52 MB and adequate upload timeouts (at least 120 seconds). The application accepts 50 MB per PDF.
4. Existing S3 application permissions must cover upload/delete/get of the two public prefixes above; no public bucket ACL or policy change is required for the signed download route.
5. Test a real PDF from the title workspace and two catalogs from storefront settings. With a signed-out browser, confirm the book Marketing materials tile and storefront Catalog tile list all files and download them. Confirm a user from another tenant cannot upload or access management lists.

Files
Frontend: src/components/ARCUploadCard.tsx; src/features/marketplace/PublicPdfUploader.tsx (new); public-documents.css (new); Manage.tsx; Marketplace.tsx; MarketplaceSidebar.tsx; src/app/api/marketplace/[...path]/route.ts; tests/marketplace-messaging.test.cjs.
Backend: app/marketplace/documents_repository.py, documents_service.py, documents_routes.py (new); routes.py; catalog_repository.py; catalog_service.py; migrations/036_marketplace_public_documents.sql (new); tests/marketplace_documents_integration.py (new); tests/messaging_integration.py; docs/MARKETPLACE_PUBLIC_DOCUMENTS.md (new).

Direct PDF uploads (Amplify)
Catalog and title marketing PDFs now use authenticated POST .../uploads, a signed S3 POST, then authenticated POST .../uploads/complete. Binary bytes bypass Amplify. Each PDF is limited to 50 MiB; file count remains unrestricted. Completion checks permissions again, validates the scoped private key, metadata, expiry, size and PDF signature, then publishes through the existing asset transaction. Staging stays outside assets and /public/ and is deleted after completion (including failed validation). Abandoned uploads can remain in document-staging; an S3 expiry lifecycle rule for that prefix is optional maintenance.

Deploy backend and frontend together. No additional SQL migration is needed if 036 is already applied. The bucket-wide S3 POST CORS configured for ARC also covers these uploads; keep staging private and do not add public access. The backend role needs GetObject, PutObject and DeleteObject on its tenants' staging and destination prefixes. Existing multipart endpoints remain for compatibility, but browser PDF uploads no longer depend on their proxy body-size limit.

Validation: 66 disposable REST/SQL requests pass, including direct 8 MiB catalog and marketing uploads, invalid PDFs, expired uploads, wrong destination and publisher isolation. Frontend tests verify large-file bytes reach S3 only, empty S3 success, safe XML-error handling and local file limits. Real browser/S3 verification remains a deployment check.
