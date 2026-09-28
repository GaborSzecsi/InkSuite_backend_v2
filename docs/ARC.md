# InkSuite ARC Library and EPUB reader

## Activate locally
1. Apply `migrations/013_marketplace_arc.sql` once to the existing InkSuite PostgreSQL database, using the same migration process as the other SQL files. It is transactional and checks the actual composite Library primary key. No production migration was executed during implementation.
2. Install backend dependencies with `python -m pip install -r requirements-arc.txt` in the backend runtime. Local user Python dependencies were installed during implementation; install these separately on AWS when deploying.
3. Run `npm install` in the frontend if using a fresh checkout. Restart the local backend and frontend.
4. The existing S3 runtime credentials need GetObject/PutObject on the title's private ARC prefix in the existing uploads bucket. Keep that prefix private in S3 and any CloudFront distribution; do not expose it under a public catch-all distribution. Existing public cover/resource access can remain scoped to its public prefixes. ARC uploads use AES256 server-side encryption and no public ACL.
5. Publish the relevant edition in the existing Marketplace catalog before accepting requests or previewing through Library. No new work, edition, or listing is automatically invented.

The SQL file and schema assessment are also supplied in the task's outputs folder. Migration 013 extends the existing Library; it does not recreate any existing table or modify catalog identities. Migration 012 is the separate earlier reader-profile change.

## Publisher workflow
- Open Project Management → title workspace → Upload ARC.
- Select an existing edition, choose an EPUB (maximum 40 MB), availability dates and whether requests are allowed.
- Reopening this action exposes Preview in Library for an existing ARC. A replacement requires explicit confirmation. Publisher preview uses an ordinary personal Library entitlement, not an authorization bypass.
- Publisher Marketplace management → Manage ARC requests. Filter pending/approved/rejected/revoked/all and decide requests. Approval and entitlement creation commit together.
- Request Banking Info opens the existing banking component and secure request flow; no banking form or backend permission model was replaced.

## Reader workflow
- A listed title with an available ARC offers Request ARC to a signed-in Marketplace actor. Requests use the selected actor; reading access belongs to the existing application user's Library.
- Approval appears in the existing notification bell and links to Read ARC.
- Library is a header icon. It includes saved titles and ARC history, with All/ARCs/Reading/Finished/Expired filters.
- Reader supports contents, chapter/page navigation, keyboard arrows within the book, CFI resume, private bookmarks, typeface/text size/line spacing/width, and dark mode.
- Progress saves at four-second intervals and on close or visibility changes. Displayed percentage is an estimate based on chapter and page position; the precise resume position is the EPUB CFI.
- Reading sessions last twenty minutes. Reopen the book when prompted to obtain fresh authorized access. Expiry/revocation/status/version checks run on every resource request, including cache hits. The visible reader also rechecks access every twenty seconds and closes content on an access failure.

## Schema and policy decisions
See `ARC-schema-assessment.md` for verified PK/FK relationships and normalization decisions.
- Existing Library primary key remains `(user_id, marketplace_book_id)`. An ARC asset FK identifies ARC entitlement type; there is no duplicate work/edition/tenant/actor ownership on Library.
- One ARC entitlement per title per application user follows that existing key. A different edition cannot silently replace a reader's entitlement. One request per asset/user prevents duplicate requests under switched actor identities. Rejected/revoked requests are terminal in V1; they cannot be resubmitted automatically.
- An asset belongs to an existing edition. Immutable version rows retain the private canonical EPUB and package manifest per revision. Existing approved access survives revision changes; old sessions reject the new revision, old CFI/progress is not applied, and earlier bookmarks remain stored but hidden for the revised file.
- Parent identity guards prevent moving ARC-bearing editions/titles between tenants or private storage identities. Approval cannot link a title to an unlisted edition.
- Revoked/expired items and reading history remain in Library. Existing remove-from-Library actions reject ARC removal, including after catalog withdrawal.
- Pending request, decision, revocation and near-expiry notices extend the existing bell and existing local dismissal mechanism.
- Watermarks use the verified application profile name, a random access identifier, and organization affiliation only when the request's organization is verified and current membership still supports that affiliation.

## Security and supported EPUBs
EPUB.js 0.3.93 is pinned. Its bundled dependency requirements include older packages; overrides pin @xmldom/xmldom 0.9.12 and lodash 4.18.1. Browser parsing uses native DOMParser. Unrelated pre-existing frontend audit findings remain; no broad dependency upgrade was performed.

Original EPUBs and storage keys are never returned by the ARC API. Browser resources use the authenticated Next proxy and FastAPI identity, not bearer access through a URL. The legacy book-assets listing/presigner explicitly excludes ARC objects. Resources are private/no-store and checked again after storage I/O. A bounded 96 MB/two-minute in-process archive cache avoids downloading the archive repeatedly for images; it does not cache authorization.

Ingestion limits archive bytes, expanded bytes, entry count, individual resource size and compression ratio; rejects encrypted EPUBs, archive traversal, duplicate names, symlinks and unsafe XML entities. Resource delivery is allowlisted and strips scripts, forms, external links/loads and active embeds. CSS is sanitized, XHTML receives a restrictive CSP, and the renderer disables scripts/popups. SVG, MathML, multimedia, encrypted/obfuscated fonts and remote content are not supported in V1. Publisher layout metadata is preserved; check complex fixed-layout titles against a representative EPUB before rollout.

This is controlled access and watermarking, not absolute DRM. Content already displayed to an authorized reader can be captured. No original EPUB download or print action is offered.

## Validation
- Disposable PostgreSQL-compatible PGlite schema populated from the existing Marketplace schema snapshots; migration 013 and production FastAPI/repository queries executed there. No live records were created.
- ARC integration covers tenant ownership, duplicate requests, atomic approval failure, publisher queue/notifications/preview, entitlement, CFI/bookmarks, cross-user denial, session expiry, entitlement expiry, version replacement, revocation and retained history.
- Combined unit/security suite: 69 tests passed; ARC integration: 34 endpoint checks plus resource/state assertions.
- EPUB unit tests cover archive attacks, XML entities, active content, external resources, CSS and resource allowlisting.
- Existing Marketplace regression script: 138 real route/SQL requests passed. Existing Marketplace security/media/profile suite: 52 passed.
- Browser verified sanitized EPUB chapter rendering and the reader layout using a synthetic title. A full authenticated upload-to-read smoke test against the user's running application still requires applying migration 013 and using a representative EPUB.
- Local frontend Library page and backend health both returned HTTP 200.
- ARC/frontend integration files passed TypeScript diagnostics. Full project type-check remains blocked by existing unrelated errors, including legacy Project Management typing issues.

## Deployment and maintenance
Deploy backend, frontend, dependency lockfile and SQL together. Do not remove the ARC exclusion from the legacy file listing when rolling back UI code. Retain version/audit/history records. Expired session rows may be deleted periodically after an appropriate operational retention period; never delete Library history as session cleanup. Failed uploads before a DB commit can leave an unreferenced private object; reconcile private objects against version storage keys before any administrative cleanup. No email invitations or notifications were sent during development.
