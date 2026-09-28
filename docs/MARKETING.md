# InkSuite Marketing

Marketing extends the existing Project Management module. The authenticated frontend is
`/app/project_management/marketing`; tenant API routes start at
`/api/tenants/{tenant_slug}/marketing`.

## Implementation

- Campaigns support one title, multiple titles and publisher-wide activity. Metadata is read from the existing catalog, not copied.
- Calendar, campaigns, scheduled posts, drafts/history, publisher/campaign assets, account management and a post composer share the same records.
- The existing title Marketing phase links to a filtered Marketing view.
- Title media is discovered using the authorized work's `uid` and its public S3 prefix. Selecting it creates a database reference, never an S3 copy. Marketing cannot delete title assets.
- New publisher files use `tenants/{slug}/assets/marketing/public/library/`; campaign originals and derivatives use the campaign's `original/` and `generated/` directories. Derivatives retain their source asset ID.
- Square JPEG generation uses letterboxing to preserve the complete cover. Video metadata is inspected with ffprobe before scheduling.
- Launch milestones use catalog publication dates and create drafts only. Later publication-date changes do not move approved posts.
- Provider adapters cover Facebook Pages, Instagram professional accounts, Pinterest Pins and TikTok Direct Post. LinkedIn and X are not implemented. The registry is extensible without scheduler changes.
- Common text/media can be overridden per destination. Pinterest boards are fetched from the selected account. TikTok renders current creator options and requires explicit visibility and consent.
- OAuth states are tenant/user bound, hashed, expiring and consumed before token exchange. Tokens are encrypted with Fernet and bound to their tenant inside the encrypted payload. Browser responses exclude credentials.
- PostgreSQL jobs survive process restarts. Claims lock post, target and job rows using SKIP LOCKED. Accepted asynchronous media is polled. Partial delivery retains independent target state.
- Transient failures use bounded 5/15/60-minute retries. A lost response to a publishing request is marked `delivery_unknown` and cannot be blindly retried. A stale worker claim is treated the same way after 30 minutes. This deliberately avoids claiming exactly-once delivery from APIs without idempotency guarantees.
- TikTok video uses a verified public URL when configured, or an encrypted upload URL and checkpointed chunk transfer. Photos require a verified public URL prefix.

## Local installation / activation

The current local backend connects to the AWS database through port 5433. Running the
migration changes that shared database even while both web servers run on localhost.

1. Install `requirements-marketing.txt` in the backend's existing Python environment.
2. Run `python scripts/migrate_marketing.py` for read-only preflight.
3. After reviewing `migrations/006_marketing.sql`, run `python scripts/migrate_marketing.py --apply`.
4. Restart/reload the existing API and Next.js servers if needed. No separate frontend application is required.
5. Configure provider credentials below. Use normal environment/secret management; never commit actual values.
6. Connect social destinations from Marketing while signed in as a tenant administrator.
7. Only when test accounts and posts are ready, run `python -m app.marketing.worker` in a separate backend terminal.

The API never starts the worker or runs migrations implicitly. Keep OAuth callback query strings out of proxy/access logs; the frontend immediately clears its callback URL and sends the code to the backend in a POST body. Existing `data/banking.json`
changes belong to the user and are unrelated to this module.

## Configuration

| Variable | Purpose |
|---|---|
| `MARKETING_TOKEN_KEY` | Fernet key supplied through deployment secrets. Keep it stable and backed up; losing it requires reconnecting accounts. |
| `MARKETING_PUBLIC_BASE_URL` | Frontend origin, such as the approved localhost callback origin for development or InkSuite HTTPS origin for production. |
| `MARKETING_META_CLIENT_ID`, `MARKETING_META_CLIENT_SECRET` | Meta app credentials used for Facebook and Instagram. |
| `MARKETING_META_API_VERSION` | Explicit Graph API version enabled and tested for the Meta app; no guessed default. |
| `MARKETING_PINTEREST_CLIENT_ID`, `MARKETING_PINTEREST_CLIENT_SECRET` | Pinterest app credentials. |
| `MARKETING_TIKTOK_CLIENT_ID`, `MARKETING_TIKTOK_CLIENT_SECRET` | TikTok client key and secret. |
| `MARKETING_TIKTOK_VERIFIED_URL_PREFIXES` | Comma-separated HTTPS prefixes actually verified in TikTok's developer portal. Setting this variable alone does not verify ownership. |
| `FFPROBE_BINARY` | ffprobe executable path, or install ffprobe on PATH. Required to ingest video. |

Reuse existing `DATABASE_URL`, `S3_BUCKET` / `TENANT_BUCKET`, `AWS_REGION` and AWS credential configuration.
New Marketing public prefixes must have the intended read policy before a provider can retrieve them.
No existing title ACL is changed. The module does not change bucket policies automatically.

Register these frontend OAuth callback routes for the relevant configured origin:

```
/app/project_management/marketing/oauth/facebook
/app/project_management/marketing/oauth/instagram
/app/project_management/marketing/oauth/pinterest
/app/project_management/marketing/oauth/tiktok
```

The user must retain their InkSuite login through the callback. Provider-specific app
reviews, HTTPS callback requirements, scopes and account eligibility still apply.
TikTok unaudited clients have restricted visibility; internal-only applications may
not qualify for a Direct Post audit. Check the intended-use requirements before promising
public delivery. Do not work around provider review restrictions.

## AWS deployment

The service file follows the existing Meetings worker paths. Verify those deployment
paths before installation. Deploy frontend/backend through the existing InkSuite flow;
the Marketing worker is a separate systemd service. It is not enabled by this change.
Use the same persistent database and encryption key across API and worker processes.

Rollback the application by removing its router/navigation integration or disabling the
worker. Preserve the additive tables and records; do not drop them as an automatic rollback.

## Validation and remaining activation checks

- Run `python -m pytest tests/test_marketing.py -q` in the backend environment.
- The isolated SQL check used an embedded PostgreSQL engine with minimal existing-table
  fixtures, exercised the migration twice, tenant rejection and due-job query semantics.
- A separate real PostgreSQL integration run used a disposable schema on the existing database and mocked providers. It passed concurrent two-worker delivery, campaign associations, tenant isolation, scheduling/rescheduling, cancellation, async polling, retries, permanent failures, stale recovery and OAuth replay. The schema was removed afterward.
- Live OAuth exchange, remote media retrieval, real publishing, real process reboot recovery
  and authenticated browser flows need configured test accounts. The migration was applied with user approval on September 10, 2026; all 11 tables were verified.
- The existing frontend has unrelated TypeScript errors in catalog/contributor/contracts
  files. Marketing-specific diagnostics are checked separately.
- Post queries are paginated in batches of up to 500. The workspace loads successive pages so calendar and history do not silently omit older campaigns.
- Conservative initial media limits are 25 MB image uploads, 500 MB video uploads,
  with smaller provider-specific scheduling limits. Pinterest currently publishes one-image Pins.

## Provider references checked during implementation

- Meta-maintained Instagram collection: https://www.postman.com/meta/instagram/documentation/6yqw8pt/instagram-api
- Pinterest authentication: https://developer.pinterest.com/docs/getting-started/set-up-authentication-and-authorization/
- Pinterest create Pin collection: https://www.postman.com/pinterest/pinterest-collections/request/jw05m3l/create-pin
- TikTok Direct Post: https://developers.tiktok.com/docs/en/content-posting-api-reference-direct-post
- TikTok photos: https://developers.tiktok.com/docs/en/content-posting-api-reference-photo-post
- TikTok delivery status: https://developers.tiktok.com/docs/en/content-posting-api-reference-get-video-status
- TikTok intended use and required UX: https://developers.tiktok.com/docs/en/content-sharing-guidelines

### Meta account selection
Facebook and Instagram callbacks now return `selection_required`, a list of available destinations, and an opaque encrypted selection token. Nothing is inserted into `social_accounts` until the administrator confirms `account_ids` through `POST social/oauth/{provider}/select`. No accounts are preselected in the picker.

The selection is tenant- and user-bound, expires after ten minutes, and consumes a one-time nonce in the existing `marketing_oauth_states` table in the same transaction as the account upserts. No schema migration is required. Keep the selection token in component memory only, never in URLs, storage, or logs. Previously connected accounts remain connected; use Social Accounts to disconnect any unwanted destinations from the earlier automatic-import flow. Per-post destination checkboxes determine where a post publishes, with Select all and Clear controls.


### Instagram preparation and media picker
The composer offers Prepare for Instagram for each selected asset. Image presets are portrait 1080x1350 (default), square 1080x1080, and landscape 1080x566. Fit preserves the entire image with a blurred or white background; Fill is explicitly selected cropping. Original validates the source without generating another file. JPEG exports are kept below 8 MB.

Selecting a video opens the preparation dialog. The server inspects the original with FFprobe. Already preferred-format Reels are reused; otherwise Fit or Fill creates a 1080x1920 H.264/yuv420p MP4 at 30 fps, with AAC 48 kHz stereo audio when the source has audio. Fit defaults to a blurred background. Source duration is preserved, limited to 3–900 seconds, with no automatic trimming. Select only one video for an Instagram destination to publish as a Reel; the existing publisher treats multi-asset posts as carousels.

Generated files retain source_asset_id and work_id, appear in Title Assets for the matching title and campaign, and remain in Campaign Assets or Publisher Assets. The open picker refreshes after generation. A preview and Use in this post step control substitution; the original is never overwritten.

Server deployment requires FFmpeg and FFprobe (libx264 and AAC). Set FFMPEG_BINARY and FFPROBE_BINARY, or install both on PATH. Windows development can use the ignored .tools/ffmpeg folder. Conversion is currently a bounded synchronous request (four-minute encode limit; five-minute frontend proxy timeout); configure the hosting request timeout accordingly. Very long or complex videos may require a shorter source. The original upload limit remains 500 MB; Reel outputs are validated below 1 GB. No schema migration is required.

References: https://www.postman.com/meta/instagram/folder/830j7my/reels-publishing and https://ffmpeg.org/download.html


### Marketing storage quota and cleanup
Each tenant has a 1,000,000,000-byte Marketing storage quota. Usage is calculated from current objects under tenants/{slug}/assets/marketing/public/ in the configured bucket, including orphan objects and generated outputs; Title Assets are excluded. Uploads and derivative writes acquire the tenant row lock before checking actual S3 usage and writing. Draft attachment edits and deletions use the same lock to prevent attachment/quota races. Existing over-quota storage is not deleted automatically; new writes are rejected with HTTP 413 until users free space.

GET /storage reports usage. Administrator DELETE /storage?confirmation=DELETE%20MARKETING%20ASSETS clears the snapshot of removable Marketing objects, committing each removal separately and reporting deleted/protected/failed counts. Individual DELETE /assets/{id} uses the same protection. Assets referenced by unfinished, draft or scheduled posts are protected. Published/cancelled post media associations are removed on deletion, so history previews lose those media; external social posts are not deleted. Derivatives remain independent when their source is removed. Original title storage and other tenant prefixes are never deleted. No automatic cleanup or database migration is performed. S3 version-history retention, if configured separately, is outside this logical current-object quota.

