# Native Marketplace media

## Implemented locally

One shared pipeline for existing Marketplace actors: text, up to ten images, or one video, with optional text and existing book attachments. The original image/profile endpoints and existing posts remain compatible. No external-platform publishing is added. Assets are reusable across multiple native posts; future Marketing/external integrations must add reference checks before using the same deletion lifecycle.

The browser requests `POST /api/marketplace/media/uploads`, uploads directly to the returned S3 presigned POST, then calls `POST /api/marketplace/native-media/{id}/complete?actor_id=...`. FastAPI handles metadata only. Completion HEAD-checks exact size and pins the S3 version, atomically changing status to `uploaded`. Replayed upload forms cannot replace that pinned version. A durable PostgreSQL queue follows the existing separate Marketing-worker pattern; the worker polls the database every five seconds, **not S3**. Multiple workers claim different rows safely. S3/SQS events can later call the same idempotent completion path if abandoned-browser completion needs recovery; SQS is not required now.

Statuses: `pending_upload → uploaded → processing → ready`, with `failed` after three automatic attempts. Explicit retry allows up to six total attempts. A lease token prevents stale attempts from overwriting a newer result. Processing never runs in the API process. Every attempt writes unique immutable keys under `processed/{media_id}/{lease_token}/`. Unknown or incomplete media cannot be attached. Cross-actor IDs, duplicates, mixed video/image selections, and deleting an asset referenced by a non-deleted post are rejected. The database additionally validates attachment ownership/readiness.

Images are actually decoded and re-encoded as WebP at 480/960/1920 bounding sizes, preserving aspect ratio without upscaling and stripping metadata. Supported input JPEG/PNG/WebP; animation and images over 40 million pixels are rejected. Videos are inspected with FFprobe, limited to one video stream, three minutes and 4096×2160 pixels, then encoded as H.264/AAC MP4, yuv420p, 30 fps, faststart, with a JPEG poster. Portrait/landscape bounds preserve the whole frame. A 720p rendition is always generated; 1080p is added when source dimensions warrant it. No automatic sound or autoplay; browser playback uses `preload="none"`. HLS, captions, custom video covers and automated moderation are future work for this native pipeline; existing Marketing cover functionality is unchanged. A moderation hook belongs before the ready transition in the worker.

The picker shows upload progress, local previews, processing/failed states, retry, removal from a draft, and an existing-media selector with individual storage deletion. Media-only posts are supported. Publishing waits for all selected media to be ready. Images use lazy loading and responsive derivatives. Signed URLs are refreshed by normal feed polling; video playback is not reset during playback by polling updates.

## SQL and data safety

Review and manually apply `migrations/010_marketplace_native_media.sql` after 007. It creates `marketplace_media` and `marketplace_post_media`, adds their indexes/triggers, adds a published-author index, and permits an empty post body when validated native attachments are supplied by the service. Existing catalog/user records and asset files are not modified. No migration has been applied to the connected AWS database by this implementation.

The actor/user foreign keys restrict deletion. Metadata contains stable keys, never permanent signed URLs or binary blobs. Individual removal soft-deletes the asset, then the worker removes all associated versions after a 24-hour grace period. An upload never completed is abandoned after one day; unreferenced ready/failed assets older than 30 days are marked for cleanup. Referenced originals remain private and retained. Orphaned outputs and replayed original versions older than one day are pruned in bounded maintenance batches, keeping the pinned source and current derivatives. Monitor cleanup lag and S3 actual bytes.

The default **1 GiB per-actor reservation** includes the original plus a derivative allowance (16 MiB/image, 256 MiB/video). Pending uploads reserve capacity too. Capacity is released after physical deletion is confirmed. This is a conservative application quota, not an AWS hard spending cap: temporary retries, incomplete cleanup and upload replays can use more physical bytes until cleanup. Existing tenant Marketing storage has its separate existing quota. Set AWS budgets/alerts and monitor actual storage as well as reservations.

## AWS preparation — not deployed or verified live

This shell has no AWS credentials. Existing code/configuration and naming conventions were inspected, but live bucket policies, CloudFront distributions and EC2 role grants could not be audited. Review them before deploying. Do not reuse the private tenant bucket.

`deploy/marketplace-media.yaml` provides a dedicated versioned bucket with Block Public Access, bucket-owner-enforced ownership, encryption, explicit CORS, CloudFront OAC, a trusted signing-key group, and scoped policies attached to supplied API/worker role names. OAC can read only `processed/*`. Every viewer request requires a CloudFront signature; originals cannot be delivered by that distribution. HTTPS is required. No Redis, Valkey or ElastiCache resources are created.

CloudFront caches immutable objects for up to a year. Browser responses override this to private/four minutes; signed URLs expire in four–five minutes. Blocking/deletion stops newly authorized URLs but cannot revoke an already-issued URL immediately or erase a downloaded file. Existing legacy media retains its previous signed-S3 behavior. Never log signed URLs or signing material. Restrict the signing private key to the API service account, rotate via key groups, and keep keys out of Git and client environment variables.

Use a separate stack/bucket for development. The default environment is `development` and requires a bucket name ending in `-dev`. Production requires explicit `MARKETPLACE_MEDIA_ENV=production` and a name ending in `-prod`; this prevents a development configuration from silently selecting the production bucket. Example AWS CLI commands to run **after review**, with your actual region and role names:

```powershell
aws cloudformation validate-template --template-body file://deploy/marketplace-media.yaml
aws cloudformation deploy --template-file deploy/marketplace-media.yaml --stack-name inksuite-social-dev --capabilities CAPABILITY_IAM --parameter-overrides file://social-stack-parameters.json
aws cloudformation describe-stacks --stack-name inksuite-social-dev --query "Stacks[0].Outputs"
```

Supply `BucketName`, `AllowedOrigins`, `ApiRoleName`, `WorkerRoleName`, and `CloudFrontPublicKey` in the CLI parameter file. AllowedOrigins is an explicit comma-separated list, e.g. `http://localhost:3000,https://www.inksuite.io` for the development stack. Production should list only actual production origins. Generate a dedicated RSA 2048 key pair using your secure key-management process. CloudFrontPublicKey is the PEM **public** key. Keep the private PEM on the API host; do not include it in CloudFormation parameters. CloudFormation schema validation and a real S3/CloudFront smoke test are required deployment checks; the template has not been submitted to AWS here.

Set backend environment values (restart API after changes):

```dotenv
AWS_REGION=<existing bucket/EC2 region>
MARKETPLACE_MEDIA_ENV=development
MARKETPLACE_MEDIA_BUCKET=<stack Bucket output>
MARKETPLACE_MEDIA_CDN_URL=<stack CdnUrl output>
MARKETPLACE_MEDIA_KEY_PAIR_ID=<stack KeyPairId output>
MARKETPLACE_MEDIA_PRIVATE_KEY_FILE=<absolute private PEM path>
MARKETPLACE_IMAGE_MAX_BYTES=20971520
MARKETPLACE_VIDEO_MAX_BYTES=524288000
MARKETPLACE_MEDIA_QUOTA_BYTES=1073741824
# Optional; otherwise uses existing .tools/ffmpeg binaries on Windows or PATH:
FFMPEG_BINARY=<absolute ffmpeg executable>
FFPROBE_BINARY=<absolute ffprobe executable>
```

Use IAM roles, not committed access keys. Worker needs S3 GetObjectVersion on originals, PutObject on processed, and scoped version-list/delete for cleanup. API needs PutObject for presigning and GetObject for HEAD on originals. No anonymous S3 read or tenant-bucket CDN permissions. CORS allows direct POST only; credentials are in presigned form fields. Upload forms expire after ten minutes, with exact MIME and content-length constraints; the worker independently validates content. Before enabling, test blocked direct S3 GET, unsigned CloudFront GET, original-path CloudFront GET, allowed signed derivative GET, Range playback and a browser upload from each intended origin.

## Local commands

After manually applying SQL and configuring dedicated media infrastructure:

```powershell
cd C:\Users\szecs\Documents\InkSuite_backend_v2
python -m pip install -r requirements-marketplace.txt
python -m uvicorn main:app --host 127.0.0.1 --port 8000 --reload
# Separate terminal with the same environment / .env:
python -m app.marketplace.native_worker
# Frontend terminal:
cd C:\Users\szecs\marble-frontend
npm run dev
```

Existing auth dependencies and database configuration remain required. Install maintained FFmpeg/FFprobe binaries for the worker. `deploy/inksuite-marketplace-media-worker.service` is a Linux deployment example with restricted filesystem, private temp directory, memory/CPU limits and file-size limit; adapt paths/user/environment to the existing EC2 host. Do not start it against production accidentally. The API does not start workers. A missing migration or media configuration disables the new picker while existing text/legacy posts continue working.

## Repeatable tests

```powershell
cd C:\Users\szecs\Documents\InkSuite_backend_v2
python -m pip install pytest httpx
python -m pytest tests/test_marketplace_security.py tests/test_marketplace_native_media.py -q
npm --prefix tests/marketplace_pg install
python tests/marketplace_integration.py
```

The integration harness uses disposable in-memory PGlite and schema-only catalog fixtures, production services/SQL, mocked AWS and mocked SMTP. It never opens DATABASE_URL or sends email. It covers existing social/messaging security plus migration 010, media completion, actual image processing, attachment authorization, reuse, private media, deletion references, paging and feed query bounds. Unit tests cover presigned constraints, malformed input, metadata removal, no upscaling, signed URL generation and real FFmpeg when installed. Real AWS/browser end-to-end upload and production load tests remain deployment checks, not results claimed by these mocks.

## Operational considerations

Costs come from original + derivative storage/version retention, CloudFront requests/egress, S3 requests, and worker CPU. No new cache cluster or queue service charge is introduced. Videos dominate processing and transfer costs; monitor queue age, retry counts, failed jobs, cleanup lag, storage reservations vs physical bytes, CloudFront errors, worker disk/memory, API latency and DB connections. Add alarms/budgets using your existing AWS monitoring. Keep FFmpeg/Pillow patched; run workers with the limited role and OS isolation rather than privileged API access. Failed jobs expose safe error codes, with IDs/error types in server logs rather than raw decoder output or user content.

AWS references: [OAC and S3 access](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/private-content-restricting-access-to-s3.html), [private content delivery](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/private-content-overview.html), [signed URLs](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/private-content-signed-urls.html).

## Change inventory and rollout

Backend compatibility modules modified: `core.py`, `schemas.py`, `routes.py`, `identity.py`, `catalog.py`, `social.py`, `messages.py`, `media.py`, `invitations.py`, `notifications.py`. Each of the last seven now delegates to new `*_routes.py`, `*_service.py`, `*_repository.py` modules. New feed batching: `feed_service.py`, `feed_repository.py`. New media implementation: `native_routes.py`, `native_service.py`, `native_repository.py`, `native_storage.py`, `native_processing.py`, `native_worker.py`, `native_worker_repository.py`. Timing: `observability.py`. Dependencies: `requirements-marketplace.txt` (cryptography for CloudFront signing).

Frontend modified: `src/features/marketplace/Feed.tsx`, `api.ts`, `marketplace.css`; added `NativeMedia.tsx`. Added migration 010, the CloudFormation template and systemd example, both architecture documents, `tests/test_marketplace_native_media.py`, `tests/marketplace_integration.py` and its schema-only `tests/marketplace_pg` fixtures. Existing authorization tests are preserved.

Rollout order: review infrastructure and SQL → deploy dedicated media stack → apply migration 010 through your normal PostgreSQL migration process (`psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f migrations/010_marketplace_native_media.sql` in a shell with that variable already set) → deploy backend and install its dependencies → configure environment/signing key → install/start the separate worker using your existing EC2 service process → deploy frontend through your Amplify-connected Git workflow → run the browser/AWS smoke tests above. On Windows PowerShell use `$env:DATABASE_URL` rather than `$DATABASE_URL`. No Git push, Amplify deployment, AWS stack change, production SQL execution or production worker startup has been performed here.

Keep existing service names/paths from your real EC2 deployment; this repository does not provide enough verified live deployment configuration to claim an exact existing backend restart command. The provided worker unit uses explicit example paths that must be adapted before `sudo systemctl enable --now inksuite-marketplace-media-worker`. The user continues to manage Amplify deployment.

## Local verification results (2026-09-20)

- 38 unit/security tests passed, including real FFmpeg transcoding and CloudFront signing with a temporary test key.
- 138 production route/SQL integration requests passed against disposable PostgreSQL, including multiple-image and video association, bounded failed retries, reuse and private media. The 20-post feed with attached books stayed within the nine-query assertion.
- 57 existing invitation API/SQL regression requests passed; no email was sent.
- Existing public-book regression suite: 72 route/SQL requests passed.
- Marketplace TypeScript check: zero diagnostics. Local `/marketplace` returned HTTP 200.
- CloudFormation YAML parsed/formatted successfully; AWS resource-schema validation and real browser→S3→worker→CloudFront tests are still required after configuration. No load/capacity claim is made.
