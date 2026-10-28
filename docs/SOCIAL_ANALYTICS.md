# Social analytics

Analytics dashboard, combined analytics authorization, migration 042 and the worker
are deployed. The published-post comment-management update described at the end of
this document remains local pending approval. No new migration is required for
comments. Platform permission grants and App Review submissions are separate from
implementation. Collector availability does not establish live access.

## Architecture and schema review

The existing Next.js Marketing module uses `src/features/marketing`, the authenticated
`/api/marketing/[...path]` proxy, FastAPI's tenant-scoped Marketing router, `s.access`
and `s.admin`, encrypted social-account credentials, and PostgreSQL. Publishing has
its own durable jobs and explicit worker. Nivo is already installed.

Multi-tenant use is a release requirement, not a Marble Press specialization. There
are no publisher-specific IDs, slugs, account names or default tokens in this module.
The Marketing proxy now uses the existing `workspace_company` login cookie and
verifies that workspace in the authenticated user's memberships. It no longer
silently picks the first membership. Backend path-based membership checks and
tenant-scoped resource checks remain authoritative. The same external social account
or media ID can exist in two authorized workspaces without sharing their stored
observations or campaign links. The worker schedules eligible accounts across all
tenants; it does not assume a current/default publisher.

Read-only production inspection preceded preparation of migration 042. Inspected
columns and foreign/unique constraints on `social_accounts`, `social_posts`,
`social_post_targets`, `social_publish_jobs`, `social_media_assets`,
`social_post_assets`, `marketing_campaigns`, `marketing_campaign_works`,
`marketing_oauth_states`, and `marketing_audit`. There were no analytics tables.
Existing `(id,tenant_id)` unique keys are reused for ownership constraints.

**Database target:** the running API uses database **inksuite**, user
`inksuite_app`, PostgreSQL 16.13, UTC. The supplied connection string naming
`postgres` points to a database with no Marketing tables. Do not apply 042 there.
Use the existing protected production DATABASE_URL rather than putting passwords
in scripts or shell history.

## Database additions

`migrations/042_social_analytics.sql`, applied explicitly and transactionally:

- `social_analytics_definitions`: provider-specific, versioned metric definitions.
- `social_analytics_media`: external-media metadata keyed by account/tenant/external
  ID. InkSuite post/campaign association is resolved through existing targets.
  No duplicated authentication, campaign, asset, or publishing-job tables.
- `social_analytics_observations`: numeric values or explicit availability states,
  reporting dates, day/snapshot/lifetime scope, API and definition versions,
  observation timestamp and run ID. Historical observations are append-only;
  replaying a run is idempotent.
- `social_analytics_sync`: account schedule, lease/run fence, attempts, safe error
  category, six-hour scheduling, incremental backfill and opaque discovery cursors.

Adds reporting/observation indexes and an existing-target lookup index. Composite
foreign keys prevent cross-tenant account references. The migration has five-second
lock and sixty-second statement timeouts; failures roll back. There is no startup
migration and no production application has occurred.

Definitions are seeded by the worker for each actual provider collection. Definition
version `2026-10-27.1` is intentionally separate from each observed API version.
Meta collection is limited to tested Graph **v25.0**; unsupported configured versions
fail explicitly until their metrics are validated. Deprecated impression/reach
fallbacks are not silently substituted.

## API

Under `/api/tenants/{tenant_slug}/marketing/analytics`:

- GET `/filters`: accounts, safe sync state, campaigns, platform/metric definitions.
- GET `/overview`: filtered overview, charts, posts, campaigns and limitations.
- GET `/accounts`, `/timeseries`, `/posts`, `/campaigns`: the same normalized report
  contract, allowing clients to reuse identical filter semantics.
- GET `/posts/{media_id}`: a tenant-authorized post's observation history, newest
  first, limited to 5,000 observations.
- GET `/sync`: safe account synchronization status.
- POST `/sync/{account_id}`: administrator-only queued manual refresh (202),
  throttled for five minutes; processing runs cannot be queued concurrently.

Reporting endpoints share `start`, `end`, `provider`, `account_id`, and `campaign_id`.
Dates are UTC and bounded to 90 inclusive days. Default is the last 30 days.
Account and campaign UUIDs are checked for tenant ownership. Pydantic response
models exclude credentials and internal storage fields. Responses use `no-store`.
All GETs read persisted data and do not call providers or queue jobs.

## Exact provider integrations

All credentials are read server-side using existing tenant-bound encryption and
refresh logic. The analytics refresh path requests redacted provider errors; the
existing publishing path's behavior is preserved.

### Facebook Pages (Graph v25.0)

- GET `/{page_id}?fields=followers_count`.
- GET `/{page_id}/insights`: `page_media_view` and
  `page_total_media_view_unique`, period `day` and bounded dates.
- GET `/{page_id}/published_posts` and GET `/{post_id}`: publication metadata,
  permalink, image and shares. Reactions/comment summary edges are separate optional
  calls, attempted only when recorded grants include `pages_read_user_content`.
  Their absence or rejection does not block core post discovery or Page Insights,
  and does not produce an account-wide analytics reauthorization warning. This
  read-only permission is included in the normal connection request.
- GET `/{post_id}/insights`: `post_media_view` and
  `post_total_media_view_unique`, period `lifetime`.
- Analytics scopes: `read_insights`, `pages_read_engagement`, `pages_read_user_content`; Page discovery uses
  existing `pages_show_list`. Publishing scopes are retained in the same connection flow.

Comment summary counts are analytics, not comment text, replies or moderation.
The analytics collector performs no moderation. The shared connection now also
requests comment-management scopes for the History popup (see SOCIAL_COMMENTS.md).

### Instagram Login (Graph Instagram v25.0)

- GET `/{ig_user_id}?fields=followers_count`.
- GET `/{ig_user_id}/insights?metric=reach&period=day`.
- GET `/{ig_user_id}/insights` for `views`, `total_interactions`, `likes`, `comments`, and `shares`,
  `period=day&metric_type=total_value`, one completed UTC day per request.
- GET `/{ig_user_id}/media` and GET `/{media_id}`: publication metadata, permalink,
  thumbnail and like/comment counts.
- GET `/{media_id}/insights`: `reach`, `views`, `shares`, `saved`,
  `total_interactions`, collected independently with lifetime semantics.
- Analytics authorization adds `instagram_business_manage_insights` to the
  existing `instagram_business_basic` and `instagram_business_content_publish`.
  The analytics portion adds no messaging or Facebook-Login Instagram scopes.
  The History popup also requests `instagram_business_manage_comments`.

Metrics are attempted independently. Format-specific or legacy metric rejection
is recorded as unavailable; no retired `impressions` fallback is invented.

### Pinterest API v5

- GET `/v5/user_account`: follower count when returned.
- GET `/v5/user_account/analytics`: `IMPRESSION`, `ENGAGEMENT`, `SAVE`,
  `PIN_CLICK`, `OUTBOUND_CLICK`, daily records, `split_field=NO_SPLIT`.
- GET `/v5/pins`, GET `/v5/pins/{pin_id}`, GET `/v5/pins/{pin_id}/analytics`:
  daily `IMPRESSION`, `SAVE`, `PIN_CLICK`, `OUTBOUND_CLICK` records.
- Existing `user_accounts:read`, `pins:read`, `boards:read` scopes cover these
  selected endpoints. No advertising permissions are added.

Only `READY` daily data is stored. Native account engagements are not fabricated
by adding potentially overlapping Pin clicks and outbound clicks. Pin engagement
totals and engagement rate may therefore be unavailable. API sandbox analytics
availability differs from production; the existing configurable API base is reused.

### TikTok Display API v2

- GET `/v2/user/info/?fields=follower_count` (`user.info.stats`).
- POST `/v2/video/list/`, and POST `/v2/video/query/` for known published video IDs:
  ID/title/timestamp/cover/share URL and `like_count`, `comment_count`,
  `share_count`, `view_count` (`video.list`).
- The normal connection requests `user.info.basic,video.publish,video.list,user.info.stats`
  together for publishing and read-only analytics.

These are lifetime public-video counters, not daily activity, unique reach,
impressions, saves, or a full TikTok Business analytics feed. Photo posts or
non-public content not returned by Display API remain unavailable. No Research API
or advertising endpoints are used. TikTok engagement is likes + comments + shares;
its rate is that total divided by lifetime views, only when all components exist.

## Authorization and App Review

POST `social/connect/{provider}` requests publishing and read-only analytics access
in one administrator connection flow. The legacy `analytics` query flag is accepted
for compatibility but no longer removes analytics scopes. The Analytics page has no
separate authorization action. OAuth state validation and callback/token-exchange
mechanisms are reused. No production environment or Meta app settings were changed.

Successful connection updates queue an existing account's analytics sync immediately
and clear stale non-processing permission errors. Running jobs keep their fence;
if their credentials were replaced, they yield to a pending retry with the new token.
Facebook connections save the actual granted permission names returned during login.
OAuth remains usable if the optional analytics schema is not installed yet.

Stored Meta scope lists historically represented requested scopes, not proof of
all granted permissions. They are advisory; `scope_information_known` is false for
these connections. Actual API success/failure is authoritative. Missing/expired
authorization has a concise status; reconnect through the existing Social Accounts tab. TikTok tokens expose their
returned scopes; whitespace/comma-separated legacy Pinterest lists are normalized.

Meta external-user analytics generally needs approval/advanced access for the exact
selected scopes. TikTok Display/statistics scopes also require the applicable product
and approval. Pinterest access tier, connected-account eligibility and token grants
must permit organic analytics. Do not submit permission review solely because a
permission appears in an OAuth URL.

Review demonstration, after approved deployment and authorized test-account grants:

1. Sign in as a tenant administrator and show Marketing -> Analytics.
2. Authorize analytics for the intended connected Page/account with the explicit
   analytics button, completing provider consent and InkSuite's callback.
3. Show the selected platform/account and date range; queue a refresh.
4. Wait for the analytics worker and show last sync time and real account metrics.
5. Open post performance and observation history, then open the original post.
6. Change date/account/campaign filters and show corresponding real data.
7. Show missing/unsupported metrics as Not Available rather than claiming zeros.

A connection-only screencast does not demonstrate insights. Do not include comment
management, DMs, advertising, ROI or unsupported metrics in the review claim.

## Synchronization and reporting semantics

Explicit worker: `python -m app.marketing.analytics_worker`. Deployment unit:
`deploy/inksuite-analytics-worker.service`; paths follow existing worker examples and
must be checked against the deployment. No worker starts with the HTTP API.

Normal refresh interval: six hours. Initial 90-day backfill uses seven-day chunks,
approximately five minutes apart when successful, rather than fetching all history
every iteration. Recent daily data is refreshed over a seven-day overlap. Older
known posts are refreshed in a bounded least-recently-collected rotation (five
Instagram posts, fifteen for the other platforms per iteration). Opaque discovery
cursors resume bounded pagination; next URLs from providers are never followed.
Request count and eight-minute wall-time budgets bound each account collection.
Transient/rate-limit failures use exponentially increasing backoff capped at six hours,
with a longer valid provider Retry-After honored up to 24 hours;
other permanent errors are retried after six hours or explicit reauthorization.

Claiming uses database locks, SKIP LOCKED, processing leases and a run ID fence.
Each provider request holds a shared lock on its account, not an account-wide lock
through the whole multi-minute sync. Publishing can proceed between requests.
A completed disconnect or credential replacement prevents subsequent provider calls
and final persistence with that authorization. An in-flight request may delay a
disconnect for the bounded request timeout. Stale workers cannot commit another
worker's result. One account/metric failure does not block other accounts.

Account daily flows, follower snapshots and post lifetime counters are kept distinct.
Platforms are never combined into an apparent unique audience. Reach is labeled
as summed observations, not deduplicated campaign people. A post-date filter selects
the publication cohort; lifetime values are explicitly not activity exclusively
inside that date range. Daily campaign/Pin values are date-filtered.

Unavailable values are null with statuses, not zeros. Percentage comparisons require
complete comparable daily coverage and equivalent completed-day windows. Rates require
compatible numerator/denominator scopes and sufficient coverage. Missing chart days
remain gaps. Follower growth needs real snapshots at both boundaries for every
selected account; no pre-connection history or sales attribution is synthesized.

Financial metrics are deliberately unavailable: there is no verified expenditure,
conversion or revenue attribution source. Existing campaign IDs provide the future
join point for a separate financial-attribution service without conflating raw social
metrics with money or inferring book sales from engagement.

## Data retention and deletion

Disconnecting stops collection, while retained analytics follows the existing privacy
policy. No new retention promise or automatic purge of marketing assets is introduced.
For an eligible verified data-deletion request, stop/disconnect collection first,
then remove the requested tenant/account's analytics observations/media/sync state
using a parameterized administrative operation. Deleting the social-account record
cascades its analytics via tenant-bound foreign keys, but existing publishing/history
foreign keys must be reviewed first. Do not delete publisher-created posts, assets or
campaigns merely to remove analytics, and do not delete global metric definitions as
if they were a user's personal data.

## Verification and limitations

- Schema inspection was read-only on the running app database. `postgres` was also
  checked and has no Marketing tables. No supplied password was copied into code.
- New SQL is tested in the existing isolated PGlite/PostgreSQL engine; migrations 006,
  040 and 042 are applied there, with actual constraints and production reporting,
  refresh and worker SQL exercised. No test points at the live database.
- Backend tests cover parsing, zero vs missing, unsupported metrics, safe errors,
  mixed scope rejection, state/authorization, tenant boundaries, cache control,
  idempotency, preserved history, refresh throttling, worker recovery/scheduling,
  disconnect guards, resumed pagination, comparisons and follower growth.
- Frontend tests cover missing vs zero, percentages, sparse dates, platform separation,
  date presets, initial server rendering and tab placement. Nivo's browser bundle is
  compiled by Next; the CJS unit runner stubs only that browser renderer.
- Marketing route compiled with Nivo and returned HTTP 200 on the local dev server.
  This compilation request is not a valid authenticated end-to-end analytics session.
- Authorized live read-only calls returned actual Pinterest organic daily analytics
  and followers; those normalized values were persisted to an isolated PostgreSQL
  database and read back through the real report service successfully.
- Facebook returned follower counts. Tested v25 unique-viewer insight metric names
  were accepted with HTTP 200 but empty data; one Page had permission limitations.
  Empty insights are not proof of working reach/view reporting and are not stored as zero.
- Instagram returned follower/like/comment counts. Insights requests reported missing
  permission. Full
  account/media insights remain unverified until authorization is granted.
- TikTok's existing token has publishing/basic scopes only. Analytics is blocked by
  missing `video.list` and `user.info.stats`; no live TikTok analytics success is claimed.
- The pre-existing general provider error-redaction regression tests for HTTP 400/401
  fail on the unchanged publishing error path. Analytics uses its redacted
  refresh path and separately tested safe collector. This change does not claim to fix
  the unrelated default publishing error-message behavior.
- Full-project TypeScript checking has existing diagnostics outside Analytics; no
  diagnostics were reported for the new Analytics component or MarketingDashboard.
  No diagnostics were reported for the changed Marketing proxy either. The normal
  Windows production build failed with `spawn EPERM`. A validation-only threaded
  build compiled the optimized bundle successfully in 79 seconds but failed during
  static-page generation with `()=>null could not be cloned`. The complete production
  build has therefore NOT passed. This runner does not change the repository's Next
  configuration or dependencies.

Local verification commands (no production access):

```text
python -m pytest -p no:cacheprovider tests/test_marketing_analytics.py tests/test_marketing_selection.py -q
node src/features/marketing/analytics.test.cjs
node src/lib/marketingProxy.test.cjs
```

## Deployment gate and rollback

No commit, push or deployment is authorized by this implementation.

Before approval, review migration 042 against the **inksuite** database and back it
up using the established process. Apply the migration explicitly, deploy the backend,
verify startup/imports and existing publishing, activate the separate analytics worker,
then deploy the frontend. Existing tokens will not gain new scopes automatically;
use the explicit analytics authorization flow with eligible test accounts.

Release requires the normal deployment build, authenticated UI checks on desktop and
mobile, worker concurrency checks in the deployment environment, and verified real
Facebook/Instagram/TikTok insights after their required grants. Current code and mock
tests alone are not confirmation of end-to-end production operation.

To roll back code, stop the analytics worker and restore the previous frontend/backend;
the additive tables can remain so historical data is preserved. If an approved rollback
must discard analytics data, separately back it up and explicitly drop observations,
media, sync, then definitions and the analytics target lookup index. Do not drop existing
Marketing tables. Dropping analytics tables destroys their new history and is not an
automatic migration step.

## Files

Backend: `app/marketing/analytics.py`, `analytics_providers.py`, `analytics_worker.py`,
`analytics_routes.py`; existing `routes.py`, `oauth.py`, `providers.py`, `publishing.py`;
`migrations/042_social_analytics.sql`; `deploy/inksuite-analytics-worker.service`;
`tests/test_marketing_analytics.py`, `tests/marketplace_pg/analytics_rpc.cjs`; this document.

Frontend: `src/features/marketing/Analytics.tsx`, `analytics.css`, `analytics.test.cjs`,
the existing `MarketingDashboard.tsx`, `src/lib/marketingProxy.ts`, and
`src/lib/marketingProxy.test.cjs`. Proxy tests cover a second tenant membership,
unknown/missing workspace rejection, missing session, CSRF, and correct publishing
route scoping. The previously edited privacy policy remains
unchanged by this analytics implementation. No packages/dependencies were added.

## Primary references

- https://developers.facebook.com/docs/instagram-platform/insights/
- https://developers.facebook.com/docs/graph-api/reference/page/insights/
- https://github.com/facebook/facebook-python-business-sdk/blob/main/facebook_business/adobjects/insightsresult.py
- https://github.com/pinterest/api-description/blob/main/v5/openapi.yaml
- https://developers.tiktok.com/docs/en/tiktok-api-v2-video-list
- https://developers.tiktok.com/docs/en/tiktok-api-v2-get-user-info
- https://developers.tiktok.com/docs/en/tiktok-api-v2-video-object


## Compact analytics presentation update

Platform panels use provider accents and show only supported metrics with collected
values (including real zeroes). Null or unsupported metrics are omitted from panel
cards, chart choices and empty table columns. Missing cells within a mixed-platform
table remain an em dash. Measurement definitions remain available in tooltips;
reporting-scope and missing-day semantics are unchanged in the API and chart data.
Filters align in six desktop columns and reflow using the available container width.
One compact selectable trend replaces three charts. Post tables scroll internally;
campaign comparison is collapsed initially. The Analytics tab omits the dashboard's
connection summary and all separate analytics authorization controls.

Existing tokens must be reauthorized once for newly requested permissions. Code
cannot grant platform approvals or user consent. No schema changes are needed for
this update, and all tenant authorization and filtering remain enforced server-side.

## Published-post comment management

The normal Facebook and Instagram connection flow now also requests comment
management access for the History popup: `pages_manage_engagement` for Facebook
and `instagram_business_manage_comments` for Instagram. Existing analytics scopes
are retained. No messaging scopes are added. The analytics collectors themselves
remain read-only. See SOCIAL_COMMENTS.md for the on-demand moderation endpoints,
workspace checks, action confirmation, and live-verification limits.
