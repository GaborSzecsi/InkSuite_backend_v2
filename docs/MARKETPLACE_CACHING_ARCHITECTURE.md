# Marketplace architecture and future caching

## Today

`HTTP adapters (*_routes.py) → domain services (*_service.py) → data access (*_repository.py) → PostgreSQL`.

The original implementation put SQL, business rules and routing in the same modules. The new adapters retain the same URL, dependency, request model and return shape. The old `identity`, `catalog`, `social`, `messages`, `media`, `invitations`, and `notifications` imports are compatibility aliases to their services. This preserves existing integrations and test patches. Service methods take explicit validated inputs and authenticated users; they can also be called without HTTP. Existing HTTPException domain errors are retained for compatibility.

Services own transaction lifetimes, authorization, rate limits, validation, response projections and mutations. SQL functions were extracted without changing their semantics. `core.py` remains the shared transaction/authorization helper, including its existing permission queries; it is not a second ORM. Repositories accept the transaction cursor. No cache client, cache configuration, Redis dependency, Valkey cluster or new cache infrastructure exists. PostgreSQL is the sole authority.

The feed previously performed separate author, engagement, book and edition queries per post. `feed_repository.py` now batches author metadata, counts, likes and full public book projections; `feed_service.hydrate` combines these with one batch of native attachments. Correlated aggregates use the existing indexes and preserve public book filtering. A 20-post integration fixture with attached books asserts at most nine SQL executions, including transaction setup and viewer authorization. Legacy S3 cover discovery is still an existing compatibility path; it is not a database query and should eventually be replaced with stored cover keys.

Existing indexes already cover likes in both directions, comments by post, follows, connections, blocks, participants and conversation history. Migration 010 adds only media ownership/queue/cleanup/reverse-reference indexes and a published-author chronological index. The latter differs from the existing created-at author index and the existing public-only feed index. Counters remain transactional PostgreSQL aggregates rather than potentially inconsistent denormalized values.

Existing OFFSET paging remains compatible and bounded. The next pagination evolution is an additive opaque cursor containing `(published_at, id)` and the scope/author filter, using the same descending tuple comparison. Keep OFFSET during frontend migration; do not silently change existing clients. No deep-page cache is planned.

## Future optional cache boundary

Add targeted reads inside services, after authoritative authorization. Callers and HTTP adapters remain unchanged. Return to PostgreSQL on cache misses, timeouts, errors, flushes or complete loss of the cache. Never cache signed media URLs, raw authorization decisions or direct-message bodies. Cache failures must not break InkSuite.

| Candidate | Proposed key | TTL | Successful mutation / invalidation boundary |
|---|---|---|---|
| Public profile projection | `marketplace:profile:{id}:public:v{version}` | 5–15 min | `identity_service.save_profile`, `media_service.identity_image` |
| Public post data, without viewer fields | `marketplace:post:{id}:v{version}` | 1–5 min | `social_service.create_post`, `delete_post` |
| Counts | `marketplace:post:{id}:counts` | 30–120 sec | like/unlike, create/delete comment, comment interactions |
| Following IDs | `marketplace:following:{actor}:v{version}` | 1–5 min | follow/unfollow/block |
| Authorized first feed page | `marketplace:feed:{actor}:{scope}:first:v{relationship_version}` | 30–60 sec | short TTL; viewer relationship version |
| Public organization projection | `marketplace:organization:{id}:public:v{version}` | 5–15 min | `catalog_service.save_org`, identity-image changes |

These keys and TTLs are proposals, not implemented caching. Viewer fields such as `liked` must never leak into shared objects. Recheck blocked/deleted/suspended/restricted content against PostgreSQL before returning it or signing media. Profile privacy and accepted connections have separate projections. Organization membership revocation must apply immediately. Do not serve stale security-sensitive results while revalidating.

All invalidation must happen **after the transaction commits**, not within a repository operation or immediately before a service returns from inside a transaction context. For reliable event delivery later, use a transactional outbox and an idempotent consumer. Do not enumerate every follower to invalidate their feeds; use short TTLs and versioning, then consider hybrid fan-out only after measuring scale.

Future stampede protection: bounded cache timeouts, TTL jitter, request coalescing for identical keys and short-lived rebuild locks with PostgreSQL fallback. Stale-while-revalidate is appropriate only for non-sensitive public metadata with fresh authorization.

## Media is a separate subsystem

PostgreSQL stores metadata, associations and durable processing state. A dedicated private S3 bucket stores originals and immutable derivatives. CloudFront delivers only processed objects using OAC and signed URLs. The standalone media worker claims PostgreSQL jobs with `FOR UPDATE SKIP LOCKED`; upload completion writes the job state transactionally. This reuses the existing Marketing worker deployment model without polling S3 for new files. No binary belongs in PostgreSQL or a future cache.

## Measurement and AWS evolution

`observability.py` logs HTTP route templates, status and elapsed time, plus transaction query counts and SQL time. Slow queries log a hash of the SQL shape, never parameters, bodies, tokens or signed URLs. Enable the `inksuite.marketplace.performance` logger in production. Use PostgreSQL `pg_stat_statements` and AWS database monitoring separately for connection counts, DB CPU/load, slow-query distributions and saturation. Service timings are not a load test or a promise of capacity for 1,000 users.

Investigate caching when measured feed latency repeatedly approaches 300–500 ms, database load/connection pressure is sustained, or repeated identical reads are a demonstrated bottleneck. Tune SQL and connection pooling before introducing a cache. The current DB helper opens a connection per transaction; pooling is a separate measured deployment improvement, not part of this compatibility refactor.

Future topology: Amplify → FastAPI/EC2 → PostgreSQL, with optional ElastiCache/Valkey accessed privately inside the appropriate VPC. No public cache endpoint. PostgreSQL continues to function alone if that cluster is deleted.

## Remaining measured follow-ups

The feed batch removes the principal per-post database expansion. Existing bounded catalog/library edition expansion, inbox/network actor projection and comment attribution still make per-item reads in their respective services. Those are identified follow-ups for batching if endpoint timings justify it; this change does not claim that every Marketplace list is constant-query. Existing invitation mail submission also remains synchronous within its transaction to preserve its established failure/approval semantics. Neither behavior is hidden by a cache.
