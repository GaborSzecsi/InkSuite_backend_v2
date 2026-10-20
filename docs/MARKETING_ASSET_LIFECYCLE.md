# Marketing asset library and post media

The reusable Assets library is separate from media retained with posts. New posts default to moving their media to **Post media** only after all destinations publish successfully. The composer offers **Keep this media in reusable Assets after publishing**. A shared asset stays in the library if another non-cancelled post explicitly keeps it. Existing posts retain the old behavior until changed. Title source files are unaffected.

Assets offers **Move selected to Post media** for existing media attached to posts. This removes library clutter without changing posts or freeing bytes. Post media offers **Add to reusable Assets**. Both views support the normal reviewed permanent deletion flow. Storage counts both views, including private editor files.

Permanent deletion first lists affected posts and projects, then requires a current server-generated confirmation. It deletes the selected object, linked draft/failed/cancelled/scheduled local posts, and dependent editable projects and queued exports. Published and partially published history, provider URLs, and other files remain. Remaining deliveries of a partially published post are cancelled. Existing social-platform copies are never deleted by these endpoints. Rendering or delivery already in progress must finish/cancel before deletion. Book source files remain protected.

Bulk deletion rechecks each plan. Removing an earlier selected file may remove shared posts/projects; a smaller dependency list can proceed, while new or changed dependencies require another review. The tenant storage lock, row locks, current plan token, and storage reference checks prevent silent removal of new dependencies. The separate Clear storage action continues to protect active dependencies.

## Deployment

Deploy the backend, frontend, and publishing worker together. Before starting the new backend/worker, run migration **040_marketing_asset_library.sql** against `inksuite`:

```sql
\i /home/ubuntu/inksuite/app/migrations/040_marketing_asset_library.sql
```

Restart `inksuite-api` and the existing marketing publishing worker after pulling the new code. The video worker does not need lifecycle code changes. Migration 040 is additive and repeatable; no existing media is moved or deleted by the migration. It has only been exercised in a rollback-only transaction, not permanently applied to production.

## S3 privacy fix applied 2026-10-20

The live `inksuite-data` policy retains its existing public asset/title rules and adds a private-video read denial scoped to `tenants/*/assets/marketing/private/video/*`. `s3:authType` permits authenticated header requests and signed query-string requests, rejecting anonymous reads. A second denial excludes authenticated principals from other AWS accounts, so the existing broad public allow cannot expose private videos to an arbitrary AWS account. InkSuite-issued signed URLs use its own account identity. Deployment probe: anonymous GET 403, signed GET 200, production `ensure_private_storage` passes. Synthetic probe files were removed. No customer export was queued or customer media deleted.

## Validation

64 focused backend tests and 12 frontend tests pass. PostgreSQL QA exercised migration reruns, visibility changes, shared-asset retention, linked deletion, queued/running guards, publication-history/URL retention, and finished-export/other-file preservation. All QA rows were rolled back; S3 deletion was mocked. No type errors were reported in modified marketing/video files; the wider frontend retains existing unrelated type errors. A broader backend run has one existing failure in provider-message redaction (`test_error_classification_redacts_provider_message[400-False-False]`); provider code was unchanged by this feature.
