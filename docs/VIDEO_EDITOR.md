# InkSuite integrated video editor V1

## Integration and user workflow

The editor lives in the existing Assets browser and the scheduler's media picker. There is no new application page or navigation item. Drop a single video onto Assets to open it in the editor; existing videos have Edit Video. Create Video starts blank, and Create Video from Selection uses selected photographs/videos. Editing opens in a 96% viewport modal with optional full screen. Save project keeps editable source; Export MP4 creates a separate asset. Originals are never overwritten. Use exported video refreshes the picker and attaches the output only when the user selects it; export never publishes or schedules a post.

The modal uses globals.css tokens, shared button styling, rounded surfaces, border colors and the InkSuite typography. Media library, center preview, right inspector and bottom timeline adapt for tablets.

## Platform and persistence

- Frontend: src/features/video/{model,Preview,Timeline,Properties,VideoEditor}.tsx/ts and video.css. Marketing editors.tsx supplies all entry points.
- Backend: app/video/{schema,composition,render,service,routes,worker,privacy,scratch}.py.
- Existing marketing authentication, tenant access, asset rows, S3 client, storage accounting and scheduler APIs are reused. No second asset-management system was created.
- Migrations 037 and 038 add video_projects, video_render_jobs and video_asset_metadata. Both were applied through the user's localhost:5433 PostgreSQL tunnel on 2026-10-19. No existing customer rows were deleted.
- Project document is schema_version 1 JSON: canvas, ordered tracks, clips, optional work_ids and campaign_id. Project identity, tenant, name, revision, optional source asset and timestamps are API metadata. No React/DOM or renderer-specific objects are persisted.
- Clip start, duration, trim, animation/keyframe time are seconds. Source playback time = trim + (timeline time - start) * speed. Transform x/y are normalized center coordinates, scale is dimensionless, rotation is clockwise degrees, opacity is 0..1. Crop is a normalized source rectangle.
- Later tracks overlay earlier tracks; clips within a track draw in document order. Source media may be reused. Track and clip IDs are stable. JSON preserves transform, text styles, crop, keyframes, audio and transition/animation settings.
- PUT requires the current revision; mismatched edits return 409. Autosave is debounced. Save failures remain visible and never overwrite a competing revision. Undo/redo is local to an open editor. Concurrent real-time editing is not implemented.
- Blank, brand and book workflows share the same document schema and renderer. Books and campaigns are optional. Existing title images can be imported. Brand templates are editable projects that can be named and duplicated, using existing logos/media and saved canvas/font choices. No duplicate tenant-branding table was added.

## Functional tools

Layered visual, text and audio tracks; clip drag/drop and reordering; compatible-track moves; edge trimming; source in point; speed changes; split; duplicate; remove; zoom; snapping; frame-stepped scrubbing; track mute/visibility; undo/redo. Image/video transforms include normalized position, scale, rotation, opacity, crop, fit/fill, brightness, contrast and saturation. Position/scale/rotation/opacity keyframes interpolate linearly.

Text layers have wrapping, font/size/weight/color/alignment/background/outline/shadow and independent timing. Bundled Nunito Sans (SIL OFL) is used by browser and Pillow; its license is included. Motion and entrance/exit presets include fades, slides, zooms, pan, pop, rise, bounce, wipe and typewriter. Transition controls overlap adjacent same-track clips rather than inserting blank frames; dissolve is a conventional opacity dissolve, not a pixel-noise transition.

Audio imports and original video sound support trimming, speed, volume, mute, looping and fades. FFmpeg mixes tracks into AAC. The approved-music gallery is populated only from uploaded audio with administrator-maintained license metadata (category, source, license type, attribution and permitted usage). No music was downloaded or redistributed. Templates provide editable text/color starting points and can be duplicated into reusable brand projects.

Keyboard: Space play/pause; S split; Delete remove; Ctrl/Cmd S save; Ctrl/Cmd Z undo; Ctrl/Cmd Shift Z redo; left/right on a focused clip moves by a frame (Shift moves one second).

## Rendering and privacy

The web API only queues jobs. Run app.video.worker separately; it does not require an open browser and does not reuse the publishing queue. Each job stores an immutable project revision snapshot. Worker downloads tenant-validated S3 references, uses FFmpeg to decode video, Pillow to composite frames, FFmpeg/libx264 for H.264, and FFmpeg for AAC mixing. No shell interpolation or arbitrary project URLs/paths are executed. Temporary directories are cleaned. Worker checks cancellation/progress, detects stale jobs, and supports retry by queuing another snapshot.

720p is 720 x 1280 for 9:16 or 1280 x 720 for 16:9. 1080p is 1080 x 1920 or 1920 x 1080; square/4:5 use the same short-side resolution. Export is 30 fps. Original orientation is handled by FFmpeg autorotation and Pillow EXIF transposition. Make editing copy queues a private H.264/AAC (or AAC-only) normalization copy for browser-incompatible media. It preserves source aspect ratio and replaces matching timeline references when completed in the open editor.

New editor imports, normalization outputs and exports use tenants/{slug}/assets/marketing/private/video/. Uploads request AES256 encryption and no public ACL. Exported videos are registered in social_media_assets and linked through video_asset_metadata. Authenticated APIs return expiring signed URLs; post loading refreshes those URLs for provider delivery. The existing marketing proxy now streams binary responses correctly.

Storage accounting includes the new private prefix. Deletion protects files used by saved video projects and queued/running snapshots, as well as existing protected posts. Clearing marketing storage never deletes a project's source media.

### Required production privacy fix (not applied)

Live QA found that the bucket currently serves a synthetic object anonymously from the new private path. Signed download succeeded, but anonymous download also returned HTTP 200. The synthetic files were removed and all QA database records rolled back.

privacy.py now performs a harmless sentinel probe BEFORE importing customer media or exporting an output. It refuses writes if unsigned access is not denied. Therefore private uploads/exports intentionally remain blocked on the current bucket configuration.

Merge deploy/video-private-policy-statement.json into the existing bucket policy (preserve all other statements). It denies only anonymous s3:GetObject/GetObjectVersion to the new private video prefix, preserving authenticated worker access and signed downloads. The aws:PrincipalAccount anonymous condition is documented by AWS: https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_condition-keys.html#condition-keys-principalaccount . Do not replace the whole policy with the one-statement fragment. A full merged proposal is supplied for review separately. No bucket policy was changed and no production deployment occurred.

## Deployment requirements

1. Review/apply the private-prefix bucket policy correction with explicit approval. Verify signed GET 200 and unsigned GET 403 using a disposable object before enabling writes.
2. Deploy backend and frontend together. Both migrations have already been applied to the supplied inksuite database; apply them in order to other environments. Do not run destructive migrations.
3. Install existing backend requirements (Pillow, psycopg, requests, existing S3 SDK). FFmpeg/ffprobe must be available; FFMPEG_BINARY and FFPROBE_BINARY can override their paths. No Remotion or new paid service is required. The existing local FFmpeg distribution is GPLv3; preserve its license and review deployment packaging obligations.
4. Start python -m app.video.worker alongside the existing web and publishing services. deploy/inksuite-video-worker.service is a template; adjust the actual checkout/virtualenv paths before enabling it. A dedicated worker was not enabled on production.
5. API and worker need existing S3 GetObject/PutObject/DeleteObject permissions for the new private prefix. Signed URLs use that existing identity. Keep browser GET/Range CORS working for the application origins. Do not add anonymous access.
6. Configure at least 3 GB RAM and sufficient temporary disk; service template bounds memory/tasks and uses low CPU priority. Limits: five-minute composition, 80 clips, 12 tracks, at most 16 simultaneous clips, 500 MB per source/output, 2 GB source budget per job, two queued/running jobs per tenant, one running render per tenant, and a 30-minute processing timeout. Uploads use the existing multipart form API; resumable mobile uploads remain future work.

## Validation and limits

Final focused suite including cleanup: 47 backend tests and 8 frontend tests passed. The modified video/marketing/proxy files have no TypeScript errors; the broader repository still has existing unrelated type errors.


Automated tests cover generic schema, invalid timing/types, tenant isolation, revision conflicts, animation/keyframe evaluation, split timing, undo/redo, snapping, export resolutions, H.264/AAC streams, image-only export, audio mixing and cancellation. PostgreSQL transaction QA exercised create/save/reopen/list/duplicate/delete, revision conflicts, queueing and cancellation, then rolled all test data back. A real worker/S3 test created an encrypted MP4 in the existing asset system and verified signed download; it discovered the anonymous access issue above, so the run is not reported as a privacy pass.

Browser QA exercised the actual editor component with synthetic media and a test-only API adapter: text editing, aspect ratio change, save, frame stepping and splitting. The temporary QA route and media fixtures were removed. No authentication was bypassed on real app routes.

Browser preview uses Canvas and native media elements; final rendering uses Pillow/FFmpeg. Timing equations, project format and font files match, but antialiasing, font metrics and color-filter implementations can differ slightly between browser and export. V1 does not claim pixel-identical rendering. The encoded export is authoritative. Large/high-layer projects can preview below 30 fps on slower tablets. There is no supplied commercial music catalog, advanced color grading, stabilization, real-time collaboration, mobile UI, AI editing, or AI generation. Saved branding/template projects replace a separate brand-management UI in this version.
## Optional cleanup after export

After a successful composition export, **Keep finished video only…** opens a review dialog. Keeping the editable project remains the default. The dialog lists each file, its size, and whether it will be deleted or kept, and includes a final-video preview. Nothing is deleted merely by exporting, downloading, or using a video in a social post.

By default, cleanup deletes only media explicitly imported into that project from inside the editor, along with the editable project. A separate unchecked option, **Also remove unused original uploads from Assets**, includes exclusive original library inputs such as a video originally dropped onto Assets. Book assets, campaign library assets, licensed music, finished exports, sources owned by other projects, and files referenced by any social post or another active project/render remain protected.

Cleanup requires a completed export of the current project revision, no active exports for that project, a current server-generated confirmation token, and a nonempty finished object verified in S3. Tenant administrators perform deletion, matching existing asset-deletion permissions. The backend recomputes the plan and rechecks references before each file deletion. Failed removals are reported; remaining files can be reviewed in Assets. The completed video remains a normal reusable asset. Re-editing it later starts a new edit from the flattened video, rather than recovering the removed layers.

Migration **039_video_project_cleanup.sql** adds working-project ownership and a durable finished-video marker to existing video asset metadata. The worker marks new composition exports as finished and associates normalization copies with the project that requested them. Existing legacy prepared assets are conservatively retained. Editor uploads without a reliable ownership marker are kept by default and require the explicit original-upload option.

Deploy backend, worker, and frontend together after applying migration 039. In an existing psql session connected to the `inksuite` database:

```sql
\i /home/ubuntu/inksuite/app/migrations/039_video_project_cleanup.sql
```

Then restart both services after pulling the code:

```bash
sudo systemctl restart inksuite-api inksuite-video-worker
sudo systemctl status inksuite-api inksuite-video-worker --no-pager -l
```

Migration 039 was validated twice inside a rolled-back transaction, and cleanup endpoints were exercised against PostgreSQL with synthetic records and mocked S3 operations. No actual customer objects or projects were removed. The migration has **not** been permanently applied, and this cleanup feature has **not** been deployed.

Cleanup validation: 47 focused backend tests and 8 frontend tests passed. Browser QA verified the review dialog and completed cleanup flow with a test-only API adapter. The temporary QA route was removed.
