# Published-post comments — local implementation

## Product flow

Marketing > Drafts / History > a published Facebook or Instagram destination >
Post & comments. The existing external View post link remains available. A popup
shows the provider's published caption/media when readable, falling back to the
saved InkSuite preview when needed. Each published Page/account has its own tab.
Failed/unpublished destinations and other platforms have no comment-management entry.

The first version loads on demand, paginates 25 comments/replies at a time, and
provides Refresh. There is no inbox, background comment polling, or webhook service.
Available actions are public reply, hide/unhide, and permanent deletion with an
explicit confirmation. Facebook actions follow the per-comment can_comment,
can_hide and can_remove flags. Instagram actions remain subject to Meta's API
restrictions. Replies to replies are posted into their original top-level thread.
A failed/uncertain write is never retried automatically. Refresh before retrying.

## Permissions and review

The existing combined connection flow adds:
- Facebook: pages_manage_engagement. Existing pages_read_user_content and
  pages_read_engagement support reading. Existing publishing/analytics scopes remain.
- Instagram Login: instagram_business_manage_comments, alongside basic,
  content_publish and manage_insights. No messaging scope is requested.

Reconnect existing accounts after approval/granting these permissions. External
users require the appropriate App Review / Advanced Access. Test with real
professional Instagram accounts and owned Facebook Pages. A review video should
show selecting a published post, opening comments, replying and demonstrating
only the moderation actions the app is requesting and can actually perform.

References: Meta's Instagram API collection and Graph Comment reference:
https://www.postman.com/meta/instagram/documentation/6yqw8pt/instagram-api
https://developers.facebook.com/docs/instagram-platform/instagram-api-with-instagram-login/comment-moderation/
https://developers.facebook.com/docs/graph-api/reference/comment/
Meta's web documentation was rate-limited during implementation. Read-only live
probes verified both post previews and the Instagram comment-list field selection.
Actual live mutations still require a controlled acceptance test after authorization.

## API and isolation

Existing tenant-authenticated Marketing router/proxy:
- GET social/targets/{target_uuid}/discussion
- GET social/targets/{target_uuid}/comments?cursor=...&parent=...
- POST social/targets/{target_uuid}/comments/actions
  JSON: {handle, action: reply|hide|unhide|delete, message?}

Reads require Marketing module access. Mutations require the existing tenant-admin
or superadmin publishing permission. CSRF/origin protection remains in the frontend
proxy. Account/target lookups bind tenant_id, reject non-published destinations and
recheck the account's connected state under a row lock for each request. A completed
disconnect therefore prevents subsequent calls. Provider calls use existing token
refresh/encryption and redacted errors; no credentials are sent to the browser.

Every comment and cursor has an encrypted, authenticated 30-minute handle bound
to tenant, target, account, published provider post, and operation kind. Handles
are minted only from provider edges rooted at the owned published post. A caller
cannot substitute a raw comment ID, a parent from another post, a cursor from another
thread, or a handle from another workspace. Fixed API hosts and validated numeric
object IDs are used; paging.next URLs are never fetched or exposed. Server checks
both administrator authorization and the signed action capabilities.

No new tables or migrations. Comment bodies, commenter names and reply drafts are
not persisted. No-store responses contain only the current page of data. Existing
marketing_audit records action type, provider and comment ID, without text or
participant identities. No automatic token revocation or publishing behavior changes.

## Validation and remaining acceptance

Automated tests exercise real SQL tenant ownership, foreign targets/handles,
thread-bound cursors, signature/expiry rejection, disconnect/unpublished guards,
provider-specific mutation paths, unavailable capabilities, administrator enforcement,
reply length checks, redacted errors, uncertain writes and audit privacy.
Frontend tests cover published-only entry points, platform tabs, saved preview,
role/capability controls, and escaped text. Existing analytics/proxy checks remain.

Read-only production probe (no deployment or live mutations): Facebook published
preview succeeded, but comments were rejected by Meta with permission error #10.
Instagram published preview and comments listing succeeded (the selected post had
no comments). Application permission setup alone does not update an existing token.

Local browser interaction tests use explicitly labeled SAMPLE POST fixtures; those
comments/replies and preview artwork are not live user data. Verify reply, hide,
unhide, delete confirmation and tab switching there. Desktop/mobile screenshots
are design previews, not evidence that live Meta mutations have been accepted.

Deployment is pending user approval. After deployment/reconnect, use a disposable
comment to validate live actions on each platform and record the review screencast.

## Empty lists versus unavailable comment content

An empty comments edge does not establish that a post has no comments. The first
empty page is now cross-checked with the provider's comment counter. The API returns
reported_count and visibility (available, not_returned, or unknown). Nonzero counters
with an empty list display a Meta visibility/access message; a verified zero can
show No comments yet. Counter failure remains unknown. Malformed list responses
are errors rather than silently coerced to an empty array. No API retry loop or
extra moderation action is performed. Counter requests are limited to empty first
pages, not every loaded comment or pagination request.

Instagram reconnection now records permission/scope values only if Meta actually
returns them in its OAuth token responses. The previous fixed basic/publish list
was not a reliable record of granted comment access. Missing metadata is left
unknown; a requested permission is never recorded as a confirmed grant.

Read-only investigation on October 28, 2026: Meta reported 11 comments on New deal
Katie Horwitch and 3 on PUB DAY: Chasing the Light, but returned empty Instagram
comments lists even with fields=id. The Facebook test post returned 1 like and 0
comments; its empty comment list matched the live counter. The Facebook Page token
was valid and had pages_read_user_content and pages_manage_engagement granted.
Instagram's remaining empty-list cause needs the app's mode/access level checked;
no definitive missing permission or development-mode diagnosis has been established.
No live comments were changed and no production source/configuration was modified.

## Verified Facebook-linked read fallback for Instagram

An October 28 follow-up reproduced five readable Instagram comments through
graph.facebook.com using the workspace's existing Facebook Page credential,
while graph.instagram.com returned an empty list for that same media. The
comment reader now attempts that path when the original first page is empty
and Meta reports a nonzero count. It does not copy test tokens or replace
stored Instagram credentials, and does not change publishing or analytics.

The original Instagram credential must first verify that the target media's
owner matches the connected Instagram account. Only connected Facebook Pages
in the same tenant are candidates. Each Page's linked Instagram ID must match
the owner returned for that exact media through Facebook Graph. This bridges
the different Instagram ID namespaces without matching mutable usernames.
No Page from another workspace can supply credentials.

The selected read account is sealed into comment and cursor handles. Replies
and subsequent pages revalidate that connected Page and both ownership checks;
disconnect or linkage changes invalidate the view. Moderation continues using
the original Instagram connection and its Instagram Login comment permission.
This is a read fallback, not proof that the legacy moderation permission has
been granted or credited by App Review. No migration or new dependency is needed.
