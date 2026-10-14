InkSuite Marketplace messaging and reader fixes — implementation review

Status
Implemented in the existing frontend and FastAPI repositories. No production deployment, database write, or migration was performed. The user confirmed the missing publisher appeared after changing its draft status. The inspected database had InkSuite Demo / inksuite-demo in draft and Marble Press / marble-press active; drafts remain excluded intentionally. If these differ from what the UI now shows, confirm the running environment and that the publication change was saved. Do not expose all draft publishers to readers.

Reader fixes
Corrected UTF-8 mojibake in Visit publisher arrows, Marketplace loading/search labels, profile/storefront separators and catalog administration labels. Existing canonical publisher links are retained. URL segments are decoded for storefront lookup. Moved reader Marketplace mounting into its shared layout so navigating its pages preserves the messaging session and floating conversation.

Reused infrastructure and PostgreSQL review
Reused Marketplace actors, profiles, organizations, accepted connections, blocking/reporting, role and tenant permissions, authentication through the existing server proxy, transaction helpers, conversations, participants and messages.
Live schema inspection was read-only with TLS through the configured localhost PostgreSQL tunnel.
marketplace_conversations: UUID key, direct type, ordered low/high actor IDs, unique pair, creation/update/activity timestamps. Deferred foreign keys ensure both conversation participants exist.
marketplace_conversation_participants: composite (conversation_id, actor_id) primary key; joined_at, last_read_at, archived_at.
marketplace_messages: UUID key, conversation/sender/creator FKs, body, status, created/edited/deleted timestamps. Composite sender FK requires conversation participation.
Existing triggers initialize both participants and update last-message activity on insert. Existing history indexes are reused.

Messaging behavior
Default Inbox: newest 15 conversations ordered by most recent message timestamp, creation timestamp and stable ID. No offset pagination or loading controls in the default Inbox. Existing history is never deleted by displacement. Search is separate and scans participant names/usernames and non-deleted message content across stored conversations. The Unread view retrieves unread conversations regardless of Inbox displacement; search/unread result pages and accepted connections may paginate. Badge counts unread conversations consistently, including those outside the Inbox.
Opening a conversation loads its latest 50 messages and allows backward history retrieval. Previously loaded messages stay visible when newer messages displace them from the latest page. Activity in an old conversation returns it to the top. Only accepted connections can start or send messages, even when messaging preference previously permitted anyone. Blocked conversations retain participant-authorized read-only history; sending is denied.
Enter sends; Shift+Enter adds a line. Plain text is escaped by React. Send errors preserve the draft and client-generated UUID for retry. Idempotency applies per sender actor and rejects reuse for different content/conversations. Read cursors include message UUIDs to handle equal timestamps and cannot regress. Reads are marked only in a visible, unminimized conversation while near the bottom. Presence shows online/offline without last-seen timestamps. Connections search covers all accepted connections, prioritizes online and then recent contact, 15 per page. Pending requests are excluded.

Live architecture
REST remains the persistent source of truth. The session endpoint authenticates using the existing HTTP-only cookie proxy and issues a random, hashed, one-use ticket valid for 60 seconds. The browser passes the ticket in the first WebSocket frame, never a URL or log. Origin allowlist, handshake/frame limits and session caps apply. Each API process shares one dedicated PostgreSQL LISTEN connection and fans invalidations only to subscribed actors belonging to the conversation. PostgreSQL NOTIFY triggers fire after committed messages/read updates and work across API workers/instances. Notifications contain no message bodies or authentication tokens. A dedicated thread supports Windows and Linux event loops. Listener reconnect and browser reconnect trigger REST recovery. Browser reconnect uses capped exponential backoff and jitter; periodic REST refresh is a fallback.
Each browser/device session has its own row. Heartbeats every 25 seconds expire presence after 90 seconds. Closing one session leaves other sessions online; disconnect cleanup is shielded from cancellation. Stale ephemeral rows are removed when new sessions are requested. Socket heartbeats and invalidation delivery recheck current actor/user permissions. Existing per-user message rate limits and 10,000-character limits remain. Message content travels through authorized REST, not socket subscriptions. Relevant visible chats suppress duplicate arrival notices; other incoming messages update the badge and show an unobtrusive notice.

New/updated API endpoints, under /api/marketplace
GET /conversations?actor_id=... — fixed latest 15; old offset parameters cannot expose subsequent Inbox pages.
GET /conversations/search?actor_id=...&q=...&unread_only=true&offset=... — historical name/content and unread retrieval.
GET /unread?actor_id=... — unread conversation count.
POST /conversations — existing, accepted connection required.
GET /conversations/{id}/messages?actor_id=...&before=... — existing persistent history.
POST /conversations/{id}/messages — adds optional client_message_id UUID.
PUT /conversations/{id}/read/{message_id}?actor_id=... — stable, monotonic cursor.
GET /messaging/connections?actor_id=...&q=...&offset=... — accepted connections/presence.
POST /messaging/session?actor_id=... — authenticated single-use WebSocket ticket.
WS /messaging/live — first-frame ticket authentication and invalidations.

Files changed — frontend
src/app/api/marketplace/[...path]/route.ts
src/app/marketplace/layout.tsx (new)
src/app/marketplace/page.tsx
src/app/marketplace/[...path]/page.tsx
src/features/marketplace/Discovery.tsx
src/features/marketplace/Manage.tsx
src/features/marketplace/Marketplace.tsx
src/features/marketplace/Messages.tsx
src/features/marketplace/api.ts
src/features/marketplace/shared.tsx
src/features/marketplace/marketplace.css
src/features/marketplace/useMessagingLive.ts (new)
tests/marketplace-messaging.test.cjs (new)

Files changed — backend
app/marketplace/messages_repository.py
app/marketplace/messages_routes.py
app/marketplace/messages_service.py
app/marketplace/realtime_routes.py (new)
app/marketplace/realtime_service.py (new)
app/marketplace/routes.py
app/marketplace/schemas.py
migrations/035_marketplace_messaging.sql (new)
requirements-marketplace.txt
 tests/marketplace_integration.py
 tests/messaging_integration.py (new)
 tests/test_marketplace_realtime.py (new)
docs/MARKETPLACE_MESSAGING.md (new)
An existing user modification to tests/test_distribution_sql.py was left untouched.

Validation
PASS: python tests/messaging_integration.py — 71 real REST/SQL requests against disposable PGlite, including migration, 36 conversations, cap/displacement, name/content/literal-wildcard search, historical unread access, restored history, backward pagination, old-conversation activity, cursor ties/non-regression, authorization, block enforcement, idempotency, one-use tickets, presence expiry and multiple sessions.
PASS: python -m pytest tests/test_marketplace_realtime.py tests/test_marketplace_security.py tests/test_marketplace_native_media.py -q -p no:cacheprovider -k 'not images_are and not malformed_media and not cloudfront_signatures and not real_video' — 38 tests. WebSocket routing uses the actual FastAPI route with a mocked shared notification transport; production PostgreSQL LISTEN/NOTIFY has not been tested end-to-end.
PASS: node tests/marketplace-messaging.test.cjs — 4 frontend tests: default Inbox and controls, restored text/escaping, reconnect/backoff/cleanup and first-frame tickets, readable publisher labels and canonical links. Tests use real React server rendering and isolated transport/data fixtures.
PASS: Python compile checks and git diff whitespace checks.
Frontend TypeScript check: no Marketplace diagnostics; full project still fails with unrelated errors in book metadata, book development and other areas.
Full existing Marketplace integration: reaches native-media processing, then fails with Windows PermissionError on temporary media work. Four existing native-media tests requiring temporary directories could not run because of filesystem permissions.
Browser/visual QA: blocked because Next dev server child-process spawn fails with EPERM. Mobile layout and two real authenticated browser accounts still require verification. No production build or deployed end-to-end claim is made.

Rollout requirements
1. Explicit approval is required before applying migration 035 to a production database or deploying. It is transactional, adds client_message_id, a unique idempotency index, last_read_message_id, an activity index, an ephemeral session table and notification triggers; it does not delete conversation history or alter publisher status. Apply after 007 using the existing migration process. Existing conversations and messages remain compatible; legacy read timestamps treat all equal-time messages as read until a new UUID cursor is saved.
2. Install requirements-marketplace.txt (psycopg[binary]>=3.2.4 for notification timeout/buffering).
3. Backend ALLOW_ORIGINS must list exact frontend origins, comma-separated, including scheme and port. The WebSocket endpoint fails closed without an allowed Origin.
4. Frontend server MARKETPLACE_WEBSOCKET_URL must be a browser-reachable wss://.../api/marketplace/messaging/live URL in production. Local development may derive ws://... from the existing backend origin. Keep the frontend ticket route same-origin and preserve its Origin checks. Do not put tokens in URL parameters.
5. Configure the reverse proxy/load balancer to upgrade the WebSocket route and allow heartbeats/long-lived connections. Use a session-capable PostgreSQL connection for LISTEN (not a transaction-pooling proxy). Reserve one listener connection per backend process; all workers must use the same database. Existing database credentials must permit session-table writes and NOTIFY/LISTEN. TLS must be enabled for the connected database, which rejected an unencrypted connection during inspection.
6. Deploy/restart frontend and backend together after migration. Verify the ticket route, origin and proxy configuration with two real authenticated accounts before calling messaging live.

Manual acceptance with two accounts
Accept a Marketplace connection. Send A to B with B's panel closed; confirm badge/notice and saved unread conversation. Open B's chat and confirm read synchronization across B's two tabs. Take B offline, send from A, and confirm retrieval after B reconnects. Disable/restore the socket and verify reconnect recovery without duplicate messages. Create more than 15 active conversations: verify Inbox cap, no pagination, older search and unread retrieval. Send in an old conversation and verify it moves to the top. Load older message pages, minimize/restore, and navigate between Marketplace pages. Block the other identity and verify sending denial with saved participant history. Check both publisher cards, Visit publisher links, readable loading text, and responsive mobile navigation.

Remaining limitations
No production rollout or real-account browser QA has occurred. Search uses parameterized literal ILIKE substring matching; large-history query plans and load performance need profiling before asserting capacity for thousands of users. PostgreSQL notifications are transient invalidations; durable REST history plus reconnect/fallback refresh recovers missed updates. Message read/delivery state is implemented per conversation; per-message delivery ticks and typing indicators are not included. Existing retention/deletion policies remain responsible for permanent history lifecycle. No email or browser push notifications were added.
