# Marketplace invitations (local implementation)

Apply `migrations/008_marketplace_invitations.sql` after 007, using the existing database process. This migration has not been applied by Codex. It adds access requests and typed grants; it creates no users, memberships, or storefronts and preserves linked users with RESTRICT foreign keys.

Restart the local backend. The homepage Sign up / Log in link opens /login, with a Publisher / Reader / Bookstore account-type dropdown and a Request invitation dialog. The same dialog is available at /marketplace/auth.

Requests contain name (bookstore name for bookstores), email, and reader/bookstore access. The existing questionnaire SMTP sender sends the request to szecsi.gabor@gmail.com. The review link requires that exact signed-in account AND platform superadmin. GET links never approve anything. The reviewer explicitly approves or declines in the app. Approval emails a seven-day, single-use signup link. Resending invalidates the previous link. The backend stores only the token hash; signup URLs carry the token in the fragment rather than the HTTP URL. The browser keeps it in sessionStorage while signing in and removes it on acceptance.

Configure MARKETPLACE_FRONTEND_URL=http://localhost:3000 for local testing (default). These local links work only on a device with that local frontend running. For eventual public deployment, explicitly set the production HTTPS origin. Email uses routers.contract_invites SMTP helpers, tenant_email_settings, and the existing AWS Secrets Manager SMTP secret, exactly as the book-development questionnaires do. MARKETPLACE_MAIL_TENANT defaults to marble-press; it is server-controlled, never supplied by the requester. No SES setup is needed. No live notification emails were sent during automated tests.

New accounts are created through the existing Cognito pool, with default platform_role=user, a private Marketplace profile and a personal actor. No publisher tenant memberships or administrator permissions are granted. The approved reader/librarian/bookstore classification is stored in marketplace_access_grants. Bookstore commerce, fulfillment, commissions and specialized librarian tools are not enabled by this classification. Existing accounts must sign in as the invited email to accept; invitations never reset existing passwords.

The runtime needs its existing Cognito admin create/set-password permissions, plus delete permission for cleanup of a new identity if signup fails. Email delivery errors are surfaced, not reported as successful. Cognito and PostgreSQL cannot share a transaction; new-account failure cleanup is best effort. If identity creation succeeds but cleanup cannot complete after a database failure, use Cognito recovery and accept with the existing account rather than resetting its password through the invitation.

## Retired test accounts

Read-only checks found no users or Marketplace profiles for szecsiwork@gmail.com or klizsu9@gmail.com. Codex's shell has no AWS credentials, so Cognito existence/removal remains unverified. Run scripts/remove_marketplace_test_accounts.py in the same AWS-authenticated environment as the backend, with the backend on PYTHONPATH. It only targets those two exact emails, stops if a database user now exists, verifies the Cognito email, signs out and deletes the test identity. It never prints credentials. This cleanup is separate from normal invitation processing.

## Checks

Disposable PGlite migration/API tests use production route code with mocked SMTP and Cognito. Coverage includes approval authorization, no approval on GET, duplicate requests, supported account types, expiry, replay, resend rotation, existing-account password protection, signup cleanup, email failure rollback and no tenant privileges. Live SQL checks were read-only. Frontend scoped TypeScript checks include the public login/homepage and all invitation screens.

Login requires approved reader/bookstore access. Bookstore names are checked against the approved invitation name, ignoring case and repeated spaces. Existing librarian grants can sign in under Reader; new librarian invitations are no longer offered. No new migration is required for these login/SMTP changes.

## Email approval without login

New request notifications link to /marketplace/approval with a 256-bit opaque token in the URL fragment. The pending request stores only its hash and a 48-hour expiry using existing token_hash/expires_at columns. Opening/inspecting the link does not mutate approval state. Explicit POST Approve/Decline locks the row and consumes the capability: approval replaces it with a different seven-day signup token, rejection clears it. Pending approval tokens cannot be used for signup and signup tokens cannot approve requests. No login is required for these email-capability endpoints. The authenticated platform administration queue remains available separately.

Old review URLs offer a button that emails a replacement capability only to the configured APPROVER, with a two-minute cooldown. The request ID alone never grants approval or reveals applicant details. No additional migration is needed. Restart the backend to load these routes, then use the replacement link or request again. Do not forward approval links.
