# Recipient request verification and photo uploads

Recipients do not need InkSuite accounts. Opening a request exposes no title,
recipient address, contact details or prefill until the recipient enters the
code emailed to the original address stored in bookdev_requests. The caller
cannot choose another verification address.

## Deploy together

1. Apply migrations/025_bookdev_request_verification.sql to the backend database.
   The migration is additive and can be applied more than once.
2. Set BOOKDEV_VERIFICATION_SECRET to at least 32 random bytes (for example,
   a randomly generated 64-character hex value) in the backend secret manager.
   Never put it in frontend environment variables or commit it. Verification
   fails closed with HTTP 503 if the secret is absent or too short.
3. Deploy the backend and frontend together. Require HTTPS. Configure the same
   tenant SMTP settings used for existing book development request emails.
4. Reissue old requests that lack contributor_party_id in payload_json. New
   requests pin an existing contributor assigned to the selected tenant/book;
   ambiguous assignments must be selected explicitly.
5. Check a real request in an incognito browser: unverified API GET must return
   only verification_required, email must reach the original recipient, and
   submitting must permanently close that request. Test a different browser
   with the forwarded URL and confirm it still requires the email code.

## Controls

- Link expiry continues to use BOOKDEV_REQUEST_EXPIRES_DAYS (default 14 days).
- Eight-digit codes expire after 10 minutes and are consumed on verification.
  HMAC hashes, five failed attempts, a 60-second resend cooldown and five email
  sends per hour are stored in PostgreSQL and survive backend restarts.
- Verification grants a random, hashed, request-scoped session for at most one
  hour and never beyond the link expiry. It grants no application account access.
- The frontend proxy stores proof in a Secure, HttpOnly, SameSite=Strict cookie
  scoped to that request's API path, strips it from JSON responses, and clears
  the cookie after completion. Direct API clients must present the same proof.
- All submissions check the stored tenant/book/contributor identity and lock
  the request row before changes and completion. Completed, expired and revoked
  requests cannot return prefill, verify or submit again.
- Photos use POST /requests/{token}/photo-upload. They upload and complete the
  request in one operation; the old /photo metadata submission rejects input.
  The client cannot select storage keys, book IDs, contributor IDs or URLs.
- Raw uploads are limited to 10 MiB, multipart requests to 10 MiB + 64 KiB and
  dimensions to 25 million pixels. JPEG, PNG and WebP are decoded, verified,
  and re-encoded as a clean JPEG without EXIF or trailing content. S3 identity
  and the canonical contributor key come from saved database records.
- Generic upload mutations now require account authentication even when
  REQUIRE_AUTH is disabled. Generic contributor-photo uploads also enforce
  tenant membership and use the same image validation and file-size limits.
  Public request uploads use their separate verification.
- Token pages use no-store, no-referrer, noindex and nosniff headers.

## Verification commands

Backend (disposable PostgreSQL through the existing PGlite test helper):

    python tests/bookdev_security_integration.py

Frontend (from repository root):

    node --preserve-symlinks --preserve-symlinks-main src/app/api/project-management/[...path]/route.test.cjs

No real SMTP messages, S3 uploads or production database changes occur in tests.

## Limits of this review

Forwarding cannot be detected. Sharing only the URL does not share a verified
browser session. A recipient who deliberately shares their code, or an attacker
who controls their mailbox/device, can still gain that request's access.

The request row lock and completion are transactional in PostgreSQL. An S3 write
can succeed before a database commit fails; retries overwrite the same scoped
photo key rather than creating unlimited objects. S3 integration and SMTP
configuration still require staging checks.

This is a scoped implementation review, not a platform security certification.
Production should also have edge rate/concurrency limits, current dependencies,
restricted database/S3/IAM permissions, monitoring and an independent penetration
test. Existing unrelated upload/read routes and the rest of the platform were
not comprehensively audited here.
