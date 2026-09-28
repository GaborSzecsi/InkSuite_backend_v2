# Banking information under Financials

Local page: /app/financials/banking

## Deployment
1. Back up the database and apply migrations/014_banking_access.sql to the existing InkSuite database. The secure_payments collection tables must already exist. This additive migration does not modify payment accounts or their ciphertext.
2. In the backend runtime, install `python -m pip install -r requirements-banking.txt`, then restart the backend.
3. Deploy the frontend changes. The dedicated Next banking proxy forwards the signed-in access token and enforces same-origin POST requests.
4. Confirm the backend IAM identity has kms:Encrypt and kms:Decrypt on BANKING_KMS_KEY_ID. Existing collection uses kms:GenerateDataKey. Authenticator encryption uses context purpose=banking-authenticator, tenant_id=<tenant UUID>, user_id=<user UUID>. Account decryption retains the original purpose=payment-account, tenant_id=<tenant slug>, payment_account_id=<account UUID>. Do not broaden permissions to unrelated keys.

## Access
A current tenant membership is required, with role tenant_admin or module_permissions.banking=true. Financials module access alone does not grant banking access. Platform administrators have no automatic cross-tenant bypass. To show the Financials sidebar for a delegated banking user, also grant the existing financials module permission.

The list returns active submitted accounts only, a recipient name from collection requests, bank name and last four account/IBAN characters. Replacement accounts pending approval are not silently substituted. Existing contributor collection and approval workflows are unchanged.

## Authenticator setup and recovery
Sign out and sign in immediately before first setup or replacement (fresh Cognito auth_time within ten minutes is required). Scan the QR with Google Authenticator, Microsoft Authenticator or another TOTP app, then confirm a current code. Secrets are KMS-encrypted and setup expires after ten minutes. Save the ten recovery codes displayed once. Only their keyed hashes are stored. A recovery code or current TOTP code plus a fresh sign-in can replace the authenticator. There is deliberately no administrator bypass if both phone and recovery codes are lost.

Each reveal verifies a fresh six-digit TOTP code under a database row lock. Codes are single-use across reveal operations within the accepted time step. Five failures lock verification for fifteen minutes. A code used to confirm setup cannot immediately be reused to reveal a record; wait for the next code.

Details are not sent until permissions and verification pass. Reveals are logged without banking numbers or codes. The browser masks the record after at most sixty seconds and on window blur, tab hiding, close or navigation. Requests/responses are no-store; no banking secrets are placed in browser storage, logs or URLs. This cannot prevent an authorized viewer from retaining information they have already seen.

## Validation
Run tests/banking_access_integration.py with the repository Python dependencies and tests/marketplace_pg Node dependencies. It uses disposable PGlite and fake KMS with encryption-context validation, never AWS or real banking records. The test covers idempotent migration, tenant/role isolation, masked responses, authenticator setup, replay prevention, lockouts, recovery and audited reveal. Real KMS permissions and the production schema still need deployment verification.
