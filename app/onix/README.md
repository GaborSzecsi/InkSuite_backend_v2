# ONIX Feed module

- **Router**: `app/onix/router.py` — mount at `/api` (e.g. `/api/onix/products`).
- **DB**: Run `migrations/001_onix_feed.sql` after the main schema (tenants, works, editions).
- **Secrets**: SFTP passwords and SSH keys are stored in AWS Secrets Manager only; Postgres holds `secret_arn`.
- **Env**: `ONIX_EXPORT_BUCKET` (optional) for S3 storage of generated XML. AWS credentials for Secrets Manager and S3. `paramiko` for SFTP.
