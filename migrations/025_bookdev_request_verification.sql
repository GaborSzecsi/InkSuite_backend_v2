-- Apply before deploying recipient verification. No existing request token is changed.
BEGIN;
CREATE TABLE IF NOT EXISTS bookdev_request_verification (
    request_id uuid PRIMARY KEY REFERENCES bookdev_requests(id) ON DELETE CASCADE,
    code_hash text,
    code_expires_at timestamptz,
    failed_attempts integer NOT NULL DEFAULT 0,
    last_sent_at timestamptz,
    window_started_at timestamptz NOT NULL DEFAULT now(),
    sends_in_window integer NOT NULL DEFAULT 0,
    session_hash text,
    session_expires_at timestamptz,
    verified_at timestamptz
);
COMMIT;
