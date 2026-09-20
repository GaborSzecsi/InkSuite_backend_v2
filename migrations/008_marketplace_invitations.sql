-- Apply once after 007_marketplace.sql. No account or catalog data is changed.
BEGIN;
SET LOCAL lock_timeout = '5s';
CREATE TABLE public.marketplace_access_requests (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
 name text NOT NULL CHECK (length(btrim(name)) BETWEEN 1 AND 100),
 email text NOT NULL CHECK (email = lower(email)),
 access_type text NOT NULL CHECK (access_type IN ('reader','librarian','bookstore')),
 status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','approved','rejected','accepted','revoked')),
 token_hash text UNIQUE,
 expires_at timestamptz,
 reviewed_by uuid REFERENCES public.users(id) ON DELETE RESTRICT,
 reviewed_at timestamptz,
 user_id uuid REFERENCES public.users(id) ON DELETE RESTRICT,
 notified_at timestamptz,
 accepted_at timestamptz,
 created_at timestamptz NOT NULL DEFAULT now(),
 CHECK ((token_hash IS NULL) = (expires_at IS NULL))
);
CREATE UNIQUE INDEX marketplace_access_requests_open_email ON public.marketplace_access_requests(email) WHERE status IN ('pending','approved');
CREATE INDEX marketplace_access_requests_queue ON public.marketplace_access_requests(status,created_at DESC);
CREATE TABLE public.marketplace_access_grants (
 user_id uuid PRIMARY KEY REFERENCES public.users(id) ON DELETE RESTRICT,
 access_type text NOT NULL CHECK (access_type IN ('reader','librarian','bookstore')),
 request_id uuid NOT NULL UNIQUE REFERENCES public.marketplace_access_requests(id) ON DELETE RESTRICT,
 granted_at timestamptz NOT NULL DEFAULT now()
);
COMMIT;
