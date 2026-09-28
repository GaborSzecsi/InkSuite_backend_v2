BEGIN;
CREATE TABLE IF NOT EXISTS public.book_development_notes (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES public.tenants(id) ON DELETE CASCADE,
    user_id uuid NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
    work_id uuid NOT NULL REFERENCES public.works(id) ON DELETE CASCADE,
    content text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS book_development_notes_owner_work_updated_idx
    ON public.book_development_notes (tenant_id, user_id, work_id, updated_at DESC, id DESC);
COMMIT;
