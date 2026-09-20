-- Review and apply manually after 007. No catalog records are changed.
BEGIN;
CREATE TABLE public.marketplace_media (
 id uuid PRIMARY KEY,
 owner_actor_id uuid NOT NULL REFERENCES public.marketplace_actors(id) ON DELETE RESTRICT,
 created_by_user_id uuid NOT NULL REFERENCES public.users(id) ON DELETE RESTRICT,
 media_type text NOT NULL CHECK (media_type IN ('image','video')),
 status text NOT NULL DEFAULT 'pending_upload' CHECK(status IN ('pending_upload','uploaded','processing','ready','failed','deleted')),
 bucket text NOT NULL,
 original_key text NOT NULL UNIQUE,
 original_version text,
 original_filename text NOT NULL,
 declared_mime text NOT NULL,
 file_size bigint NOT NULL CHECK(file_size>0),
 reserved_bytes bigint NOT NULL CHECK(reserved_bytes>=file_size),
 mime_type text,
 width integer,
 height integer,
 duration double precision,
 variants jsonb NOT NULL DEFAULT '[]' CHECK(jsonb_typeof(variants)='array'),
 error_code text,
 attempts integer NOT NULL DEFAULT 0,
 lease_token uuid,
 lease_until timestamptz,
 next_attempt_at timestamptz NOT NULL DEFAULT now(),
 created_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now(),
 deleted_at timestamptz,
 purged_at timestamptz,
 storage_checked_at timestamptz
);
CREATE TABLE public.marketplace_post_media (
 post_id uuid NOT NULL REFERENCES public.marketplace_posts(id) ON DELETE CASCADE,
 media_id uuid NOT NULL REFERENCES public.marketplace_media(id) ON DELETE RESTRICT,
 position smallint NOT NULL CHECK(position BETWEEN 0 AND 9),
 PRIMARY KEY(post_id,media_id), UNIQUE(post_id,position)
);
CREATE INDEX marketplace_media_owner ON public.marketplace_media(owner_actor_id,created_at DESC,id DESC);
CREATE INDEX marketplace_media_jobs ON public.marketplace_media(next_attempt_at,created_at) WHERE status IN ('uploaded','processing');
CREATE INDEX marketplace_media_cleanup ON public.marketplace_media(deleted_at) WHERE status='deleted' AND purged_at IS NULL;
CREATE INDEX marketplace_media_maintenance ON public.marketplace_media(storage_checked_at NULLS FIRST,id) WHERE status IN ('ready','failed','uploaded');
CREATE INDEX marketplace_post_media_reverse ON public.marketplace_post_media(media_id,post_id);
-- Existing author index orders created_at; feed orders published_at including connections.
CREATE INDEX marketplace_posts_published_author ON public.marketplace_posts(author_actor_id,published_at DESC,id DESC) WHERE status='published';
ALTER TABLE public.marketplace_posts DROP CONSTRAINT marketplace_posts_body_check;
ALTER TABLE public.marketplace_posts ADD CONSTRAINT marketplace_posts_body_check CHECK(char_length(btrim(body)) BETWEEN 0 AND 10000);
CREATE TRIGGER marketplace_media_touch BEFORE UPDATE ON public.marketplace_media FOR EACH ROW EXECUTE FUNCTION public.marketplace_touch_updated_at();
CREATE TRIGGER marketplace_media_identity BEFORE UPDATE ON public.marketplace_media FOR EACH ROW EXECUTE FUNCTION public.marketplace_immutable_fields('id','owner_actor_id','created_by_user_id','bucket','original_key');
CREATE FUNCTION public.marketplace_validate_post_media() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM public.marketplace_posts p JOIN public.marketplace_media m ON m.id=NEW.media_id
 WHERE p.id=NEW.post_id AND p.author_actor_id=m.owner_actor_id AND m.status='ready') THEN
 RAISE EXCEPTION 'Marketplace media ownership or readiness mismatch' USING ERRCODE='23514'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER marketplace_post_media_owner BEFORE INSERT OR UPDATE ON public.marketplace_post_media FOR EACH ROW EXECUTE FUNCTION public.marketplace_validate_post_media();
COMMENT ON TABLE public.marketplace_media IS 'Reusable native social assets; originals private, derivatives immutable. Durable PostgreSQL worker queue; no cache required.';
COMMIT;
