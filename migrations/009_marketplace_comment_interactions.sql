-- Additive Marketplace comment interactions. Apply after 007_marketplace.sql.
-- No catalog/user records are changed. Existing comments remain root comments.
BEGIN;
ALTER TABLE public.marketplace_comments ADD COLUMN parent_comment_id uuid;
ALTER TABLE public.marketplace_comments ADD CONSTRAINT marketplace_comment_id_post UNIQUE(id,post_id);
ALTER TABLE public.marketplace_comments ADD CONSTRAINT marketplace_comment_parent_post
  FOREIGN KEY(parent_comment_id,post_id) REFERENCES public.marketplace_comments(id,post_id) ON DELETE CASCADE;
ALTER TABLE public.marketplace_comments ADD CONSTRAINT marketplace_comment_not_self CHECK(parent_comment_id IS DISTINCT FROM id);
CREATE INDEX marketplace_comment_thread ON public.marketplace_comments(post_id,parent_comment_id,created_at,id);
CREATE TRIGGER marketplace_comment_parent_identity BEFORE UPDATE ON public.marketplace_comments
  FOR EACH ROW EXECUTE FUNCTION public.marketplace_immutable_fields('parent_comment_id');
CREATE TABLE public.marketplace_comment_likes (
  comment_id uuid NOT NULL REFERENCES public.marketplace_comments(id) ON DELETE CASCADE,
  actor_id uuid NOT NULL REFERENCES public.marketplace_actors(id) ON DELETE RESTRICT,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(comment_id,actor_id)
);
CREATE INDEX marketplace_comment_likes_actor ON public.marketplace_comment_likes(actor_id);
COMMIT;
