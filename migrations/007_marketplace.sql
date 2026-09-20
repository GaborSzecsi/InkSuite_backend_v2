-- InkSuite Marketplace V1 -- PROPOSED, NOT APPLIED
-- Prepared against the read-only inspection of public schema on 2026-09-17.
-- PostgreSQL 16. One-time, atomic migration: deliberate failure on name conflicts.
-- Does not seed users, organizations, books, memberships or public content.
-- Does not modify existing table definitions, grants, RLS or application data.
-- Application authorization is REQUIRED; these constraints are not user permissions.
BEGIN;
SET LOCAL search_path = public, pg_catalog;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Fail before creating objects if required core columns are missing or incompatible.
DO $$
DECLARE r record;
BEGIN
  FOR r IN SELECT * FROM (VALUES
    ('users','id','uuid'), ('tenants','id','uuid'),
    ('memberships','tenant_id','uuid'), ('memberships','user_id','uuid'),
    ('memberships','role','text'), ('memberships','module_permissions','jsonb'),
    ('works','id','uuid'), ('works','tenant_id','uuid'),
    ('editions','id','uuid'), ('editions','tenant_id','uuid'), ('editions','work_id','uuid')
  ) AS required(table_name,column_name,udt_name)
  LOOP
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns c
      WHERE c.table_schema='public' AND c.table_name=r.table_name
        AND c.column_name=r.column_name AND c.udt_name=r.udt_name) THEN
      RAISE EXCEPTION 'Marketplace prerequisite missing: public.%.% (%)',
        r.table_name,r.column_name,r.udt_name;
    END IF;
  END LOOP;
END $$;

CREATE TABLE public.marketplace_profiles (
  user_id uuid PRIMARY KEY REFERENCES public.users(id) ON DELETE RESTRICT,
  username text NOT NULL UNIQUE CHECK (username ~ '^[a-z0-9][a-z0-9_]{2,29}$'),
  display_name text NOT NULL CHECK (char_length(btrim(display_name)) BETWEEN 1 AND 100),
  bio text NOT NULL DEFAULT '' CHECK (char_length(bio)<=2000),
  avatar_asset_ref jsonb CHECK (avatar_asset_ref IS NULL OR jsonb_typeof(avatar_asset_ref)='object'),
  location_text text NOT NULL DEFAULT '' CHECK (char_length(location_text)<=200),
  profile_visibility text NOT NULL DEFAULT 'public' CHECK (profile_visibility IN ('public','private')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE public.marketplace_organizations (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid UNIQUE REFERENCES public.tenants(id) ON DELETE RESTRICT,
  organization_type text NOT NULL CHECK (organization_type IN ('publisher','bookstore')),
  name text NOT NULL CHECK (char_length(btrim(name)) BETWEEN 1 AND 200),
  slug text NOT NULL UNIQUE CHECK (slug ~ '^[a-z0-9]+(-[a-z0-9]+)*$' AND char_length(slug)<=100),
  description text NOT NULL DEFAULT '' CHECK (char_length(description)<=5000),
  website text NOT NULL DEFAULT '' CHECK (char_length(website)<=2048),
  logo_asset_ref jsonb CHECK (logo_asset_ref IS NULL OR jsonb_typeof(logo_asset_ref)='object'),
  banner_asset_ref jsonb CHECK (banner_asset_ref IS NULL OR jsonb_typeof(banner_asset_ref)='object'),
  location_text text NOT NULL DEFAULT '' CHECK (char_length(location_text)<=200),
  verified_status text NOT NULL DEFAULT 'unverified' CHECK (verified_status IN ('unverified','verified')),
  status text NOT NULL DEFAULT 'draft' CHECK (status IN ('draft','active','hidden','suspended')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- One canonical actor per personal profile or organization. No second user system.
CREATE TABLE public.marketplace_actors (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id uuid UNIQUE REFERENCES public.marketplace_profiles(user_id) ON DELETE RESTRICT,
  organization_id uuid UNIQUE REFERENCES public.marketplace_organizations(id) ON DELETE RESTRICT,
  messaging_preference text NOT NULL DEFAULT 'anyone'
    CHECK (messaging_preference IN ('anyone','connections_only','nobody')),
  status text NOT NULL DEFAULT 'active' CHECK (status IN ('active','suspended','deactivated')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (num_nonnulls(user_id,organization_id)=1)
);

CREATE TABLE public.marketplace_books (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  publisher_organization_id uuid NOT NULL REFERENCES public.marketplace_organizations(id) ON DELETE RESTRICT,
  work_id uuid NOT NULL UNIQUE REFERENCES public.works(id) ON DELETE RESTRICT,
  slug text NOT NULL UNIQUE CHECK (slug ~ '^[a-z0-9]+(-[a-z0-9]+)*$' AND char_length(slug)<=180),
  marketplace_status text NOT NULL DEFAULT 'draft' CHECK (marketplace_status IN ('draft','public','hidden')),
  discoverable boolean NOT NULL DEFAULT false,
  featured boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- An explicit edition allowlist, not copied product metadata.
-- Public catalog queries must join this table: never serialize every work edition.
CREATE TABLE public.marketplace_book_editions (
  marketplace_book_id uuid NOT NULL REFERENCES public.marketplace_books(id) ON DELETE CASCADE,
  edition_id uuid NOT NULL UNIQUE REFERENCES public.editions(id) ON DELETE RESTRICT,
  is_primary boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (marketplace_book_id,edition_id)
);
CREATE UNIQUE INDEX marketplace_book_one_primary_edition
  ON public.marketplace_book_editions(marketplace_book_id) WHERE is_primary;

CREATE TABLE public.marketplace_library_items (
  user_id uuid NOT NULL REFERENCES public.marketplace_profiles(user_id) ON DELETE RESTRICT,
  marketplace_book_id uuid NOT NULL REFERENCES public.marketplace_books(id) ON DELETE RESTRICT,
  status text NOT NULL DEFAULT 'saved' CHECK (status IN ('saved','want_to_read','reading','read')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id,marketplace_book_id)
);

CREATE TABLE public.marketplace_posts (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  author_actor_id uuid NOT NULL REFERENCES public.marketplace_actors(id) ON DELETE RESTRICT,
  created_by_user_id uuid NOT NULL REFERENCES public.users(id) ON DELETE RESTRICT,
  body text NOT NULL CHECK (char_length(btrim(body)) BETWEEN 1 AND 10000),
  media_asset_ref jsonb CHECK (media_asset_ref IS NULL OR jsonb_typeof(media_asset_ref)='object'),
  visibility text NOT NULL DEFAULT 'public' CHECK (visibility IN ('public','connections')),
  status text NOT NULL DEFAULT 'draft' CHECK (status IN ('draft','published','deleted','hidden')),
  published_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (status<>'published' OR published_at IS NOT NULL)
);
CREATE TABLE public.marketplace_post_books (
  post_id uuid NOT NULL REFERENCES public.marketplace_posts(id) ON DELETE CASCADE,
  marketplace_book_id uuid NOT NULL REFERENCES public.marketplace_books(id) ON DELETE RESTRICT,
  position smallint NOT NULL DEFAULT 0 CHECK (position>=0),
  PRIMARY KEY (post_id,marketplace_book_id),
  UNIQUE (post_id,position)
);
CREATE TABLE public.marketplace_comments (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  post_id uuid NOT NULL REFERENCES public.marketplace_posts(id) ON DELETE CASCADE,
  author_actor_id uuid NOT NULL REFERENCES public.marketplace_actors(id) ON DELETE RESTRICT,
  created_by_user_id uuid NOT NULL REFERENCES public.users(id) ON DELETE RESTRICT,
  body text NOT NULL CHECK (char_length(btrim(body)) BETWEEN 1 AND 4000),
  status text NOT NULL DEFAULT 'published' CHECK (status IN ('published','deleted','hidden')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE public.marketplace_likes (
  actor_id uuid NOT NULL REFERENCES public.marketplace_actors(id) ON DELETE RESTRICT,
  post_id uuid NOT NULL REFERENCES public.marketplace_posts(id) ON DELETE CASCADE,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (actor_id,post_id)
);
CREATE TABLE public.marketplace_follows (
  follower_actor_id uuid NOT NULL REFERENCES public.marketplace_actors(id) ON DELETE RESTRICT,
  followed_actor_id uuid NOT NULL REFERENCES public.marketplace_actors(id) ON DELETE RESTRICT,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (follower_actor_id,followed_actor_id),
  CHECK (follower_actor_id<>followed_actor_id)
);
CREATE TABLE public.marketplace_connections (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  requester_actor_id uuid NOT NULL REFERENCES public.marketplace_actors(id) ON DELETE RESTRICT,
  recipient_actor_id uuid NOT NULL REFERENCES public.marketplace_actors(id) ON DELETE RESTRICT,
  status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','accepted','declined','cancelled')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  responded_at timestamptz,
  CHECK (requester_actor_id<>recipient_actor_id),
  CHECK ((status='pending' AND responded_at IS NULL) OR (status<>'pending' AND responded_at IS NOT NULL))
);
CREATE UNIQUE INDEX marketplace_connections_pair ON public.marketplace_connections
  (least(requester_actor_id,recipient_actor_id),greatest(requester_actor_id,recipient_actor_id));

CREATE TABLE public.marketplace_conversations (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  conversation_type text NOT NULL DEFAULT 'direct' CHECK (conversation_type='direct'),
  actor_low_id uuid NOT NULL REFERENCES public.marketplace_actors(id) ON DELETE RESTRICT,
  actor_high_id uuid NOT NULL REFERENCES public.marketplace_actors(id) ON DELETE RESTRICT,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  last_message_at timestamptz,
  CHECK (actor_low_id<actor_high_id),
  UNIQUE (actor_low_id,actor_high_id)
);
CREATE TABLE public.marketplace_conversation_participants (
  conversation_id uuid NOT NULL REFERENCES public.marketplace_conversations(id) ON DELETE CASCADE,
  actor_id uuid NOT NULL REFERENCES public.marketplace_actors(id) ON DELETE RESTRICT,
  joined_at timestamptz NOT NULL DEFAULT now(),
  last_read_at timestamptz,
  archived_at timestamptz,
  PRIMARY KEY (conversation_id,actor_id)
);
-- A committed direct conversation must retain both original participants.
ALTER TABLE public.marketplace_conversations
  ADD CONSTRAINT marketplace_conversation_low_participant
    FOREIGN KEY (id,actor_low_id) REFERENCES public.marketplace_conversation_participants(conversation_id,actor_id)
    DEFERRABLE INITIALLY DEFERRED,
  ADD CONSTRAINT marketplace_conversation_high_participant
    FOREIGN KEY (id,actor_high_id) REFERENCES public.marketplace_conversation_participants(conversation_id,actor_id)
    DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE public.marketplace_messages (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  conversation_id uuid NOT NULL REFERENCES public.marketplace_conversations(id) ON DELETE CASCADE,
  sender_actor_id uuid NOT NULL,
  created_by_user_id uuid NOT NULL REFERENCES public.users(id) ON DELETE RESTRICT,
  body text NOT NULL CHECK (char_length(btrim(body)) BETWEEN 1 AND 10000),
  status text NOT NULL DEFAULT 'sent' CHECK (status IN ('sent','deleted')),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  edited_at timestamptz,
  deleted_at timestamptz,
  FOREIGN KEY (conversation_id,sender_actor_id)
    REFERENCES public.marketplace_conversation_participants(conversation_id,actor_id) ON DELETE RESTRICT,
  CHECK ((status='sent' AND deleted_at IS NULL) OR (status='deleted' AND deleted_at IS NOT NULL))
);
CREATE TABLE public.marketplace_blocks (
  blocker_actor_id uuid NOT NULL REFERENCES public.marketplace_actors(id) ON DELETE RESTRICT,
  blocked_actor_id uuid NOT NULL REFERENCES public.marketplace_actors(id) ON DELETE RESTRICT,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (blocker_actor_id,blocked_actor_id),
  CHECK (blocker_actor_id<>blocked_actor_id)
);
CREATE TABLE public.marketplace_reports (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  reporter_actor_id uuid NOT NULL REFERENCES public.marketplace_actors(id) ON DELETE RESTRICT,
  target_actor_id uuid REFERENCES public.marketplace_actors(id) ON DELETE RESTRICT,
  target_post_id uuid REFERENCES public.marketplace_posts(id) ON DELETE RESTRICT,
  target_comment_id uuid REFERENCES public.marketplace_comments(id) ON DELETE RESTRICT,
  target_message_id uuid REFERENCES public.marketplace_messages(id) ON DELETE RESTRICT,
  reason text NOT NULL CHECK (char_length(btrim(reason)) BETWEEN 1 AND 2000),
  status text NOT NULL DEFAULT 'open' CHECK (status IN ('open','reviewed','dismissed','resolved')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (num_nonnulls(target_actor_id,target_post_id,target_comment_id,target_message_id)=1)
);

-- Indexes for both sides of relationships, public catalog/feed and inbox paging.
CREATE INDEX marketplace_organizations_directory ON public.marketplace_organizations(organization_type,name,id) WHERE status='active';
CREATE INDEX marketplace_books_storefront ON public.marketplace_books(publisher_organization_id,marketplace_status,featured,id);
CREATE INDEX marketplace_books_discovery ON public.marketplace_books(updated_at DESC,id DESC) WHERE marketplace_status='public' AND discoverable;
CREATE INDEX marketplace_library_recent ON public.marketplace_library_items(user_id,updated_at DESC,marketplace_book_id);
CREATE INDEX marketplace_library_book ON public.marketplace_library_items(marketplace_book_id);
CREATE INDEX marketplace_posts_feed ON public.marketplace_posts(published_at DESC,id DESC) WHERE status='published' AND visibility='public';
CREATE INDEX marketplace_posts_author ON public.marketplace_posts(author_actor_id,created_at DESC,id DESC);
CREATE INDEX marketplace_posts_creator ON public.marketplace_posts(created_by_user_id);
CREATE INDEX marketplace_post_books_book ON public.marketplace_post_books(marketplace_book_id);
CREATE INDEX marketplace_comments_post ON public.marketplace_comments(post_id,created_at,id);
CREATE INDEX marketplace_comments_author ON public.marketplace_comments(author_actor_id);
CREATE INDEX marketplace_comments_creator ON public.marketplace_comments(created_by_user_id);
CREATE INDEX marketplace_likes_post ON public.marketplace_likes(post_id);
CREATE INDEX marketplace_followers ON public.marketplace_follows(followed_actor_id,created_at DESC);
CREATE INDEX marketplace_connections_requester ON public.marketplace_connections(requester_actor_id,status,updated_at DESC);
CREATE INDEX marketplace_connections_recipient ON public.marketplace_connections(recipient_actor_id,status,updated_at DESC);
CREATE INDEX marketplace_conversations_recent ON public.marketplace_conversations(last_message_at DESC NULLS LAST,id);
CREATE INDEX marketplace_conversations_high ON public.marketplace_conversations(actor_high_id);
CREATE INDEX marketplace_participants_inbox ON public.marketplace_conversation_participants(actor_id,conversation_id);
CREATE INDEX marketplace_messages_history ON public.marketplace_messages(conversation_id,created_at DESC,id DESC);
CREATE INDEX marketplace_messages_sender ON public.marketplace_messages(conversation_id,sender_actor_id);
CREATE INDEX marketplace_messages_creator ON public.marketplace_messages(created_by_user_id);
CREATE INDEX marketplace_blocks_reverse ON public.marketplace_blocks(blocked_actor_id,blocker_actor_id);
CREATE INDEX marketplace_reports_queue ON public.marketplace_reports(status,created_at,id);
CREATE INDEX marketplace_reports_reporter ON public.marketplace_reports(reporter_actor_id);
CREATE INDEX marketplace_reports_actor ON public.marketplace_reports(target_actor_id) WHERE target_actor_id IS NOT NULL;
CREATE INDEX marketplace_reports_post ON public.marketplace_reports(target_post_id) WHERE target_post_id IS NOT NULL;
CREATE INDEX marketplace_reports_comment ON public.marketplace_reports(target_comment_id) WHERE target_comment_id IS NOT NULL;
CREATE INDEX marketplace_reports_message ON public.marketplace_reports(target_message_id) WHERE target_message_id IS NOT NULL;

CREATE FUNCTION public.marketplace_touch_updated_at() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
BEGIN NEW.updated_at=now(); RETURN NEW; END $$;

-- Protect stable identity/ownership columns; normal edits use other fields.
CREATE FUNCTION public.marketplace_immutable_fields() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE field_name text;
BEGIN
  FOREACH field_name IN ARRAY TG_ARGV LOOP
    IF (to_jsonb(NEW)->field_name) IS DISTINCT FROM (to_jsonb(OLD)->field_name) THEN
      RAISE EXCEPTION 'Marketplace %.% cannot be reassigned',TG_TABLE_NAME,field_name USING ERRCODE='23514';
    END IF;
  END LOOP;
  RETURN NEW;
END $$;

-- Core tables have single-column UUID keys, not UNIQUE(id,tenant_id).
-- Validate ownership on new Marketplace rows without altering those core tables.
CREATE FUNCTION public.marketplace_check_catalog_owner() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE valid boolean;
BEGIN
  IF TG_TABLE_NAME='marketplace_books' THEN
    SELECT EXISTS(SELECT 1 FROM public.marketplace_organizations o
      JOIN public.works w ON w.tenant_id=o.tenant_id
      WHERE o.id=NEW.publisher_organization_id AND o.organization_type='publisher' AND w.id=NEW.work_id) INTO valid;
  ELSE
    SELECT EXISTS(SELECT 1 FROM public.marketplace_books b
      JOIN public.marketplace_organizations o ON o.id=b.publisher_organization_id
      JOIN public.works w ON w.id=b.work_id AND w.tenant_id=o.tenant_id
      JOIN public.editions e ON e.work_id=w.id AND e.tenant_id=o.tenant_id
      WHERE b.id=NEW.marketplace_book_id AND e.id=NEW.edition_id) INTO valid;
  END IF;
  IF NOT valid THEN RAISE EXCEPTION 'Marketplace catalog ownership mismatch' USING ERRCODE='23514'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER marketplace_book_owner BEFORE INSERT OR UPDATE ON public.marketplace_books
  FOR EACH ROW EXECUTE FUNCTION public.marketplace_check_catalog_owner();
CREATE TRIGGER marketplace_edition_owner BEFORE INSERT OR UPDATE ON public.marketplace_book_editions
  FOR EACH ROW EXECUTE FUNCTION public.marketplace_check_catalog_owner();

CREATE FUNCTION public.marketplace_check_participant() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
BEGIN
  IF NOT EXISTS(SELECT 1 FROM public.marketplace_conversations c WHERE c.id=NEW.conversation_id
    AND NEW.actor_id IN (c.actor_low_id,c.actor_high_id)) THEN
    RAISE EXCEPTION 'Identity is not a direct conversation participant' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER marketplace_participant_pair BEFORE INSERT OR UPDATE ON public.marketplace_conversation_participants
  FOR EACH ROW EXECUTE FUNCTION public.marketplace_check_participant();
CREATE FUNCTION public.marketplace_initialize_participants() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
BEGIN
  INSERT INTO public.marketplace_conversation_participants(conversation_id,actor_id)
    VALUES(NEW.id,NEW.actor_low_id),(NEW.id,NEW.actor_high_id);
  RETURN NEW;
END $$;
CREATE TRIGGER marketplace_conversation_participants AFTER INSERT ON public.marketplace_conversations
  FOR EACH ROW EXECUTE FUNCTION public.marketplace_initialize_participants();
CREATE FUNCTION public.marketplace_message_activity() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
BEGIN
  UPDATE public.marketplace_conversations SET last_message_at=greatest(last_message_at,NEW.created_at)
    WHERE id=NEW.conversation_id;
  RETURN NEW;
END $$;
CREATE TRIGGER marketplace_message_activity AFTER INSERT ON public.marketplace_messages
  FOR EACH ROW EXECUTE FUNCTION public.marketplace_message_activity();

DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['marketplace_profiles','marketplace_organizations','marketplace_actors',
    'marketplace_books','marketplace_library_items','marketplace_posts','marketplace_comments',
    'marketplace_connections','marketplace_conversations','marketplace_reports'] LOOP
    EXECUTE format('CREATE TRIGGER marketplace_touch BEFORE UPDATE ON public.%I FOR EACH ROW EXECUTE FUNCTION public.marketplace_touch_updated_at()',t);
  END LOOP;
END $$;

CREATE TRIGGER marketplace_profile_identity BEFORE UPDATE ON public.marketplace_profiles
  FOR EACH ROW EXECUTE FUNCTION public.marketplace_immutable_fields('user_id');
CREATE TRIGGER marketplace_org_identity BEFORE UPDATE ON public.marketplace_organizations
  FOR EACH ROW EXECUTE FUNCTION public.marketplace_immutable_fields('id','tenant_id','organization_type');
CREATE TRIGGER marketplace_actor_identity BEFORE UPDATE ON public.marketplace_actors
  FOR EACH ROW EXECUTE FUNCTION public.marketplace_immutable_fields('id','user_id','organization_id');
CREATE TRIGGER marketplace_book_identity BEFORE UPDATE ON public.marketplace_books
  FOR EACH ROW EXECUTE FUNCTION public.marketplace_immutable_fields('id','work_id','publisher_organization_id');
CREATE TRIGGER marketplace_post_identity BEFORE UPDATE ON public.marketplace_posts
  FOR EACH ROW EXECUTE FUNCTION public.marketplace_immutable_fields('id','author_actor_id','created_by_user_id');
CREATE TRIGGER marketplace_comment_identity BEFORE UPDATE ON public.marketplace_comments
  FOR EACH ROW EXECUTE FUNCTION public.marketplace_immutable_fields('id','post_id','author_actor_id','created_by_user_id');
CREATE TRIGGER marketplace_conversation_identity BEFORE UPDATE ON public.marketplace_conversations
  FOR EACH ROW EXECUTE FUNCTION public.marketplace_immutable_fields('id','conversation_type','actor_low_id','actor_high_id');
CREATE TRIGGER marketplace_participant_identity BEFORE UPDATE ON public.marketplace_conversation_participants
  FOR EACH ROW EXECUTE FUNCTION public.marketplace_immutable_fields('conversation_id','actor_id');
CREATE TRIGGER marketplace_message_identity BEFORE UPDATE ON public.marketplace_messages
  FOR EACH ROW EXECUTE FUNCTION public.marketplace_immutable_fields('id','conversation_id','sender_actor_id','created_by_user_id','created_at');

COMMENT ON TABLE public.marketplace_books IS 'Thin public listing; catalog fields remain in works/editions. Public API must verify current tenant ownership and explicit visibility.';
COMMENT ON TABLE public.marketplace_book_editions IS 'Only these editions may be projected publicly for a listing. No copied edition metadata.';
COMMENT ON TABLE public.marketplace_actors IS 'Identity reference, not authorization. Resolve current user and existing tenant memberships for every protected request.';
COMMENT ON COLUMN public.marketplace_posts.media_asset_ref IS 'Reference only to existing asset infrastructure. Backend validates schema, ownership and public eligibility; never accepts arbitrary storage keys.';
COMMENT ON TABLE public.marketplace_conversations IS 'Private direct threads. Actor pair is canonical and immutable. Participants are inserted automatically.';
COMMENT ON TABLE public.marketplace_messages IS 'Private messages. API must check active actor, participant permission, blocking, recipient preference and rate limit in a transaction.';
COMMENT ON TABLE public.marketplace_blocks IS 'Directional block record. Application must reject new conversations/messages when either participant has blocked the other.';

COMMIT;
