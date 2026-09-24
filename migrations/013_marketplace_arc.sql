-- ARC extension. See docs/ARC.md and the pre-migration schema assessment.
BEGIN;
SET LOCAL lock_timeout = '5s';
DO $$ BEGIN
 IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='marketplace_library_items'::regclass AND contype='p' AND pg_get_constraintdef(oid)='PRIMARY KEY (user_id, marketplace_book_id)') THEN
 RAISE EXCEPTION 'Unexpected Library primary key; inspect schema before migrating'; END IF;
END $$;
CREATE TABLE marketplace_arc_assets (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
 edition_id uuid NOT NULL UNIQUE REFERENCES editions(id) ON DELETE RESTRICT,
 uploaded_by uuid NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
 revision integer NOT NULL DEFAULT 1 CHECK(revision>0),
 status text NOT NULL DEFAULT 'active' CHECK(status IN ('active','inactive','archived')),
 request_enabled boolean NOT NULL DEFAULT true,
 available_from timestamptz, available_until timestamptz,
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
 CHECK(available_until IS NULL OR available_from IS NULL OR available_until>available_from)
);
CREATE TABLE marketplace_arc_versions (
 asset_id uuid NOT NULL REFERENCES marketplace_arc_assets(id) ON DELETE RESTRICT,
 revision integer NOT NULL CHECK(revision>0), storage_key text NOT NULL UNIQUE,
 original_filename text NOT NULL, file_size bigint NOT NULL CHECK(file_size>0),
 sha256 text NOT NULL CHECK(length(sha256)=64), package jsonb NOT NULL,
 uploaded_by uuid NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
 created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(asset_id,revision)
);
ALTER TABLE marketplace_arc_assets ADD CONSTRAINT arc_current_version FOREIGN KEY(id,revision)
 REFERENCES marketplace_arc_versions(asset_id,revision) DEFERRABLE INITIALLY DEFERRED;
CREATE TABLE marketplace_arc_requests (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
 asset_id uuid NOT NULL REFERENCES marketplace_arc_assets(id) ON DELETE RESTRICT,
 requester_actor_id uuid NOT NULL REFERENCES marketplace_actors(id) ON DELETE RESTRICT,
 requested_by uuid NOT NULL REFERENCES marketplace_profiles(user_id) ON DELETE RESTRICT,
 message text NOT NULL DEFAULT '' CHECK(length(message)<=2000),
 status text NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','approved','rejected','revoked')),
 requested_at timestamptz NOT NULL DEFAULT now(), decided_at timestamptz,
 decided_by uuid REFERENCES users(id) ON DELETE RESTRICT,
 reason text NOT NULL DEFAULT '' CHECK(length(reason)<=1000), revoked_at timestamptz,
 UNIQUE(asset_id,requested_by)
);
CREATE INDEX arc_requests_queue ON marketplace_arc_requests(asset_id,status,requested_at DESC);
ALTER TABLE marketplace_library_items
 ADD COLUMN arc_asset_id uuid REFERENCES marketplace_arc_assets(id) ON DELETE RESTRICT,
 ADD COLUMN arc_access_code text UNIQUE,
 ADD COLUMN granted_at timestamptz,
 ADD COLUMN expires_at timestamptz,
 ADD COLUMN revoked_at timestamptz,
 ADD COLUMN reading_revision integer,
 ADD COLUMN last_location text NOT NULL DEFAULT '' CHECK(length(last_location)<=2000),
 ADD COLUMN reading_progress numeric NOT NULL DEFAULT 0 CHECK(reading_progress BETWEEN 0 AND 100),
 ADD COLUMN last_opened_at timestamptz,
 ADD COLUMN finished_at timestamptz,
 ADD CONSTRAINT arc_entitlement_complete CHECK((arc_asset_id IS NULL AND arc_access_code IS NULL AND granted_at IS NULL) OR (arc_asset_id IS NOT NULL AND arc_access_code IS NOT NULL AND granted_at IS NOT NULL));
CREATE INDEX library_arc_asset ON marketplace_library_items(arc_asset_id) WHERE arc_asset_id IS NOT NULL;
CREATE FUNCTION marketplace_arc_library_relationship() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.arc_asset_id IS NOT NULL AND NOT EXISTS (
 SELECT 1 FROM marketplace_arc_assets a JOIN marketplace_book_editions be ON be.edition_id=a.edition_id
 WHERE a.id=NEW.arc_asset_id AND be.marketplace_book_id=NEW.marketplace_book_id
 ) THEN RAISE EXCEPTION 'ARC edition must belong to the listed book' USING ERRCODE='23514'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER arc_library_relationship BEFORE INSERT OR UPDATE OF arc_asset_id,marketplace_book_id ON marketplace_library_items FOR EACH ROW EXECUTE FUNCTION marketplace_arc_library_relationship();
CREATE TABLE marketplace_reader_sessions (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), user_id uuid NOT NULL, marketplace_book_id uuid NOT NULL,
 revision integer NOT NULL, issued_at timestamptz NOT NULL DEFAULT now(), expires_at timestamptz NOT NULL,
 FOREIGN KEY(user_id,marketplace_book_id) REFERENCES marketplace_library_items(user_id,marketplace_book_id) ON DELETE CASCADE,
 CHECK(expires_at>issued_at)
);
CREATE INDEX reader_sessions_owner ON marketplace_reader_sessions(user_id,marketplace_book_id);
CREATE INDEX reader_sessions_expiry ON marketplace_reader_sessions(expires_at);
CREATE TABLE marketplace_reader_bookmarks (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), user_id uuid NOT NULL, marketplace_book_id uuid NOT NULL,
 revision integer NOT NULL, location text NOT NULL CHECK(length(location) BETWEEN 1 AND 2000),
 chapter text NOT NULL DEFAULT '' CHECK(length(chapter)<=300), label text NOT NULL DEFAULT '' CHECK(length(label)<=300),
 created_at timestamptz NOT NULL DEFAULT now(),
 FOREIGN KEY(user_id,marketplace_book_id) REFERENCES marketplace_library_items(user_id,marketplace_book_id) ON DELETE CASCADE,
 UNIQUE(user_id,marketplace_book_id,revision,location)
);
CREATE TABLE marketplace_arc_audit (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), asset_id uuid NOT NULL REFERENCES marketplace_arc_assets(id) ON DELETE RESTRICT,
 user_id uuid NOT NULL REFERENCES users(id) ON DELETE RESTRICT, event text NOT NULL,
 details jsonb NOT NULL DEFAULT '{}', created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX arc_audit_asset ON marketplace_arc_audit(asset_id,created_at DESC);
CREATE TRIGGER arc_asset_identity BEFORE UPDATE ON marketplace_arc_assets FOR EACH ROW EXECUTE FUNCTION marketplace_immutable_fields('edition_id');
CREATE FUNCTION marketplace_arc_version_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 RAISE EXCEPTION 'ARC versions are immutable; upload a new revision' USING ERRCODE='23514';
END $$;
CREATE TRIGGER arc_version_immutable BEFORE UPDATE OR DELETE ON marketplace_arc_versions FOR EACH ROW EXECUTE FUNCTION marketplace_arc_version_immutable();
-- A private file cannot silently change publisher or title through catalog reassignment.
CREATE FUNCTION marketplace_arc_parent_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_TABLE_NAME='editions' THEN
   IF (NEW.work_id,NEW.tenant_id) IS DISTINCT FROM (OLD.work_id,OLD.tenant_id)
      AND EXISTS(SELECT 1 FROM marketplace_arc_assets WHERE edition_id=OLD.id) THEN
      RAISE EXCEPTION 'An edition with ARC history cannot be reassigned' USING ERRCODE='23514'; END IF;
 ELSE
   IF (NEW.tenant_id,NEW.uid) IS DISTINCT FROM (OLD.tenant_id,OLD.uid)
      AND EXISTS(SELECT 1 FROM marketplace_arc_assets a JOIN editions e ON e.id=a.edition_id WHERE e.work_id=OLD.id) THEN
      RAISE EXCEPTION 'A title with ARC history cannot change its private storage identity' USING ERRCODE='23514'; END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER arc_edition_identity BEFORE UPDATE OF work_id,tenant_id ON editions FOR EACH ROW EXECUTE FUNCTION marketplace_arc_parent_identity();
CREATE TRIGGER arc_work_identity BEFORE UPDATE OF tenant_id,uid ON works FOR EACH ROW EXECUTE FUNCTION marketplace_arc_parent_identity();
COMMIT;
