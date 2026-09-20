"""Native media SQL. Call within a service-owned transaction."""

from .core import one, rows


def available(cur):
    return bool(one(cur, "SELECT to_regclass('public.marketplace_media') AS t")["t"])


def lock_owner(cur, owner):
    one(
        cur,
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
        ("native-media:" + str(owner),),
    )


def usage(cur, owner):
    return one(
        cur,
        "SELECT COALESCE(sum(reserved_bytes),0) AS bytes FROM marketplace_media WHERE owner_actor_id=%s AND purged_at IS NULL",
        (owner,),
    )["bytes"]


def insert(cur, values):
    return one(
        cur,
        """INSERT INTO marketplace_media(id,owner_actor_id,created_by_user_id,media_type,bucket,original_key,original_filename,declared_mime,file_size,reserved_bytes)
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *""",
        values,
    )


def owned(cur, media_id, owner):
    return one(
        cur,
        "SELECT * FROM marketplace_media WHERE id=%s AND owner_actor_id=%s AND status<>'deleted' FOR UPDATE",
        (media_id, owner),
    )


def complete(cur, row, version):
    return one(
        cur,
        "UPDATE marketplace_media SET original_version=%s,status='uploaded',error_code=NULL,next_attempt_at=now() WHERE id=%s RETURNING *",
        (version, row["id"]),
    )


def attachments(cur, posts):
    if not posts or not available(cur):
        return []
    return rows(
        cur,
        """SELECT m.*,pm.post_id FROM marketplace_post_media pm JOIN marketplace_media m ON m.id=pm.media_id
    WHERE pm.post_id=ANY(%s::uuid[]) AND m.status='ready' ORDER BY pm.post_id,pm.position""",
        (posts,),
    )


def lock_assets(cur, ids):
    return rows(
        cur,
        "SELECT * FROM marketplace_media WHERE id=ANY(%s::uuid[]) ORDER BY id FOR UPDATE",
        (ids,),
    )


def attach(cur, post, ids):
    for position, media_id in enumerate(ids):
        cur.execute(
            "INSERT INTO marketplace_post_media(post_id,media_id,position) VALUES(%s,%s,%s)",
            (post, media_id, position),
        )


def referenced(cur, row):
    return one(
        cur,
        """SELECT 1 FROM marketplace_post_media pm JOIN marketplace_posts p ON p.id=pm.post_id
    WHERE pm.media_id=%s AND p.status<>'deleted' LIMIT 1""",
        (row["id"],),
    )


def remove(cur, row):
    cur.execute(
        "UPDATE marketplace_media SET status='deleted',deleted_at=now() WHERE id=%s",
        (row["id"],),
    )


def list_owned(cur, owner, offset):
    return rows(
        cur,
        "SELECT * FROM marketplace_media WHERE owner_actor_id=%s AND status<>'deleted' ORDER BY created_at DESC,id DESC LIMIT 31 OFFSET %s",
        (owner, offset),
    )
