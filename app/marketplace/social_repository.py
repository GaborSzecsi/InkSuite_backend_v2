"""Social data access, extracted without changing SQL semantics. Caller owns transactions."""

from .core import one, rows


def post_access_query_1(cur, p):
    return one(
        cur,
        "SELECT a.status,o.status AS org_status FROM marketplace_actors a\n      LEFT JOIN marketplace_organizations o ON o.id=a.organization_id WHERE a.id=%s",
        (p["author_actor_id"],),
    )


def post_access_query_2(cur, post_id):
    return one(
        cur,
        "SELECT * FROM marketplace_posts WHERE id=%s AND status='published'",
        (post_id,),
    )


def content_author_query_1(cur, actor_id):
    return one(
        cur,
        "SELECT p.display_name FROM marketplace_actors a\n        JOIN marketplace_profiles p ON p.user_id=a.user_id\n        WHERE a.id=%s AND a.status='active'",
        (actor_id,),
    )


def comment_interactions_ready_query_1(cur):
    return one(cur, "SELECT to_regclass('public.marketplace_comment_likes') AS t")


def comment_access_query_1(cur, comment_id):
    return one(
        cur,
        "SELECT * FROM marketplace_comments WHERE id=%s AND status='published'",
        (comment_id,),
    )


def post_view_query_1(cur, PUBLIC_BOOK, BOOK_JOIN, p):
    return rows(
        cur,
        "SELECT b.id "
        + BOOK_JOIN
        + " JOIN marketplace_post_books pb ON pb.marketplace_book_id=b.id WHERE pb.post_id=%s AND "
        + PUBLIC_BOOK
        + " ORDER BY pb.position",
        (p["id"],),
    )


def post_view_query_2(cur, p):
    return one(
        cur, "SELECT count(*) AS n FROM marketplace_likes WHERE post_id=%s", (p["id"],)
    )


def post_view_query_3(cur, p):
    return one(
        cur,
        "SELECT count(*) AS n FROM marketplace_comments WHERE post_id=%s AND status='published'",
        (p["id"],),
    )


def post_view_query_4(cur, active, p):
    return one(
        cur,
        "SELECT 1 FROM marketplace_likes WHERE post_id=%s AND actor_id=%s",
        (p["id"], active),
    )


def feed_query_1(cur, where, params, offset):
    return rows(
        cur,
        "SELECT p.* FROM marketplace_posts p JOIN marketplace_actors a ON a.id=p.author_actor_id\n          LEFT JOIN marketplace_organizations o ON o.id=a.organization_id WHERE "
        + where
        + " ORDER BY p.published_at DESC,p.id DESC LIMIT 21 OFFSET %s",
        params + [offset],
    )


def media_query_1(cur, offset, o):
    return rows(
        cur,
        "SELECT id,filename,public_url FROM social_media_assets WHERE tenant_id=%s AND media_type='image' ORDER BY created_at DESC,id LIMIT 101 OFFSET %s",
        (o["tenant_id"], offset),
    )


def create_post_query_1(cur, media, body, user):
    return one(
        cur,
        "INSERT INTO marketplace_posts(author_actor_id,created_by_user_id,body,visibility,status,published_at,media_asset_ref)\n          VALUES(%s,%s,%s,%s,'published',now(),%s) RETURNING *",
        (body.actor_id, user["id"], body.body, body.visibility, media),
    )


def create_post_query_2(cur, b, i, p):
    return cur.execute(
        "INSERT INTO marketplace_post_books(post_id,marketplace_book_id,position) VALUES(%s,%s,%s)",
        (p["id"], b, i),
    )


def delete_post_query_1(cur, post_id, actor_id):
    return one(
        cur,
        "UPDATE marketplace_posts SET status='deleted' WHERE id=%s AND author_actor_id=%s RETURNING id",
        (post_id, actor_id),
    )


def comments_query_1(cur, where, params, offset):
    return rows(
        cur,
        "SELECT c.* FROM marketplace_comments c WHERE "
        + where
        + " ORDER BY c.created_at,c.id LIMIT 31 OFFSET %s",
        params + [offset],
    )


def comments_query_2(cur, parent_comment_id, post_id):
    return one(
        cur,
        "SELECT * FROM marketplace_comments WHERE id=%s AND post_id=%s AND status IN ('published','deleted')",
        (parent_comment_id, post_id),
    )


def comments_query_3(cur, c):
    return one(
        cur,
        "SELECT count(*) AS n FROM marketplace_comment_likes WHERE comment_id=%s",
        (c["id"],),
    )


def comments_query_4(cur, actor_id, c):
    return one(
        cur,
        "SELECT 1 FROM marketplace_comment_likes WHERE comment_id=%s AND actor_id=%s",
        (c["id"], actor_id),
    )


def comments_query_5(cur, c):
    return one(
        cur,
        "SELECT count(*) AS n FROM marketplace_comments WHERE parent_comment_id=%s AND status='published'",
        (c["id"],),
    )


def create_comment_query_1(cur, post_id, body, user):
    return one(
        cur,
        "INSERT INTO marketplace_comments(post_id,author_actor_id,created_by_user_id,body) VALUES(%s,%s,%s,%s) RETURNING id",
        (post_id, body.actor_id, user["id"], body.body),
    )


def create_comment_query_2(cur, post_id, root_id, body, user):
    return one(
        cur,
        "INSERT INTO marketplace_comments(post_id,author_actor_id,created_by_user_id,body,parent_comment_id) VALUES(%s,%s,%s,%s,%s) RETURNING id",
        (post_id, body.actor_id, user["id"], body.body, root_id),
    )


def like_comment_query_1(cur, comment_id, actor_id):
    return cur.execute(
        "INSERT INTO marketplace_comment_likes(comment_id,actor_id) VALUES(%s,%s) ON CONFLICT DO NOTHING",
        (comment_id, actor_id),
    )


def unlike_comment_query_1(cur, comment_id, actor_id):
    return cur.execute(
        "DELETE FROM marketplace_comment_likes WHERE comment_id=%s AND actor_id=%s",
        (comment_id, actor_id),
    )


def delete_comment_query_1(cur, comment_id, actor_id):
    return one(
        cur,
        "UPDATE marketplace_comments SET status='deleted' WHERE id=%s AND author_actor_id=%s RETURNING id",
        (comment_id, actor_id),
    )


def like_query_1(cur, actor_id, post_id):
    return cur.execute(
        "INSERT INTO marketplace_likes(actor_id,post_id) VALUES(%s,%s) ON CONFLICT DO NOTHING",
        (actor_id, post_id),
    )


def unlike_query_1(cur, actor_id, post_id):
    return cur.execute(
        "DELETE FROM marketplace_likes WHERE actor_id=%s AND post_id=%s",
        (actor_id, post_id),
    )


def network_query_1(cur, actor_id, offset):
    return rows(
        cur,
        "SELECT followed_actor_id AS id FROM marketplace_follows WHERE follower_actor_id=%s ORDER BY created_at DESC,followed_actor_id LIMIT 31 OFFSET %s",
        (actor_id, offset),
    )


def network_query_2(cur, actor_id, offset):
    return rows(
        cur,
        "SELECT id,requester_actor_id,recipient_actor_id,status FROM marketplace_connections WHERE (requester_actor_id=%s OR recipient_actor_id=%s) AND status IN ('pending','accepted') ORDER BY updated_at DESC,id LIMIT 31 OFFSET %s",
        (actor_id, actor_id, offset),
    )


def network_query_3(cur, actor_id, offset):
    return rows(
        cur,
        "SELECT blocked_actor_id AS id FROM marketplace_blocks WHERE blocker_actor_id=%s ORDER BY created_at DESC,blocked_actor_id LIMIT 31 OFFSET %s",
        (actor_id, offset),
    )


def relationship_query_1(cur, actor_id, target_id):
    return one(
        cur,
        "SELECT status,recipient_actor_id FROM marketplace_connections\n            WHERE (requester_actor_id=%s AND recipient_actor_id=%s)\n               OR (requester_actor_id=%s AND recipient_actor_id=%s)",
        (actor_id, target_id, target_id, actor_id),
    )


def relationship_query_2(cur, actor_id, target_id):
    return one(
        cur,
        "SELECT 1 FROM marketplace_follows WHERE follower_actor_id=%s AND followed_actor_id=%s",
        (actor_id, target_id),
    )


def follow_query_1(cur, body):
    return cur.execute(
        "INSERT INTO marketplace_follows(follower_actor_id,followed_actor_id) VALUES(%s,%s) ON CONFLICT DO NOTHING",
        (body.actor_id, body.target_id),
    )


def unfollow_query_1(cur, actor_id, target_id):
    return cur.execute(
        "DELETE FROM marketplace_follows WHERE follower_actor_id=%s AND followed_actor_id=%s",
        (actor_id, target_id),
    )


def connect_query_1(cur, body):
    return one(
        cur,
        "SELECT * FROM marketplace_connections WHERE least(requester_actor_id,recipient_actor_id)=least(%s::uuid,%s::uuid)\n          AND greatest(requester_actor_id,recipient_actor_id)=greatest(%s::uuid,%s::uuid)",
        (body.actor_id, body.target_id, body.actor_id, body.target_id),
    )


def connect_query_2(cur, body, old):
    return one(
        cur,
        "UPDATE marketplace_connections SET requester_actor_id=%s,recipient_actor_id=%s,status='pending',responded_at=NULL WHERE id=%s RETURNING id,status",
        (body.actor_id, body.target_id, old["id"]),
    )


def connect_query_3(cur, body):
    return one(
        cur,
        "INSERT INTO marketplace_connections(requester_actor_id,recipient_actor_id) VALUES(%s,%s) RETURNING id,status",
        (body.actor_id, body.target_id),
    )


def connection_action_query_1(cur, status, connection_id):
    return cur.execute(
        "UPDATE marketplace_connections SET status=%s,responded_at=now() WHERE id=%s",
        (status, connection_id),
    )


def connection_action_query_2(cur, connection_id):
    return one(
        cur,
        "SELECT * FROM marketplace_connections WHERE id=%s FOR UPDATE",
        (connection_id,),
    )


def block_query_1(cur, body):
    return cur.execute(
        "INSERT INTO marketplace_blocks(blocker_actor_id,blocked_actor_id) VALUES(%s,%s) ON CONFLICT DO NOTHING",
        (body.actor_id, body.target_id),
    )


def block_query_2(cur, body):
    return cur.execute(
        "UPDATE marketplace_connections SET status='cancelled',responded_at=now() WHERE (requester_actor_id=%s AND recipient_actor_id=%s) OR (requester_actor_id=%s AND recipient_actor_id=%s)",
        (body.actor_id, body.target_id, body.target_id, body.actor_id),
    )


def block_query_3(cur, body):
    return cur.execute(
        "DELETE FROM marketplace_follows WHERE (follower_actor_id=%s AND followed_actor_id=%s) OR (follower_actor_id=%s AND followed_actor_id=%s)",
        (body.actor_id, body.target_id, body.target_id, body.actor_id),
    )


def unblock_query_1(cur, actor_id, target_id):
    return cur.execute(
        "DELETE FROM marketplace_blocks WHERE blocker_actor_id=%s AND blocked_actor_id=%s",
        (actor_id, target_id),
    )


def report_query_1(cur, body):
    return one(
        cur,
        f"INSERT INTO marketplace_reports(reporter_actor_id,target_{body.target_type}_id,reason) VALUES(%s,%s,%s) RETURNING id",
        (body.actor_id, body.target_id, body.reason),
    )


def report_query_2(cur, body):
    return one(
        cur, "SELECT post_id FROM marketplace_comments WHERE id=%s", (body.target_id,)
    )


def report_query_3(cur, body):
    return one(
        cur,
        "SELECT m.id FROM marketplace_messages m JOIN marketplace_conversation_participants p ON p.conversation_id=m.conversation_id\n              WHERE m.id=%s AND p.actor_id=%s",
        (body.target_id, body.actor_id),
    )
