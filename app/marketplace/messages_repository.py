"""Messages data access, extracted without changing SQL semantics. Caller owns transactions."""

from .core import one, rows


def thread_query_1(cur, conversation_id, actor_id):
    return one(
        cur,
        "SELECT c.* FROM marketplace_conversations c JOIN marketplace_conversation_participants p ON p.conversation_id=c.id\n      WHERE c.id=%s AND p.actor_id=%s",
        (conversation_id, actor_id),
    )


def start_query_1(cur, low, high):
    return one(
        cur,
        "INSERT INTO marketplace_conversations(actor_low_id,actor_high_id) VALUES(%s,%s)\n          ON CONFLICT(actor_low_id,actor_high_id) DO UPDATE SET actor_low_id=EXCLUDED.actor_low_id RETURNING id",
        (low, high),
    )


def inbox_query_1(cur, actor_id, offset):
    return rows(
        cur,
        "SELECT c.id,c.actor_low_id,c.actor_high_id,c.last_message_at,\n          (SELECT CASE WHEN m.status='deleted' THEN 'Message removed' ELSE left(m.body,160) END FROM marketplace_messages m\n            WHERE m.conversation_id=c.id ORDER BY m.created_at DESC,m.id DESC LIMIT 1) AS preview,\n          (SELECT count(*) FROM marketplace_messages m WHERE m.conversation_id=c.id AND m.sender_actor_id<>p.actor_id\n            AND m.status='sent' AND (p.last_read_at IS NULL OR m.created_at>p.last_read_at)) AS unread\n          FROM marketplace_conversations c JOIN marketplace_conversation_participants p ON p.conversation_id=c.id\n          WHERE p.actor_id=%s ORDER BY c.last_message_at DESC NULLS LAST,c.created_at DESC,c.id LIMIT 31 OFFSET %s",
        (actor_id, offset),
    )


def unread_query_1(cur, actor_id):
    return one(
        cur,
        "SELECT count(*) AS count FROM marketplace_messages m JOIN marketplace_conversation_participants p ON p.conversation_id=m.conversation_id\n          WHERE p.actor_id=%s AND m.sender_actor_id<>p.actor_id AND m.status='sent' AND (p.last_read_at IS NULL OR m.created_at>p.last_read_at)",
        (actor_id,),
    )


def history_query_1(cur, where, params):
    return rows(
        cur,
        "SELECT id,sender_actor_id,CASE WHEN status='deleted' THEN 'Message removed' ELSE body END AS body,created_at,status FROM marketplace_messages WHERE conversation_id=%s"
        + where
        + " ORDER BY created_at DESC,id DESC LIMIT 51",
        params,
    )


def history_query_2(cur, before, conversation_id):
    return one(
        cur,
        "SELECT created_at,id FROM marketplace_messages WHERE id=%s AND conversation_id=%s",
        (before, conversation_id),
    )


def send_query_1(cur, conversation_id, body, user):
    return one(
        cur,
        "INSERT INTO marketplace_messages(conversation_id,sender_actor_id,created_by_user_id,body) VALUES(%s,%s,%s,%s) RETURNING id",
        (conversation_id, body.actor_id, user["id"], body.body),
    )


def mark_read_query_1(cur, conversation_id, actor_id, m):
    return cur.execute(
        "UPDATE marketplace_conversation_participants SET last_read_at=greatest(last_read_at,%s) WHERE conversation_id=%s AND actor_id=%s",
        (m["created_at"], conversation_id, actor_id),
    )


def mark_read_query_2(cur, message_id, conversation_id):
    return one(
        cur,
        "SELECT created_at FROM marketplace_messages WHERE id=%s AND conversation_id=%s",
        (message_id, conversation_id),
    )


def delete_message_query_1(cur, message_id, actor_id):
    return one(
        cur,
        "UPDATE marketplace_messages SET status='deleted',deleted_at=now(),body='Message removed' WHERE id=%s AND sender_actor_id=%s RETURNING id",
        (message_id, actor_id),
    )
