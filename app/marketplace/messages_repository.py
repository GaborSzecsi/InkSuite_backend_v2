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


# Stable read cursor includes the UUID to distinguish equal server timestamps.
UNREAD = """m.sender_actor_id<>p.actor_id AND m.status='sent' AND
 (p.last_read_at IS NULL OR (m.created_at,m.id)>(p.last_read_at,COALESCE(p.last_read_message_id,'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid)))"""


def inbox_query_1(cur, actor_id, offset=0, q="", unread_only=False, search=False):
    # The default Inbox is always exactly the most recent 15, never paginated.
    filters = ""
    params = [actor_id]
    if q:
        filters += """ AND (EXISTS(SELECT 1 FROM marketplace_actors a
          LEFT JOIN marketplace_profiles pr ON pr.user_id=a.user_id
          LEFT JOIN marketplace_organizations o ON o.id=a.organization_id
          WHERE a.id=CASE WHEN c.actor_low_id=p.actor_id THEN c.actor_high_id ELSE c.actor_low_id END
          AND (COALESCE(pr.display_name,o.name,'') ILIKE %s OR pr.username ILIKE %s))
          OR EXISTS(SELECT 1 FROM marketplace_messages sm WHERE sm.conversation_id=c.id
          AND sm.status='sent' AND sm.body ILIKE %s))"""
        pattern = '%' + q.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_') + '%'
        params += [pattern] * 3
    if unread_only:
        filters += " AND EXISTS(SELECT 1 FROM marketplace_messages m WHERE m.conversation_id=c.id AND " + UNREAD + ")"
    limit = 31 if search else 15
    params += [limit]
    pagination = ""
    if search:
        pagination = " OFFSET %s"
        params.append(offset)
    return rows(cur, """SELECT c.id,c.actor_low_id,c.actor_high_id,c.last_message_at,
      (SELECT CASE WHEN m.status='deleted' THEN 'Message removed' ELSE left(m.body,160) END
       FROM marketplace_messages m WHERE m.conversation_id=c.id ORDER BY m.created_at DESC,m.id DESC LIMIT 1) AS preview,
      (SELECT count(*) FROM marketplace_messages m WHERE m.conversation_id=c.id AND """ + UNREAD + """) AS unread
      FROM marketplace_conversations c JOIN marketplace_conversation_participants p ON p.conversation_id=c.id
      WHERE p.actor_id=%s""" + filters + " ORDER BY c.last_message_at DESC NULLS LAST,c.created_at DESC,c.id LIMIT %s" + pagination, params)


def unread_query_1(cur, actor_id):
    # Badge convention: unread conversations, including those outside the Inbox.
    return one(cur,"SELECT count(DISTINCT m.conversation_id) AS count FROM marketplace_messages m JOIN marketplace_conversation_participants p ON p.conversation_id=m.conversation_id WHERE p.actor_id=%s AND " + UNREAD,(actor_id,))


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
    result = one(cur,"""INSERT INTO marketplace_messages(conversation_id,sender_actor_id,created_by_user_id,body,client_message_id)
      VALUES(%s,%s,%s,%s,%s) ON CONFLICT(sender_actor_id,client_message_id) WHERE client_message_id IS NOT NULL DO NOTHING RETURNING id""",
      (conversation_id,body.actor_id,user["id"],body.body,body.client_message_id))
    if result:
        return result
    return existing_message(cur,conversation_id,body)


def existing_message(cur,conversation_id,body):
    if body.client_message_id is None:
        return None
    existing = one(cur,"SELECT id,conversation_id,body FROM marketplace_messages WHERE sender_actor_id=%s AND client_message_id=%s",(body.actor_id,body.client_message_id))
    if not existing:
        return None
    if str(existing['conversation_id']) != str(conversation_id) or existing['body'] != body.body:
        from fastapi import HTTPException
        raise HTTPException(409,"This message ID was already used for another message.")
    return {"id":existing['id']}



def mark_read_query_1(cur, conversation_id, actor_id, m):
    return cur.execute("""UPDATE marketplace_conversation_participants SET last_read_at=%s,last_read_message_id=%s
      WHERE conversation_id=%s AND actor_id=%s AND (last_read_at IS NULL OR
      (last_read_at,COALESCE(last_read_message_id,'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid))<(%s,%s))""",
      (m['created_at'],m['id'],conversation_id,actor_id,m['created_at'],m['id']))


def mark_read_query_2(cur, message_id, conversation_id):
    return one(cur,"SELECT created_at,id FROM marketplace_messages WHERE id=%s AND conversation_id=%s",(message_id,conversation_id))


def delete_message_query_1(cur, message_id, actor_id):
    return one(
        cur,
        "UPDATE marketplace_messages SET status='deleted',deleted_at=now(),body='Message removed' WHERE id=%s AND sender_actor_id=%s RETURNING id",
        (message_id, actor_id),
    )
