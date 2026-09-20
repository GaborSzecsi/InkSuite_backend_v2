"""Invitations data access, extracted without changing SQL semantics. Caller owns transactions."""

from .core import one, rows


def ready_query_1(cur):
    return one(cur, "SELECT to_regclass('public.marketplace_access_requests') AS ready")


def request_access_query_1(cur, email):
    return one(
        cur,
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
        ("marketplace-invite:" + email,),
    )


def request_access_query_2(cur, email):
    return one(
        cur,
        "SELECT *, (notified_at IS NULL OR notified_at < now()-interval '10 minutes') AS can_notify FROM marketplace_access_requests WHERE email=%s AND status IN ('pending','approved') FOR UPDATE",
        (email,),
    )


def request_access_query_3(cur, existing):
    return cur.execute(
        "UPDATE marketplace_access_requests SET status='revoked',token_hash=NULL,expires_at=NULL WHERE id=%s",
        (existing["id"],),
    )


def request_access_query_4(cur, email):
    return one(
        cur,
        "SELECT id FROM marketplace_access_requests WHERE email=%s AND created_at>now()-interval '1 day' LIMIT 1",
        (email,),
    )


def request_access_query_5(cur, email, body):
    return one(
        cur,
        "INSERT INTO marketplace_access_requests(name,email,access_type) VALUES(%s,%s,%s) RETURNING *",
        (body.name, email, body.access_type),
    )


def review_queue_query_1(cur):
    return rows(
        cur,
        "SELECT id,name,email,access_type,status,created_at,expires_at FROM marketplace_access_requests ORDER BY created_at DESC LIMIT 100",
    )


def review_query_1(cur, request_id):
    return one(
        cur,
        "SELECT id,name,email,access_type,status,created_at,expires_at FROM marketplace_access_requests WHERE id=%s",
        (request_id,),
    )


def decide_query_1(cur, request_id):
    return one(
        cur,
        "SELECT * FROM marketplace_access_requests WHERE id=%s FOR UPDATE",
        (request_id,),
    )


def apply_decision_query_1(cur, reviewer_id, row, hashlib, token):
    return cur.execute(
        "UPDATE marketplace_access_requests SET status='approved',token_hash=%s,expires_at=now()+interval '7 days',reviewed_by=%s,reviewed_at=now() WHERE id=%s",
        (hashlib.sha256(token.encode()).hexdigest(), reviewer_id, row["id"]),
    )


def apply_decision_query_2(cur, reviewer_id, row):
    return cur.execute(
        "UPDATE marketplace_access_requests SET status='rejected',token_hash=NULL,expires_at=NULL,reviewed_by=%s,reviewed_at=now() WHERE id=%s",
        (reviewer_id, row["id"]),
    )


def invitation_query_1(cur, hashlib, token):
    return one(
        cur,
        "SELECT * FROM marketplace_access_requests WHERE token_hash=%s AND status='approved' AND expires_at>now() FOR UPDATE",
        (hashlib.sha256(token.encode()).hexdigest(),),
    )


def grant_query_1(cur, user_id):
    return one(
        cur, "SELECT status FROM marketplace_actors WHERE user_id=%s", (user_id,)
    )


def grant_query_2(cur, user_id, username, row):
    return cur.execute(
        "INSERT INTO marketplace_profiles(user_id,username,display_name,profile_visibility) VALUES(%s,%s,%s,'private') ON CONFLICT(user_id) DO NOTHING",
        (user_id, username, row["name"]),
    )


def grant_query_3(cur, user_id):
    return cur.execute(
        "INSERT INTO marketplace_actors(user_id) VALUES(%s) ON CONFLICT(user_id) DO NOTHING",
        (user_id,),
    )


def grant_query_4(cur, user_id, row):
    return cur.execute(
        "INSERT INTO marketplace_access_grants(user_id,access_type,request_id) VALUES(%s,%s,%s) ON CONFLICT(user_id) DO UPDATE SET access_type=EXCLUDED.access_type,request_id=EXCLUDED.request_id,granted_at=now()",
        (user_id, row["access_type"], row["id"]),
    )


def grant_query_5(cur, user_id, row):
    return cur.execute(
        "UPDATE marketplace_access_requests SET status='accepted',token_hash=NULL,expires_at=NULL,user_id=%s,accepted_at=now() WHERE id=%s",
        (user_id, row["id"]),
    )


def accept_query_1(cur, row):
    return one(cur, "SELECT id FROM users WHERE lower(email)=%s", (row["email"],))


def accept_query_2(cur, sub, row):
    return one(
        cur,
        "INSERT INTO users(cognito_sub,email,full_name) VALUES(%s,%s,%s) RETURNING id",
        (sub, row["email"], row["name"]),
    )


def send_approval_link_query_1(cur, row, hashlib, token):
    return cur.execute(
        "UPDATE marketplace_access_requests SET token_hash=%s,expires_at=now()+interval '48 hours',notified_at=now() WHERE id=%s",
        (hashlib.sha256(token.encode()).hexdigest(), row["id"]),
    )


def pending_approval_query_1(cur, hashlib, token):
    return one(
        cur,
        "SELECT * FROM marketplace_access_requests WHERE token_hash=%s AND status='pending' AND expires_at>now() FOR UPDATE",
        (hashlib.sha256(token.encode()).hexdigest(),),
    )


def resend_approval_link_query_1(cur, request_id):
    return one(
        cur,
        "SELECT *, (notified_at IS NULL OR notified_at<now()-interval '2 minutes') AS can_send FROM marketplace_access_requests WHERE id=%s AND status='pending' FOR UPDATE",
        (request_id,),
    )
