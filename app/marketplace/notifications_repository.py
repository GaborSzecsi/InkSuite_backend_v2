"""Notifications data access, extracted without changing SQL semantics. Caller owns transactions."""

from .core import one, rows


def request_sender_query_1(cur, actor_id):
    return one(
        cur,
        "SELECT p.display_name FROM marketplace_actors a\n        JOIN marketplace_profiles p ON p.user_id=a.user_id\n        WHERE a.id=%s AND a.status='active'",
        (actor_id,),
    )


def connection_notifications_query_1(cur, user):
    return rows(
        cur,
        "SELECT c.id,c.requester_actor_id,c.recipient_actor_id,c.created_at,c.responded_at,\n            CASE WHEN c.status='pending' THEN 'request' ELSE 'accepted' END AS kind,\n            COALESCE(o.name,p.display_name,'Your profile') AS recipient_name,\n            COUNT(*) OVER() AS total\n          FROM marketplace_connections c\n          JOIN marketplace_actors a ON a.id=CASE WHEN c.status='pending' THEN c.recipient_actor_id ELSE c.requester_actor_id END AND a.status='active'\n          LEFT JOIN marketplace_organizations o ON o.id=a.organization_id\n          LEFT JOIN marketplace_profiles p ON p.user_id=a.user_id\n          LEFT JOIN memberships m ON m.tenant_id=o.tenant_id AND m.user_id=%s\n          WHERE c.status IN ('pending','accepted') AND\n            (a.user_id=%s OR (o.id IS NOT NULL AND o.status<>'suspended' AND\n              (%s OR m.role='tenant_admin' OR COALESCE(m.module_permissions->>'marketplace','false')='true')))\n          ORDER BY (c.status='pending') DESC,COALESCE(c.responded_at,c.created_at) DESC,c.id LIMIT 100",
        (user["id"], user["id"], user.get("platform_role") == "superadmin"),
    )


def email_connection_request_query_1(cur, connection_id):
    return one(
        cur,
        "SELECT c.requester_actor_id,a.user_id,o.tenant_id,t.slug,\n                COALESCE(o.name,p.display_name,'your profile') AS recipient_name\n              FROM marketplace_connections c JOIN marketplace_actors a ON a.id=c.recipient_actor_id AND a.status='active'\n              LEFT JOIN marketplace_organizations o ON o.id=a.organization_id\n              LEFT JOIN tenants t ON t.id=o.tenant_id\n              LEFT JOIN marketplace_profiles p ON p.user_id=a.user_id\n              WHERE c.id=%s AND c.status='pending'",
        (connection_id,),
    )


def email_connection_request_query_2(cur, c):
    return rows(
        cur,
        "SELECT DISTINCT u.email FROM memberships m JOIN users u ON u.id=m.user_id WHERE m.tenant_id=%s AND m.role='tenant_admin'",
        (c["tenant_id"],),
    )


def email_connection_request_query_3(cur, c):
    return rows(cur, "SELECT email FROM users WHERE id=%s", (c["user_id"],))
