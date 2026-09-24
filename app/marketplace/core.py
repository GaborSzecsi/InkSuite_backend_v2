from contextlib import contextmanager
from .observability import TimedCursor, log
from uuid import UUID
from urllib.parse import urlsplit
import psycopg
from psycopg.rows import dict_row
from fastapi import Depends, HTTPException
from app.core.db import db_conn
from app.auth.dependencies import get_current_user
from app.auth.service import get_user_db_record_from_claims


def one(cur, sql, params=()):
    cur.execute(sql, params)
    return cur.fetchone()


def rows(cur, sql, params=()):
    cur.execute(sql, params)
    return cur.fetchall()


def required(value, message="Not found"):
    if value is None:
        raise HTTPException(404, message)
    return value


@contextmanager
def transaction():
    try:
        with db_conn() as conn, conn.transaction(), conn.cursor(
            row_factory=dict_row
        ) as raw_cursor:
            cur = TimedCursor(raw_cursor)
            if not one(
                cur, "SELECT to_regclass('public.marketplace_profiles') AS ready"
            )["ready"]:
                raise HTTPException(
                    503, "Marketplace is awaiting its approved database migration."
                )
            try:
                yield cur
            finally:
                log.info(
                    "marketplace_transaction queries=%s sql_ms=%s",
                    cur.count,
                    round(cur.ms),
                )
    except psycopg.errors.UniqueViolation:
        raise HTTPException(
            409, "This username, slug or relationship already exists."
        ) from None
    except (psycopg.errors.CheckViolation, psycopg.errors.ForeignKeyViolation):
        raise HTTPException(
            409, "The selected records are unavailable or do not belong together."
        ) from None
    except psycopg.Error:
        raise HTTPException(
            503,
            "Marketplace could not complete the database request. Please try again.",
        ) from None


def current_user(claims=Depends(get_current_user)):
    try:
        user = get_user_db_record_from_claims(claims)
    except psycopg.Error:
        raise HTTPException(
            503, "The account service is temporarily unavailable."
        ) from None
    if not user:
        raise HTTPException(401, "Please sign in.")
    return user


def organization_permission(cur, user, organization_id, admin=False, messaging=False):
    org = required(
        one(
            cur,
            "SELECT * FROM marketplace_organizations WHERE id=%s",
            (organization_id,),
        )
    )
    if user.get("platform_role") == "superadmin":
        return org
    member = one(
        cur,
        "SELECT role,module_permissions FROM memberships WHERE user_id=%s AND tenant_id=%s",
        (user["id"], org["tenant_id"]),
    )
    if not member:
        raise HTTPException(403, "You cannot act for this organization.")
    perms = member["module_permissions"] or {}
    if member["role"] != "tenant_admin" and (
        admin
        or not perms.get("marketplace")
        or (messaging and not perms.get("marketplace_messages"))
    ):
        raise HTTPException(403, "Your organization role does not allow this action.")
    return org


def actor(cur, user, actor_id, messaging=False):
    a = required(
        one(
            cur,
            "SELECT * FROM marketplace_actors WHERE id=%s AND status='active'",
            (actor_id,),
        )
    )
    if a["user_id"]:
        if str(a["user_id"]) != str(user["id"]):
            raise HTTPException(403, "You cannot act as this person.")
    else:
        org = organization_permission(
            cur, user, a["organization_id"], messaging=messaging
        )
        if org["status"] == "suspended":
            raise HTTPException(403, "This organization is unavailable.")
    return a


def safe_url(value):
    value = (value or "").strip()
    try:
        u = urlsplit(value)
        return (
            value
            if u.scheme == "https" and u.hostname and not u.username and not u.password
            else ""
        )
    except ValueError:
        return ""


def image_ref(ref):
    # Only references validated by this module have this shape. Never return keys.
    if ref and ref.get("key"):
        from .media import signed_image

        return signed_image(ref)
    return safe_url((ref or {}).get("url"))


def public_actor(cur, actor_id, owner=False, viewer=None):
    a = required(
        one(
            cur,
            """SELECT a.id,a.user_id,a.organization_id,a.status,
      p.username,p.display_name,p.bio,p.avatar_asset_ref,p.location_text,p.profile_visibility,
      o.name,o.slug,o.description,o.logo_asset_ref,o.status AS organization_status
      FROM marketplace_actors a LEFT JOIN marketplace_profiles p ON p.user_id=a.user_id
      LEFT JOIN marketplace_organizations o ON o.id=a.organization_id WHERE a.id=%s""",
            (actor_id,),
        )
    )
    visible = a["status"] == "active" and (
        a["profile_visibility"] == "public"
        if a["user_id"]
        else a["organization_status"] == "active"
    )
    # Accepted connections can recognize each other without opening private profiles.
    recognizable = bool(
        a["user_id"]
        and a["status"] == "active"
        and viewer
        and connected(cur, viewer, actor_id)
        and not blocked(cur, viewer, actor_id)
    )
    return {
        "id": a["id"],
        "kind": "person" if a["user_id"] else "organization",
        "name": (
            (a["display_name"] or a["name"])
            if visible or owner or recognizable
            else "Private profile"
        ),
        "href": (
            (
                "/" + a["username"]
                if a["user_id"]
                else "/marketplace/publishers/" + a["slug"]
            )
            if visible or owner
            else None
        ),
        "image": (
            image_ref(a["avatar_asset_ref"] or a["logo_asset_ref"])
            if visible or owner or recognizable
            else ""
        ),
        "bio": (a["bio"] or a["description"] or "") if visible or owner else "",
    }


def pair_lock(cur, left, right):
    key = ":".join(sorted([str(left), str(right)]))
    one(
        cur,
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
        ("marketplace-pair:" + key,),
    )


def blocked(cur, left, right):
    return bool(
        one(
            cur,
            """SELECT 1 FROM marketplace_blocks WHERE
      (blocker_actor_id=%s AND blocked_actor_id=%s) OR (blocker_actor_id=%s AND blocked_actor_id=%s)""",
            (left, right, right, left),
        )
    )


def connected(cur, left, right):
    return bool(
        one(
            cur,
            """SELECT 1 FROM marketplace_connections WHERE status='accepted' AND
      ((requester_actor_id=%s AND recipient_actor_id=%s) OR (requester_actor_id=%s AND recipient_actor_id=%s))""",
            (left, right, right, left),
        )
    )


def message_allowed(cur, sender, recipient):
    a = required(one(cur, "SELECT * FROM marketplace_actors WHERE id=%s", (recipient,)))
    unavailable = a["status"] != "active" or blocked(cur, sender, recipient)
    if a["organization_id"]:
        org = one(
            cur,
            "SELECT status FROM marketplace_organizations WHERE id=%s",
            (a["organization_id"],),
        )
        unavailable = unavailable or org["status"] != "active"
    if (
        unavailable
        or a["messaging_preference"] == "nobody"
        or (
            a["messaging_preference"] == "connections_only"
            and not connected(cur, sender, recipient)
        )
    ):
        raise HTTPException(403, "Messaging is unavailable for this identity.")


def rate_limit(cur, user_id, table, maximum=30):
    # Per real user, shared across API workers and acting identities.
    if table not in (
        "marketplace_posts",
        "marketplace_comments",
        "marketplace_messages",
    ):
        raise ValueError("Invalid rate-limit table")
    one(
        cur,
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
        ("marketplace-rate:" + str(user_id),),
    )
    n = one(
        cur,
        f"SELECT count(*) AS n FROM {table} WHERE created_by_user_id=%s AND created_at>now()-interval '1 minute'",
        (user_id,),
    )["n"]
    if n >= maximum:
        raise HTTPException(429, "Please wait a minute before trying again.")
