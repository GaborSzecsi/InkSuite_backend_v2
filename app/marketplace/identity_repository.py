"""Identity data access, extracted without changing SQL semantics. Caller owns transactions."""

from .core import one, rows


def login_query_1(cur, user):
    return one(
        cur,
        "SELECT g.access_type,r.name FROM marketplace_access_grants g JOIN marketplace_access_requests r ON r.id=g.request_id WHERE g.user_id=%s",
        (user["id"],),
    )


def login_query_2(cur):
    return one(cur, "SELECT to_regclass('public.marketplace_access_grants') AS ready")


def me_query_1(cur, user):
    return one(
        cur,
        "SELECT username,display_name,bio,location_text,profile_visibility,COALESCE(to_jsonb(p)->'contact_details','{}'::jsonb) AS contact_details FROM marketplace_profiles p WHERE user_id=%s",
        (user["id"],),
    )


def me_query_2(cur, user):
    return rows(
        cur,
        "SELECT t.id,t.slug,t.name,m.role,m.module_permissions FROM tenants t\n          JOIN memberships m ON m.tenant_id=t.id WHERE m.user_id=%s ORDER BY t.name",
        (user["id"],),
    )


def me_query_3(cur):
    return rows(
        cur,
        "SELECT o.id,o.name,o.slug,o.status,o.tenant_id,a.id AS actor_id FROM marketplace_organizations o\n          JOIN marketplace_actors a ON a.organization_id=o.id WHERE a.status='active' ORDER BY o.name",
    )


def me_query_4(cur, user):
    return one(
        cur,
        "SELECT id,messaging_preference FROM marketplace_actors WHERE user_id=%s AND status='active'",
        (user["id"],),
    )


def me_query_5(cur):
    return rows(
        cur,
        "SELECT id,slug,name,'tenant_admin' AS role,'{}'::jsonb AS module_permissions FROM tenants ORDER BY name",
    )


def save_profile_query_1(cur, user):
    return one(
        cur, "SELECT status FROM marketplace_actors WHERE user_id=%s", (user["id"],)
    )


def save_profile_query_2(cur, user, body):
    return cur.execute(
        "INSERT INTO marketplace_profiles(user_id,username,display_name,bio,location_text,profile_visibility)\n          VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id) DO UPDATE SET username=EXCLUDED.username,display_name=EXCLUDED.display_name,\n          bio=EXCLUDED.bio,location_text=EXCLUDED.location_text,profile_visibility=EXCLUDED.profile_visibility",
        (
            user["id"],
            body.username,
            body.display_name,
            body.bio,
            body.location_text,
            body.profile_visibility,
        ),
    )


def save_profile_query_3(cur, user, body):
    return one(
        cur,
        "INSERT INTO marketplace_actors(user_id,messaging_preference) VALUES(%s,%s)\n          ON CONFLICT(user_id) DO UPDATE SET messaging_preference=EXCLUDED.messaging_preference RETURNING id",
        (user["id"], body.messaging_preference),
    )


def profile_query_1(cur, username):
    return one(
        cur,
        "SELECT a.id,p.location_text,COALESCE(to_jsonb(p)->'contact_details','{}'::jsonb) AS contact_details FROM marketplace_profiles p JOIN marketplace_actors a ON a.user_id=p.user_id\n          WHERE p.username=%s AND p.profile_visibility='public' AND a.status='active' ",
        (username,),
    )


def profile_query_2(cur, p):
    return one(
        cur,
        "SELECT count(*) AS n FROM marketplace_follows WHERE followed_actor_id=%s",
        (p["id"],),
    )


def save_contact_details(cur, user, details):
    from psycopg.types.json import Jsonb
    from fastapi import HTTPException
    column = one(cur, "SELECT 1 AS ready FROM information_schema.columns WHERE table_schema='public' AND table_name='marketplace_profiles' AND column_name='contact_details'")
    if not column:
        raise HTTPException(409, "Reader profiles need database migration 012_reader_profile_contacts.sql before saving.")
    cur.execute("UPDATE marketplace_profiles SET contact_details=%s WHERE user_id=%s", (Jsonb(details.model_dump()), user["id"]))


def save_verified_email(cur, user_id, email):
    cur.execute("UPDATE users SET email=%s WHERE id=%s", (email, user_id))
    column = one(cur, "SELECT 1 AS ready FROM information_schema.columns WHERE table_schema='public' AND table_name='marketplace_profiles' AND column_name='contact_details'")
    if column:
        cur.execute("UPDATE marketplace_profiles SET contact_details=jsonb_set(contact_details,'{email}',to_jsonb(%s::text)) WHERE user_id=%s", (email,user_id))
