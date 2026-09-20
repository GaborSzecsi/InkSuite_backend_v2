"""Media data access, extracted without changing SQL semantics. Caller owns transactions."""

from .core import one, rows


def validated_reference_query_1(cur, org):
    return one(
        cur, "SELECT id FROM tenants WHERE id=%s FOR UPDATE", (org["tenant_id"],)
    )


def validated_reference_query_2(cur, user):
    return one(cur, "SELECT id FROM users WHERE id=%s FOR UPDATE", (user["id"],))


def validated_reference_query_3(cur, asset_id, org):
    return one(
        cur,
        "SELECT s3_key,s3_bucket,source_type,work_id FROM social_media_assets WHERE id=%s AND tenant_id=%s AND media_type='image'",
        (asset_id, org["tenant_id"]),
    )


def validated_reference_query_4(cur, org):
    return one(cur, "SELECT slug FROM tenants WHERE id=%s", (org["tenant_id"],))


def validated_reference_query_5(cur, asset, org):
    return one(
        cur,
        "SELECT uid FROM works WHERE id=%s AND tenant_id=%s",
        (asset["work_id"], org["tenant_id"]),
    )


def upload_image_query_1(cur, user):
    return one(cur, "SELECT id FROM users WHERE id=%s FOR UPDATE", (user["id"],))


def upload_image_query_2(cur, o):
    return one(cur, "SELECT id,slug FROM tenants WHERE id=%s", (o["tenant_id"],))


def delete_image_query_1(cur, user):
    return one(cur, "SELECT id FROM users WHERE id=%s FOR UPDATE", (user["id"],))


def delete_image_query_2(cur, key):
    return one(
        cur,
        "SELECT 1 FROM marketplace_profiles WHERE avatar_asset_ref->>'key'=%s",
        (key,),
    )


def delete_image_query_3(cur, key):
    return one(
        cur,
        "SELECT 1 FROM marketplace_posts WHERE media_asset_ref->>'key'=%s AND status NOT IN ('deleted','hidden')",
        (key,),
    )


def identity_image_query_1(cur, ref, user, Jsonb):
    return cur.execute(
        "UPDATE marketplace_profiles SET avatar_asset_ref=%s WHERE user_id=%s",
        (Jsonb(ref) if ref else None, user["id"]),
    )


def identity_image_query_2(cur, purpose, ref, a, Jsonb):
    return cur.execute(
        f"UPDATE marketplace_organizations SET {purpose}_asset_ref=%s WHERE id=%s",
        (Jsonb(ref) if ref else None, a["organization_id"]),
    )
