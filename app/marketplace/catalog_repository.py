"""Catalog data access, extracted without changing SQL semantics. Caller owns transactions."""

from .core import one, rows


def legacy_cover_query_1(cur, tenant_id):
    return one(cur, "SELECT slug FROM tenants WHERE id=%s", (tenant_id,))


def book_view_query_1(cur, b):
    return rows(
        cur,
        "SELECT p.display_name AS name,c.contributor_role AS role FROM work_contributors c\n      JOIN parties p ON p.id=c.party_id AND p.tenant_id=c.tenant_id\n      WHERE c.work_id=%s AND c.tenant_id=%s ORDER BY c.sequence_number,c.id",
        (b["work_id"], b["tenant_id"]),
    )


def book_view_query_2(cur, b):
    return rows(
        cur,
        "SELECT e.id,e.product_form AS format,e.isbn13,\n      COALESCE(CASE WHEN left(btrim(e.cover_image_link), 8) = 'https://' THEN btrim(e.cover_image_link) END,(SELECT v.resource_link FROM edition_supporting_resources r\n        JOIN edition_supporting_resource_versions v ON v.resource_id=r.id AND v.tenant_id=r.tenant_id\n        WHERE r.tenant_id=e.tenant_id AND r.edition_id=e.id AND r.resource_content_type='01'\n        AND NULLIF(btrim(v.resource_link),'') IS NOT NULL ORDER BY r.is_primary DESC,r.item_order,v.item_order,v.created_at LIMIT 1)) AS cover_image_link,\n      (SELECT min(d.date_value) FROM edition_publishing_dates d WHERE d.edition_id=e.id AND d.tenant_id=e.tenant_id AND d.date_role='01') AS publication_date\n      FROM marketplace_book_editions m JOIN editions e ON e.id=m.edition_id\n      WHERE m.marketplace_book_id=%s AND e.work_id=%s AND e.tenant_id=%s ORDER BY m.is_primary DESC,e.created_at,e.id",
        (b["id"], b["work_id"], b["tenant_id"]),
    )


def book_view_query_3(cur, PUBLIC_BOOK, BOOK_JOIN, book_id):
    return one(
        cur,
        "SELECT b.id,b.slug,b.featured,w.title,w.subtitle,w.main_description AS description,\n      w.id AS work_id,w.uid AS upload_uid,o.id AS organization_id,o.tenant_id,o.name AS publisher,o.slug AS publisher_slug\n      "
        + BOOK_JOIN
        + " WHERE b.id=%s AND "
        + PUBLIC_BOOK,
        (book_id,),
    )


def book_view_query_4(cur, e, b):
    return rows(
        cur,
        "SELECT p.price_amount AS amount,p.currency_code AS currency,\n          p.territory_country_included AS territory FROM edition_prices p\n          JOIN edition_supply_details s ON s.id=p.supply_detail_id AND s.tenant_id=p.tenant_id\n          WHERE s.edition_id=%s AND s.tenant_id=%s AND p.price_type_code IN ('01','02')\n          AND p.currency_code<>'' AND (p.price_effective_from IS NULL OR p.price_effective_from<=current_date)\n          AND (p.price_effective_until IS NULL OR p.price_effective_until>=current_date)\n          ORDER BY p.item_order,p.id LIMIT 8",
        (e["id"], b["tenant_id"]),
    )


def book_view_query_5(cur, e, b):
    return rows(
        cur,
        "SELECT scheme_id,subject_code,heading_text FROM edition_subjects\n          WHERE edition_id=%s AND tenant_id=%s ORDER BY is_main DESC,item_order,id LIMIT 20",
        (e["id"], b["tenant_id"]),
    )


def book_view_query_6(cur, b):
    return one(
        cur,
        "SELECT id FROM marketplace_actors WHERE organization_id=%s AND status='active'",
        (b["organization_id"],),
    )


def book_view_query_7(cur, e, b):
    return rows(
        cur,
        "SELECT sales_rights_type,countries_included,regions_included,\n                countries_excluded,regions_excluded FROM edition_sales_rights\n                WHERE edition_id=%s AND tenant_id=%s ORDER BY item_order,id",
        (e["id"], b["tenant_id"]),
    )


def book_view_query_8(cur, e, b):
    return rows(
        cur,
        "SELECT copyright_holder,copyright_notice,language_code,\n                COALESCE(NULLIF(sales_rights_type,''),row_sales_rights_type) AS sales_rights_type,\n                COALESCE(NULLIF(countries_included,''),exclusive_rights_country) AS countries_included,countries_excluded,\n                COALESCE(NULLIF(regions_included,''),exclusive_rights_territory) AS regions_included,regions_excluded\n                FROM edition_rights WHERE edition_id=%s AND tenant_id=%s ORDER BY item_order,id",
        (e["id"], b["tenant_id"]),
    )


def books_query_1(cur, where, BOOK_JOIN, params, offset, limit):
    return rows(
        cur,
        "SELECT b.id "
        + BOOK_JOIN
        + " WHERE "
        + where
        + " ORDER BY b.featured DESC,w.title,b.id LIMIT %s OFFSET %s",
        params + [limit + 1, offset],
    )


def book_query_1(cur, slug):
    return one(cur, "SELECT id FROM marketplace_books WHERE slug=%s", (slug,))


def publishers_query_1(cur, offset):
    return rows(
        cur,
        "SELECT name,slug,description,logo_asset_ref FROM marketplace_organizations WHERE status='active' AND organization_type='publisher' ORDER BY name,id LIMIT 31 OFFSET %s",
        (offset,),
    )


def publisher_query_1(cur, slug):
    return one(
        cur,
        "SELECT id,name,slug,description,website,location_text,logo_asset_ref,banner_asset_ref,verified_status FROM marketplace_organizations WHERE slug=%s AND status='active' AND organization_type='publisher'",
        (slug,),
    )


def publisher_query_2(cur, o):
    return one(
        cur,
        "SELECT id FROM marketplace_actors WHERE organization_id=%s AND status='active'",
        (o["id"],),
    )


def publisher_query_3(cur, a):
    return one(
        cur,
        "SELECT count(*) AS n FROM marketplace_follows WHERE followed_actor_id=%s",
        (a["id"],),
    )


def initialize_query_1(cur, tenant_id, user):
    return one(
        cur,
        "SELECT role FROM memberships WHERE tenant_id=%s AND user_id=%s",
        (tenant_id, user["id"]),
    )


def initialize_query_2(cur, tenant_id, tenant):
    return one(
        cur,
        "INSERT INTO marketplace_organizations(tenant_id,organization_type,name,slug) VALUES(%s,'publisher',%s,%s) ON CONFLICT(tenant_id) DO UPDATE SET tenant_id=EXCLUDED.tenant_id RETURNING id",
        (tenant_id, tenant["name"], tenant["slug"]),
    )


def initialize_query_3(cur, org):
    return one(
        cur,
        "INSERT INTO marketplace_actors(organization_id) VALUES(%s) ON CONFLICT(organization_id) DO UPDATE SET organization_id=EXCLUDED.organization_id RETURNING id",
        (org["id"],),
    )


def initialize_query_4(cur, tenant_id):
    return one(cur, "SELECT id,slug,name FROM tenants WHERE id=%s", (tenant_id,))


def save_org_query_1(cur, org_id, body):
    return one(
        cur,
        "UPDATE marketplace_organizations SET name=%s,slug=%s,description=%s,website=%s,location_text=%s,status=%s\n          WHERE id=%s AND status<>'suspended' RETURNING id",
        (
            body.name,
            body.slug,
            body.description,
            body.website,
            body.location_text,
            body.status,
            org_id,
        ),
    )


def manage_catalog_query_1(cur, org_id, offset, o, q):
    return rows(
        cur,
        "SELECT w.id,w.title,b.id AS listing_id,b.slug,b.marketplace_status,b.discoverable,b.featured\n          FROM works w LEFT JOIN marketplace_books b ON b.work_id=w.id AND b.publisher_organization_id=%s\n          WHERE w.tenant_id=%s AND w.title ILIKE %s ORDER BY w.title,w.id LIMIT 31 OFFSET %s",
        (org_id, o["tenant_id"], "%" + q + "%", offset),
    )


def manage_catalog_query_2(cur, w, o):
    return rows(
        cur,
        "SELECT e.id,e.product_form AS format,e.isbn13,e.status,\n              EXISTS(SELECT 1 FROM marketplace_book_editions be WHERE be.edition_id=e.id AND be.marketplace_book_id=%s) AS selected\n              FROM editions e WHERE e.work_id=%s AND e.tenant_id=%s ORDER BY e.created_at,e.id",
        (w["listing_id"], w["id"], o["tenant_id"]),
    )


def save_listing_query_1(cur, org_id, body):
    return one(
        cur,
        "INSERT INTO marketplace_books(publisher_organization_id,work_id,slug,marketplace_status,discoverable,featured)\n          VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT(work_id) DO UPDATE SET slug=EXCLUDED.slug,marketplace_status=EXCLUDED.marketplace_status,\n          discoverable=EXCLUDED.discoverable,featured=EXCLUDED.featured WHERE marketplace_books.publisher_organization_id=EXCLUDED.publisher_organization_id RETURNING id",
        (
            org_id,
            body.work_id,
            body.slug,
            body.marketplace_status,
            body.discoverable,
            body.featured,
        ),
    )


def save_listing_query_2(cur, result):
    return cur.execute(
        "DELETE FROM marketplace_book_editions WHERE marketplace_book_id=%s",
        (result["id"],),
    )


def save_listing_query_3(cur, body, o):
    return one(
        cur,
        "SELECT id FROM works WHERE id=%s AND tenant_id=%s",
        (body.work_id, o["tenant_id"]),
    )


def save_listing_query_4(cur, edition, result, i):
    return cur.execute(
        "INSERT INTO marketplace_book_editions(marketplace_book_id,edition_id,is_primary) VALUES(%s,%s,%s)",
        (result["id"], edition, i == 0),
    )


def library_query_1(cur, PUBLIC_BOOK, offset, user):
    return rows(
        cur,
        "SELECT l.marketplace_book_id,l.status FROM marketplace_library_items l\n          JOIN marketplace_books b ON b.id=l.marketplace_book_id JOIN marketplace_organizations o ON o.id=b.publisher_organization_id\n          JOIN works w ON w.id=b.work_id AND w.tenant_id=o.tenant_id WHERE l.user_id=%s AND "
        + PUBLIC_BOOK
        + " ORDER BY l.updated_at DESC,l.marketplace_book_id LIMIT 31 OFFSET %s",
        (user["id"], offset),
    )


def save_library_query_1(cur, book_id, user, body):
    return cur.execute(
        "INSERT INTO marketplace_library_items(user_id,marketplace_book_id,status) VALUES(%s,%s,%s)\n          ON CONFLICT(user_id,marketplace_book_id) DO UPDATE SET status=EXCLUDED.status",
        (user["id"], book_id, body.status),
    )


def save_library_query_2(cur, user):
    return one(
        cur, "SELECT user_id FROM marketplace_profiles WHERE user_id=%s", (user["id"],)
    )


def remove_library_query_1(cur, book_id, user):
    return cur.execute(
        "DELETE FROM marketplace_library_items WHERE user_id=%s AND marketplace_book_id=%s",
        (user["id"], book_id),
    )


def publisher_catalogs_query_1(cur, org):
    return rows(
        cur,
        "SELECT id,asset_name,public_url,file_format FROM publishing_assets\n            WHERE tenant_id=%s AND is_public AND is_active AND lower(asset_type) IN ('catalog','catalogue')\n            ORDER BY created_at DESC,id",
        (org["tenant_id"],),
    )


def publisher_catalogs_query_2(cur, slug):
    return one(
        cur,
        "SELECT tenant_id FROM marketplace_organizations WHERE slug=%s AND status='active' AND organization_type='publisher'",
        (slug,),
    )
