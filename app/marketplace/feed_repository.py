"""Batched feed hydration. Query count does not grow with post/attachment count."""

from .core import rows


def metadata(cur, ids, viewer):
    return rows(
        cur,
        """SELECT p.id,
      a.id AS actor_id,a.user_id,p.author_actor_id,o.name,o.slug,o.description,o.logo_asset_ref,
      pr.display_name,pr.username,pr.bio,pr.profile_visibility,pr.avatar_asset_ref,
      (SELECT count(*) FROM marketplace_likes l WHERE l.post_id=p.id) AS likes,
      (SELECT count(*) FROM marketplace_comments c WHERE c.post_id=p.id AND c.status='published') AS comments,
      EXISTS(SELECT 1 FROM marketplace_likes l WHERE l.post_id=p.id AND l.actor_id=%s) AS liked
      FROM marketplace_posts p JOIN marketplace_actors a ON a.id=p.author_actor_id
      LEFT JOIN marketplace_profiles pr ON pr.user_id=a.user_id
      LEFT JOIN marketplace_organizations o ON o.id=a.organization_id
      WHERE p.id=ANY(%s::uuid[])""",
        (viewer, ids),
    )


def books(cur, ids):
    # Correlated aggregates use existing FK indexes. One round trip, no Python per-book SQL.
    return rows(
        cur,
        """SELECT pb.post_id,pb.position,b.id,b.slug,b.featured,w.title,w.subtitle,w.main_description AS description,
      o.id AS organization_id,o.name AS publisher,o.slug AS publisher_slug,t.slug AS tenant_slug,w.uid AS upload_uid,w.id AS work_id,
      COALESCE((SELECT jsonb_agg(jsonb_build_object('name',p.display_name,'role',c.contributor_role) ORDER BY c.sequence_number,c.id)
        FROM work_contributors c JOIN parties p ON p.id=c.party_id AND p.tenant_id=c.tenant_id WHERE c.work_id=w.id AND c.tenant_id=w.tenant_id),'[]') AS contributors,
      COALESCE((SELECT jsonb_agg(jsonb_build_object('id',e.id,'format',e.product_form,'isbn13',e.isbn13,
        'publication_date',(SELECT min(d.date_value) FROM edition_publishing_dates d WHERE d.edition_id=e.id AND d.tenant_id=e.tenant_id AND d.date_role='01'),
        'cover',COALESCE(CASE WHEN left(btrim(e.cover_image_link),8)='https://' THEN btrim(e.cover_image_link) END,
          (SELECT v.resource_link FROM edition_supporting_resources r JOIN edition_supporting_resource_versions v ON v.resource_id=r.id AND v.tenant_id=r.tenant_id
            WHERE r.tenant_id=e.tenant_id AND r.edition_id=e.id AND r.resource_content_type='01' AND NULLIF(btrim(v.resource_link),'') IS NOT NULL
            ORDER BY r.is_primary DESC,r.item_order,v.item_order,v.created_at LIMIT 1)),
        'prices',COALESCE((SELECT jsonb_agg(x) FROM (SELECT p.price_amount AS amount,p.currency_code AS currency,p.territory_country_included AS territory
          FROM edition_prices p JOIN edition_supply_details s ON s.id=p.supply_detail_id AND s.tenant_id=p.tenant_id
          WHERE s.edition_id=e.id AND s.tenant_id=e.tenant_id AND p.price_type_code IN ('01','02') AND p.currency_code<>''
          AND (p.price_effective_from IS NULL OR p.price_effective_from<=current_date) AND (p.price_effective_until IS NULL OR p.price_effective_until>=current_date)
          ORDER BY p.item_order,p.id LIMIT 8) x),'[]'),
        'subjects',COALESCE((SELECT jsonb_agg(x) FROM (SELECT scheme_id,subject_code,heading_text FROM edition_subjects
          WHERE edition_id=e.id AND tenant_id=e.tenant_id ORDER BY is_main DESC,item_order,id LIMIT 20) x),'[]')) ORDER BY m.is_primary DESC,e.created_at,e.id)
        FROM marketplace_book_editions m JOIN editions e ON e.id=m.edition_id AND e.work_id=w.id AND e.tenant_id=w.tenant_id WHERE m.marketplace_book_id=b.id),'[]') AS editions
      FROM marketplace_post_books pb JOIN marketplace_books b ON b.id=pb.marketplace_book_id
      JOIN marketplace_organizations o ON o.id=b.publisher_organization_id JOIN works w ON w.id=b.work_id AND w.tenant_id=o.tenant_id
      JOIN tenants t ON t.id=o.tenant_id
      WHERE pb.post_id=ANY(%s::uuid[]) AND b.marketplace_status='public' AND o.status='active' AND o.organization_type='publisher'
      ORDER BY pb.post_id,pb.position""",
        (ids,),
    )


def page(cur, actor_id, author_id, scope, offset):
    from .social_repository import feed_query_1

    # Chronological feed includes recent public posts and connected private posts.
    cond = "p.visibility='public'"
    params = []
    if actor_id:
        cond += """ OR p.author_actor_id=%s OR EXISTS(SELECT 1 FROM marketplace_connections c WHERE c.status='accepted'
          AND ((c.requester_actor_id=%s AND c.recipient_actor_id=p.author_actor_id) OR (c.recipient_actor_id=%s AND c.requester_actor_id=p.author_actor_id)))"""
        params += [actor_id] * 3
    where = (
        "p.status='published' AND a.status='active' AND (o.id IS NULL OR o.status='active') AND ("
        + cond
        + ")"
    )
    if author_id:
        where += " AND p.author_actor_id=%s"
        params.append(author_id)
    if actor_id and scope == "following":
        where += """ AND (p.author_actor_id=%s OR EXISTS(SELECT 1 FROM marketplace_follows f WHERE f.follower_actor_id=%s AND f.followed_actor_id=p.author_actor_id)
          OR EXISTS(SELECT 1 FROM marketplace_connections c WHERE c.status='accepted' AND
          ((c.requester_actor_id=%s AND c.recipient_actor_id=p.author_actor_id) OR (c.recipient_actor_id=%s AND c.requester_actor_id=p.author_actor_id))))"""
        params += [actor_id] * 4
    if actor_id:
        where += """ AND NOT EXISTS(SELECT 1 FROM marketplace_blocks x WHERE
          (x.blocker_actor_id=%s AND x.blocked_actor_id=p.author_actor_id) OR (x.blocked_actor_id=%s AND x.blocker_actor_id=p.author_actor_id))"""
        params += [actor_id, actor_id]
    return feed_query_1(cur, where, params, offset)
