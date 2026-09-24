from psycopg.types.json import Jsonb
from .core import one, rows


ASSET_JOIN = """ FROM marketplace_arc_assets a JOIN editions e ON e.id=a.edition_id
 JOIN works w ON w.id=e.work_id AND w.tenant_id=e.tenant_id
 JOIN marketplace_organizations o ON o.tenant_id=w.tenant_id
 LEFT JOIN marketplace_book_editions be ON be.edition_id=e.id
 LEFT JOIN marketplace_books b ON b.id=be.marketplace_book_id AND b.publisher_organization_id=o.id """


def ready(cur):
    return one(
        cur,
        "SELECT to_regclass('public.marketplace_arc_assets') AS ready",
    )["ready"]


def edition(cur, edition_id):
    return one(
        cur,
        """SELECT e.id,e.work_id,w.uid,w.title,t.slug,t.s3_prefix,
        o.id AS organization_id,o.status AS organization_status
        FROM editions e
        JOIN works w ON w.id=e.work_id AND w.tenant_id=e.tenant_id
        JOIN tenants t ON t.id=w.tenant_id
        JOIN marketplace_organizations o
          ON o.tenant_id=t.id
         AND o.organization_type='publisher'
        WHERE e.id=%s
        FOR UPDATE OF e""",
        (edition_id,),
    )


def asset(cur, asset_id):
    return one(
        cur,
        "SELECT a.*,o.id AS organization_id,o.status AS organization_status,"
        "b.id AS book_id,b.marketplace_status,w.title"
        + ASSET_JOIN
        + " WHERE a.id=%s FOR UPDATE OF a",
        (asset_id,),
    )


def existing(cur, edition_id):
    return one(
        cur,
        "SELECT * FROM marketplace_arc_assets "
        "WHERE edition_id=%s FOR UPDATE",
        (edition_id,),
    )


def upload(
    cur,
    aid,
    eid,
    revision,
    key,
    filename,
    data,
    package,
    user,
    start,
    end,
    enabled,
):
    one(
        cur,
        """INSERT INTO marketplace_arc_assets(
            id,
            edition_id,
            uploaded_by,
            revision,
            available_from,
            available_until,
            request_enabled
        )
        VALUES(%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT(id) DO UPDATE SET
            revision=EXCLUDED.revision,
            available_from=EXCLUDED.available_from,
            available_until=EXCLUDED.available_until,
            request_enabled=EXCLUDED.request_enabled,
            status='active',
            updated_at=now()
        RETURNING id""",
        (
            aid,
            eid,
            user["id"],
            revision,
            start,
            end,
            enabled,
        ),
    )

    one(
        cur,
        """INSERT INTO marketplace_arc_versions(
            asset_id,
            revision,
            storage_key,
            original_filename,
            file_size,
            sha256,
            package,
            uploaded_by
        )
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING asset_id""",
        (
            aid,
            revision,
            key,
            filename,
            len(data),
            package["sha256"],
            Jsonb(package),
            user["id"],
        ),
    )


def audit(cur, aid, user, event, details=None):
    one(
        cur,
        """INSERT INTO marketplace_arc_audit(
            asset_id,
            user_id,
            event,
            details
        )
        VALUES(%s,%s,%s,%s)
        RETURNING id""",
        (
            aid,
            user["id"],
            event,
            Jsonb(details or {}),
        ),
    )


def book_assets(cur, bid, uid):
    return rows(
        cur,
        """SELECT
            a.id,
            a.edition_id,
            a.available_from,
            a.available_until,
            a.request_enabled,
            a.status,
            r.status AS request_status,
            l.revoked_at,
            l.expires_at,
            l.granted_at,
            (
                a.status='active'
                AND o.status='active'
                AND b.marketplace_status='public'
                AND (
                    a.available_from IS NULL
                    OR a.available_from<=now()
                )
                AND (
                    a.available_until IS NULL
                    OR a.available_until>now()
                )
            ) AS available
        """
        + ASSET_JOIN
        + """
        LEFT JOIN marketplace_arc_requests r
          ON r.asset_id=a.id
         AND r.requested_by=%s
        LEFT JOIN marketplace_library_items l
          ON l.arc_asset_id=a.id
         AND l.user_id=%s
        WHERE b.id=%s
          AND b.marketplace_status='public'
          AND o.status='active'""",
        (uid, uid, bid),
    )


def request(cur, aid, uid):
    return one(
        cur,
        """SELECT *
        FROM marketplace_arc_requests
        WHERE asset_id=%s
          AND requested_by=%s
        FOR UPDATE""",
        (aid, uid),
    )


def new_request(cur, aid, actor_id, user, message):
    return one(
        cur,
        """INSERT INTO marketplace_arc_requests(
            asset_id,
            requester_actor_id,
            requested_by,
            message
        )
        VALUES(%s,%s,%s,%s)
        RETURNING id,status""",
        (
            aid,
            actor_id,
            user["id"],
            message,
        ),
    )


def reopen_request(cur, request_id, actor_id, message):
    """
    Reuse the existing request relationship after rejection or revocation.

    The database intentionally permits only one request row for a
    (ARC asset, requester) pair. Request history belongs in
    marketplace_arc_audit rather than duplicate request rows.
    """
    return one(
        cur,
        """UPDATE marketplace_arc_requests
        SET requester_actor_id=%s,
            message=%s,
            status='pending',
            requested_at=now(),
            decided_at=NULL,
            decided_by=NULL,
            reason='',
            revoked_at=NULL
        WHERE id=%s
          AND status IN ('rejected','revoked')
        RETURNING id,status""",
        (
            actor_id,
            message,
            request_id,
        ),
    )


def request_by_id(cur, rid):
    return one(
        cur,
        """SELECT *
        FROM marketplace_arc_requests
        WHERE id=%s
        FOR UPDATE""",
        (rid,),
    )


def queue(cur, oid, status):
    return rows(
        cur,
        """SELECT
            r.id,
            r.message,
            r.status,
            r.requested_at,
            r.reason,
            r.asset_id,
            p.display_name AS requester_name,
            CASE
                WHEN ra.user_id IS NOT NULL
                    THEN 'reader'
                ELSE ro.organization_type
            END AS actor_type,
            ro.name AS requester_organization,
            w.title,
            b.id AS book_id
        FROM marketplace_arc_requests r
        JOIN marketplace_profiles p
          ON p.user_id=r.requested_by
        JOIN marketplace_actors ra
          ON ra.id=r.requester_actor_id
        LEFT JOIN marketplace_organizations ro
          ON ro.id=ra.organization_id
        JOIN marketplace_arc_assets a
          ON a.id=r.asset_id
        JOIN editions e
          ON e.id=a.edition_id
        JOIN works w
          ON w.id=e.work_id
        JOIN marketplace_organizations o
          ON o.tenant_id=w.tenant_id
        LEFT JOIN marketplace_books b
          ON b.work_id=w.id
        WHERE o.id=%s
          AND (%s='all' OR r.status=%s)
        ORDER BY r.requested_at DESC
        LIMIT 200""",
        (
            oid,
            status,
            status,
        ),
    )


def decide(cur, r, status, user, reason):
    one(
        cur,
        """UPDATE marketplace_arc_requests
        SET status=%s,
            decided_by=%s,
            decided_at=now(),
            reason=%s,
            revoked_at=CASE
                WHEN %s='revoked' THEN now()
                ELSE NULL
            END
        WHERE id=%s
        RETURNING id""",
        (
            status,
            user["id"],
            reason,
            status,
            r["id"],
        ),
    )


def grant(cur, uid, bid, aid, code, expires):
    # Any old reading sessions for this Library item become invalid
    # before a new/replacement entitlement is granted.
    cur.execute(
        """DELETE FROM marketplace_reader_sessions
        WHERE user_id=%s
          AND marketplace_book_id=%s""",
        (uid, bid),
    )

    one(
        cur,
        """INSERT INTO marketplace_library_items(
            user_id,
            marketplace_book_id,
            arc_asset_id,
            arc_access_code,
            granted_at,
            expires_at
        )
        VALUES(%s,%s,%s,%s,now(),%s)
        ON CONFLICT(user_id,marketplace_book_id)
        DO UPDATE SET
            arc_asset_id=EXCLUDED.arc_asset_id,
            arc_access_code=EXCLUDED.arc_access_code,
            granted_at=now(),
            expires_at=EXCLUDED.expires_at,
            revoked_at=NULL,
            last_location='',
            reading_progress=0,
            reading_revision=NULL,
            finished_at=NULL
        RETURNING user_id""",
        (
            uid,
            bid,
            aid,
            code,
            expires,
        ),
    )


def revoke(cur, uid, aid):
    return one(
        cur,
        """UPDATE marketplace_library_items
        SET revoked_at=now()
        WHERE user_id=%s
          AND arc_asset_id=%s
        RETURNING user_id""",
        (uid, aid),
    )


def library(cur, user):
    return rows(
        cur,
        """SELECT
            l.*,
            w.title,
            o.name AS publisher,
            b.slug,
            a.revision,
            a.status AS asset_status,
            a.available_from,
            a.available_until,
            o.status AS organization_status,
            b.marketplace_status,

            coalesce(
                e.cover_image_link,
                (
                    SELECT se.cover_image_link
                    FROM marketplace_book_editions sb
                    JOIN editions se
                      ON se.id=sb.edition_id
                    WHERE sb.marketplace_book_id=b.id
                      AND se.cover_image_link<>''
                    LIMIT 1
                ),
                ''
            ) AS cover_url,

            (
                SELECT string_agg(p.display_name, ', ')
                FROM work_contributors wc
                JOIN parties p
                  ON p.id=wc.party_id
                 AND p.tenant_id=wc.tenant_id
                WHERE wc.work_id=w.id
                  AND wc.tenant_id=w.tenant_id
                  AND wc.contributor_role='A01'
            ) AS author

        FROM marketplace_library_items l
        JOIN marketplace_books b
          ON b.id=l.marketplace_book_id
        JOIN works w
          ON w.id=b.work_id
        JOIN marketplace_organizations o
          ON o.id=b.publisher_organization_id
        LEFT JOIN marketplace_arc_assets a
          ON a.id=l.arc_asset_id
        LEFT JOIN editions e
          ON e.id=a.edition_id

        WHERE l.user_id=%s
          AND (
              l.arc_asset_id IS NOT NULL
              OR (
                  b.marketplace_status='public'
                  AND o.status='active'
              )
          )

        ORDER BY l.updated_at DESC
        LIMIT 300""",
        (user["id"],),
    )


def entitlement(cur, uid, bid):
    return one(
        cur,
        """SELECT
            l.*,
            a.revision,
            a.status AS asset_status,
            a.available_from,
            a.available_until,
            v.storage_key,
            v.package,
            v.sha256,
            o.status AS organization_status,
            b.marketplace_status,
            w.title,
            p.display_name,

            (
                SELECT ro.name
                FROM marketplace_arc_requests rq
                JOIN marketplace_actors ra
                  ON ra.id=rq.requester_actor_id
                JOIN marketplace_organizations ro
                  ON ro.id=ra.organization_id
                WHERE rq.asset_id=a.id
                  AND rq.requested_by=l.user_id
                  AND ro.verified_status='verified'
                  AND ro.status='active'
                  AND EXISTS (
                      SELECT 1
                      FROM memberships m
                      WHERE m.user_id=l.user_id
                        AND m.tenant_id=ro.tenant_id
                  )
            ) AS verified_organization

        FROM marketplace_library_items l
        JOIN marketplace_arc_assets a
          ON a.id=l.arc_asset_id
        JOIN marketplace_arc_versions v
          ON v.asset_id=a.id
         AND v.revision=a.revision
        JOIN marketplace_book_editions be
          ON be.edition_id=a.edition_id
         AND be.marketplace_book_id=l.marketplace_book_id
        JOIN marketplace_books b
          ON b.id=l.marketplace_book_id
        JOIN works w
          ON w.id=b.work_id
        JOIN marketplace_organizations o
          ON o.id=b.publisher_organization_id
        JOIN marketplace_profiles p
          ON p.user_id=l.user_id

        WHERE l.user_id=%s
          AND l.marketplace_book_id=%s
          AND EXISTS (
              SELECT 1
              FROM marketplace_actors pa
              WHERE pa.user_id=l.user_id
                AND pa.status='active'
          )""",
        (
            uid,
            bid,
        ),
    )


def session(cur, sid, uid):
    return one(
        cur,
        """SELECT *
        FROM marketplace_reader_sessions
        WHERE id=%s
          AND user_id=%s
          AND expires_at>now()""",
        (
            sid,
            uid,
        ),
    )


def create_session(cur, uid, bid, revision):
    one(
        cur,
        """UPDATE marketplace_library_items
        SET last_opened_at=now()
        WHERE user_id=%s
          AND marketplace_book_id=%s
        RETURNING user_id""",
        (
            uid,
            bid,
        ),
    )

    return one(
        cur,
        """INSERT INTO marketplace_reader_sessions(
            user_id,
            marketplace_book_id,
            revision,
            expires_at
        )
        VALUES(
            %s,
            %s,
            %s,
            now()+interval '20 minutes'
        )
        RETURNING id,expires_at""",
        (
            uid,
            bid,
            revision,
        ),
    )


def progress(cur, uid, bid, revision, location, percent):
    return one(
        cur,
        """UPDATE marketplace_library_items
        SET reading_revision=%s,
            last_location=%s,
            reading_progress=%s,
            status=CASE
                WHEN %s>=99.5 THEN 'read'
                ELSE 'reading'
            END,
            finished_at=CASE
                WHEN %s>=99.5
                    THEN coalesce(finished_at,now())
                ELSE NULL
            END
        WHERE user_id=%s
          AND marketplace_book_id=%s
        RETURNING user_id""",
        (
            revision,
            location,
            percent,
            percent,
            percent,
            uid,
            bid,
        ),
    )


def bookmarks(cur, uid, bid, revision):
    return rows(
        cur,
        """SELECT id,location,chapter,label
        FROM marketplace_reader_bookmarks
        WHERE user_id=%s
          AND marketplace_book_id=%s
          AND revision=%s
        ORDER BY created_at""",
        (
            uid,
            bid,
            revision,
        ),
    )


def add_bookmark(cur, uid, bid, revision, body):
    return one(
        cur,
        """INSERT INTO marketplace_reader_bookmarks(
            user_id,
            marketplace_book_id,
            revision,
            location,
            chapter,
            label
        )
        VALUES(%s,%s,%s,%s,%s,%s)
        ON CONFLICT(
            user_id,
            marketplace_book_id,
            revision,
            location
        )
        DO UPDATE SET
            chapter=EXCLUDED.chapter,
            label=EXCLUDED.label
        RETURNING id,location,chapter,label""",
        (
            uid,
            bid,
            revision,
            body.location,
            body.chapter,
            body.label,
        ),
    )


def delete_bookmark(cur, uid, bid, bookmark):
    return one(
        cur,
        """DELETE FROM marketplace_reader_bookmarks
        WHERE user_id=%s
          AND marketplace_book_id=%s
          AND id=%s
        RETURNING id""",
        (
            uid,
            bid,
            bookmark,
        ),
    )


def notices(cur, user):
    return rows(
        cur,
        """SELECT
            r.id,
            r.status,
            r.requested_at,
            r.decided_at,
            w.title,
            b.id AS book_id,

            CASE
                WHEN r.status='approved'
                     AND a.available_until IS NOT NULL
                     AND a.available_until<=now()
                    THEN 'expired'

                WHEN r.status='approved'
                     AND a.available_until IS NOT NULL
                     AND a.available_until<=now()+interval '7 days'
                    THEN 'expiring'

                ELSE r.status
            END AS notice_status,

            (r.requested_by=%s) AS own

        FROM marketplace_arc_requests r
        JOIN marketplace_arc_assets a
          ON a.id=r.asset_id
        JOIN editions e
          ON e.id=a.edition_id
        JOIN works w
          ON w.id=e.work_id
        JOIN marketplace_organizations o
          ON o.tenant_id=w.tenant_id
        LEFT JOIN marketplace_books b
          ON b.work_id=w.id

        WHERE (
            r.requested_by=%s
            AND r.status<>'pending'
        )
        OR (
            r.status='pending'
            AND EXISTS (
                SELECT 1
                FROM memberships m
                WHERE m.tenant_id=w.tenant_id
                  AND m.user_id=%s
                  AND m.role='tenant_admin'
            )
        )

        ORDER BY coalesce(
            r.decided_at,
            r.requested_at
        ) DESC

        LIMIT 30""",
        (
            user["id"],
            user["id"],
            user["id"],
        ),
    )


def library_asset(cur, uid, bid):
    return one(
        cur,
        """SELECT arc_asset_id
        FROM marketplace_library_items
        WHERE user_id=%s
          AND marketplace_book_id=%s
          AND arc_asset_id IS NOT NULL""",
        (
            uid,
            bid,
        ),
    )