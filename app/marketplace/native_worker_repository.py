"""Processing queue and cleanup data access; worker owns transactions."""

from .core import one, rows


def claim_query_1(cur):
    return cur.execute(
        "UPDATE marketplace_media SET status='failed',error_code='processing_failed' WHERE status='processing' AND lease_until<now() AND attempts>=3"
    )


def claim_query_2(cur, token):
    return one(
        cur,
        "UPDATE marketplace_media SET status='processing',attempts=attempts+1,lease_token=%s,lease_until=now()+interval '45 minutes'\n        WHERE id=(SELECT id FROM marketplace_media WHERE (status='uploaded' OR (status='processing' AND lease_until<now()))\n        AND next_attempt_at<=now() AND attempts<6 ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING *",
        (token,),
    )


def process_query_1(cur, Jsonb, output, metadata, row):
    return cur.execute(
        "UPDATE marketplace_media SET status='ready',variants=%s,width=%s,height=%s,duration=%s,mime_type=%s,\n            error_code=NULL,lease_until=NULL WHERE id=%s AND status='processing' AND lease_token=%s",
        (
            Jsonb(output),
            metadata["width"],
            metadata["height"],
            metadata["duration"],
            metadata["mime_type"],
            row["id"],
            row["lease_token"],
        ),
    )


def cleanup_query_1(cur):
    return cur.execute(
        "UPDATE marketplace_media m SET status='deleted',deleted_at=now()\n        WHERE ((status='pending_upload' AND created_at<now()-interval '1 day') OR\n          (status IN ('ready','failed') AND created_at<now()-interval '30 days'))\n        AND NOT EXISTS(SELECT 1 FROM marketplace_post_media pm JOIN marketplace_posts p ON p.id=pm.post_id\n            WHERE pm.media_id=m.id AND p.status<>'deleted')"
    )


def cleanup_query_2(cur):
    return rows(
        cur,
        "SELECT * FROM marketplace_media WHERE status='deleted' AND deleted_at<now()-interval '1 day' AND purged_at IS NULL ORDER BY deleted_at LIMIT 10",
    )


def cleanup_query_3(cur):
    return rows(
        cur,
        "SELECT * FROM marketplace_media WHERE status IN ('ready','failed','uploaded') AND created_at<now()-interval '1 day' ORDER BY storage_checked_at NULLS FIRST,id LIMIT 20",
    )


def cleanup_query_4(cur, row):
    return cur.execute(
        "UPDATE marketplace_media SET purged_at=now() WHERE id=%s AND status='deleted'",
        (row["id"],),
    )


def cleanup_query_5(cur, row):
    return cur.execute(
        "UPDATE marketplace_media SET storage_checked_at=now() WHERE id=%s",
        (row["id"],),
    )


def tick_query_1(cur, row):
    return cur.execute(
        "UPDATE marketplace_media SET status=%s,error_code='processing_failed',lease_until=NULL,\n            next_attempt_at=now()+interval '5 minutes' WHERE id=%s AND status='processing' AND lease_token=%s",
        (
            "uploaded" if row["attempts"] < 3 else "failed",
            row["id"],
            row["lease_token"],
        ),
    )
