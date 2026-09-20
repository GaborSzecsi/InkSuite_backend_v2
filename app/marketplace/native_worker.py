from . import native_worker_repository as _repository

"""Run separately: python -m app.marketplace.native_worker. Never imported by API startup."""

import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4
from psycopg.types.json import Jsonb
from .core import transaction, one, rows
from .native_storage import s3, config
from .native_processing import image_variants, video_variants

log = logging.getLogger(__name__)


def claim():
    token = uuid4()
    with transaction() as cur:
        # Jobs abandoned by a killed worker eventually exhaust their retry budget.
        _repository.claim_query_1(cur)
        return _repository.claim_query_2(cur, token)


def process(row):
    client = s3()
    if row["bucket"] != config()[0]:
        raise ValueError("wrong_bucket")
    with TemporaryDirectory(prefix="inksuite-social-") as folder:
        source = Path(folder) / "source"
        response = client.get_object(
            Bucket=row["bucket"],
            Key=row["original_key"],
            VersionId=row["original_version"],
        )
        if response["ContentLength"] != row["file_size"]:
            raise ValueError("size_mismatch")
        count = 0
        try:
            with source.open("wb") as handle:
                for chunk in response["Body"].iter_chunks(1024 * 1024):
                    count += len(chunk)
                    if count > row["file_size"]:
                        raise ValueError("size_mismatch")
                    handle.write(chunk)
        finally:
            response["Body"].close()
        if count != row["file_size"]:
            raise ValueError("size_mismatch")
        metadata, variants = (
            image_variants if row["media_type"] == "image" else video_variants
        )(source, folder)
        total = count + sum(v["path"].stat().st_size for v in variants)
        if total > row["reserved_bytes"]:
            raise ValueError("derivative_limit")
        # Future moderation can reject here, before any ready transition / attachment.
        output = []
        for variant in variants:
            path = variant.pop("path")
            key = f"processed/{row['id']}/{row['lease_token']}/{path.name}"
            client.upload_file(
                str(path),
                row["bucket"],
                key,
                ExtraArgs={
                    "ContentType": variant["content_type"],
                    "CacheControl": "public, max-age=31536000, immutable",
                },
            )
            output.append(dict(variant, key=key))
        with transaction() as cur:
            _repository.process_query_1(cur, Jsonb, output, metadata, row)


def cleanup():
    with transaction() as cur:
        _repository.cleanup_query_1(cur)
        candidates = _repository.cleanup_query_2(cur)
    for row in candidates:
        if row["bucket"] != config()[0]:
            continue
        for prefix in (
            f"originals/{row['owner_actor_id']}/{row['id']}/",
            f"processed/{row['id']}/",
        ):
            for page in (
                s3()
                .get_paginator("list_object_versions")
                .paginate(Bucket=row["bucket"], Prefix=prefix)
            ):
                objects = [
                    {"Key": x["Key"], "VersionId": x["VersionId"]}
                    for x in page.get("Versions", []) + page.get("DeleteMarkers", [])
                ]
                if objects:
                    result = s3().delete_objects(
                        Bucket=row["bucket"], Delete={"Objects": objects, "Quiet": True}
                    )
                    if result.get("Errors"):
                        raise RuntimeError("cleanup_failed")
        with transaction() as cur:
            _repository.cleanup_query_4(cur, row)
    # Remove expired upload replays and orphaned outputs from interrupted attempts.
    # The selected original version remains private and reusable indefinitely while referenced.
    with transaction() as cur:
        assets = _repository.cleanup_query_3(cur)
    cutoff = datetime.now(timezone.utc) - timedelta(days=1)
    for row in assets:
        if row["bucket"] != config()[0]:
            continue
        keep = {v["key"] for v in row["variants"]}
        for prefix in (
            f"originals/{row['owner_actor_id']}/{row['id']}/",
            f"processed/{row['id']}/",
        ):
            for page in (
                s3()
                .get_paginator("list_object_versions")
                .paginate(Bucket=row["bucket"], Prefix=prefix)
            ):
                obsolete = [
                    {"Key": v["Key"], "VersionId": v["VersionId"]}
                    for v in page.get("Versions", [])
                    if v["LastModified"] < cutoff
                    and v["Key"] not in keep
                    and v["VersionId"] != row["original_version"]
                ]
                if obsolete:
                    result = s3().delete_objects(
                        Bucket=row["bucket"],
                        Delete={"Objects": obsolete, "Quiet": True},
                    )
                    if result.get("Errors"):
                        raise RuntimeError("cleanup_failed")
        with transaction() as cur:
            _repository.cleanup_query_5(cur, row)


def tick():
    row = claim()
    if not row:
        return False
    started = time.monotonic()
    try:
        process(row)
        log.info(
            "marketplace_media_processed id=%s elapsed_ms=%s",
            row["id"],
            round((time.monotonic() - started) * 1000),
        )
    except Exception as exc:
        with transaction() as cur:
            _repository.tick_query_1(cur, row)
        log.warning(
            "marketplace_media_failed id=%s error_type=%s",
            row["id"],
            type(exc).__name__,
        )
    return True


def main():
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
    logging.basicConfig(level=logging.INFO)
    config()
    last_cleanup = 0
    while True:
        try:
            if time.monotonic() - last_cleanup > 3600:
                cleanup()
                last_cleanup = time.monotonic()
            if not tick():
                time.sleep(5)
        except Exception as exc:
            log.error(
                "marketplace_media_worker_unavailable error_type=%s", type(exc).__name__
            )
            time.sleep(15)


if __name__ == "__main__":
    main()
