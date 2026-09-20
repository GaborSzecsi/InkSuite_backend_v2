"""No AWS calls or database required."""

from pathlib import Path
from uuid import uuid4
from unittest.mock import patch, MagicMock
import pytest
from PIL import Image
from pydantic import ValidationError
from fastapi import HTTPException
from app.marketplace.schemas import Post
from app.marketplace.native_processing import image_variants
from app.marketplace import native_storage, native_repository, native_service


def test_media_only_posts_and_duplicates():
    owner = uuid4()
    asset = uuid4()
    assert Post(actor_id=owner, media_ids=[asset]).body == ""
    with pytest.raises(ValidationError):
        Post(actor_id=owner, body=" ")
    with pytest.raises(ValidationError):
        Post(actor_id=owner, media_ids=[asset, asset])
    with pytest.raises(ValidationError):
        Post(actor_id=owner, media_ids=[asset], media_key="legacy")


def test_images_are_reencoded_without_metadata_or_upscaling(tmp_path):
    source = tmp_path / "source"
    picture = Image.new("RGB", (2000, 1000), "white")
    exif = Image.Exif()
    exif[270] = "Private location text"
    picture.save(source, "JPEG", exif=exif)
    meta, variants = image_variants(source, tmp_path)
    assert meta["width"] == 2000
    assert [x["width"] for x in variants] == [480, 960, 1920]
    for v in variants:
        with Image.open(v["path"]) as result:
            assert not result.getexif()
            assert result.format == "WEBP"
    picture = Image.new("RGB", (10, 8))
    picture.save(source, "PNG")
    _, variants = image_variants(source, tmp_path)
    assert all(x["width"] == 10 for x in variants)


def test_malformed_media_rejected(tmp_path):
    source = tmp_path / "source"
    source.write_text("<script>not an image</script>")
    with pytest.raises(Exception):
        image_variants(source, tmp_path)


def test_never_fall_back_to_private_tenant_storage(monkeypatch):
    monkeypatch.setenv("MARKETPLACE_MEDIA_BUCKET", "inksuite-data")
    with pytest.raises(HTTPException):
        native_storage.config()


def test_presigned_upload_restricts_size_and_type():
    client = MagicMock()
    with patch.object(native_storage, "s3", return_value=client):
        native_storage.upload_form(
            {
                "bucket": "social",
                "original_key": "originals/uuid/source",
                "declared_mime": "image/jpeg",
                "file_size": 123,
            }
        )
    args = client.generate_presigned_post.call_args.kwargs
    assert ["content-length-range", 123, 123] in args["Conditions"]
    assert args["ExpiresIn"] == 600


@pytest.mark.parametrize(
    "status,owner_ok", [("processing", True), ("ready", False), ("deleted", True)]
)
def test_attachment_ownership_and_ready_required(status, owner_ok):
    owner = uuid4()
    asset = uuid4()
    with patch.object(
        native_repository,
        "rows",
        return_value=[
            {
                "id": asset,
                "owner_actor_id": owner if owner_ok else uuid4(),
                "status": status,
                "media_type": "image",
            }
        ],
    ):
        with pytest.raises(HTTPException):
            native_service.attach(MagicMock(), uuid4(), [asset], owner)


def test_existing_routes_are_thin_adapters():
    import ast

    root = Path(__file__).resolve().parents[1] / "app" / "marketplace"
    for path in root.glob("*_routes.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                assert node.func.attr not in ("execute", "fetchone", "fetchall"), path


def test_cloudfront_signatures_hide_originals(tmp_path, monkeypatch):
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives.serialization import (
        Encoding,
        PrivateFormat,
        NoEncryption,
    )

    key = tmp_path / "private.pem"
    key.write_bytes(
        rsa.generate_private_key(public_exponent=65537, key_size=2048).private_bytes(
            Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()
        )
    )
    for name, value in {
        "MARKETPLACE_MEDIA_BUCKET": "dedicated-social-dev",
        "MARKETPLACE_MEDIA_CDN_URL": "https://media.example.invalid",
        "MARKETPLACE_MEDIA_KEY_PAIR_ID": "TESTKEY",
        "MARKETPLACE_MEDIA_PRIVATE_KEY_FILE": str(key),
    }.items():
        monkeypatch.setenv(name, value)
    url = native_storage.delivery("processed/test/v1/medium.webp")
    assert "Signature=" in url and "Expires=" in url and "Key-Pair-Id=TESTKEY" in url
    with pytest.raises(ValueError):
        native_storage.delivery("originals/test/source")


def test_real_video_transcoding_and_poster(tmp_path):
    import shutil, subprocess, json
    from app.marketplace.native_processing import video_variants, binary

    executable = binary("ffmpeg")
    if not Path(executable).exists() and not shutil.which(executable):
        pytest.skip("Install FFmpeg to run video integration check")
    source = tmp_path / "source.mp4"
    subprocess.run(
        [
            executable,
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            "color=c=blue:s=320x240:d=2",
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            str(source),
        ],
        check=True,
        timeout=30,
    )
    meta, variants = video_variants(source, tmp_path)
    assert meta["duration"] == 2
    assert {v["name"] for v in variants} == {"720p", "poster"}
    playback = next(v["path"] for v in variants if v["name"] == "720p")
    result = subprocess.run(
        [
            binary("ffprobe"),
            "-v",
            "error",
            "-show_streams",
            "-of",
            "json",
            str(playback),
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    stream = json.loads(result.stdout)["streams"][0]
    assert (stream["width"], stream["height"]) == (320, 240)
    assert stream["codec_name"] == "h264" and stream["pix_fmt"] == "yuv420p"
    with Image.open(tmp_path / "poster.jpg") as poster:
        assert not poster.getexif()


@pytest.mark.parametrize(
    "mime,size", [("text/html", 10), ("image/jpeg", 20971521), ("video/mp4", 524288001)]
)
def test_invalid_or_oversized_upload_rejected_before_storage(mime, size):
    from app.marketplace.native_routes import Upload

    body = Upload(actor_id=uuid4(), filename="test", content_type=mime, file_size=size)
    with patch.object(native_storage, "upload_form") as upload:
        with pytest.raises(HTTPException) as exc:
            native_service.initiate(body, {"id": uuid4()})
        assert exc.value.status_code == 422
        upload.assert_not_called()


def test_development_cannot_use_production_bucket(monkeypatch):
    monkeypatch.setenv("MARKETPLACE_MEDIA_ENV", "development")
    monkeypatch.setenv("MARKETPLACE_MEDIA_BUCKET", "inksuite-social-prod")
    with pytest.raises(HTTPException):
        native_storage.config()
