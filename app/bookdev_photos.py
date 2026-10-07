"""Photo input is decoded and re-encoded; caller supplies only DB-bound identity."""
import io
import warnings
from fastapi import HTTPException
from PIL import Image, ImageOps

MAX_PHOTO_BYTES = 10 * 1024 * 1024
MAX_PHOTO_PIXELS = 25_000_000
ALLOWED_FORMATS = {"JPEG", "PNG", "WEBP"}


def normalize_photo(file):
    if file.content_type not in {"image/jpeg", "image/jpg", "image/png", "image/webp"}:
        raise HTTPException(415, "Choose a JPEG, PNG, or WebP photo.")
    data = file.file.read(MAX_PHOTO_BYTES + 1)
    if len(data) > MAX_PHOTO_BYTES:
        raise HTTPException(413, "The photo must be no larger than 10 MB.")
    if not data:
        raise HTTPException(400, "Choose a nonempty photo.")
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(io.BytesIO(data)) as source:
                if source.format not in ALLOWED_FORMATS or source.width * source.height > MAX_PHOTO_PIXELS:
                    raise HTTPException(400, "Use a JPEG, PNG, or WebP image of at most 25 megapixels.")
                expected_mime = {"JPEG": {"image/jpeg", "image/jpg"}, "PNG": {"image/png"}, "WEBP": {"image/webp"}}[source.format]
                if file.content_type not in expected_mime:
                    raise HTTPException(415, "The photo format does not match its declared file type.")
                source.verify()
            with Image.open(io.BytesIO(data)) as source:
                source.load()
                image = ImageOps.exif_transpose(source)
                rgba = image.convert("RGBA")
                rgb = Image.new("RGB", rgba.size, "white")
                rgb.paste(rgba, mask=rgba.getchannel("A"))
                output = io.BytesIO()
                # No EXIF, user metadata or trailing payload is copied into the new JPEG.
                rgb.save(output, format="JPEG", quality=90)
                encoded = output.getvalue()
                if len(encoded) > MAX_PHOTO_BYTES:
                    raise HTTPException(413, "The converted photo is too large.")
                return encoded, rgb.width, rgb.height
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(400, "This file is not a valid photo.")


def save_request_photo(row, party_id, data, width, height):
    from routers import uploads
    work = uploads._resolve_work(row["work_id"])
    if str(work["tenant_id"]) != row["tenant_id"]:
        raise HTTPException(403, "The photo does not belong to this request.")
    role = "author" if row["request_type"] == "AUTHOR_PHOTO" else "illustrator"
    people = [person for person in uploads._photo_contributors(work)
              if person["party_id"] == party_id and person["role"] == role and person["tenant_slug"] == row["tenant_slug"]]
    if len(people) != 1:
        raise HTTPException(409, "The contributor is no longer assigned to this book.")
    person = people[0]
    filename = f"{party_id}__{role}_photo.jpg"
    key = uploads._contributor_photo_key(person)
    if not uploads.USE_UPLOADS_S3:
        # The established contributor-photo reader uses S3; do not report a local
        # upload as complete when it cannot appear on the contributor tab.
        raise HTTPException(503, "Contributor photo storage is not configured.")
    try:
        uploads._s3_client().put_object(Bucket=uploads.S3_BUCKET, Key=key, Body=data, ContentType="image/jpeg")
        url = uploads._s3_url_for_key(key)
    except Exception:
        raise HTTPException(503, "The photo could not be saved. Please try again later.")
    return {"kind": f"{role}_photo", "filename": filename, "url": url, "key": key,
            "mime": "image/jpeg", "size": len(data), "width": width, "height": height,
            "work_id": row["work_id"], "party_id": party_id}
