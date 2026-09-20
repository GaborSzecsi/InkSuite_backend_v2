"""Runs production route code/SQL against disposable PGlite. No AWS calls or DSN."""

import sys, json, subprocess, contextlib
from pathlib import Path
from uuid import uuid4

root = Path(__file__).parent
sys.path.insert(0, str(root.parent))
import psycopg
from psycopg.types.json import Jsonb
from fastapi import FastAPI
from fastapi.testclient import TestClient
from app.marketplace import (
    core,
    identity,
    social,
    notifications,
    catalog as catalog_module,
)

real_connection_email = notifications.email_connection_request
mail_calls = []
notifications.email_connection_request = lambda connection_id: mail_calls.append(
    connection_id
)
catalog_module._legacy_cover_url = lambda *args: ""
from app.marketplace.routes import router
from app.auth.dependencies import require_session

proc = subprocess.Popen(
    [
        "node",
        "--preserve-symlinks",
        "--preserve-symlinks-main",
        str(root / "marketplace_pg/rpc.cjs"),
    ],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    text=True,
    encoding="utf-8",
)
assert json.loads(proc.stdout.readline()).get("ready")


def rpc(sql, params=(), execute=False):
    params = [
        (
            json.dumps(v.obj)
            if isinstance(v, Jsonb)
            else str(v) if isinstance(v, __import__("uuid").UUID) else v
        )
        for v in params
    ]
    proc.stdin.write(
        json.dumps({"sql": sql, "params": params, "exec": execute}, default=str) + "\n"
    )
    proc.stdin.flush()
    r = json.loads(proc.stdout.readline())
    if "error" in r:
        print("SQL ERROR:", r["error"])
        raise psycopg.errors.lookup(r.get("code") or "XX000")(r["error"])
    return r["result"]


query_count = 0


class Cursor:
    def execute(self, sql, params=()):
        global query_count
        query_count += 1
        i = 0
        while "%s" in sql:
            i += 1
            sql = sql.replace("%s", f"${i}", 1)
        self.data = rpc(sql, params)["rows"]
        return self

    def fetchone(self):
        return self.data[0] if self.data else None

    def fetchall(self):
        return self.data

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class Connection:
    @contextlib.contextmanager
    def transaction(self):
        rpc("BEGIN", execute=True)
        try:
            yield
        except BaseException:
            rpc("ROLLBACK", execute=True)
            raise
        else:
            rpc("COMMIT", execute=True)

    def cursor(self, **kwargs):
        return Cursor()


@contextlib.contextmanager
def db():
    yield Connection()


core.db_conn = db
user = {"id": str(uuid4()), "platform_role": "user"}
admin = dict(user)
reader = {"id": str(uuid4()), "platform_role": "user"}
outsider = {"id": str(uuid4()), "platform_role": "user"}
claims = {"sub": "test"}
identity.auth.get_user_db_record_from_claims = lambda claims: user
social.get_user_db_record_from_claims = lambda claims: user
app = FastAPI()
app.include_router(router, prefix="/api")
app.dependency_overrides[core.current_user] = lambda: user
app.dependency_overrides[require_session] = lambda: claims
client = TestClient(app)
checks = 0


def call(method, path, body=None, status=200):
    global checks
    r = client.request(method, "/api/marketplace/" + path, json=body)
    assert r.status_code == status, (method, path, r.status_code, r.text)
    checks += 1
    return r.json()


def seed(sql, params=()):
    return rpc(sql, params)["rows"]


try:
    tid = str(uuid4())
    tid2 = str(uuid4())
    work = str(uuid4())
    work2 = str(uuid4())
    edition = str(uuid4())
    edition2 = str(uuid4())
    seed(
        "INSERT INTO tenants(id,slug,name,s3_prefix) VALUES($1,'test','Test','test/'),($2,'other','Other','other/')",
        [tid, tid2],
    )
    for i, u in enumerate([admin, reader, outsider]):
        seed(
            "INSERT INTO users(id,cognito_sub,email) VALUES($1,$2,'private@example.invalid')",
            [u["id"], "fixture-" + str(i)],
        )
    seed(
        "INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'tenant_admin')",
        [tid, admin["id"]],
    )
    seed(
        "INSERT INTO works(id,tenant_id,uid,title,main_description) VALUES($1,$2,gen_random_uuid(),'Fixture Book','Public description'),($3,$4,gen_random_uuid(),'Private acquisition','Secret')",
        [work, tid, work2, tid2],
    )
    seed(
        "INSERT INTO editions(id,tenant_id,work_id,isbn13,product_form) VALUES($1,$2,$3,'9781234567890','BC'),($4,$5,$6,'9781234567891','BB')",
        [edition, tid, work, edition2, tid2, work2],
    )
    personal = call(
        "PUT",
        "profile",
        {"username": "publisher_admin", "display_name": "Publisher Admin"},
    )["id"]
    org = call("POST", "organizations/from-tenant/" + tid)["id"]
    session = call("GET", "me")
    oa = session["organizations"][0]["actor_id"]
    assert call("GET", "publishers")["items"] == []
    call(
        "PUT",
        "organizations/" + org,
        {"name": "Test", "slug": "test", "status": "active"},
    )
    assert call("GET", "publishers")["items"][0]["name"] == "Test"
    listing = {
        "work_id": work,
        "slug": "fixture-book",
        "edition_ids": [edition],
        "marketplace_status": "public",
        "discoverable": True,
    }
    bid = call("PUT", f"organizations/{org}/listings", listing)["id"]
    call(
        "PUT",
        f"organizations/{org}/listings",
        dict(listing, edition_ids=[edition2]),
        409,
    )
    catalog = call("GET", "books")
    assert len(catalog["items"]) == 1
    serialized = json.dumps(catalog)
    assert (
        "private@example" not in serialized
        and "Secret" not in serialized
        and "tenant_id" not in serialized
    )
    assert len(call("GET", "books/fixture-book")["editions"]) == 1
    call("GET", "books?q=Fixture&period=coming&subject=fiction")
    post = call(
        "POST", "posts", {"actor_id": oa, "body": "A new book", "book_ids": [bid]}
    )["id"]
    user = reader
    ra = call(
        "PUT",
        "profile",
        {
            "username": "reader_one",
            "display_name": "Reader One",
            "profile_visibility": "private",
        },
    )["id"]
    call("POST", "posts", {"actor_id": oa, "body": "Impersonation"}, 403)
    call("GET", f"organizations/{org}/catalog", status=403)
    call("PUT", "library/" + bid, {"status": "reading"})
    call("PUT", "library/" + bid, {"status": "read"})
    assert len(call("GET", "library")["items"]) == 1
    call("PUT", "follow", {"actor_id": ra, "target_id": oa})
    assert len(call("GET", f"feed?actor_id={ra}&scope=following")["items"]) == 1
    call("PUT", f"posts/{post}/like?actor_id={ra}")
    call("PUT", f"posts/{post}/like?actor_id={ra}")
    assert call("GET", f"feed?actor_id={ra}")["items"][0]["likes"] == 1
    comment = call(
        "POST", f"posts/{post}/comments", {"actor_id": ra, "body": "Looks great"}
    )["id"]
    assert call("GET", f"posts/{post}/comments")["items"][0]["body"] == "Looks great"
    # Existing-schema fallback works; new interactions fail clearly until migration.
    assert call("GET", f"posts/{post}/comments")["interactions_available"] is False
    call("PUT", f"comments/{comment}/like?actor_id={ra}", status=503)
    rpc(
        (root.parent / "migrations/009_marketplace_comment_interactions.sql").read_text(
            encoding="utf-8"
        ),
        execute=True,
    )
    comment_page = call("GET", f"posts/{post}/comments?actor_id={ra}")
    assert comment_page["interactions_available"] is True
    assert comment_page["items"][0]["author"]["name"] == "Reader One"
    assert call("GET", f"posts/{post}/comments")["items"][0]["author"]["href"] is None
    ownpost = call("POST", "posts", {"actor_id": ra, "body": "Reader post"})["id"]
    ownfeed = call("GET", f"feed?actor_id={ra}&author_id={ra}")
    assert ownfeed["items"][0]["author"]["name"] == "Reader One"
    publicfeed = call("GET", f"feed?author_id={ra}")
    assert publicfeed["items"][0]["author"]["name"] == "Reader One"
    assert (
        publicfeed["items"][0]["author"]["href"] is None
        and publicfeed["items"][0]["author"]["bio"] == ""
    )
    call("PUT", f"comments/{comment}/like?actor_id={oa}", status=403)
    call("PUT", f"comments/{comment}/like?actor_id={ra}")
    call("PUT", f"comments/{comment}/like?actor_id={ra}")
    liked = call("GET", f"posts/{post}/comments?actor_id={ra}")["items"][0]
    assert liked["likes"] == 1 and liked["liked"]
    call("DELETE", f"comments/{comment}/like?actor_id={ra}")
    assert call("GET", f"posts/{post}/comments?actor_id={ra}")["items"][0]["likes"] == 0
    call(
        "POST",
        f"posts/{ownpost}/comments",
        {"actor_id": ra, "body": "Wrong post", "parent_comment_id": comment},
        422,
    )
    reply = call(
        "POST",
        f"posts/{post}/comments",
        {"actor_id": ra, "body": "First reply", "parent_comment_id": comment},
    )["id"]
    reply2 = call(
        "POST",
        f"posts/{post}/comments",
        {"actor_id": ra, "body": "Reply in thread", "parent_comment_id": reply},
    )["id"]
    roots = call("GET", f"posts/{post}/comments?actor_id={ra}")
    assert len(roots["items"]) == 1 and roots["items"][0]["replies"] == 2
    thread = call(
        "GET", f"posts/{post}/comments?actor_id={ra}&parent_comment_id={comment}"
    )
    assert (
        len(thread["items"]) == 2 and thread["items"][1]["parent_comment_id"] == comment
    )
    privatepost = call(
        "POST",
        "posts",
        {"actor_id": ra, "body": "Connections only", "visibility": "connections"},
    )["id"]
    privatecomment = call(
        "POST",
        f"posts/{privatepost}/comments",
        {"actor_id": ra, "body": "Private comment"},
    )["id"]
    user = admin
    call("GET", f"posts/{privatepost}/comments?actor_id={oa}", status=404)
    call("PUT", f"comments/{privatecomment}/like?actor_id={oa}", status=404)
    call(
        "POST",
        f"posts/{privatepost}/comments",
        {"actor_id": oa, "body": "Not permitted", "parent_comment_id": privatecomment},
        404,
    )
    call("PUT", f"comments/{reply}/like?actor_id={oa}")
    call("DELETE", f"comments/{reply}?actor_id={oa}", status=404)
    user = reader
    temporary = call(
        "POST", f"posts/{post}/comments", {"actor_id": ra, "body": "Remove parent"}
    )["id"]
    call(
        "POST",
        f"posts/{post}/comments",
        {"actor_id": ra, "body": "Keep child", "parent_comment_id": temporary},
    )
    call("DELETE", f"comments/{temporary}?actor_id={ra}")
    tombstone = [
        c
        for c in call("GET", f"posts/{post}/comments?actor_id={ra}")["items"]
        if c["id"] == temporary
    ][0]
    assert (
        tombstone["deleted"]
        and tombstone["body"] == "Comment deleted."
        and tombstone["replies"] == 1
    )
    call("PUT", f"comments/{temporary}/like?actor_id={ra}", status=404)
    call(
        "POST",
        f"posts/{post}/comments",
        {"actor_id": ra, "body": "No new reply", "parent_comment_id": temporary},
        404,
    )
    connection = call("POST", "connections", {"actor_id": ra, "target_id": oa})["id"]
    assert (
        call("GET", f"relationship?actor_id={ra}&target_id={oa}")["status"] == "pending"
    )
    assert len(mail_calls) == 1
    assert (
        call("POST", "connections", {"actor_id": ra, "target_id": oa})["id"]
        == connection
    )
    assert len(mail_calls) == 1
    assert call("GET", "connection-notifications")["count"] == 0
    user = outsider
    assert call("GET", "connection-notifications")["count"] == 0
    user = admin
    notices = call("GET", "connection-notifications")
    assert notices["count"] == 1 and notices["items"][0]["recipient_actor_id"] == oa
    from unittest.mock import patch
    from types import ModuleType

    smtp = ModuleType("routers.contract_invites")
    smtp._load_tenant_email_settings_or_400 = lambda slug: {
        "smtp_secret_id": "fixture",
        "smtp_host": "localhost",
        "smtp_port": 587,
        "tls_mode": "starttls",
        "from_email": "sender@example.invalid",
        "from_name": "Fixture",
    }
    smtp._load_smtp_secret = lambda secret: ("fixture", "fixture")
    deliveries = []
    smtp._send_email_smtp = lambda **kwargs: deliveries.append(kwargs)
    with patch.dict(sys.modules, {"routers.contract_invites": smtp}):
        real_connection_email(connection)
    assert len(deliveries) == 1
    assert deliveries[0]["to_email"] == "private@example.invalid"
    assert "Reader One" in deliveries[0]["body_text"]
    assert notices["items"][0]["sender"]["name"] == "Reader One"
    assert notices["items"][0]["sender"]["href"] is None
    seed(
        "UPDATE marketplace_profiles SET profile_visibility='public' WHERE user_id=$1",
        [reader["id"]],
    )
    user = reader
    conv = call("POST", "conversations", {"actor_id": ra, "target_id": oa})["id"]
    assert (
        call("POST", "conversations", {"actor_id": ra, "target_id": oa})["id"] == conv
    )
    msg = call(
        "POST",
        f"conversations/{conv}/messages",
        {"actor_id": ra, "body": "Hello publisher"},
    )["id"]
    user = admin
    call("PUT", "connections/" + connection, {"actor_id": oa, "action": "accept"})
    assert call("GET", "connection-notifications")["count"] == 0
    user = reader
    accepted = call("GET", "connection-notifications")
    assert accepted["count"] == 1 and accepted["items"][0]["kind"] == "accepted"
    assert accepted["items"][0]["sender"]["name"] == "Test"
    assert accepted["items"][0]["notification_key"]
    assert (
        call("GET", f"relationship?actor_id={ra}&target_id={oa}")["status"]
        == "accepted"
    )
    user = outsider
    assert call("GET", "connection-notifications")["count"] == 0
    call("GET", f"relationship?actor_id={ra}&target_id={oa}", status=403)
    user = admin
    assert call("GET", "unread?actor_id=" + oa)["count"] == 1
    history = call("GET", f"conversations/{conv}/messages?actor_id={oa}")
    assert history["items"][0]["body"] == "Hello publisher"
    call("PUT", f"conversations/{conv}/read/{msg}?actor_id={oa}")
    assert call("GET", "unread?actor_id=" + oa)["count"] == 0
    call("POST", f"conversations/{conv}/messages", {"actor_id": oa, "body": "Welcome"})
    call(
        "POST",
        "reports",
        {
            "actor_id": oa,
            "target_type": "message",
            "target_id": msg,
            "reason": "Test reporting",
        },
    )
    call("PUT", "block", {"actor_id": oa, "target_id": ra})
    user = reader
    call("PUT", f"comments/{comment}/like?actor_id={ra}", status=404)
    call(
        "POST",
        f"posts/{post}/comments",
        {"actor_id": ra, "body": "Blocked reply", "parent_comment_id": comment},
        404,
    )
    call(
        "POST",
        f"conversations/{conv}/messages",
        {"actor_id": ra, "body": "Blocked"},
        403,
    )
    call("POST", "conversations", {"actor_id": ra, "target_id": oa}, 403)
    assert (
        len(call("GET", f"conversations/{conv}/messages?actor_id={ra}")["items"]) == 2
    )
    user = outsider
    other_actor = call(
        "PUT", "profile", {"username": "outsider", "display_name": "Outsider"}
    )["id"]
    call("GET", f"conversations/{conv}/messages?actor_id={other_actor}", status=404)
    call(
        "POST",
        "reports",
        {
            "actor_id": other_actor,
            "target_type": "message",
            "target_id": msg,
            "reason": "Unauthorized",
        },
        404,
    )
    call("DELETE", f"comments/{comment}?actor_id={other_actor}", status=404)
    # Image storage adapter is fake; production validation and SQL still run.
    from app.marketplace import media
    from PIL import Image
    import io

    class Store:
        objects = {}

        def get_paginator(self, name):
            return self

        def paginate(self, **kw):
            return [
                {
                    "Contents": [
                        {"Key": k, "Size": len(v), "LastModified": k}
                        for k, v in self.objects.items()
                        if k.startswith(kw["Prefix"])
                    ]
                }
            ]

        def put_object(self, **kw):
            self.objects[kw["Key"]] = kw["Body"]

        def head_object(self, **kw):
            assert kw["Key"] in self.objects
            return {}

        def delete_object(self, **kw):
            self.objects.pop(kw["Key"], None)

        def generate_presigned_url(self, *args, **kwargs):
            return "https://example.invalid/approved-image.jpg"

    store = Store()
    media.storage = lambda: (store, "fixture-bucket")
    user = reader
    image = io.BytesIO()
    Image.new("RGB", (20, 20), "white").save(image, "PNG")
    upload = client.post(
        "/api/marketplace/images?actor_id=" + ra,
        files={"file": ("test.png", image.getvalue(), "image/png")},
    )
    assert upload.status_code == 200, upload.text
    key = upload.json()["key"]
    iid = key.split("/")[-1].replace(".jpg", "")
    call("PUT", f"identity-image?actor_id={ra}&purpose=avatar&key={key}")
    assert (
        call("GET", "users/reader_one")["image"]
        == "https://example.invalid/approved-image.jpg"
    )
    call("DELETE", f"images/{iid}?actor_id={ra}", status=409)
    imagepost = call(
        "POST", "posts", {"actor_id": ra, "body": "An image", "media_key": key}
    )["id"]
    user = outsider
    call(
        "POST",
        "posts",
        {"actor_id": other_actor, "body": "Stolen image", "media_key": key},
        403,
    )
    user = reader
    call("PUT", f"identity-image?actor_id={ra}&purpose=avatar")
    call("DELETE", f"images/{iid}?actor_id={ra}", status=409)
    call("DELETE", f"posts/{imagepost}?actor_id={ra}")
    call("DELETE", f"images/{iid}?actor_id={ra}")
    assert store.objects == {}
    # Revoking a membership takes effect immediately, not after session refresh.
    seed("DELETE FROM memberships WHERE user_id=$1", [admin["id"]])
    user = admin
    call("GET", f"conversations/{conv}/messages?actor_id={oa}", status=403)
    # Hiding a listing removes it from discovery and attached post cards.
    seed("UPDATE marketplace_books SET marketplace_status='hidden' WHERE id=$1", [bid])
    assert call("GET", "books")["items"] == []
    assert call("GET", "feed")["items"][0]["books"] == []
    call("GET", "books/fixture-book", status=404)
    # Additive native media migration runs against this disposable database only.
    rpc(
        (root.parent / "migrations/010_marketplace_native_media.sql").read_text(
            encoding="utf-8"
        ),
        execute=True,
    )
    from app.marketplace import native_storage as ns, native_worker as nw
    from botocore.response import StreamingBody

    user = reader
    ns.config = lambda: (
        "fixture-social",
        "https://cdn.example.invalid",
        "key",
        "unused",
    )
    nw.config = ns.config
    ns.check_delivery = lambda: None
    ns.delivery = lambda key: "https://cdn.example.invalid/" + key + "?test-signature"

    class NativeStore:
        objects = {}

        def generate_presigned_post(self, **kwargs):
            return {
                "url": "https://upload.example.invalid",
                "fields": {"key": kwargs["Key"]},
            }

        def head_object(self, **kwargs):
            return {
                "ContentLength": len(self.objects[kwargs["Key"]]),
                "VersionId": "version-one",
            }

        def get_object(self, **kwargs):
            assert kwargs["VersionId"] == "version-one"
            data = self.objects[kwargs["Key"]]
            return {
                "ContentLength": len(data),
                "Body": StreamingBody(io.BytesIO(data), len(data)),
            }

        def upload_file(self, filename, bucket, key, ExtraArgs):
            self.objects[key] = Path(filename).read_bytes()

    native_store = NativeStore()
    ns.s3 = lambda: native_store
    nw.s3 = ns.s3
    image = io.BytesIO()
    Image.new("RGB", (120, 80), "blue").save(image, "PNG")
    pixels = image.getvalue()
    upload_body = {
        "actor_id": ra,
        "filename": "sample.png",
        "content_type": "image/png",
        "file_size": len(pixels),
    }
    upload = call("POST", "media/uploads", upload_body)
    mid = upload["id"]
    native_store.objects[upload["upload"]["fields"]["key"]] = pixels
    call("POST", "posts", {"actor_id": ra, "body": "", "media_ids": [mid]}, 422)
    call("POST", f"native-media/{mid}/complete?actor_id={ra}")
    call("POST", f"native-media/{mid}/complete?actor_id={ra}")  # Idempotent completion.
    assert nw.tick()
    ready_asset = call("GET", f"native-media/{mid}?actor_id={ra}")
    assert ready_asset["status"] == "ready" and len(ready_asset["variants"]) == 3
    assert "original_key" not in ready_asset
    # Clear fixture rate-limit timestamps so this test does not depend on earlier cases.
    seed(
        "UPDATE marketplace_posts SET created_at=now()-interval '2 minutes' WHERE created_by_user_id=$1",
        [reader["id"]],
    )
    native_post = call(
        "POST",
        "posts",
        {"actor_id": ra, "body": "", "media_ids": [mid], "visibility": "connections"},
    )["id"]
    call("DELETE", f"native-media/{mid}?actor_id={ra}", status=409)
    second_post = call(
        "POST",
        "posts",
        {
            "actor_id": ra,
            "body": "Reuse",
            "media_ids": [mid],
            "visibility": "connections",
        },
    )["id"]
    user = outsider
    call("GET", f"native-media/{mid}?actor_id={other_actor}", status=404)
    call(
        "POST",
        "posts",
        {"actor_id": other_actor, "body": "Stolen", "media_ids": [mid]},
        422,
    )
    assert all(
        p["id"] != native_post
        for p in call("GET", f"feed?actor_id={other_actor}")["items"]
    )
    user = reader
    assert any(
        p["id"] == native_post and len(p["media"]) == 1
        for p in call("GET", f"feed?actor_id={ra}")["items"]
    )
    call("DELETE", f"posts/{native_post}?actor_id={ra}")
    call("DELETE", f"native-media/{mid}?actor_id={ra}", status=409)
    call("DELETE", f"posts/{second_post}?actor_id={ra}")
    call("DELETE", f"native-media/{mid}?actor_id={ra}")
    call("GET", f"native-media/{mid}?actor_id={ra}", status=404)
    # Multi-image posts use the same native pipeline and preserve order.
    seed(
        "UPDATE marketplace_posts SET created_at=now()-interval '2 minutes' WHERE created_by_user_id=$1",
        [reader["id"]],
    )
    pair = []
    for _ in range(2):
        item = call("POST", "media/uploads", upload_body)
        native_store.objects[item["upload"]["fields"]["key"]] = pixels
        call("POST", f"native-media/{item['id']}/complete?actor_id={ra}")
        assert nw.tick()
        pair.append(item["id"])
    multi = call("POST", "posts", {"actor_id": ra, "media_ids": pair})
    assert [x["id"] for x in multi["media"]] == pair
    user = outsider
    call("POST", "media/uploads", upload_body, 403)
    user = reader
    # A malformed original reaches a safe failed state after bounded retries.
    bad = call("POST", "media/uploads", dict(upload_body, file_size=8))
    native_store.objects[bad["upload"]["fields"]["key"]] = b"notimage"
    call("POST", f"native-media/{bad['id']}/complete?actor_id={ra}")
    for _ in range(3):
        assert nw.tick()
        seed(
            "UPDATE marketplace_media SET next_attempt_at=now()-interval '1 second' WHERE id=$1",
            [bad["id"]],
        )
    assert call("GET", f"native-media/{bad['id']}?actor_id={ra}")["status"] == "failed"
    call("POST", "posts", {"actor_id": ra, "media_ids": [bad["id"]]}, 422)
    call("POST", f"native-media/{bad['id']}/retry?actor_id={ra}")
    assert (
        call("GET", f"native-media/{bad['id']}?actor_id={ra}")["status"] == "uploaded"
    )
    call("DELETE", f"native-media/{bad['id']}?actor_id={ra}")
    # Exercise video upload/association with mocked encoding (real FFmpeg is covered separately).
    original_video_processor = nw.video_variants
    nw.video_variants = lambda source, folder: (
        {"width": 320, "height": 240, "duration": 2, "mime_type": "video/mp4"},
        [{"name": "720p", "path": source, "content_type": "video/mp4"}],
    )
    video = call(
        "POST",
        "media/uploads",
        dict(upload_body, filename="clip.mp4", content_type="video/mp4", file_size=8),
    )
    native_store.objects[video["upload"]["fields"]["key"]] = b"fixture!"
    call("POST", f"native-media/{video['id']}/complete?actor_id={ra}")
    assert nw.tick()
    nw.video_variants = original_video_processor
    clip = call("POST", "posts", {"actor_id": ra, "media_ids": [video["id"]]})
    assert clip["media"][0]["media_type"] == "video"
    call("POST", "posts", {"actor_id": ra, "media_ids": [video["id"], pair[0]]}, 422)
    # Feed query count must be independent of page size, including linked books.
    seed("UPDATE marketplace_books SET marketplace_status='public' WHERE id=$1", [bid])
    ids = []
    for i in range(25):
        fid = str(uuid4())
        ids.append(fid)
        seed(
            "INSERT INTO marketplace_posts(id,author_actor_id,created_by_user_id,body,status,published_at) VALUES($1,$2,$3,'Batch','published',now())",
            [fid, ra, reader["id"]],
        )
        seed(
            "INSERT INTO marketplace_post_books(post_id,marketplace_book_id) VALUES($1,$2)",
            [fid, bid],
        )
    before = query_count
    batch = call("GET", f"feed?actor_id={ra}&author_id={ra}")
    assert len(batch["items"]) == 20 and batch["has_more"]
    assert query_count - before <= 9, query_count - before
    assert all(p["books"] for p in batch["items"])
    later = call("GET", f"feed?actor_id={ra}&author_id={ra}&offset=20")
    assert not (
        set(p["id"] for p in batch["items"]) & set(p["id"] for p in later["items"])
    )
    print(
        "Native media: upload completion, real image processing, ownership, reuse, protected feed, deletion and bounded query count passed."
    )

    # Anonymous requests may read only the public projection.
    claims = None
    call("GET", "feed?actor_id=" + ra, status=401)
    assert call("GET", "me")["authenticated"] is False
    print(
        f"PASS: {checks} real route/SQL requests. Tenant impersonation, message privacy, revocation, visibility, blocking, library, feed, connections and unread state verified. No live writes."
    )
finally:
    client.close()
    proc.stdin.close()
    proc.wait(timeout=15)
