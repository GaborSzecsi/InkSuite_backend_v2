"""Feed response assembly / future cache boundary. Authorization happens before hydration."""

from collections import defaultdict
from .core import image_ref, safe_url
from . import feed_repository as repo, native_repository, native_storage


def hydrate(cur, posts, viewer):
    if not posts:
        return []
    ids = [p["id"] for p in posts]
    metadata = {str(x["id"]): x for x in repo.metadata(cur, ids, viewer)}
    books = defaultdict(list)
    from .catalog import _legacy_cover_url

    for b in repo.books(cur, ids):
        post_id = b.pop("post_id")
        b.pop("position")
        for e in b["editions"]:
            e["cover"] = safe_url(e["cover"])
        b["cover"] = next((e["cover"] for e in b["editions"] if e["cover"]), "")
        if not b["cover"]:
            b["cover"] = _legacy_cover_url(
                b["tenant_slug"], str(b["upload_uid"] or b["work_id"])
            )
        b["publication_date"] = (
            b["editions"][0]["publication_date"] if b["editions"] else None
        )
        for name in ("tenant_slug", "upload_uid", "work_id"):
            b.pop(name)
        books[str(post_id)].append(b)
    media = defaultdict(list)
    for item in native_repository.attachments(cur, ids):
        media[str(item["post_id"])].append(native_storage.describe(item))
    output = []
    for p in posts:
        m = metadata[str(p["id"])]
        own = str(viewer) == str(p["author_actor_id"])
        public = not m["user_id"] or m["profile_visibility"] == "public" or own
        author = {
            "id": m["actor_id"],
            "kind": "person" if m["user_id"] else "organization",
            "name": m["display_name"] or m["name"],
            "href": (
                (
                    "/marketplace/users/" + m["username"]
                    if m["user_id"]
                    else "/marketplace/publishers/" + m["slug"]
                )
                if public
                else None
            ),
            "image": (
                image_ref(m["avatar_asset_ref"] or m["logo_asset_ref"])
                if public
                else ""
            ),
            "bio": (m["bio"] or m["description"] or "") if public else "",
        }
        output.append(
            {
                "id": p["id"],
                "body": p["body"],
                "created_at": p["published_at"],
                "author": author,
                "image": image_ref(p["media_asset_ref"]),
                "books": books[str(p["id"])],
                "media": media[str(p["id"])],
                "likes": m["likes"],
                "comments": m["comments"],
                "liked": m["liked"],
                "can_delete": own,
            }
        )
    return output
