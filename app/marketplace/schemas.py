from typing import Literal
from uuid import UUID
from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class Profile(Model):
    username: str = Field(pattern=r"^[a-z0-9][a-z0-9_]{2,29}$")
    display_name: str = Field(min_length=1, max_length=100)
    bio: str = Field(default="", max_length=2000)
    location_text: str = Field(default="", max_length=200)
    profile_visibility: Literal["public", "private"] = "public"
    messaging_preference: Literal["anyone", "connections_only", "nobody"] = "anyone"


class Organization(Model):
    name: str = Field(min_length=1, max_length=200)
    slug: str = Field(pattern=r"^[a-z0-9]+(-[a-z0-9]+)*$", max_length=100)
    description: str = Field(default="", max_length=5000)
    website: str = Field(default="", max_length=2048)
    location_text: str = Field(default="", max_length=200)
    status: Literal["draft", "active", "hidden"] = "draft"


class Listing(Model):
    work_id: UUID
    slug: str = Field(pattern=r"^[a-z0-9]+(-[a-z0-9]+)*$", max_length=180)
    edition_ids: list[UUID] = Field(default_factory=list, max_length=50)
    marketplace_status: Literal["draft", "public", "hidden"] = "draft"
    discoverable: bool = False
    featured: bool = False


class Post(Model):
    actor_id: UUID
    body: str = Field(default="", max_length=10000)
    media_ids: list[UUID] = Field(default_factory=list, max_length=10)
    book_ids: list[UUID] = Field(default_factory=list, max_length=5)
    media_asset_id: UUID | None = None
    media_key: str | None = Field(default=None, max_length=200)
    visibility: Literal["public", "connections"] = "public"

    @model_validator(mode="after")
    def content(self):
        if not self.body and not self.media_ids:
            raise ValueError("Add text or native media to your post.")
        if len(set(self.media_ids)) != len(self.media_ids):
            raise ValueError("Select each media asset only once.")
        if self.media_ids and (self.media_asset_id or self.media_key):
            raise ValueError("Choose native media or a legacy image, not both.")
        return self


class Comment(Model):
    parent_comment_id: UUID | None = None
    actor_id: UUID
    body: str = Field(min_length=1, max_length=4000)


class Relationship(Model):
    actor_id: UUID
    target_id: UUID


class ConnectionAction(Model):
    actor_id: UUID
    action: Literal["accept", "decline", "cancel", "remove"]


class LibraryItem(Model):
    status: Literal["saved", "want_to_read", "reading", "read"] = "saved"


class Message(Model):
    actor_id: UUID
    body: str = Field(min_length=1, max_length=10000)


class Report(Model):
    actor_id: UUID
    target_type: Literal["actor", "post", "comment", "message"]
    target_id: UUID
    reason: str = Field(min_length=1, max_length=2000)
