# app/onix/models.py
# Pydantic request/response models for ONIX Feed API.
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ProductListQuery(BaseModel):
    q: Optional[str] = None
    isbn: Optional[str] = None
    title: Optional[str] = None
    contributor: Optional[str] = None
    format: Optional[str] = None
    status: Optional[str] = None
    validation_status: Optional[str] = None
    page: int = Field(1, ge=1)
    page_size: int = Field(50, ge=1, le=200)
    sort: str = "title"


class ExportRequest(BaseModel):
    edition_ids: Optional[List[str]] = None
    isbns: Optional[List[str]] = None
    export_mode: str = Field(..., pattern="^(preview|download|transfer)$")
    recipient_id: Optional[str] = None
    file_mode: str = Field("combined", pattern="^(combined|separate)$")
    filters_json: Optional[Dict[str, Any]] = None
    include_raw_compare: bool = False
    save_to_s3: bool = True


class RecipientCreate(BaseModel):
    name: str
    protocol: str = "sftp"
    host: str = ""
    port: int = 22
    username: str = ""
    auth_type: str = Field("password", pattern="^(password|ssh_key)$")
    remote_path: str = ""
    filename_pattern: str = ""
    is_active: bool = True
    secret_arn: Optional[str] = None
    password: Optional[str] = None
    private_key: Optional[str] = None
    passphrase: Optional[str] = None


class RecipientUpdate(BaseModel):
    name: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    username: Optional[str] = None
    auth_type: Optional[str] = Field(None, pattern="^(password|ssh_key)$")
    remote_path: Optional[str] = None
    filename_pattern: Optional[str] = None
    is_active: Optional[bool] = None
    secret_arn: Optional[str] = None
    password: Optional[str] = None
    private_key: Optional[str] = None
    passphrase: Optional[str] = None
