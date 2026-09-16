from datetime import date
from typing import Literal
from uuid import UUID
from pydantic import BaseModel, Field, model_validator, field_validator
from .domain import association

class Campaign(BaseModel):
    name: str = Field(min_length=1, max_length=250)
    description: str = Field(default='', max_length=20000)
    association_type: Literal['single_work','multi_work','publisher'] = 'publisher'
    work_ids: list[UUID] = Field(default_factory=list, max_length=100)
    start_date: date | None = None
    end_date: date | None = None
    status: Literal['draft','active','completed','archived'] = 'draft'

    @model_validator(mode='after')
    def valid(self):
        self.name = self.name.strip()
        if not self.name: raise ValueError('Campaign name is required.')
        association(self.association_type, self.work_ids)
        if self.start_date and self.end_date and self.end_date < self.start_date:
            raise ValueError('End date must be on or after start date.')
        return self

class Target(BaseModel):
    social_account_id: UUID
    provider_payload: dict = Field(default_factory=dict)

    @field_validator('provider_payload')
    @classmethod
    def settings_types(cls,value):
        for key in ('text','title','link','board_id','privacy_level'):
            if key in value and not isinstance(value[key],str): raise ValueError(f'{key} must be text.')
        for key in ('consent','allow_comment','allow_duet','allow_stitch','brand_content_toggle','brand_organic_toggle','is_aigc'):
            if key in value and not isinstance(value[key],bool): raise ValueError(f'{key} must be a boolean.')
        if any(key in value for key in ('access_token','refresh_token','client_secret')): raise ValueError('Credentials must not be supplied in post settings.')
        if 'asset_ids' in value and (not isinstance(value['asset_ids'],list) or any(not isinstance(x,str) for x in value['asset_ids'])): raise ValueError('Invalid media override.')
        return value

class Post(BaseModel):
    campaign_id: UUID | None = None
    title: str = Field(default='', max_length=250)
    content_text: str = Field(default='', max_length=63000)
    work_ids: list[UUID] = Field(default_factory=list, max_length=100)
    asset_ids: list[UUID] = Field(default_factory=list, max_length=35)
    targets: list[Target] = Field(default_factory=list, max_length=30)
    timezone: str = 'UTC'

class Schedule(BaseModel):
    local_datetime: str
    timezone: str
    fold: Literal[0,1] | None = None

class AssetReference(BaseModel):
    work_id: UUID
    s3_key: str

class AssetMetadata(BaseModel):
    display_name: str = Field(max_length=250)
    alt_text: str = Field(default='', max_length=2000)

class Derivative(BaseModel):
    campaign_id: UUID | None = None
    width: int = Field(default=1080,ge=320,le=4096)
    height: int = Field(default=1080,ge=320,le=4096)
    mode: Literal['fit','fill','original'] = 'fit'
    background: Literal['blur','solid'] = 'blur'

class Template(BaseModel):
    timezone: str = 'America/Los_Angeles'

class OAuthCallback(BaseModel):
    state: str = Field(min_length=1,max_length=500)
    code: str = Field(min_length=1,max_length=4096)

class OAuthSelection(BaseModel):
    selection_token: str = Field(min_length=1,max_length=2000000)
    account_ids: list[str] = Field(min_length=1,max_length=500)
