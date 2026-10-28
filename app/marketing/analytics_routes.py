from datetime import date, datetime
from typing import Literal
from uuid import UUID
from fastapi import APIRouter, Depends, Response
from pydantic import BaseModel, Field
from psycopg.rows import dict_row
from app.core.db import db_conn
from . import analytics, service as s

def no_store(response:Response):
    response.headers['Cache-Control']='no-store'


router = APIRouter(prefix='/analytics',tags=['Marketing analytics'],dependencies=[Depends(no_store)])


Platform = Literal['facebook','instagram','pinterest','tiktok']


class MetricValue(BaseModel):
    value: float|None
    status: Literal['available','partial','unsupported','permission_missing','authorization_expired','temporary_failure','not_returned','not_synced']
    basis: str
    coverage: float = Field(ge=0,le=1)
    change_percent: float|None
    denominator: str|None = None


class AccountState(BaseModel):
    id: UUID
    provider: Platform
    display_name: str
    status: str
    sync_status: str|None
    last_attempt_at: datetime|None
    last_success_at: datetime|None
    next_run_at: datetime|None
    error_category: str|None
    backfill_before: date|None
    analytics_scopes: list[str]
    missing_scopes: list[str]
    scope_information_known: bool


class CampaignFilter(BaseModel):
    id: UUID
    name: str
    start_date: date|None
    end_date: date|None


class MetricDefinition(BaseModel):
    label: str
    unit: str
    aggregation: str
    description: str


class AnalyticsFilters(BaseModel):
    accounts: list[AccountState]
    campaigns: list[CampaignFilter]
    platforms: list[Platform]
    definitions: dict[str,MetricDefinition]


class PlatformSummary(BaseModel):
    provider: Platform
    metrics: dict[str,MetricValue]
    post_count: int


class PostPerformance(BaseModel):
    id: UUID
    account_id: UUID
    external_id: str
    provider: Platform
    display_name: str
    media_type: str
    title: str
    permalink: str|None
    thumbnail_url: str|None
    published_at: datetime|None
    campaign_id: UUID|None
    campaign_name: str|None
    post_id: UUID|None
    source: Literal['inksuite','external']
    metrics: dict[str,MetricValue]


class SeriesPoint(BaseModel):
    provider: Platform
    metric: str
    scope: Literal['day','snapshot','lifetime']
    date: date
    value: float


class CampaignPerformance(CampaignFilter):
    post_count: int
    platforms: list[PlatformSummary]


class AnalyticsReport(BaseModel):
    start: date
    end: date
    accounts: list[AccountState]
    overview: list[PlatformSummary]
    series: list[SeriesPoint]
    posts: list[PostPerformance]
    campaigns: list[CampaignPerformance]
    definitions: dict[str,MetricDefinition]
    financials_available: bool
    notes: list[str]


@router.get('/filters',response_model=AnalyticsFilters)
def filters(ctx=Depends(s.access)):
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        return analytics.filters(cur,ctx['tenant']['id'])


@router.get('/overview',response_model=AnalyticsReport)
@router.get('/accounts',response_model=AnalyticsReport)
@router.get('/timeseries',response_model=AnalyticsReport)
@router.get('/posts',response_model=AnalyticsReport)
@router.get('/campaigns',response_model=AnalyticsReport)
def report(start:date|None=None,end:date|None=None,provider:str|None=None,
           account_id:UUID|None=None,campaign_id:UUID|None=None,ctx=Depends(s.access)):
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        return analytics.report(cur,ctx['tenant']['id'],start,end,provider,account_id,campaign_id)


@router.get('/posts/{media_id}')
def detail(media_id:UUID,ctx=Depends(s.access)):
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        return analytics.post_detail(cur,ctx['tenant']['id'],media_id)


@router.get('/sync')
def status(ctx=Depends(s.access)):
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        return {'accounts':analytics.filters(cur,ctx['tenant']['id'])['accounts']}


@router.post('/sync/{account_id}',status_code=202)
def refresh(account_id:UUID,ctx=Depends(s.admin)):
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
        return analytics.refresh(cur,ctx,account_id)
