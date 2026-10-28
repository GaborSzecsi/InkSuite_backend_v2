"""Read-only official API collectors. No comment/message management or raw payload storage.

Daily flows, follower snapshots and lifetime post counters intentionally stay separate.
Provider errors are categorized without logging URLs, tokens or response bodies.
"""
import math
import re
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse, parse_qs

import requests
from .providers import graph, instagram_graph, pinterest_api_base

PROVIDERS = ('facebook', 'instagram', 'pinterest', 'tiktok')
VERSION = '2026-10-27.1'
METRICS = {
    'reach': ('Reach / unique viewers', 'audience', 'non_additive', 'Instagram reach or Facebook total unique media viewers within one reporting period. Sums across dates/posts are not deduplicated people.'),
    'views': ('Views', 'views', 'sum', 'Provider-defined views; not interchangeable with impressions or unique reach.'),
    'impressions': ('Impressions', 'impressions', 'sum', 'Times content appeared; repeated appearances can be counted.'),
    'engagements': ('Engagements', 'actions', 'sum', 'Provider total interactions; TikTok uses likes + comments + shares. Definitions differ by platform.'),
    'likes': ('Likes / reactions', 'actions', 'sum', 'Instagram/TikTok likes or Facebook reactions.'),
    'comments': ('Comments', 'actions', 'sum', 'Comment count only; comment text and identities are not collected.'),
    'shares': ('Shares', 'actions', 'sum', 'Provider-reported shares.'),
    'saves': ('Saves', 'actions', 'sum', 'Instagram saves or Pinterest saves.'),
    'followers': ('Followers', 'people', 'last', 'Account follower count at collection time, not followers acquired in the selected period.'),
    'pin_clicks': ('Pin clicks', 'actions', 'sum', 'Clicks opening a Pin; distinct from outbound clicks.'),
    'outbound_clicks': ('Outbound clicks', 'actions', 'sum', 'Clicks from Pinterest to an external destination.'),
}
REQUIRED = {
    'facebook': ['read_insights', 'pages_read_engagement', 'pages_read_user_content'],
    'instagram': ['instagram_business_basic', 'instagram_business_manage_insights'],
    'pinterest': ['user_accounts:read', 'pins:read', 'boards:read'],
    'tiktok': ['user.info.stats', 'video.list'],
}
SUPPORTED = {
    'facebook': {'reach','views','likes','comments','shares','engagements','followers'},
    'instagram': {'reach','views','likes','comments','shares','saves','engagements','followers'},
    'pinterest': {'impressions','engagements','saves','pin_clicks','outbound_clicks','followers'},
    'tiktok': {'views','likes','comments','shares','engagements','followers'},
}


def scopes(account):
    raw = account.get('scopes') or []
    return set(re.split(r'[,\s]+', ' '.join(raw) if isinstance(raw, list) else str(raw))) - {''}


class AnalyticsError(Exception):
    def __init__(self, category, retryable=False, retry_after=0):
        self.category, self.retryable = category, retryable
        self.retry_after=retry_after
        super().__init__(category)


def retry_after(response):
    value=response.headers.get('Retry-After')
    if not isinstance(value,str):return 0
    try:
        seconds=float(value)
    except ValueError:
        try:seconds=(parsedate_to_datetime(value)-datetime.now(timezone.utc)).total_seconds()
        except (ValueError,TypeError):return 0
    return min(86400,max(0,seconds)) if math.isfinite(seconds) else 0


def numeric(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        return None
    return value


def instant(value):
    try:
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value, timezone.utc)
        parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError, OverflowError):
        return None


def safe_url(value):
    if not isinstance(value, str) or len(value) > 8192:
        return None
    parsed = urlparse(value)
    if {'access_token','client_secret','refresh_token','code'} & {key.lower() for key in parse_qs(parsed.query)}:
        return None
    return value if parsed.scheme == 'https' and parsed.hostname and not parsed.username and not parsed.password else None


@dataclass
class Collection:
    observations: list = field(default_factory=list)
    media: list = field(default_factory=list)
    limitations: set = field(default_factory=set)
    calls: int = 0
    api_version: str = ''
    cursors: dict = field(default_factory=dict)
    retry_after: float = 0

    def add(self, metric, value, start, end=None, scope='day', subject='', availability=None):
        value = numeric(value)
        self.observations.append(dict(metric=metric, value=value,
            availability=availability or ('available' if value is not None else 'not_returned'),
            period_start=start, period_end=end or start, scope=scope, subject=subject))


class Collector:
    def __init__(self, account, token, start, end, max_calls=180, known_posts=None, checkpoint=None, guard=None):
        self.account, self.token, self.start, self.end = account, token, start, end
        self.result = Collection()
        self.max_calls = max_calls
        self.deadline = datetime.now(timezone.utc) + timedelta(minutes=8)
        self.known_posts = known_posts or []
        self.guard = guard or nullcontext
        checkpoint = checkpoint or {}
        self.positions = checkpoint.get('positions',{}) if checkpoint.get('start')==str(start) and checkpoint.get('end')==str(end) else {}
        self.result.cursors = {'start':str(start),'end':str(end),'positions':dict(self.positions)}
        self.insights_denied = False
        self.last_error = None

    def call(self, method, url, **kwargs):
        with self.guard():
            return self._call(method,url,**kwargs)

    def _call(self, method, url, **kwargs):
        if self.result.calls >= self.max_calls or datetime.now(timezone.utc) > self.deadline:
            raise AnalyticsError('collection_limit', True)
        self.result.calls += 1
        try:
            response = requests.request(method, url, headers={'Authorization': 'Bearer ' + self.token},
                                        timeout=(5, 20), allow_redirects=False, **kwargs)
            data = response.json()
        except requests.RequestException:
            raise AnalyticsError('temporary_failure', True) from None
        except ValueError:
            raise AnalyticsError('invalid_response', True) from None
        error = data.get('error') if isinstance(data, dict) else None
        code = str(error.get('code')) if isinstance(error, dict) else ''
        if response.status_code == 429 or code in ('4', '17', '32', '613', 'rate_limit_exceeded'):
            raise AnalyticsError('rate_limited', True,retry_after(response))
        if response.status_code >= 500:
            raise AnalyticsError('temporary_failure', True,retry_after(response))
        if response.status_code == 401 or code in ('190', 'access_token_invalid', 'access_token_expired'):
            raise AnalyticsError('authorization_expired')
        if response.status_code == 403 or code in ('10', '200', 'scope_not_authorized'):
            raise AnalyticsError('permission_missing')
        if not 200 <= response.status_code < 300 or (error and code not in ('None', 'ok', '0')):
            raise AnalyticsError('unsupported')
        if not isinstance(data, dict):
            raise AnalyticsError('invalid_response')
        return data

    def optional(self, function):
        self.last_error = None
        try:
            return function()
        except AnalyticsError as error:
            self.last_error = error.category
            self.result.limitations.add(error.category)
            self.result.retry_after=max(self.result.retry_after,error.retry_after)
            if error.retryable or error.category in ('authorization_expired','disconnected','authorization_changed'):
                raise
            return None

    def pages(self, url, params, kind='meta', body=None):
        """Follow opaque cursors only on the original endpoint; never follow provider next URLs."""
        key=url.rstrip('/').rsplit('/',1)[-1]
        initial=self.positions.get(key)
        if initial is not None:
            if kind=='tiktok':body={**body,'cursor':initial}
            else:params={**params,('bookmark' if kind=='pinterest' else 'after'):initial}
        seen = {str(initial)} if initial is not None else set()
        for _ in range(30):
            data = self.call('POST' if kind == 'tiktok' else 'GET', url, params=params,
                             **({'json': body} if kind == 'tiktok' else {}))
            if kind == 'tiktok':
                payload = data.get('data', {})
                yield from payload.get('videos', [])
                more, cursor = payload.get('has_more'), payload.get('cursor')
                if more:
                    body = {**body, 'cursor': cursor}
            elif kind == 'pinterest':
                yield from data.get('items', [])
                cursor = data.get('bookmark')
                more = bool(cursor)
                params = {**params, 'bookmark': cursor}
            else:
                yield from data.get('data', [])
                paging = data.get('paging', {})
                more, cursor = bool(paging.get('next')), paging.get('cursors', {}).get('after')
                params = {**params, 'after': cursor}
            if not more:
                self.result.cursors['positions'].pop(key,None)
                return
            if cursor is None or str(cursor) in seen:
                raise AnalyticsError('invalid_pagination')
            seen.add(str(cursor))
            self.result.cursors['positions'][key]=cursor
        raise AnalyticsError('collection_limit', True)

    def media(self, item, published, kind, title='', url=None, thumb=None):
        identifier = str(item.get('id', ''))
        if not identifier or len(identifier) > 255 or not re.fullmatch(r'[\w-]+', identifier):
            raise AnalyticsError('invalid_response')
        self.result.media.append(dict(external_id=identifier, media_type=kind or 'unknown',
            title=str(title or '')[:500], published_at=published, permalink=safe_url(url), thumbnail_url=safe_url(thumb)))
        return identifier

    def meta_insights(self, root, mapping, subject='', extra=None):
        for remote, metric in mapping.items():
            if self.insights_denied:
                self.result.add(metric,None,self.end,scope='lifetime' if subject else 'day',subject=subject,availability='permission_missing')
                continue
            params = {'metric': remote, 'period': 'lifetime' if subject else 'day', **(extra or {})}
            if not subject:
                params.update(since=self.start.isoformat(), until=(self.end + timedelta(days=1)).isoformat())
            response = self.optional(lambda: self.call('GET', root + '/insights', params=params))
            if response is None:
                if self.last_error=='permission_missing':self.insights_denied=True
                self.result.add(metric, None, self.end, scope='lifetime' if subject else 'day', subject=subject,
                                availability='permission_missing' if self.last_error=='permission_missing' else 'unsupported')
                continue
            found = False
            for series in response.get('data', []):
                if series.get('name') != remote:
                    continue
                if 'total_value' in series:
                    # Account totals over a window must not be invented as daily observations.
                    if subject:
                        self.result.add(metric, series['total_value'].get('value'), self.end, scope='lifetime', subject=subject)
                        found = True
                    continue
                for point in series.get('values', []):
                    timestamp = instant(point.get('end_time'))
                    day = timestamp.date() - timedelta(days=1) if timestamp else self.end
                    if subject or self.start <= day <= self.end:
                        self.result.add(metric, point.get('value'), self.end if subject else day,
                                        scope='lifetime' if subject else 'day', subject=subject)
                        found = True
            if not found:
                self.result.add(metric, None, self.end, scope='lifetime' if subject else 'day', subject=subject)

    def instagram(self):
        root = instagram_graph() + '/' + self.account['provider_account_id']
        self.result.api_version = instagram_graph().rsplit('/', 1)[-1]
        if self.result.api_version!='v25.0':
            raise AnalyticsError('unsupported_api_version')
        profile = self.optional(lambda: self.call('GET', root, params={'fields': 'followers_count'}))
        if profile:
            self.result.add('followers', profile.get('followers_count'), datetime.now(timezone.utc).date(), scope='snapshot')
        # Reach has time-series semantics. Other account insight metrics now use
        # total_value; collect daily windows independently to preserve their scope.
        self.meta_insights(root, {'reach': 'reach'})
        for day_offset in range((self.end - self.start).days + 1):
            day = self.start + timedelta(days=day_offset)
            for metric in ('views', 'total_interactions','likes','comments','shares'):
                data = None if self.insights_denied else self.optional(lambda: self.call('GET', root + '/insights', params={
                    'metric': metric, 'period': 'day', 'metric_type': 'total_value',
                    'since': day.isoformat(), 'until': (day + timedelta(days=1)).isoformat()}))
                if self.last_error=='permission_missing':self.insights_denied=True
                value = next((x.get('total_value', {}).get('value') for x in (data or {}).get('data', []) if x.get('name') == metric), None)
                self.result.add('engagements' if metric == 'total_interactions' else metric, value, day,
                    availability='permission_missing' if self.insights_denied else None)
        fields = 'id,caption,media_type,media_product_type,timestamp,permalink,thumbnail_url,like_count,comments_count'
        for post in self.known_posts:
            item = self.optional(lambda: self.call('GET',instagram_graph()+'/'+post['external_id'],params={'fields':fields}))
            if item:
                self.instagram_media(item)
        seen = {m['external_id'] for m in self.result.media}
        for item in self.pages(root + '/media', {'fields': fields, 'limit': 5}):
            published = instant(item.get('timestamp'))
            if published and published.date() < self.start:
                self.result.cursors['positions'].pop('media',None)
                break
            if str(item.get('id')) not in seen:
                self.instagram_media(item)

    def instagram_media(self,item):
        identifier = self.media(item, instant(item.get('timestamp')), item.get('media_product_type') or item.get('media_type'), item.get('caption'), item.get('permalink'), item.get('thumbnail_url'))
        self.result.add('likes', item.get('like_count'), self.end, scope='lifetime', subject=identifier)
        self.result.add('comments', item.get('comments_count'), self.end, scope='lifetime', subject=identifier)
        self.meta_insights(instagram_graph() + '/' + identifier,
            {'reach': 'reach', 'views': 'views', 'shares': 'shares', 'saved': 'saves', 'total_interactions': 'engagements'}, identifier)

    def facebook(self):
        root = graph() + '/' + self.account['provider_account_id']
        self.result.api_version = graph().rsplit('/', 1)[-1]
        if self.result.api_version!='v25.0':
            raise AnalyticsError('unsupported_api_version')
        data = self.optional(lambda: self.call('GET', root, params={'fields': 'followers_count'}))
        if data:
            self.result.add('followers', data.get('followers_count'), datetime.now(timezone.utc).date(), scope='snapshot')
        # Current views metric; retired impressions/engaged_users names are not used.
        self.meta_insights(root, {'page_media_view': 'views','page_total_media_view_unique':'reach'})
        # Summary edges for user-generated reactions/comments require additional
        # access. Never let them block Page discovery or read_insights metrics.
        fields = 'id,created_time,message,permalink_url,full_picture,shares'
        for post in self.known_posts:
            item = self.optional(lambda: self.call('GET',graph()+'/'+post['external_id'],params={'fields':fields}))
            if item:
                self.facebook_media(item)
        seen = {m['external_id'] for m in self.result.media}
        for item in self.pages(root + '/published_posts', {'fields': fields,
            'since': self.start.isoformat(), 'until': (self.end + timedelta(days=1)).isoformat(), 'limit': 5}):
            if str(item.get('id')) not in seen:
                self.facebook_media(item)

    def facebook_media(self,item):
        identifier = self.media(item, instant(item.get('created_time')), 'post', item.get('message'), item.get('permalink_url'), item.get('full_picture'))
        for key, remote in (('likes', 'reactions'), ('comments', 'comments')):
            value=None
            availability='permission_missing'
            if 'pages_read_user_content' in scopes(self.account):
                try:
                    summary=self.call('GET',graph()+'/'+identifier,params={'fields':remote+'.limit(0).summary(true)'})
                    value=summary.get(remote,{}).get('summary',{}).get('total_count')
                    availability=None
                except AnalyticsError as error:
                    if error.retryable or error.category in ('authorization_expired','disconnected','authorization_changed'):
                        raise
                    availability='permission_missing' if error.category=='permission_missing' else 'unsupported'
            self.result.add(key, value, self.end, scope='lifetime', subject=identifier,availability=availability)
        self.result.add('shares', item.get('shares', {}).get('count'), self.end, scope='lifetime', subject=identifier)
        self.meta_insights(graph() + '/' + identifier, {'post_media_view': 'views','post_total_media_view_unique':'reach'}, identifier)

    def pinterest_daily(self, response, subject=''):
        # NO_SPLIT must be used; mixing device/content splits double-counts values.
        data = response.get('all', response.get('ALL', {}))
        mapping = {'IMPRESSION': 'impressions', 'ENGAGEMENT': 'engagements', 'SAVE': 'saves',
                   'PIN_CLICK': 'pin_clicks', 'OUTBOUND_CLICK': 'outbound_clicks'}
        for point in data.get('daily_metrics', []):
            day = date.fromisoformat(point['date'])
            if point.get('data_status') != 'READY' or not self.start <= day <= self.end:
                continue
            for remote, metric in mapping.items():
                if remote in point.get('metrics', {}):
                    self.result.add(metric, point['metrics'][remote], day, subject=subject)

    def pinterest(self):
        root = pinterest_api_base() + '/v5'
        self.result.api_version = 'v5'
        params = {'start_date': self.start.isoformat(), 'end_date': self.end.isoformat(),
                  'metric_types': 'IMPRESSION,ENGAGEMENT,SAVE,PIN_CLICK,OUTBOUND_CLICK', 'split_field': 'NO_SPLIT'}
        data = self.optional(lambda: self.call('GET', root + '/user_account/analytics', params=params))
        if data:
            self.pinterest_daily(data)
        profile = self.optional(lambda: self.call('GET', root + '/user_account'))
        if profile:
            self.result.add('followers', profile.get('follower_count'), datetime.now(timezone.utc).date(), scope='snapshot')
        for post in self.known_posts:
            item = self.optional(lambda: self.call('GET',root+'/pins/'+post['external_id']))
            if item:
                self.pinterest_pin(item,params)
        seen = {m['external_id'] for m in self.result.media}
        for item in self.pages(root + '/pins', {'page_size': 25}, 'pinterest'):
            published = instant(item.get('created_at'))
            if published and published.date() < self.start:
                continue
            if str(item.get('id')) not in seen:
                self.pinterest_pin(item,params)

    def pinterest_pin(self,item,params):
        identifier = self.media(item, instant(item.get('created_at')), (item.get('media') or {}).get('media_type'), item.get('title'), 'https://www.pinterest.com/pin/' + str(item['id']) + '/')
        pin_params = {**params, 'metric_types': 'IMPRESSION,SAVE,PIN_CLICK,OUTBOUND_CLICK'}
        data = self.optional(lambda: self.call('GET', pinterest_api_base()+'/v5/pins/'+identifier+'/analytics', params=pin_params))
        if data:
            self.pinterest_daily(data, identifier)

    def tiktok(self):
        root = 'https://open.tiktokapis.com/v2'
        self.result.api_version = 'v2'
        granted = scopes(self.account)
        if 'user.info.stats' in granted:
            user = self.optional(lambda: self.call('GET', root + '/user/info/', params={'fields': 'follower_count'}))
            if user:
                self.result.add('followers', user.get('data', {}).get('user', {}).get('follower_count'), datetime.now(timezone.utc).date(), scope='snapshot')
        else:
            self.result.limitations.add('permission_missing')
        if 'video.list' not in granted:
            raise AnalyticsError('permission_missing')
        fields = 'id,title,create_time,share_url,cover_image_url,like_count,comment_count,share_count,view_count'
        if self.known_posts:
            data = self.optional(lambda: self.call('POST',root+'/video/query/',params={'fields':fields},json={'filters':{'video_ids':[p['external_id'] for p in self.known_posts[:20]]}}))
            for item in (data or {}).get('data',{}).get('videos',[]):
                self.tiktok_media(item)
        seen = {m['external_id'] for m in self.result.media}
        for item in self.pages(root + '/video/list/', {'fields': fields}, 'tiktok', {'max_count': 20}):
            published = instant(item.get('create_time'))
            if published and published.date() < self.start:
                self.result.cursors['positions'].pop('list',None)
                break
            if str(item.get('id')) not in seen:
                self.tiktok_media(item)

    def tiktok_media(self,item):
        identifier = self.media(item, instant(item.get('create_time')), 'video', item.get('title'), item.get('share_url'), item.get('cover_image_url'))
        for remote, metric in (('like_count', 'likes'), ('comment_count', 'comments'), ('share_count', 'shares'), ('view_count', 'views')):
            self.result.add(metric, item.get(remote), self.end, scope='lifetime', subject=identifier)

    def collect(self):
        if self.account['provider'] not in PROVIDERS:
            raise AnalyticsError('unsupported')
        # Legacy Facebook scopes were not recorded; let the actual API establish access.
        granted = scopes(self.account)
        # Some legacy Meta connections recorded requested rather than granted scopes.
        # The actual API response is authoritative; never falsely mark a metric authorized.
        try:
            getattr(self, self.account['provider'])()
        except AnalyticsError as error:
            if not self.result.observations:
                raise
            self.result.limitations.add(error.category)
            self.result.retry_after=max(self.result.retry_after,error.retry_after)
        return self.result
