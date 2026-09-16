"""Pure campaign, scheduling and storage invariants."""
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from pathlib import PurePosixPath
import re

PROVIDERS = ('facebook', 'instagram', 'pinterest', 'tiktok')

def schedule_instant(wall: str, zone: str, fold: int | None = None) -> datetime:
    value = datetime.fromisoformat(wall)
    tz = ZoneInfo(zone)
    if value.tzinfo is not None:
        return value.astimezone(timezone.utc)
    choices = {value.replace(tzinfo=tz, fold=f).astimezone(timezone.utc) for f in (0, 1)
               if value.replace(tzinfo=tz, fold=f).astimezone(timezone.utc).astimezone(tz).replace(tzinfo=None) == value}
    if not choices:
        raise ValueError('That local time does not exist because of daylight saving time.')
    if len(choices) > 1 and fold is None:
        raise ValueError('That local time occurs twice. Choose the first or second occurrence.')
    return value.replace(tzinfo=tz, fold=fold or 0).astimezone(timezone.utc)

def association(kind: str, works: list) -> None:
    count = len(set(map(str, works)))
    if kind not in ('single_work', 'multi_work', 'publisher'):
        raise ValueError('Invalid campaign association.')
    if (kind == 'single_work' and count != 1) or (kind == 'multi_work' and count < 2) or (kind == 'publisher' and count):
        raise ValueError('Select one title, at least two titles, or no titles for the campaign type.')

def aggregate(statuses: list[str]) -> str:
    states = set(statuses)
    if not states: return 'draft'
    if states == {'published'}: return 'published'
    if 'published' in states: return 'partially_published'
    if states == {'cancelled'}: return 'cancelled'
    if states <= {'failed', 'cancelled'}: return 'failed'
    return 'scheduled'

def tenant_root(slug: str) -> str:
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]*', slug):
        raise ValueError('Invalid tenant storage slug.')
    return f'tenants/{slug}/'

def public_key(slug: str, key: str, *, title_uid: str | None = None) -> str:
    if '\\' in key or any(p in ('.', '..', '') for p in key.split('/')):
        raise ValueError('Invalid asset key.')
    prefix = tenant_root(slug)
    allowed = prefix + (f'data/uploads/{title_uid}/public/' if title_uid else 'assets/marketing/public/')
    if not key.startswith(allowed) or key.endswith('/'):
        raise ValueError('Asset is outside the selected tenant public folder.')
    return key

def upload_key(slug: str, asset_id: str, filename: str, campaign_id: str | None = None, category: str = 'general') -> str:
    name = re.sub(r'[^A-Za-z0-9._-]', '_', PurePosixPath(filename.replace('\\', '/')).name)
    if not name or name in ('.', '..'): raise ValueError('Invalid filename.')
    if category not in ('logos', 'branding', 'holidays', 'seasonal', 'events', 'backgrounds', 'templates', 'general'):
        raise ValueError('Invalid publisher asset category.')
    folder = f'campaigns/{campaign_id}/original' if campaign_id else f'library/{category}'
    return public_key(slug, f'{tenant_root(slug)}assets/marketing/public/{folder}/{asset_id}_{name}')
