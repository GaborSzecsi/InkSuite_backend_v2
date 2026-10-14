"""Resolve catalog artwork without making tenant storage public."""
import os
from urllib.parse import unquote, urlsplit


def artwork_resolver(cur, tenant_id):
    """Lazily resolve the tenant once per catalog response; sign only public images."""
    slug = None
    client = None
    cache = {}
    bucket = os.getenv('UPLOADS_S3_BUCKET') or os.getenv('S3_BUCKET') or 'inksuite-data'
    region = os.getenv('AWS_REGION') or os.getenv('AWS_DEFAULT_REGION') or 'us-east-2'
    hosts = {f'{bucket}.s3.{region}.amazonaws.com', f'{bucket}.s3.amazonaws.com'}

    def resolve(value):
        nonlocal slug, client
        value = str(value or '').strip()
        if not value:
            return ''
        if value in cache:
            return cache[value]
        parsed = urlsplit(value)
        if parsed.scheme:
            if parsed.hostname not in hosts:
                return value if parsed.scheme == 'https' else ''
            if parsed.username or parsed.password or parsed.port not in (None, 443):
                return ''
            key = unquote(parsed.path.lstrip('/'))
        else:
            key = value
        if slug is None:
            cur.execute('SELECT slug FROM tenants WHERE id=%s', (tenant_id,))
            row = cur.fetchone()
            slug = row['slug'] if row else ''
        prefix = f'tenants/{slug}/data/uploads/'
        relative = key[len(prefix):] if slug and key.startswith(prefix) else ''
        parts = relative.split('/')
        if (len(parts) != 3 or parts[1] != 'public'
                or any(p in ('', '.', '..') or '\\' in p for p in parts)
                or not parts[2].lower().endswith(('.jpg', '.jpeg', '.png', '.webp', '.gif'))):
            return ''
        if client is None:
            from routers.uploads import _s3_client
            client = _s3_client()
        cache[value] = client.generate_presigned_url(
            'get_object', Params={'Bucket': bucket, 'Key': key}, ExpiresIn=3600)
        return cache[value]
    return resolve
