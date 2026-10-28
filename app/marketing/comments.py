"""On-demand Meta comments. No comment bodies or participant profiles are persisted."""
import json
import re
from urllib.parse import urlparse
from cryptography.fernet import InvalidToken
from fastapi import HTTPException
from . import service as s
from .oauth import cipher
from .providers import graph, instagram_graph, request, ProviderError
from .publishing import credentials

FIELDS = {
    'facebook': 'id,message,created_time,from,is_hidden,can_comment,can_hide,can_remove',
    'instagram': 'id,text,username,timestamp,hidden,like_count',
}
PERMISSIONS = {
    'facebook': ['pages_read_engagement', 'pages_read_user_content', 'pages_manage_engagement'],
    'instagram': ['instagram_business_basic', 'instagram_business_manage_comments'],
}


def identifier(value):
    value = str(value or '')
    if not re.fullmatch(r'[0-9]+(?:_[0-9]+)?', value):
        raise HTTPException(422, 'Invalid social object identifier.')
    return value


def destination(cur, ctx, target_id):
    tenant = ctx['tenant']['id']
    target = s.owned(cur, 'social_post_targets', target_id, tenant)
    if target['provider'] not in FIELDS:
        raise HTTPException(422, 'Comments are supported for Facebook and Instagram posts.')
    if target['provider_status'] != 'published' or not target.get('provider_post_id'):
        raise HTTPException(409, 'This destination has not been published.')
    account = s.owned(cur, 'social_accounts', target['social_account_id'], tenant, True)
    if account['provider'] != target['provider']:
        raise HTTPException(409, 'The destination account does not match this post.')
    if account['status'] != 'connected':
        raise HTTPException(409, 'Reconnect this account in Social Accounts to load comments.')
    target['provider_post_id'] = identifier(target['provider_post_id'])
    return target, account


def seal(target, tenant, kind, **values):
    # The provider only returns comments from an owned post's edge. Bind every
    # action/cursor to that target; arbitrary comment IDs are never accepted.
    payload = dict(kind=kind, tenant=str(tenant), target=str(target['id']),
                   account=str(target['social_account_id']), post=target['provider_post_id'], **values)
    return cipher().encrypt(json.dumps(payload).encode()).decode()


def unseal(value, target, tenant, kind):
    try:
        data = json.loads(cipher().decrypt(value.encode(), ttl=1800))
        expected = dict(kind=kind, tenant=str(tenant), target=str(target['id']),
                        account=str(target['social_account_id']), post=target['provider_post_id'])
        if any(data.get(key) != item for key, item in expected.items()):
            raise ValueError()
        return data
    except (InvalidToken, ValueError, TypeError, AttributeError, UnicodeError):
        raise HTTPException(409, 'This comment view has expired or changed. Refresh comments and try again.') from None


def call(account, token, method, path, **kwargs):
    root = instagram_graph() if account['provider'] == 'instagram' else graph()
    return request(method, root + '/' + path, token, redact_errors=True, **kwargs)


def provider_error(error, mutation=False):
    if error.uncertain and mutation:
        raise HTTPException(409, 'The result could not be confirmed. Refresh comments before trying again; your action may already have succeeded.') from None
    if error.code in ('10', '200'):
        raise HTTPException(403, 'Comment access is not granted. Enable the platform comment permissions and reconnect this account in Social Accounts.') from None
    if error.code in ('190', 'access_token_invalid', 'invalid_access_token', 'access_token_expired'):
        raise HTTPException(409, 'Authorization expired. Reconnect this account in Social Accounts.') from None
    if error.code == 'configuration':
        raise HTTPException(503, 'Social comments are not configured on the server.') from None
    if error.retryable:
        raise HTTPException(503, 'The platform is busy or temporarily unavailable. Refresh comments later.') from None
    raise HTTPException(422, 'The platform could not complete this action. The post or comment may have changed, or this action is not supported.') from None


def comment_view(item, target, tenant, reply_to=None):
    instagram = target['provider'] == 'instagram'
    flags = dict(reply=True if instagram else item.get('can_comment') is True,
                 hide=True if instagram else item.get('can_hide') is True,
                 delete=True if instagram else item.get('can_remove') is True)
    comment_id = identifier(item.get('id'))
    return dict(id=comment_id,
                handle=seal(target, tenant, 'comment', comment=comment_id, reply_to=reply_to or comment_id, capabilities=flags),
                author=item.get('username') if instagram else (item.get('from') or {}).get('name'),
                text=item.get('text' if instagram else 'message') or '',
                created_at=item.get('timestamp' if instagram else 'created_time'),
                hidden=item.get('hidden' if instagram else 'is_hidden') is True,
                capabilities=flags)


def listing(cur, ctx, target_id, cursor=None, parent=None):
    target, account = destination(cur, ctx, target_id)
    tenant = ctx['tenant']['id']
    parent_id = unseal(parent, target, tenant, 'comment')['comment'] if parent else None
    edge = 'replies' if parent_id and target['provider'] == 'instagram' else 'comments'
    subject = identifier(parent_id or target['provider_post_id'])
    params = dict(fields=FIELDS[target['provider']], limit=25)
    if target['provider'] == 'facebook' and not parent_id:
        params.update(filter='toplevel', order='chronological')
    if cursor:
        position = unseal(cursor, target, tenant, 'cursor')
        if position.get('parent') != parent_id:
            raise HTTPException(409, 'Refresh this comment thread before loading more replies.')
        params['after'] = position['after']
    try:
        token = credentials(cur, account, redact_errors=True)
        result = call(account, token, 'GET', subject + '/' + edge, params=params)
    except ProviderError as error:
        provider_error(error)
    paging = result.get('paging') or {}
    after = (paging.get('cursors') or {}).get('after')
    return dict(comments=[comment_view(item, target, tenant, reply_to=parent_id) for item in result.get('data', [])],
                next_cursor=seal(target, tenant, 'cursor', parent=parent_id, after=after)
                if paging.get('next') and isinstance(after, str) and len(after) < 4096 else None,
                can_manage=ctx['membership_role'] in ('superadmin', 'tenant_admin'))


def https_url(value):
    return value if isinstance(value, str) and urlparse(value).scheme == 'https' else None


def preview(cur, ctx, target_id):
    target, account = destination(cur, ctx, target_id)
    instagram = target['provider'] == 'instagram'
    fields = ('id,caption,media_type,media_url,thumbnail_url,permalink,timestamp,children{media_type,media_url,thumbnail_url}'
              if instagram else 'id,message,created_time,permalink_url,full_picture')
    try:
        token = credentials(cur, account, redact_errors=True)
        item = call(account, token, 'GET', target['provider_post_id'], params={'fields': fields})
    except ProviderError as error:
        provider_error(error)
    media = []
    for asset in ((item.get('children') or {}).get('data') or [item])[:10]:
        url = https_url(asset.get('media_url') if instagram else asset.get('full_picture'))
        if url:
            media.append(dict(url=url, kind='video' if asset.get('media_type') == 'VIDEO' else 'image',
                              thumbnail=https_url(asset.get('thumbnail_url'))))
    return dict(provider=target['provider'], account=account['display_name'],
                text=item.get('caption' if instagram else 'message') or '', media=media,
                published_at=item.get('timestamp' if instagram else 'created_time'),
                permalink=https_url(item.get('permalink' if instagram else 'permalink_url')),
                required_permissions=PERMISSIONS[target['provider']])


def act(cur, ctx, target_id, handle, action, message=None):
    target, account = destination(cur, ctx, target_id)
    data = unseal(handle, target, ctx['tenant']['id'], 'comment')
    capability = 'hide' if action == 'unhide' else action
    if not data.get('capabilities', {}).get(capability):
        raise HTTPException(403, 'This action is not available for this comment.')
    comment_id = identifier(data['comment'])
    if action == 'reply' and (not message or not message.strip() or len(message) > 2000):
        raise HTTPException(422, 'Enter a reply of up to 2,000 characters.')
    try:
        token = credentials(cur, account, redact_errors=True)
        if action == 'reply':
            edge = '/replies' if target['provider'] == 'instagram' else '/comments'
            result = call(account, token, 'POST', identifier(data.get('reply_to') or comment_id) + edge, data={'message': message.strip()})
            if not result.get('id'):
                raise HTTPException(409, 'The reply result could not be confirmed. Refresh comments before trying again.')
        elif action in ('hide', 'unhide'):
            key = 'hide' if target['provider'] == 'instagram' else 'is_hidden'
            result = call(account, token, 'POST', comment_id, data={key: 'true' if action == 'hide' else 'false'})
            if result is not True and not (isinstance(result, dict) and result.get('success') is True):
                raise HTTPException(409, 'The result could not be confirmed. Refresh comments before trying again.')
        elif action == 'delete':
            result = call(account, token, 'DELETE', comment_id)
            if result is not True and not (isinstance(result, dict) and result.get('success') is True):
                raise HTTPException(409, 'The result could not be confirmed. Refresh comments before trying again.')
        else:
            raise HTTPException(422, 'Unknown comment action.')
    except ProviderError as error:
        provider_error(error, mutation=True)
    # Deliberately omit comment text, reply text and commenter identities.
    s.audit(cur, ctx, target['id'], 'social_comment_' + action,
            dict(provider=target['provider'], comment_id=comment_id))
    return {'ok': True}
