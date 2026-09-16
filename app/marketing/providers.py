"""Provider adapters. Each advance call performs one durable publishing step."""
import os
from dataclasses import dataclass, field
from urllib.parse import urlparse
import requests

class ProviderError(Exception):
    def __init__(self,code,message,retryable=False,uncertain=False):
        super().__init__(message); self.code=code; self.retryable=retryable; self.uncertain=uncertain

def request(method, url, token=None, **kwargs):
    headers = kwargs.pop('headers', {})

    if token:
        headers['Authorization'] = 'Bearer ' + token

    try:
        r = requests.request(
            method,
            url,
            headers=headers,
            timeout=(10, 60),
            allow_redirects=False,
            **kwargs,
        )
    except requests.RequestException:
        raise ProviderError(
            'network_error',
            'The provider connection was interrupted.',
            True,
            method != 'GET',
        ) from None

    try:
        data = r.json()
    except ValueError:
        raise ProviderError(
            'invalid_response',
            'The provider returned an unreadable response.',
            r.status_code >= 500,
            method != 'GET',
        ) from None

    error = data.get('error') if isinstance(data, dict) else None

    is_provider_error = (
        r.status_code >= 400
        or (
            error
            and (
                not isinstance(error, dict)
                or error.get('code') not in (None, 'ok', 0)
            )
        )
    )

    if is_provider_error:
        code = (
            str(error.get('code', 'provider_error'))
            if isinstance(error, dict)
            else 'provider_error'
        )

        print(
            'PROVIDER ERROR:',
            {
                'status': r.status_code,
                'code': code,
                'subcode': (
                    error.get('error_subcode')
                    if isinstance(error, dict)
                    else None
                ),
                'type': (
                    error.get('type')
                    if isinstance(error, dict)
                    else None
                ),
                'message': (
                    error.get('message')
                    if isinstance(error, dict)
                    else None
                ),
            },
        )

        retry = r.status_code == 429 or r.status_code >= 500

        # Only identify the problem as an expired/revoked authorization
        # when the provider actually reports an authentication/token error.
        auth_error_codes = {
            '190',
            'access_token_invalid',
            'invalid_access_token',
            'access_token_expired',
        }

        if r.status_code == 401 or code in auth_error_codes:
            msg = (
                'Authorization expired or was revoked. '
                'Reconnect the account.'
            )

        elif code == 'unaudited_client_can_only_post_to_private_accounts':
            msg = (
                'TikTok has not yet audited this app. '
                'Unaudited TikTok integrations can only publish '
                'to private TikTok accounts using Only me visibility.'
            )

        elif retry:
            msg = 'Provider temporarily unavailable or rate limited.'

        else:
            msg = (
                'Provider rejected the request. '
                'Check media and destination settings.'
            )

        raise ProviderError(
            code,
            msg,
            retry,
            r.status_code >= 500 and method != 'GET',
        )

    if not 200 <= r.status_code < 300:
        raise ProviderError(
            'unexpected_redirect',
            'Unexpected provider redirect.',
        )

    return data

def graph():
    version=os.getenv('MARKETING_META_API_VERSION','')
    if not version: raise ProviderError('configuration','Configure MARKETING_META_API_VERSION with the version enabled for the Meta app.')
    return 'https://graph.facebook.com/'+version

def instagram_graph():
    # Instagram Login uses graph.instagram.com rather than Facebook Graph.
    # Keep the same API version by default, but allow a separate override later.
    version=(
        os.getenv('MARKETING_INSTAGRAM_API_VERSION','')
        or os.getenv('MARKETING_META_API_VERSION','')
    )
    if not version:
        raise ProviderError(
            'configuration',
            'Configure MARKETING_INSTAGRAM_API_VERSION or MARKETING_META_API_VERSION.'
        )
    return 'https://graph.instagram.com/'+version

@dataclass
class Result:
    status:str
    state:dict=field(default_factory=dict)
    post_id:str|None=None
    url:str|None=None

def content(post,target): return target['provider_payload'].get('text') or post['content_text']

def media_for_target(post,target):
    chosen=target['provider_payload'].get('asset_ids')
    media=[a for a in post['assets'] if a.get('role')!='cover']
    if chosen is None: return media
    if not isinstance(chosen,list) or any(not isinstance(i,str) for i in chosen): raise ProviderError('validation','Invalid provider media selection.')
    available={str(a['id']):a for a in media}
    if len(set(chosen))!=len(chosen) or any(i not in available for i in chosen): raise ProviderError('validation','Provider media must be selected from this post.')
    return [available[i] for i in chosen]

def reel_cover(post,target):
    selected=target['provider_payload'].get('cover_asset_id')
    if not selected: return None
    cover=next((a for a in post.get('assets',[]) if str(a['id'])==str(selected)),None)
    if not cover or cover['media_type']!='image' or cover['mime_type']!='image/jpeg':
        raise ProviderError('validation','Choose a JPEG image for the Reel cover.')
    if urlparse(cover.get('public_url','')).scheme!='https' or not cover.get('width') or not cover.get('height') or not 0<cover.get('filesize_bytes',0)<=8*1024*1024:
        raise ProviderError('validation','Reel covers require an inspected public JPEG image under 8 MB.')
    return cover

class SocialPublisher:
    provider=''
    def validate_post(self,post,target,assets):
        if not content(post,target).strip() and not assets: raise ProviderError('validation','Add post text or media.')
        for a in assets:
            if not a.get('public_url') or urlparse(a['public_url']).scheme!='https': raise ProviderError('validation','Select public media with an HTTPS URL.')
            if a['media_type'] not in ('image','video'): raise ProviderError('validation','This file type cannot be published.')
            if not a.get('width') or not a.get('height') or not a.get('filesize_bytes'): raise ProviderError('validation','Inspect media dimensions and size before scheduling.')
            if a['media_type']=='video':
                if a['mime_type'] not in ('video/mp4','video/webm') or not a.get('duration_seconds'): raise ProviderError('validation','Use a video with a supported format and verified duration.')
                if a['filesize_bytes']>500*1024*1024: raise ProviderError('validation','Video exceeds 500 MB.')
            elif a['mime_type'] not in ('image/jpeg','image/png','image/webp'): raise ProviderError('validation','Use JPEG, PNG or WebP images.')
            if a['media_type']=='image' and a['filesize_bytes']>20*1024*1024: raise ProviderError('validation','Image exceeds the publishing limit of 20 MB.')
        link=target['provider_payload'].get('link')
        if link and urlparse(link).scheme not in ('http','https'): raise ProviderError('validation','Destination links must use HTTP or HTTPS.')
    def advance(self,account,token,post,target,assets): raise NotImplementedError
    def refresh_credentials(self,account):
        from .oauth import refresh
        return refresh(account)
    def disconnect(self,account):
        # Local access is removed transactionally by the account service.
        return None

class FacebookPublisher(SocialPublisher):
    provider='facebook'
    def validate_post(self,p,t,a):
        super().validate_post(p,t,a)
        if len(a)>10: raise ProviderError('validation','Facebook supports up to 10 images in this composer.')
        if any(x['media_type']=='video' for x in a) and len(a)!=1: raise ProviderError('validation','Use one video or a set of images.')
    def advance(self,a,token,p,t,media):
        root=graph()+'/'+a['provider_account_id']; state=t['provider_state']; text=content(p,t)
        if media and media[0]['media_type']=='video':
            if state.get('video_id'):
                data=request('GET',graph()+'/'+state['video_id'],token,params={'fields':'status,permalink_url'})
                phase=data.get('status',{}).get('video_status')
                if phase=='ready': return Result('published',state,state['video_id'],data.get('permalink_url'))
                if phase=='error': raise ProviderError('processing_failed','Facebook could not process the video.')
                return Result('submitted',state)
            data=request('POST',root+'/videos',token,data={'file_url':media[0]['public_url'],'description':text})
            return Result('submitted',{'video_id':str(data['id'])})
        photo_ids=state.get('photo_ids',[])
        if len(photo_ids)<len(media):
            data=request('POST',root+'/photos',token,data={'url':media[len(photo_ids)]['public_url'],'published':'false'})
            return Result('submitted',{'photo_ids':photo_ids+[str(data['id'])]})
        body={'message':text}
        if photo_ids:
            import json
            for i,pid in enumerate(photo_ids): body[f'attached_media[{i}]']=json.dumps({'media_fbid':pid})
        elif t['provider_payload'].get('link'): body['link']=t['provider_payload']['link']
        data=request('POST',root+'/feed',token,data=body)
        pid=str(data['id']); return Result('published',state,pid,'https://www.facebook.com/'+pid)

class InstagramPublisher(SocialPublisher):
    provider='instagram'
    def validate_post(self,p,t,a):
        super().validate_post(p,t,a)
        cover=reel_cover(p,t)
        if cover and (len(a)!=1 or a[0]['media_type']!='video'): raise ProviderError('validation','A custom cover requires a single Reel video for this destination.')
        if not 1<=len(a)<=10: raise ProviderError('validation','Instagram requires 1–10 media files.')
        if len(content(p,t))>2200: raise ProviderError('validation','Instagram captions must be at most 2,200 characters.')
        for x in a:
            if x['media_type']=='image':
                if x['mime_type']!='image/jpeg': raise ProviderError('validation','Instagram requires JPEG images. Generate a JPEG derivative first.')
                if not x.get('width') or not x.get('height'): raise ProviderError('validation','Image dimensions must be inspected before Instagram scheduling.')
                if not .8<=x['width']/x['height']<=1.91: raise ProviderError('validation','Instagram image aspect ratio must be between 4:5 and 1.91:1.')
                if (x.get('filesize_bytes') or 0)>8*1024*1024: raise ProviderError('validation','Instagram image exceeds 8 MB.')
            elif not 3<=float(x['duration_seconds'])<=900: raise ProviderError('validation','Use Instagram video between 3 seconds and 15 minutes.')
    def advance(self,a,token,p,t,media):
        root=instagram_graph()+'/'+a['provider_account_id']; state=t['provider_state']; children=state.get('children',[])
        if not state.get('container'):
            if len(media)>1 and len(children)<len(media):
                item=media[len(children)]; body={'is_carousel_item':'true'}
                body['image_url' if item['media_type']=='image' else 'video_url']=item['public_url']
                if item['media_type']=='video': body['media_type']='VIDEO'
                d=request('POST',root+'/media',token,data=body)
                return Result('submitted',{'children':children+[str(d['id'])]})
            for child in children:
                status=request('GET',instagram_graph()+'/'+child,token,params={'fields':'status_code'})['status_code']
                if status in ('ERROR','EXPIRED'): raise ProviderError('processing_failed','Instagram could not process carousel media.')
                if status!='FINISHED': return Result('submitted',state)
            body={'caption':content(p,t)}
            if children: body.update(media_type='CAROUSEL',children=','.join(children))
            else:
                item=media[0];body['image_url' if item['media_type']=='image' else 'video_url']=item['public_url']
                if item['media_type']=='video':
                    body['media_type']='REELS'
                    cover=reel_cover(p,t)
                    if cover: body['cover_url']=cover['public_url']
            d=request('POST',root+'/media',token,data=body)
            return Result('submitted',{'container':str(d['id']),'children':children})
        if state.get('published_id'):
            d=request('GET',instagram_graph()+'/'+state['published_id'],token,params={'fields':'id,permalink'})
            return Result('published',state,str(d['id']),d.get('permalink'))
        d=request('GET',instagram_graph()+'/'+state['container'],token,params={'fields':'status_code'})
        if d['status_code'] in ('ERROR','EXPIRED'): raise ProviderError('processing_failed','Instagram media container failed or expired.')
        if d['status_code']!='FINISHED': return Result('submitted',state)
        d=request('POST',root+'/media_publish',token,data={'creation_id':state['container']})
        return Result('submitted',{**state,'published_id':str(d['id'])})

class PinterestPublisher(SocialPublisher):
    provider='pinterest'
    def validate_post(self,p,t,a):
        super().validate_post(p,t,a)
        if not t['provider_payload'].get('board_id'): raise ProviderError('validation','Select a Pinterest board.')
        if len(a)!=1 or a[0]['media_type']!='image': raise ProviderError('validation','A Pinterest Pin requires one image in this version.')
        if len(content(p,t))>800: raise ProviderError('validation','Pinterest description exceeds 800 characters.')
        if len(t['provider_payload'].get('title') or p['title'])>100: raise ProviderError('validation','Pinterest title exceeds 100 characters.')
    def advance(self,a,token,p,t,media):
        opts=t['provider_payload'];body={'board_id':opts['board_id'],'title':opts.get('title') or p['title'],'description':content(p,t),'media_source':{'source_type':'image_url','url':media[0]['public_url']},'alt_text':media[0].get('alt_text') or ''}
        if opts.get('link'): body['link']=opts['link']
        d=request('POST','https://api.pinterest.com/v5/pins',token,json=body)
        pid=str(d['id']);return Result('published',{},pid,'https://www.pinterest.com/pin/'+pid+'/')

class TikTokPublisher(SocialPublisher):
    provider='tiktok'
    base='https://open.tiktokapis.com/v2/post/publish/'
    def creator(self,token): return request('POST',self.base+'creator_info/query/',token,json={})['data']
    def validate_post(self,p,t,a):
        super().validate_post(p,t,a);opts=t['provider_payload']
        if not opts.get('consent') or not opts.get('privacy_level'): raise ProviderError('validation','Choose TikTok visibility and authorize sending this content.')
        if not a or len(a)>35: raise ProviderError('validation','TikTok needs a video or 1–35 photos.')
        if any(x['media_type']=='video' for x in a) and len(a)!=1: raise ProviderError('validation','Use one TikTok video or a set of photos.')
        prefixes=[x.strip() for x in os.getenv('MARKETING_TIKTOK_VERIFIED_URL_PREFIXES','').split(',') if x.strip()]
        if a[0]['media_type']!='video' and any(not any(x['public_url'].startswith(prefix.rstrip('/')+'/') for prefix in prefixes) for x in a):
            raise ProviderError('configuration','TikTok media URL prefix has not been configured as verified.')
        limit=2200 if a[0]['media_type']=='video' else 4000
        if len(content(p,t).encode('utf-16-le'))//2>limit: raise ProviderError('validation',f'TikTok caption exceeds {limit} characters.')
        if a[0]['media_type']=='image' and any(x['mime_type'] not in ('image/jpeg','image/webp') for x in a): raise ProviderError('validation','TikTok photos must be JPEG or WebP. Create a JPEG derivative for PNG artwork.')
    def advance(self,a,token,p,t,media):
        state=t['provider_state'];opts=t['provider_payload']
        if state.get('upload_url_encrypted') and state.get('upload_offset',0)<media[0]['filesize_bytes']:
            from .oauth import decrypt
            from routers.storage_s3 import s3
            upload_url=decrypt(state['upload_url_encrypted'],a['tenant_id']);parsed=urlparse(upload_url)
            if parsed.scheme!='https' or not (parsed.hostname or '').endswith('.tiktokapis.com'): raise ProviderError('upload_url','TikTok returned an unexpected upload destination.')
            offset=state.get('upload_offset',0);total=media[0]['filesize_bytes']
            last_chunk=offset//state['chunk_size']>=state['total_chunk_count']-1
            end=total-1 if last_chunk else offset+state['chunk_size']-1
            stream=s3().get_object(Bucket=media[0]['s3_bucket'],Key=media[0]['s3_key'],Range=f'bytes={offset}-{end}')['Body']
            try: chunk=stream.read()
            finally: stream.close()
            if len(chunk)!=end-offset+1: raise ProviderError('upload_size','Source video changed after scheduling.')
            try:
                response=requests.put(upload_url,data=chunk,headers={'Content-Type':media[0]['mime_type'],'Content-Range':f'bytes {offset}-{end}/{total}','Content-Length':str(len(chunk))},timeout=(10,90),allow_redirects=False)
            except requests.RequestException: raise ProviderError('network_error','TikTok transfer was interrupted. Check its processing status.',False,True) from None
            if response.status_code not in (201,206): raise ProviderError('upload_failed','TikTok did not confirm the media transfer.',False,True)
            next_state={**state,'upload_offset':end+1}
            if end+1==total: next_state.pop('upload_url_encrypted',None)
            return Result('submitted',next_state)
        if state.get('publish_id'):
            d=request('POST',self.base+'status/fetch/',token,json={'publish_id':state['publish_id']})['data']
            if d['status']=='PUBLISH_COMPLETE':
                ids=d.get('publicaly_available_post_id') or d.get('publicly_available_post_id') or []
                return Result('published',state,str(ids[0]) if ids else state['publish_id'])
            if d['status']=='FAILED': raise ProviderError('processing_failed','TikTok could not publish this content. Check account and media settings.')
            return Result('submitted',state)
        creator=self.creator(token)
        if opts['privacy_level'] not in creator['privacy_level_options']: raise ProviderError('validation','TikTok visibility options changed. Reopen the composer.')
        info={'privacy_level':opts['privacy_level'],'disable_comment':creator.get('comment_disabled',False) or not opts.get('allow_comment',False),'brand_content_toggle':bool(opts.get('brand_content_toggle')),'brand_organic_toggle':bool(opts.get('brand_organic_toggle'))}
        video=media[0]['media_type']=='video'
        if video:
            if float(media[0]['duration_seconds'])>creator.get('max_video_post_duration_sec',0): raise ProviderError('validation','Video exceeds this TikTok creator’s duration limit.')
            info.update(title=content(p,t),disable_duet=creator.get('duet_disabled',False) or not opts.get('allow_duet',False),disable_stitch=creator.get('stitch_disabled',False) or not opts.get('allow_stitch',False),is_aigc=bool(opts.get('is_aigc')))
            body={'post_info':info,'source_info':{'source':'PULL_FROM_URL','video_url':media[0]['public_url']}}
            prefixes=[x.strip() for x in os.getenv('MARKETING_TIKTOK_VERIFIED_URL_PREFIXES','').split(',') if x.strip()]
            if not any(media[0]['public_url'].startswith(prefix.rstrip('/')+'/') for prefix in prefixes):
                total=media[0]['filesize_bytes'];chunk=min(total,10*1024*1024)
                # Last chunk may be larger; avoid creating a tiny trailing chunk.
                count=max(1,total//chunk)
                body['source_info']={'source':'FILE_UPLOAD','video_size':total,'chunk_size':chunk,'total_chunk_count':count}
        else:
            info.update(title=(opts.get('title') or p['title'])[:90],description=content(p,t))
            body={'post_info':info,'source_info':{'source':'PULL_FROM_URL','photo_images':[x['public_url'] for x in media],'photo_cover_index':0},'post_mode':'DIRECT_POST','media_type':'PHOTO','is_aigc':bool(opts.get('is_aigc'))}
        d=request('POST',self.base+('video/init/' if video else 'content/init/'),token,json=body)
        state={'publish_id':d['data']['publish_id']}
        if video and body['source_info']['source']=='FILE_UPLOAD':
            from .oauth import encrypt
            state.update(upload_url_encrypted=encrypt(d['data']['upload_url'],a['tenant_id']),upload_offset=0,chunk_size=chunk,total_chunk_count=count)
        return Result('submitted',state)

REGISTRY={p.provider:p for p in (FacebookPublisher(),InstagramPublisher(),PinterestPublisher(),TikTokPublisher())}
def publisher(provider):
    if provider not in REGISTRY: raise ProviderError('unsupported_provider','This publishing provider is not supported.')
    return REGISTRY[provider]
