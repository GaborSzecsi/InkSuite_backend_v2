"""Add missing product artwork without replacing existing Shopify media."""
import hashlib
import io
import os
import re
from urllib.parse import unquote, urlsplit
import requests
from PIL import Image
from . import shopify as sh

MAX_IMAGE_BYTES = 25 * 1024 * 1024


def filename(url):
    name=unquote(urlsplit(url).path.rsplit('/',1)[-1]).casefold()
    # Shopify appends a UUID when another file has the same original name.
    return re.sub(r'_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(?=\.)','',name)


def fingerprint(url):
    """Read only known storage hosts, bounded in size; never follow redirects."""
    u=urlsplit(url)
    bucket=os.getenv('UPLOADS_S3_BUCKET') or os.getenv('S3_BUCKET') or 'inksuite-data'
    region=os.getenv('AWS_REGION') or os.getenv('AWS_DEFAULT_REGION') or 'us-east-2'
    allowed={f'{bucket}.s3.{region}.amazonaws.com','cdn.shopify.com','shopify-shop-assets.storage.googleapis.com'}
    if u.scheme!='https' or u.hostname not in allowed or u.username or u.password or u.port not in (None,443):return None
    try:
        with requests.get(url,stream=True,allow_redirects=False,timeout=(5,15)) as response:
            if response.status_code!=200:return None
            chunks=[];size=0
            for chunk in response.iter_content(65536):
                size+=len(chunk)
                if size>MAX_IMAGE_BYTES:return None
                chunks.append(chunk)
        body=b''.join(chunks)
        with Image.open(io.BytesIO(body)) as image:
            if image.width*image.height>25000000:return None
            image=image.convert('RGB')
            return hashlib.sha256(str(image.size).encode()+image.tobytes()).hexdigest()
    except (requests.RequestException,OSError,ValueError,Image.DecompressionBombError):return None


def missing_images(sources,existing):
    names=set();digests=set()
    for media in existing:
        if media.get('status')=='FAILED':continue
        url=(media.get('originalSource') or {}).get('url') or (media.get('image') or {}).get('url')
        if not url:raise ValueError('Shopify images are still processing. Wait for processing before updating images.')
        names.add(filename(url))
        digest=fingerprint(url)
        if digest:digests.add(digest)
    result=[]
    for url in dict.fromkeys(sources):
        name=filename(url)
        if name in names:continue
        digest=fingerprint(url)
        if digest and digest in digests:continue
        names.add(name)
        if digest:digests.add(digest)
        result.append(url)
    return result


def sync_images(cur,install,product,sources,title):
    after=None;existing=[]
    while True:
        result=sh.graphql(cur,install,'''query($id:ID!,$after:String){product(id:$id){media(first:100,after:$after){nodes{id status mediaContentType ... on MediaImage{image{url} originalSource{url}}} pageInfo{hasNextPage endCursor}}}}''',{'id':product,'after':after})['product']
        if not result:raise ValueError('Shopify product no longer exists')
        page=result['media'];existing.extend(m for m in page['nodes'] if m['mediaContentType']=='IMAGE')
        if not page['pageInfo']['hasNextPage']:break
        cursor=page['pageInfo']['endCursor']
        if not cursor or cursor==after:raise ValueError('Shopify image pagination requires review')
        after=cursor
    additions=missing_images(sources,existing)
    if not additions:return 0
    result=sh.graphql(cur,install,'''mutation($product:ProductUpdateInput!,$media:[CreateMediaInput!]){productUpdate(product:$product,media:$media){product{id} userErrors{message}}}''',{'product':{'id':product},'media':[{'originalSource':u,'mediaContentType':'IMAGE','alt':title+(' — Interior preview' if 'interior' in filename(u) else ' — Book cover')} for u in additions]})['productUpdate']
    if result.get('userErrors'):raise ValueError('Shopify could not confirm the image update. Review product media before retrying.')
    return len(additions)
