"""Refuse private media writes when the deployment's bucket exposes the new prefix."""
import requests
from app.marketing import assets,service as s
from app.marketing.domain import tenant_root

def ensure_private_storage(ctx):
    # The probe contains no customer media. Check before writing any real private object.
    key=tenant_root(ctx['tenant']['slug'])+'assets/marketing/private/video/security-check/'+str(s.uid())+'.txt'
    client=assets.s3()
    try:
        client.put_object(Bucket=assets.bucket(),Key=key,Body=b'InkSuite private media access check',ContentType='text/plain',ServerSideEncryption='AES256')
        response=requests.head(assets.public_url(key),timeout=(3,10),allow_redirects=False)
        if response.status_code not in (403,404): raise ValueError('Private video storage is not configured: anonymous reads must be denied for the video prefix before importing or exporting private media.')
    finally:
        client.delete_object(Bucket=assets.bucket(),Key=key)
