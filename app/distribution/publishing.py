"""Merchant-selected catalog publication. No orders, customers or inventory writes."""
from app.catalog_content import DESCRIPTION_SQL
import html
import ipaddress
import re
from collections import defaultdict
from urllib.parse import urlsplit
from fastapi import HTTPException
from psycopg.types.json import Jsonb
from . import service as s, shopify as sh
from .media import missing_images, sync_images

FORMATS={'BB':'Hardcover','BC':'Paperback','BA':'Book','EA':'E-book','EB':'E-book','ED':'E-book','DG':'E-book','AA':'Audiobook','AB':'Audiobook','AC':'Audiobook','AJ':'Audiobook'}

def image_url(value):
    value=(value or '').strip();u=urlsplit(value)
    if u.scheme!='https' or not u.hostname or u.username or u.password or u.query or u.fragment:return None
    if u.hostname.lower() in ('localhost','metadata.google.internal') or '.' not in u.hostname:return None
    try:
        if not ipaddress.ip_address(u.hostname).is_global:return None
    except ValueError:pass
    return value


def entries(cur,tenant,currency):
    cur.execute(f'''SELECT e.id,e.work_id,e.isbn13,e.product_form,e.onix_product_form,e.cover_image_link,
        w.title,w.subtitle,w.uid,{DESCRIPTION_SQL} AS description FROM editions e JOIN works w ON w.id=e.work_id AND w.tenant_id=e.tenant_id
        WHERE e.tenant_id=%s ORDER BY w.title,e.id''',(tenant,))
    rows=cur.fetchall()
    cur.execute("""SELECT d.edition_id,p.price_amount FROM edition_prices p JOIN edition_supply_details d
      ON d.id=p.supply_detail_id AND d.tenant_id=p.tenant_id WHERE d.tenant_id=%s
      AND p.currency_code=%s AND COALESCE(NULLIF(btrim(p.price_type_code),''),'01') IN ('01','02') AND p.price_amount>0
      AND (p.price_effective_from IS NULL OR p.price_effective_from<=current_date)
      AND (p.price_effective_until IS NULL OR p.price_effective_until>=current_date)""",(tenant,currency))
    prices=defaultdict(set)
    for row in cur.fetchall():prices[str(row['edition_id'])].add(row['price_amount'])
    cur.execute("""SELECT r.edition_id,v.resource_link FROM edition_supporting_resources r JOIN edition_supporting_resource_versions v
      ON v.resource_id=r.id AND v.tenant_id=r.tenant_id WHERE r.tenant_id=%s
      AND (r.resource_content_type IN ('01','02','03','04') OR v.resource_link ~* '[.](png|jpg|jpeg|webp|gif)$') ORDER BY r.is_primary DESC,r.item_order,v.item_order""",(tenant,))
    images=defaultdict(list)
    for row in cur.fetchall():images[str(row['edition_id'])].append(image_url(row['resource_link']))
    for e in rows:
        e['id']=str(e['id']);e['work_id']=str(e['work_id'])
        subtitle=(e.get('subtitle') or '').strip()
        if subtitle:e['title']=e['title'].strip()+': '+subtitle
        code=e.get('onix_product_form') or e.get('product_form') or ''
        e['format']=FORMATS.get(code,e.get('product_form') or code or 'Unknown format')
        e['isbn']=sh.normalized_isbn(e['isbn13'])
        values=prices[e['id']];e['price']=str(next(iter(values))) if len(values)==1 else None
        e['currency']=currency
        e['images']=list(dict.fromkeys(u for u in [image_url(e.get('cover_image_link'))]+images[e['id']] if u))[:20]
        e['description']=e.get('description') or ''
        e['issues']=[]
        if not e['isbn']:e['issues'].append('Missing or invalid ISBN')
        if not e['price']:e['issues'].append('Missing or ambiguous price in '+currency)
        if not e['images']:e['issues'].append('No public HTTPS cover/image URL')
        if not e['description']:e['issues'].append('Missing description')
    # Shopify content belongs to the book, not an individual binding. Keep edition
    # images first, but allow sibling editions of this exact work to supply artwork.
    by_work=defaultdict(list)
    for e in rows:by_work[e['work_id']].append(e)
    for siblings in by_work.values():
        shared_images=list(dict.fromkeys(u for e in siblings for u in e['images']))[:20]
        if not shared_images and siblings[0].get('uid'):
            # Only the trusted storage helper may supply a signed URL; arbitrary
            # caller-provided signed URLs remain disallowed by image_url().
            from app.marketplace.catalog_service import legacy_cover
            cover=legacy_cover(None,siblings[0]['uid'],tenant,cur)
            if cover:shared_images=[cover]
        descriptions={e['description'].strip() for e in siblings if e['description'].strip()}
        for e in siblings:
            if not e['images'] and shared_images:
                e['images']=shared_images.copy()
                e['issues'].remove('No public HTTPS cover/image URL')
            if not e['description'].strip() and len(descriptions)==1:
                e['description']=next(iter(descriptions))
                if 'Missing description' in e['issues']:e['issues'].remove('Missing description')
    for e in rows:e.pop('uid',None)
    counts=defaultdict(int)
    for e in rows:
        if e['isbn']:counts[e['isbn']]+=1
    for e in rows:
        if counts[e['isbn']]>1:e['issues'].append('ISBN belongs to multiple InkSuite editions')
    return rows


def catalog(cur,install):
    info=sh.graphql(cur,install,'{shop{currencyCode} currentAppInstallation{accessScopes{handle}}}')
    currency=info['shop']['currencyCode']
    rows=entries(cur,install['tenant_id'],currency)
    cur.execute('SELECT * FROM distribution_shopify_variants WHERE installation_id=%s AND tenant_id=%s AND active',(install['id'],install['tenant_id']))
    variants=cur.fetchall();by_isbn=defaultdict(list)
    for v in variants:
        m=v['catalog_metadata'];isbn=sh.normalized_isbn(m.get('barcode')) or sh.normalized_isbn(v['sku'])
        if isbn:by_isbn[isbn].append(v)
    for e in rows:
        matches=by_isbn[e['isbn']]
        if len(matches)==1:
            m=matches[0];a=sh.normalized_isbn(m['catalog_metadata'].get('barcode'));b=sh.normalized_isbn(m['sku'])
            if (a and b and a!=b) or (m['edition_id'] and str(m['edition_id'])!=e['id']):
                e['status']='Needs review';e['issues'].append('Existing Shopify ISBN or edition mapping conflicts')
            else:e['status']='Already in Shopify'
        elif len(matches)>1:e['status']='Needs review';e['issues'].append('Multiple Shopify variants use this ISBN')
        elif e['issues']:e['status']='Needs review'
        else:e['status']='Ready to add'
    return {'editions':rows,'can_publish':any(x['handle']=='write_products' for x in info['currentAppInstallation']['accessScopes']), 'currency':currency}


def enqueue(install,edition_ids,connection):
    with s.transaction() as cur:
        # Serialize with refresh/token rotation and other catalog publication requests.
        install=s.one(cur,'SELECT * FROM distribution_shopify_installations WHERE id=%s AND active FOR UPDATE',(install['id'],))
        if not install:raise HTTPException(409,'Store disconnected.')
        s.connection(cur,install['tenant_id'],connection)
        data=catalog(cur,install)
        if not data['can_publish']:raise HTTPException(409,'Approve the Shopify write_products permission before adding books.')
        wanted=set(map(str,edition_ids));selected=[e for e in data['editions'] if e['id'] in wanted]
        if len(selected)!=len(wanted):raise HTTPException(422,'An edition does not belong to this publisher.')
        if any(e['status']=='Needs review' for e in selected):raise HTTPException(422,'Resolve the selected editions marked Needs review first.')
        grouped=defaultdict(list)
        for e in selected:grouped[e['work_id']].append(e)
        for work,items in grouped.items():
            cur.execute('''INSERT INTO distribution_shopify_publish_jobs(tenant_id,installation_id,work_id,connection_id,selection)
              VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING''',(install['tenant_id'],install['id'],work,connection,Jsonb(items)))
        return {'queued':True,'titles':len(grouped)}

# Read the full live variant catalog before writes: stale local snapshots never authorize duplicates.
def live_variants(cur,install):
    after=None;variants=[]
    while True:
        page=sh.graphql(cur,install,sh.CATALOG_QUERY,{'after':after})['productVariants']
        variants.extend(page['nodes'])
        if not page['pageInfo']['hasNextPage']:return variants
        cursor=page['pageInfo']['endCursor']
        if not cursor or cursor==after:raise ValueError('Catalog pagination requires review')
        after=cursor


def plan(selected,variants):
    matched=[];missing=[];products=set()
    for e in selected:
        matches=[]
        for v in variants:
            a=sh.normalized_isbn(v.get('barcode'));b=sh.normalized_isbn(v.get('sku'))
            if e['isbn'] in (a,b):
                if a and b and a!=b:raise ValueError('Conflicting barcode and SKU ISBNs in Shopify')
                matches.append(v)
        if len(matches)>1:raise ValueError('Duplicate ISBN in Shopify; resolve before publishing')
        if matches:
            matched.append((e,matches[0]));products.add(matches[0]['product']['id'])
        else:missing.append(e)
    title=' '.join(selected[0]['title'].casefold().split())
    same_title={v['product']['id'] for v in variants if ' '.join(v['product']['title'].casefold().split())==title}
    products |= same_title
    if len(products)>1:raise ValueError('Title or ISBN matches multiple Shopify products; review required')
    return matched,missing,next(iter(products),None)


def save_match(cur,install,connection,e,v):
    metadata={'product_id':v['product']['id'],'product_title':v['product']['title'],'variant_title':v['title'],
              'barcode':v.get('barcode') or e['isbn'],'isbn':e['isbn'],'match_status':'MATCHED_BY_ISBN'}
    cur.execute('''INSERT INTO distribution_shopify_variants(tenant_id,installation_id,variant_id,sku,title,edition_id,connection_id,catalog_metadata)
      VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(tenant_id,installation_id,variant_id) DO UPDATE
      SET active=true,edition_id=COALESCE(distribution_shopify_variants.edition_id,excluded.edition_id),
      connection_id=COALESCE(distribution_shopify_variants.connection_id,excluded.connection_id),catalog_metadata=excluded.catalog_metadata''',
      (install['tenant_id'],install['id'],v['id'],v.get('sku') or '',e['title']+' / '+v['title'],e['id'],connection,Jsonb(metadata)))


def create_or_add(cur,install,selected):
    matched,missing,product=plan(selected,live_variants(cur,install))
    images=list(dict.fromkeys(u for e in selected for u in e['images']))[:20]
    if not missing:
        sync_images(cur,install,product,images,selected[0]['title'])
        return matched
    if any(e['issues'] for e in missing):raise ValueError('Edition metadata requires review')
    labels=[e['format'] for e in missing]
    if len(set(labels))!=len(labels):raise ValueError('Selected editions have duplicate format names; choose one edition per format')
    if product:
        p=sh.graphql(cur,install,'query($id:ID!){product(id:$id){id status options{name optionValues{name}}}}',{'id':product})['product']
        if not p or len(p['options'])!=1 or p['options'][0]['name'].casefold() not in ('format','binding','edition'):
            raise ValueError('Existing product needs a single Format option before adding editions')
        option=p['options'][0]
        if set(labels)&{v['name'] for v in option['optionValues']}:raise ValueError('Format already exists with a different ISBN')
        query='''mutation($id:ID!,$variants:[ProductVariantsBulkInput!]!){productVariantsBulkCreate(productId:$id,variants:$variants){productVariants{id title barcode sku product{id title}} userErrors{message}}}'''
        payload={'id':product,'variants':[{'barcode':e['isbn'],'price':e['price'],'inventoryItem':{'sku':e['isbn'],'tracked':True},'optionValues':[{'optionName':option['name'],'name':e['format']}]} for e in missing]}
        result=sh.graphql(cur,install,query,payload)['productVariantsBulkCreate']
        created=result.get('productVariants') or []
    else:
        images=missing_images(images,[])
        query='''mutation($input:ProductSetInput!){productSet(input:$input,synchronous:true){product{id variants(first:100){nodes{id title barcode sku product{id title}}}} userErrors{message}}}'''
        payload={'input':{'title':selected[0]['title'],'status':'DRAFT','productType':'Book',
          'descriptionHtml':'<p>'+html.escape(selected[0]['description']).replace('\n','<br/>')+'</p>',
          'productOptions':[{'name':'Format','position':1,'values':[{'name':e['format']} for e in missing]}],
          'variants':[{'barcode':e['isbn'],'price':e['price'],'sku':e['isbn'],'inventoryItem':{'tracked':True},'optionValues':[{'optionName':'Format','name':e['format']}]} for e in missing],
          'files':[{'originalSource':u,'contentType':'IMAGE','alt':selected[0]['title']} for u in images]}}
        result=sh.graphql(cur,install,query,payload)['productSet']
        created=((result.get('product') or {}).get('variants') or {}).get('nodes') or []
    if result.get('userErrors'):raise ValueError('Shopify rejected the selected product data; review the product before retrying')
    for e in missing:
        found=[v for v in created if sh.normalized_isbn(v.get('barcode'))==e['isbn']]
        if len(found)!=1:raise ValueError('Shopify result requires reconciliation; do not resubmit')
        matched.append((e,found[0]))
    if product:sync_images(cur,install,product,images,selected[0]['title'])
    return matched


def run_once():
    # Commit an intent before any Shopify mutation. Interrupted/ambiguous outcomes never retry automatically.
    with s.transaction() as cur:
        cur.execute("UPDATE distribution_shopify_publish_jobs SET status='REVIEW_REQUIRED',message='Interrupted publication: inspect Shopify before retrying' WHERE status='RUNNING' AND updated_at<now()-interval '15 minutes'")
        job=s.one(cur,"SELECT * FROM distribution_shopify_publish_jobs WHERE status='QUEUED' ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1")
        if not job:return False
        cur.execute("UPDATE distribution_shopify_publish_jobs SET status='RUNNING',updated_at=now() WHERE id=%s",(job['id'],))
    try:
        with s.transaction() as cur:
            install=s.one(cur,'SELECT * FROM distribution_shopify_installations WHERE id=%s AND active FOR UPDATE',(job['installation_id'],))
            if not install:raise ValueError('Store disconnected')
            current=catalog(cur,install)
            if not current['can_publish']:raise ValueError('Approve write_products in Shopify')
            by_id={e['id']:e for e in current['editions']}
            for e in job['selection']:
                now=by_id.get(e['id'])
                if not now or any(now[k]!=e[k] for k in ('isbn','price','currency','format','title','description')):raise ValueError('InkSuite metadata changed; review selection again')
                if [u.split('?',1)[0] for u in now['images']] != [u.split('?',1)[0] for u in e['images']]:raise ValueError('InkSuite artwork changed; review selection again')
            # Refresh trusted temporary image access immediately before Shopify copies it.
            matches=create_or_add(cur,install,[by_id[e['id']] for e in job['selection']])
            for e,v in matches:save_match(cur,install,job['connection_id'],e,v)
            cur.execute("UPDATE distribution_shopify_publish_jobs SET status='DONE',message='Matched or added selected formats and synchronized missing images. Review products and image processing in Shopify.',updated_at=now() WHERE id=%s",(job['id'],))
    except Exception as exc:
        message=str(exc) if isinstance(exc,ValueError) else 'Publication could not be confirmed. Inspect Shopify before retrying to avoid duplicates.'
        with s.transaction() as cur:cur.execute("UPDATE distribution_shopify_publish_jobs SET status='REVIEW_REQUIRED',message=%s,updated_at=now() WHERE id=%s",(message,job['id']))
    return True
