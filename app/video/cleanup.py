"""Preview-first removal of a completed video's editable project and exclusive media."""
import hashlib,json
from fastapi import HTTPException
from psycopg.types.json import Jsonb
from app.marketing import service as s,assets
from . import service as v


def references(document):
    found={str(c['asset_id']) for c in document.get('clips',[]) if c.get('asset_id')}
    background=document.get('canvas',{}).get('background_asset_id')
    if background: found.add(str(background))
    return found


def protected_reason(cur,tenant,asset,project_id):
    if asset['source_type']=='title_public_asset' or asset.get('work_id'): return 'Book asset'
    if asset.get('campaign_id'): return 'Campaign library asset'
    if asset.get('music_license'): return 'Music library asset'
    if asset.get('project_id') or asset.get('is_finished_video'): return 'Finished video'
    if asset['source_type']=='generated_derivative' and not asset.get('working_project_id'): return 'Existing prepared media or finished video'
    if asset.get('working_project_id') and str(asset['working_project_id'])!=str(project_id): return 'Working file of another project'
    identity=asset['id'];needle=Jsonb([{'asset_id':str(identity)}])
    if s.one(cur,'SELECT id FROM social_post_assets WHERE tenant_id=%s AND asset_id=%s LIMIT 1',(tenant,identity)): return 'Used by a social post'
    if s.one(cur,"SELECT id FROM video_projects WHERE tenant_id=%s AND id<>%s AND (source_asset_id=%s OR document->'clips' @> %s::jsonb OR document->'canvas'->>'background_asset_id'=%s) LIMIT 1",(tenant,project_id,identity,needle,str(identity))): return 'Used by another video project'
    if s.one(cur,"SELECT id FROM video_render_jobs WHERE tenant_id=%s AND status IN ('queued','running') AND (normalize_asset_id=%s OR document->'clips' @> %s::jsonb OR document->'canvas'->>'background_asset_id'=%s) LIMIT 1",(tenant,identity,needle,str(identity))): return 'Used by a queued or running export'
    return None


def preview(cur,ctx,project_id,render_id,include_originals=False):
    tenant=ctx['tenant']['id'];p=v.project(cur,project_id,tenant,True)
    job=s.one(cur,"SELECT * FROM video_render_jobs WHERE id=%s AND project_id=%s AND tenant_id=%s",(render_id,project_id,tenant))
    if not job or job['status']!='completed' or job.get('job_kind','composition')!='composition' or not job.get('output_asset_id'): raise HTTPException(409,'Finish exporting this project before removing its working files.')
    if p['revision']!=job['project_revision']: raise HTTPException(409,'The project has changed since this export. Export the latest version before cleanup.')
    if s.one(cur,"SELECT id FROM video_render_jobs WHERE project_id=%s AND tenant_id=%s AND status IN ('queued','running') LIMIT 1",(project_id,tenant)): raise HTTPException(409,'Finish or cancel the other exports before cleanup.')
    output=v.validate_asset(cur,ctx,job['output_asset_id'])
    ids=references(p['document'])|references(job['document'])
    if p.get('source_asset_id'): ids.add(str(p['source_asset_id']))
    candidates=s.rows(cur,'''SELECT a.*,m.working_project_id,m.music_license,m.project_id,m.is_finished_video FROM social_media_assets a
        LEFT JOIN video_asset_metadata m ON m.asset_id=a.id AND m.tenant_id=a.tenant_id
        WHERE a.tenant_id=%s AND (a.id=ANY(%s::uuid[]) OR m.working_project_id=%s) ORDER BY a.id LIMIT 501''',(tenant,list(ids),project_id))
    if len(candidates)>500: raise HTTPException(422,'This project has too many imported files for one cleanup. Remove unused imports from Assets first.')
    files=[]
    for a in candidates:
        if a['id']==output['id']: continue
        reason=protected_reason(cur,tenant,a,project_id)
        owned=str(a.get('working_project_id') or '')==str(project_id)
        if not reason and not owned and not include_originals: reason='Existing library upload (kept by default)'
        if not reason:
            # Recheck tenant ownership, configured bucket, and allowed S3 key before offering deletion.
            v.validate_asset(cur,ctx,a['id'])
        files.append({'id':str(a['id']),'name':a.get('display_name') or a['filename'],'bytes':int(a.get('filesize_bytes') or 0),'delete':not bool(reason),'reason':reason})
    payload={'project_id':str(project_id),'render_id':str(render_id),'revision':p['revision'],'include_originals':include_originals,'files':files,'output_asset_id':str(output['id'])}
    token=hashlib.sha256(json.dumps(payload,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    return {**payload,'confirmation':token,'delete_count':sum(f['delete'] for f in files),'delete_bytes':sum(f['bytes'] for f in files if f['delete']),'kept_count':sum(not f['delete'] for f in files)}
