"""Calendar adapters. Credentials stay in AWS Secrets Manager, as do SMTP secrets."""
import base64, hashlib, json, os, time
from datetime import timezone
from urllib.parse import urlencode, quote
import boto3
import requests
from .availability import instant

class ProviderError(RuntimeError):
    pass


def calendar_error(response, provider, token=False):
    """Translate provider failures without exposing responses, tokens, or event data."""
    name='Google' if provider=='google' else 'Microsoft'
    try:
        error=response.json().get('error',{})
        code=error if isinstance(error,str) else error.get('code') or error.get('status','')
        reasons={v.get('reason','') for v in error.get('errors',[])} if isinstance(error,dict) else set()
    except (ValueError,TypeError,AttributeError):
        code='';reasons=set()
    if response.status_code==429 or response.status_code>=500 or reasons & {'rateLimitExceeded','userRateLimitExceeded','quotaExceeded'}:
        return ProviderError(f'{name} is temporarily unavailable or limiting requests. Try again shortly.')
    if token:
        if code=='invalid_grant':
            return ProviderError(f'{name} authorization has expired or been revoked. Open Calendar settings and reconnect this account.')
        if code in ('invalid_client','unauthorized_client'):
            return ProviderError(f'{name} rejected the app credentials. An administrator needs to check the calendar connection configuration.')
        return ProviderError(f'{name} could not refresh the calendar connection (HTTP {response.status_code}). Try again or reconnect in Calendar settings.')
    if response.status_code==401:
        return ProviderError(f'{name} rejected the calendar authorization. Reconnect this account in Calendar settings.')
    if reasons & {'accessNotConfigured','serviceDisabled'} or code=='SERVICE_DISABLED':
        return ProviderError(f'The {name} Calendar API is not enabled for this app. An administrator needs to enable it.')
    if response.status_code==403:
        return ProviderError(f'{name} denied access to this calendar. Check calendar permissions or reconnect and allow calendar access.')
    if response.status_code==404:
        return ProviderError(f'{name} could not find this calendar or event. Refresh calendars in Calendar settings.')
    return ProviderError(f'{name} could not complete the calendar request (HTTP {response.status_code}). Try refreshing.')


def _secrets_client():
    region = (
        os.getenv("AWS_REGION")
        or os.getenv("AWS_DEFAULT_REGION")
        or "us-east-2"
    )
    return boto3.client("secretsmanager", region_name=region)


def secret_write(name, value):
    client = _secrets_client()
    try:
        client.create_secret(
            Name=name,
            SecretString=json.dumps(value),
        )
    except client.exceptions.ResourceExistsException:
        client.put_secret_value(
            SecretId=name,
            SecretString=json.dumps(value),
        )
    return name


def secret_read(name):
    result = _secrets_client().get_secret_value(
        SecretId=name
    )
    return json.loads(result["SecretString"])


def secret_delete(name):
    _secrets_client().delete_secret(
        SecretId=name,
        ForceDeleteWithoutRecovery=True,
    )

def config(provider):
    if provider not in ('google','microsoft'):raise ProviderError('Unknown calendar provider.')
    prefix='MEETINGS_'+provider.upper()
    client=os.getenv(prefix+'_CLIENT_ID');secret=os.getenv(prefix+'_CLIENT_SECRET')
    base=os.getenv('MEETINGS_CALLBACK_BASE','').rstrip('/')
    if not client or not secret or not base:raise ProviderError('Calendar connection is not configured. Ask your administrator.')
    root='https://login.microsoftonline.com/common/oauth2/v2.0'
    return dict(client_id=client,client_secret=secret,redirect_uri=base+'/api/meetings/oauth/'+provider+'/callback',
                authorize='https://accounts.google.com/o/oauth2/v2/auth' if provider=='google' else root+'/authorize',
                token='https://oauth2.googleapis.com/token' if provider=='google' else root+'/token',
                scope='openid email profile https://www.googleapis.com/auth/calendar.calendarlist.readonly https://www.googleapis.com/auth/calendar.events https://www.googleapis.com/auth/calendar.freebusy' if provider=='google' else 'openid profile email offline_access User.Read Calendars.ReadWrite')
def authorize(provider,state,verifier):
    c=config(provider)
    args={k:c[k] for k in ('client_id','redirect_uri','scope')}
    args.update(response_type='code',state=state,code_challenge=base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip('='),code_challenge_method='S256')
    if provider=='google':args.update(access_type='offline',prompt='consent')
    return c['authorize']+'?'+urlencode(args)

def exchange(provider,code,verifier):
    c=config(provider);data={k:c[k] for k in ('client_id','client_secret','redirect_uri')}
    data.update(code=code,code_verifier=verifier,grant_type='authorization_code')
    response=requests.post(c['token'],data=data,timeout=20)
    if not response.ok:
        try:
            detail=response.json()
        except ValueError:
            detail={}
        print(f"Meetings OAuth token exchange failed provider={provider} status={response.status_code} error={detail.get('error')} description={detail.get('error_description')}")
        raise ProviderError('Calendar authorization failed. Please reconnect.')
    result=response.json();result['expires_at']=time.time()+result.get('expires_in',3600)
    if not result.get('refresh_token'):raise ProviderError('Offline access was not granted. Reconnect with calendar and email permissions.')
    return result

class CalendarProvider:
    def __init__(self,provider,credentials,persist=None):self.provider=provider;self.credentials=credentials;self.persist=persist
    def access(self):
        if self.credentials.get('expires_at',0)<time.time()+120:
            c=config(self.provider)
            if not self.credentials.get('refresh_token'):raise ProviderError('This calendar connection is missing offline authorization. Reconnect it in Calendar settings.')
            response=requests.post(c['token'],data={'client_id':c['client_id'],'client_secret':c['client_secret'],'grant_type':'refresh_token','refresh_token':self.credentials['refresh_token']},timeout=20)
            if not response.ok:raise calendar_error(response,self.provider,token=True)
            self.credentials.update(response.json());self.credentials['expires_at']=time.time()+self.credentials.get('expires_in',3600)
            if self.persist:self.persist(self.credentials)
        return self.credentials['access_token']
    def api(self,method,path,**kwargs):
        base='https://www.googleapis.com' if self.provider=='google' else 'https://graph.microsoft.com/v1.0'
        headers={'Authorization':'Bearer '+self.access(),'Prefer':'outlook.timezone="UTC"'}
        headers.update(kwargs.pop('headers',{}))
        response=requests.request(method,base+path,headers=headers,timeout=20,**kwargs)
        if response.status_code==412:raise ProviderError('This meeting changed. Close and reopen it before saving again.')
        if not response.ok:raise calendar_error(response,self.provider)
        return response.json() if response.content else {}
    def identity(self):
        p=self.api('GET','/oauth2/v3/userinfo' if self.provider=='google' else '/me?$select=id,mail,userPrincipalName,displayName')
        if self.provider=='google' and not p.get('email_verified'):raise ProviderError('The connected email must be verified.')
        return {'id':p.get('sub') or p['id'],'email':p.get('email') or p.get('mail') or p.get('userPrincipalName'),'name':p.get('name') or p.get('displayName',''), 'picture':p.get('picture') if self.provider=='google' else None}
    def calendars(self):
        result=[];page=None
        while True:
            if self.provider=='google':
                data=self.api('GET','/calendar/v3/users/me/calendarList',params={'pageToken':page} if page else {})
                result.extend({'id':c['id'],'name':c.get('summary','Calendar'),'can_write':c.get('accessRole') in ('owner','writer')} for c in data.get('items',[]))
                page=data.get('nextPageToken')
                if not page:break
            else:
                data=self.api('GET',page or '/me/calendars?$select=id,name,canEdit&$top=100')
                result.extend({'id':c['id'],'name':c['name'],'can_write':c.get('canEdit',False)} for c in data.get('value',[]))
                next_url=data.get('@odata.nextLink');page=next_url.removeprefix('https://graph.microsoft.com/v1.0') if next_url else None
                if not page:break
        return result
    def busy(self,calendars,start,end,exclude_event=None):
        result=[]
        if self.provider=='google' and exclude_event:
            for cid in calendars:
                params={'timeMin':start.isoformat(),'timeMax':end.isoformat(),'singleEvents':'true','maxResults':2500,'fields':'items(id,start,end,status,transparency),nextPageToken'}
                while True:
                    data=self.api('GET','/calendar/v3/calendars/'+quote(cid,safe='')+'/events',params=params)
                    for e in data.get('items',[]):
                        if e.get('status')=='cancelled' or e.get('transparency')=='transparent' or (cid==exclude_event[0] and e['id']==exclude_event[1]):continue
                        def event_time(v):
                            if 'dateTime' in v:return instant(v['dateTime'])
                            # All-day events are conservatively blocked across timezones.
                            return instant(v['date']+'T00:00:00Z')
                        a,b=event_time(e['start']),event_time(e['end'])
                        if 'date' in e['start']:
                            from datetime import timedelta
                            a-=timedelta(days=1);b+=timedelta(days=1)
                        result.append((a,b))
                    if not data.get('nextPageToken'):break
                    params['pageToken']=data['nextPageToken']
            return result
        if self.provider=='google':
            # Google holiday calendars are useful for display, but they must not
            # participate in free/busy calculations or block meeting availability.
            calendars = [
                cid for cid in calendars
                if '#holiday@group.v.calendar.google.com' not in cid
            ]

            if not calendars:
                return result

            data=self.api(
                'POST',
                '/calendar/v3/freeBusy',
                json={
                    'timeMin': start.isoformat(),
                    'timeMax': end.isoformat(),
                    'items': [{'id': c} for c in calendars],
                },
            )
            for cid in calendars:
                c=data.get('calendars',{}).get(cid)
                if c is None or c.get('errors'):
                    raise ProviderError(
                        'Could not check all selected calendars. Try again later.'
                    )
                result.extend(
                    (instant(x['start']), instant(x['end']))
                    for x in c.get('busy',[])
                )
        else:
            for cid in calendars:
                path='/me/calendars/'+quote(cid,safe='')+'/calendarView?'+urlencode({'startDateTime':start.isoformat(),'endDateTime':end.isoformat(),'$select':'id,start,end,showAs,isCancelled','$top':'1000'})
                while path:
                    data=self.api('GET',path)
                    for x in data.get('value',[]):
                        if x.get('isCancelled') or x.get('showAs')=='free' or (exclude_event and cid==exclude_event[0] and x['id']==exclude_event[1]):continue
                        result.append((instant(x['start']['dateTime']+'Z' if not x['start']['dateTime'].endswith('Z') else x['start']['dateTime']),instant(x['end']['dateTime']+'Z' if not x['end']['dateTime'].endswith('Z') else x['end']['dateTime'])))
                    url=data.get('@odata.nextLink');path=url.removeprefix('https://graph.microsoft.com/v1.0') if url else None
        return result
    def calendar_events(self, calendar, start, end):
        """Expand recurring events inside a bounded view; all-day dates stay date-only."""
        result = []
        if self.provider == 'google':
            path = '/calendar/v3/calendars/' + quote(calendar, safe='') + '/events'
            params = {'timeMin': start.isoformat(), 'timeMax': end.isoformat(),
                      'singleEvents': 'true', 'maxResults': 2500,
                      'fields': 'items(id,summary,start,end,status),nextPageToken'}
            while True:
                data = self.api('GET', path, params=params)
                for e in data.get('items', []):
                    if e.get('status') == 'cancelled':
                        continue
                    result.append({'id': e['id'], 'title': e.get('summary') or 'Busy',
                                   'start': e['start'].get('dateTime') or e['start']['date'],
                                   'end': e['end'].get('dateTime') or e['end']['date'],
                                   'all_day': 'date' in e['start']})
                if not data.get('nextPageToken'):
                    break
                params['pageToken'] = data['nextPageToken']
        else:
            path = '/me/calendars/' + quote(calendar, safe='') + '/calendarView?' + urlencode({
                'startDateTime': start.isoformat(), 'endDateTime': end.isoformat(),
                '$select': 'id,subject,start,end,isAllDay,isCancelled', '$top': '1000'})
            while path:
                data = self.api('GET', path)
                for e in data.get('value', []):
                    if e.get('isCancelled'):
                        continue
                    def stamp(value):
                        raw = value['dateTime']
                        if e.get('isAllDay'):
                            return raw[:10]
                        return raw if raw.endswith('Z') or '+' in raw[10:] else raw + 'Z'
                    result.append({'id': e['id'], 'title': e.get('subject') or 'Busy',
                                   'start': stamp(e['start']), 'end': stamp(e['end']),
                                   'all_day': e.get('isAllDay', False)})
                url = data.get('@odata.nextLink')
                path = url.removeprefix('https://graph.microsoft.com/v1.0') if url else None
        return result

    def event_path(self, calendar, event):
        return ('/calendar/v3/calendars/' if self.provider=='google' else '/me/calendars/')+quote(calendar,safe='')+'/events/'+quote(event,safe='')

    def event_details(self, calendar, event):
        raw=self.api('GET',self.event_path(calendar,event))
        if self.provider=='google':
            attendees=[{'email':a.get('email',''),'name':a.get('displayName',''),'status':a.get('responseStatus','needsAction')} for a in raw.get('attendees',[])]
            organizer=raw.get('organizer',{})
            return {'location':raw.get('location',''),'attendees':attendees,'organizer':organizer.get('email',''),
                    'can_edit':bool(organizer.get('self')) and raw.get('status')!='cancelled' and not raw.get('attendeesOmitted',False),
                    'revision':raw.get('etag',''),'raw':raw}
        return {'location':raw.get('location',{}).get('displayName',''),
                'attendees':[{'email':a.get('emailAddress',{}).get('address',''),'name':a.get('emailAddress',{}).get('name',''),'status':a.get('status',{}).get('response','none')} for a in raw.get('attendees',[])],
                'organizer':raw.get('organizer',{}).get('emailAddress',{}).get('address',''),
                'can_edit':bool(raw.get('isOrganizer')) and not raw.get('isCancelled'),
                'revision':raw.get('@odata.etag',''),'raw':raw}

    def update_details(self, calendar, event, location, invitees, revision):
        details=self.event_details(calendar,event)
        if not details['can_edit']:raise ProviderError('Only the meeting organizer can change the location or invite guests.')
        if not revision or details['revision']!=revision:raise ProviderError('This meeting changed. Close and reopen it before saving again.')
        raw=details['raw'];attendees=list(raw.get('attendees',[]));known={a['email'].lower() for a in details['attendees']}
        known.add(details['organizer'].lower())
        for email in invitees:
            if email.lower() in known:continue
            known.add(email.lower())
            attendees.append({'email':email} if self.provider=='google' else {'emailAddress':{'address':email},'type':'required'})
        body={}
        if location!=details['location']:
            body['location']=location if self.provider=='google' else {'displayName':location}
        if len(attendees)!=len(raw.get('attendees',[])):body['attendees']=attendees
        if body:
            self.api('PATCH',self.event_path(calendar,event),json=body,headers={'If-Match':revision},
                     params={'sendUpdates':'all'} if self.provider=='google' else {})
        # A successful PATCH means the provider accepted its invitations/updates.
        return self.event_details(calendar,event)

    def event(self,booking,action):
        cid=quote(booking['external_calendar_id'],safe='');eid=booking.get('external_event_id');cfg=booking['snapshot'];guest=booking['guest']
        if self.provider=='google':
            path='/calendar/v3/calendars/'+cid+'/events'
            body={
                'summary':cfg['title'],
                'description':guest.get('notes',''),
                'start':{
                    'dateTime':booking['start_at'].astimezone(timezone.utc).isoformat(),
                    'timeZone':'UTC'
                },
                'end':{
                    'dateTime':booking['end_at'].astimezone(timezone.utc).isoformat(),
                    'timeZone':'UTC'
                },
                'location':cfg.get('location',''),
                'attendees':[{'email':guest['email'],'displayName':guest['name']}]
            }
            if action=='create':
                body['id']=str(booking['id']).replace('-','')
                # Deterministic event ID makes timeout retries safe.
                import requests as http
                response=http.get('https://www.googleapis.com'+path+'/'+body['id'],headers={'Authorization':'Bearer '+self.access()},timeout=20)
                if response.ok:return response.json()['id']
                if response.status_code!=404:raise ProviderError('Unable to reconcile calendar event.')
                return self.api('POST',path,params={'sendUpdates':'all'},json=body)['id']
        else:
            path='/me/calendars/'+cid+'/events'
            body={'subject':cfg['title'],'body':{'contentType':'text','content':guest.get('notes','')},'start':{'dateTime':booking['start_at'].isoformat(),'timeZone':'UTC'},'end':{'dateTime':booking['end_at'].isoformat(),'timeZone':'UTC'},'location':{'displayName':cfg.get('location','')},'attendees':[{'emailAddress':{'address':guest['email'],'name':guest['name']},'type':'required'}]}
            if action=='create':
                body['transactionId']=str(booking['id'])
                return self.api('POST',path,json=body)['id']
        if not eid:raise ProviderError('Calendar event is not yet synchronized.')
        path+='/'+quote(eid,safe='')
        if action=='cancel':
            # A previously deleted event is already cancelled.
            base='https://www.googleapis.com' if self.provider=='google' else 'https://graph.microsoft.com/v1.0'
            response=requests.delete(base+path,headers={'Authorization':'Bearer '+self.access()},params={'sendUpdates':'all'} if self.provider=='google' else {},timeout=20)
            if not response.ok and response.status_code not in (404,410):raise ProviderError('Unable to cancel the calendar event.')
        else:
            # Preserve attendees added from the calendar popup or the provider itself.
            current=self.api('GET',path)
            attendees=list(current.get('attendees',[]))
            key=lambda a:(a.get('email') if self.provider=='google' else a.get('emailAddress',{}).get('address','')).lower()
            known={key(a) for a in attendees}
            for a in body['attendees']:
                if key(a) not in known:attendees.append(a)
            body['attendees']=attendees
            self.api('PATCH',path,json=body,params={'sendUpdates':'all'} if self.provider=='google' else {})
        return eid
    def send(self,message):
        if self.provider=='google':self.api('POST','/gmail/v1/users/me/messages/send',json={'raw':base64.urlsafe_b64encode(message.as_bytes()).decode()})
        else:
            raw=base64.b64encode(message.as_bytes()).decode()
            response=requests.post('https://graph.microsoft.com/v1.0/me/sendMail',headers={'Authorization':'Bearer '+self.access(),'Content-Type':'text/plain'},data=raw,timeout=20)
            if not response.ok:raise ProviderError('Email delivery could not be confirmed.')
