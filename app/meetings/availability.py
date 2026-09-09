from datetime import datetime, date, time, timedelta, timezone
from zoneinfo import ZoneInfo

UTC = timezone.utc

def instant(value):
    d = datetime.fromisoformat(str(value).replace('Z', '+00:00')) if not isinstance(value, datetime) else value
    if d.tzinfo is None:
        raise ValueError('A timezone is required.')
    return d.astimezone(UTC)

def merge_busy(intervals):
    result = []
    for a,b in sorted((instant(a),instant(b)) for a,b in intervals):
        if b <= a: continue
        if result and a <= result[-1][1]: result[-1] = (result[-1][0], max(b,result[-1][1]))
        else: result.append((a,b))
    return result

def slots(config, day, busy=(), now=None):
    """UTC candidates; nonexistent wall times are omitted and DST folds deduplicated."""
    now = instant(now or datetime.now(UTC)); zone=ZoneInfo(config['timezone'])
    day = date.fromisoformat(day) if isinstance(day,str) else day
    windows=config.get('exceptions',{}).get(day.isoformat(),config.get('weekly',{}).get(str(day.weekday()),[]))
    duration=timedelta(minutes=config['duration']); before=timedelta(minutes=config.get('buffer_before',0)); after=timedelta(minutes=config.get('buffer_after',0))
    notice=now+timedelta(minutes=config.get('notice',60)); horizon=now+timedelta(days=config.get('horizon',60))
    blocked=merge_busy(busy); result=set()
    for window in windows:
        wall=datetime.combine(day,time.fromisoformat(window[0])); endwall=datetime.combine(day,time.fromisoformat(window[1]))
        while wall < endwall:
            for fold in (0,1):
                start=wall.replace(tzinfo=zone,fold=fold).astimezone(UTC); end=start+duration
                if start.astimezone(zone).replace(tzinfo=None)!=wall: continue
                if end.astimezone(zone).replace(tzinfo=None)>endwall: continue
                if start<notice or start>horizon: continue
                if start-before < datetime.combine(day,time.fromisoformat(window[0]),zone).astimezone(UTC): continue
                if end+after > endwall.replace(tzinfo=zone).astimezone(UTC): continue
                if any(start-before<b and end+after>a for a,b in blocked):continue
                result.add(start)
            wall+=timedelta(minutes=config.get('interval',15))
    return sorted(result)
