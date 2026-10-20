"""Deterministic frame evaluator shared conceptually with web model.ts; no HTTP/React dependencies."""
import math
from .schema import Clip

RATIOS={'9:16':(9,16),'4:5':(4,5),'1:1':(1,1),'16:9':(16,9)}
def dimensions(ratio,resolution):
    a,b=RATIOS[ratio]
    if a<=b: return resolution,math.floor(resolution*b/a/2+.5)*2
    return math.floor(resolution*a/b/2+.5)*2,resolution

def evaluate(c:Clip,time:float):
    local=max(0,min(c.duration,time-c.start));animated=local+c.animation_offset;length=c.animation_length or c.duration;p=animated/length
    state=c.transform.model_dump();keys=sorted(c.keyframes,key=lambda k:k.time)
    if keys:
        before=next((k for k in reversed(keys) if k.time<=local),keys[0]);after=next((k for k in keys if k.time>=local),keys[-1])
        u=0 if after.time==before.time else (local-before.time)/(after.time-before.time)
        state={k:getattr(before,k)+(getattr(after,k)-getattr(before,k))*u for k in state}
    if c.motion=='zoom_in': state['scale']*=1+.2*p
    if c.motion=='zoom_out': state['scale']*=1.2-.2*p
    if c.motion=='pan_left': state['x']+=.1-.2*p
    if c.motion=='pan_right': state['x']+=-.1+.2*p
    state['wipe']=1.;state['text_fraction']=1.
    for anim,u,entering in [(c.enter,min(1,animated/c.animation_duration),True),(c.exit,max(0,min(1,(length-animated)/c.animation_duration)),False)]:
        if anim in ('fade','dissolve'): state['opacity']*=u
        if anim in ('slide_left','slide_right'): state['x']+=(1-u)*(-1 if anim=='slide_left' else 1)
        if anim=='rise': state['y']+=(1-u)*.2
        if anim in ('pop','zoom'): state['scale']*=max(.01,u if anim=='zoom' else 1+(1-u)*math.sin(u*math.pi*2)*.25);state['opacity']*=u
        if anim=='bounce': state['y']-=abs(math.sin(u*math.pi*3))*(1-u)*.12
        if anim=='wipe': state['wipe']*=u
        if anim=='typewriter': state['text_fraction']*=u
    return state
