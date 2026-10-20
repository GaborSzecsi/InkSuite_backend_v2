"""Platform-neutral video project v1. Seconds, normalized canvas coordinates, degrees."""
import math
from typing import Literal
from uuid import UUID
from pydantic import BaseModel, Field, ConfigDict, model_validator

class Model(BaseModel):
    model_config=ConfigDict(extra='forbid',allow_inf_nan=False)

class Transform(Model):
    x:float=Field(.5,ge=-2,le=3)
    y:float=Field(.5,ge=-2,le=3)
    scale:float=Field(1,ge=.05,le=5)
    rotation:float=Field(0,ge=-360,le=360)
    opacity:float=Field(1,ge=0,le=1)

class Keyframe(Transform):
    time:float=Field(0,ge=0,le=300)

class Crop(Model):
    x:float=Field(0,ge=0,lt=1)
    y:float=Field(0,ge=0,lt=1)
    width:float=Field(1,gt=0,le=1)
    height:float=Field(1,gt=0,le=1)
    @model_validator(mode='after')
    def bounds(self):
        if self.x+self.width>1.00001 or self.y+self.height>1.00001: raise ValueError('Crop extends beyond the source.')
        return self

Animation=Literal['none','fade','slide_left','slide_right','rise','pop','zoom','typewriter','bounce','wipe','dissolve']
class Clip(Model):
    id:str=Field(min_length=1,max_length=80,pattern=r'^[A-Za-z0-9_-]+$')
    track_id:str=Field(max_length=80)
    kind:Literal['image','video','audio','text']
    asset_id:UUID|None=None
    start:float=Field(0,ge=0,le=300)
    duration:float=Field(5,ge=1/30,le=300)
    trim:float=Field(0,ge=0,le=7200)
    speed:float=Field(1,ge=.25,le=4)
    loop:bool=False
    transform:Transform=Field(default_factory=Transform)
    keyframes:list[Keyframe]=Field(default_factory=list,max_length=30)
    crop:Crop=Field(default_factory=Crop)
    fit:Literal['contain','cover']='contain'
    brightness:float=Field(1,ge=0,le=2)
    contrast:float=Field(1,ge=0,le=2)
    saturation:float=Field(1,ge=0,le=2)
    motion:Literal['none','zoom_in','zoom_out','pan_left','pan_right']='none'
    enter:Animation='none'
    exit:Animation='none'
    animation_offset:float=Field(0,ge=0,le=300)
    animation_length:float|None=Field(None,gt=0,le=300)
    animation_duration:float=Field(.5,ge=1/30,le=10)
    volume:float=Field(1,ge=0,le=2)
    muted:bool=False
    fade_in:float=Field(0,ge=0,le=30)
    fade_out:float=Field(0,ge=0,le=30)
    text:str=Field('',max_length=2000)
    font:Literal['Nunito Sans','Nunito Sans Wide']='Nunito Sans'
    font_size:float=Field(.055,ge=.01,le=.4)
    font_weight:Literal[400,600,700,800]=600
    color:str=Field('#ffffff',pattern=r'^#[0-9a-fA-F]{6}$')
    text_background:str=Field('#000000',pattern=r'^#[0-9a-fA-F]{6}$')
    background_opacity:float=Field(0,ge=0,le=1)
    align:Literal['left','center','right']='center'
    outline:int=Field(0,ge=0,le=10)
    shadow:bool=False

class Track(Model):
    id:str=Field(min_length=1,max_length=80,pattern=r'^[A-Za-z0-9_-]+$')
    name:str=Field(max_length=80)
    kind:Literal['visual','text','audio']
    muted:bool=False
    hidden:bool=False

class Canvas(Model):
    ratio:Literal['9:16','4:5','1:1','16:9']='9:16'
    fps:Literal[30]=30
    background:str=Field('#111827',pattern=r'^#[0-9a-fA-F]{6}$')
    background_asset_id:UUID|None=None
    background_blur:bool=False

class Document(Model):
    schema_version:Literal[1]=1
    canvas:Canvas=Field(default_factory=Canvas)
    tracks:list[Track]=Field(default_factory=list,max_length=12)
    clips:list[Clip]=Field(default_factory=list,max_length=80)
    work_ids:list[UUID]=Field(default_factory=list,max_length=20)
    campaign_id:UUID|None=None
    @model_validator(mode='after')
    def valid(self):
        tracks={t.id:t for t in self.tracks}
        if len(tracks)!=len(self.tracks) or len({c.id for c in self.clips})!=len(self.clips): raise ValueError('Track and clip IDs must be unique.')
        for c in self.clips:
            t=tracks.get(c.track_id)
            if not t or t.kind!=('text' if c.kind=='text' else 'audio' if c.kind=='audio' else 'visual'): raise ValueError('Clip is on an incompatible track.')
            if c.kind!='text' and not c.asset_id: raise ValueError('Media clip needs an asset.')
            if c.start+c.duration>300: raise ValueError('Projects are limited to five minutes.')
            if len({k.time for k in c.keyframes})!=len(c.keyframes) or any(k.time>c.duration for k in c.keyframes): raise ValueError('Keyframes must have unique times within the clip.')
        return self

class ProjectSave(Model):
    name:str=Field('Untitled video',min_length=1,max_length=250)
    document:Document
    revision:int=Field(0,ge=0)
    source_asset_id:UUID|None=None

class RenderRequest(Model):
    resolution:Literal[720,1080]=1080
    revision:int=Field(ge=1)

class MusicLicense(Model):
    category:Literal['Cinematic','Fantasy','Romance','Mystery','Suspense','Inspirational',"Children's",'Upbeat','Corporate','Ambient']='Ambient'
    source:str=Field(min_length=1,max_length=500)
    license_type:str=Field(min_length=1,max_length=250)
    attribution:str=Field('',max_length=2000)
    permitted_usage:str=Field(min_length=1,max_length=2000)
    approved:bool=False
