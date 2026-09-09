from datetime import date, time
from typing import Literal
from zoneinfo import ZoneInfo
from pydantic import BaseModel, Field, EmailStr, field_validator, model_validator

class Question(BaseModel):
    id: str = Field(min_length=1,max_length=50,pattern=r'^[a-zA-Z0-9_-]+$')
    label: str = Field(min_length=1,max_length=200)
    required: bool=False
    kind: Literal['short','long']='short'

class MeetingType(BaseModel):
    title: str=Field(min_length=1,max_length=120)
    description: str=Field(default='',max_length=2000)
    slug: str=Field(min_length=12,max_length=100,pattern=r'^[a-z0-9-]+$')
    duration: int=Field(default=30,ge=1,le=480)
    timezone: str='America/Los_Angeles'
    weekly: dict[str,list[list[str]]]=Field(default_factory=dict)
    exceptions: dict[str,list[list[str]]]=Field(default_factory=dict)
    notice: int=Field(default=60,ge=0,le=43200)
    horizon: int=Field(default=60,ge=1,le=365)
    interval: int=Field(default=15,ge=5,le=120)
    buffer_before: int=Field(default=0,ge=0,le=240)
    buffer_after: int=Field(default=0,ge=0,le=240)
    location_type: Literal['manual','phone','in_person','tbd']='tbd'
    location: str=Field(default='',max_length=500)
    confirmation: str=Field(default='Your meeting is booked.',max_length=1000)
    questions: list[Question]=Field(default_factory=list,max_length=10)
    conflicts: list[dict[str,str]]=Field(default_factory=list,max_length=20)
    destination: dict[str,str]=Field(default_factory=dict)
    active: bool=True
    @field_validator('timezone')
    @classmethod
    def tz(cls,v):
        try: ZoneInfo(v)
        except Exception: raise ValueError('Choose a valid IANA timezone.')
        return v
    @model_validator(mode='after')
    def validate_windows(self):
        if len(self.exceptions)>366: raise ValueError('Too many date exceptions.')
        if len({q.id for q in self.questions})!=len(self.questions):raise ValueError('Question IDs must be unique.')
        for key,windows in list(self.weekly.items())+list(self.exceptions.items()):
            if key in self.weekly and key not in [str(i) for i in range(7)]: raise ValueError('Invalid weekday.')
            if key in self.exceptions:date.fromisoformat(key)
            if len(windows)>8:raise ValueError('Use at most eight windows per day.')
            previous=None
            for w in sorted(windows):
                if len(w)!=2:raise ValueError('Enter a start and end time.')
                a,b=map(time.fromisoformat,w)
                if a>=b or (previous and a<previous):raise ValueError('Availability windows must not overlap or cross midnight.')
                previous=b
        if self.location_type=='manual' and self.location and not self.location.startswith('https://'):raise ValueError('Meeting URLs must use HTTPS.')
        return self

class Guest(BaseModel):
    name: str=Field(min_length=1,max_length=150)
    email: EmailStr
    timezone: str='UTC'
    phone: str=Field(default='',max_length=50)
    notes: str=Field(default='',max_length=2000)
    answers: dict[str,str]=Field(default_factory=dict)
    start: str
    idempotency_key: str=Field(min_length=16,max_length=100)
    @field_validator('timezone')
    @classmethod
    def tz(cls,v):ZoneInfo(v);return v
    @field_validator('answers')
    @classmethod
    def answers_limit(cls,v):
        if len(v)>10 or any(len(k)>50 or len(a)>2000 for k,a in v.items()):raise ValueError('Answers are too long.')
        return v

    @field_validator('start')
    @classmethod
    def start_valid(cls,v):
        from .availability import instant
        instant(v)
        return v
