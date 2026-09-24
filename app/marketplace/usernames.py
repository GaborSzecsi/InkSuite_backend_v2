"""Readable, unique reader handles and reserved top-level routes."""
import re
import unicodedata
from .core import one

RESERVED = set("app api marketplace login logout register about resources terms privacy book books book-info contracts dashboard financials royalty settings platform project_management meeting meetings review accept-invite reset-password banking email-requests images favicon robots sitemap admin support help www".split())

def name_username(name):
    text=unicodedata.normalize("NFKD",name).encode("ascii","ignore").decode().lower()
    base=re.sub(r"[^a-z0-9]+","_",text).strip("_")[:30].rstrip("_")
    return base if len(base)>=3 else (base+"_reader" if base else "reader")

def available_username(cur, requested, user_id=None, lock=False):
    if lock:
        # All profile/invitation writers serialize allocation, including suffix collisions.
        cur.execute("SELECT pg_advisory_xact_lock(721943801)")
    base=requested[:30]
    candidate=base
    number=1
    while candidate in RESERVED or one(cur,"SELECT 1 AS taken FROM marketplace_profiles WHERE username=%s AND (%s::uuid IS NULL OR user_id<>%s::uuid)",(candidate,user_id,user_id)):
        number+=1
        suffix="_"+str(number)
        candidate=base[:30-len(suffix)].rstrip("_")+suffix
    return candidate
