# scripts/normalize_addresses.py
import os, sys, json, copy, re
from pathlib import Path

# --- config ---
US_STATE_MAP = {
    "AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California","CO":"Colorado","CT":"Connecticut",
    "DE":"Delaware","FL":"Florida","GA":"Georgia","HI":"Hawaii","ID":"Idaho","IL":"Illinois","IN":"Indiana","IA":"Iowa",
    "KS":"Kansas","KY":"Kentucky","LA":"Louisiana","ME":"Maine","MD":"Maryland","MA":"Massachusetts","MI":"Michigan",
    "MN":"Minnesota","MS":"Mississippi","MO":"Missouri","MT":"Montana","NE":"Nebraska","NV":"Nevada",
    "NH":"New Hampshire","NJ":"New Jersey","NM":"New Mexico","NY":"New York","NC":"North Carolina","ND":"North Dakota",
    "OH":"Ohio","OK":"Oklahoma","OR":"Oregon","PA":"Pennsylvania","RI":"Rhode Island","SC":"South Carolina",
    "SD":"South Dakota","TN":"Tennessee","TX":"Texas","UT":"Utah","VT":"Vermont","VA":"Virginia","WA":"Washington",
    "WV":"West Virginia","WI":"Wisconsin","WY":"Wyoming","DC":"District of Columbia",
}
STREET_SUFFIXES = {
    "ave","avenue","blvd","boulevard","cir","circle","ct","court","dr","drive","hwy","highway",
    "ln","lane","pkwy","parkway","pl","place","rd","road","st","street","ter","terrace","trl","trail","way"
}

def clean(s):
    if s is None: return ""
    return str(s).replace("\u00A0", " ").replace("\u202F", " ").strip()

def to_full_state(s):
    s = clean(s)
    if not s: return ""
    return US_STATE_MAP.get(s.upper(), s)

def fix_country(c):
    c = clean(c)
    if not c: return ""
    if c.lower() in {"united sates","united sate","united state"} or c.upper() == "USA":
        return "United States"
    return c

def parse_city_state_zip_flexible(line):
    s = clean(line)
    m = re.match(r"^(.+?),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$", s)
    if m: return {"city": clean(m.group(1)), "state": m.group(2), "zip": m.group(3)}
    m = re.match(r"^(.+?),\s*([A-Za-z .'-]+)\s+(\d{5}(?:-\d{4})?)$", s)
    if m: return {"city": clean(m.group(1)), "state": clean(m.group(2)), "zip": m.group(3)}
    m = re.match(r"^(.+?)\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$", s)
    if m: return {"city": clean(m.group(1)), "state": m.group(2), "zip": m.group(3)}
    m = re.match(r"^(.+?),\s*([A-Z]{2})$", s)
    if m: return {"city": clean(m.group(1)), "state": m.group(2), "zip": ""}
    m = re.match(r"^(.+?)\s+(\d{5}(?:-\d{4})?)$", s)
    if m: return {"city": clean(m.group(1)), "state": "", "zip": m.group(2)}
    m = re.match(r"^([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$", s)
    if m: return {"city": "", "state": m.group(1), "zip": m.group(2)}
    m = re.match(r"^([A-Za-z .'-]+)\s+(\d{5}(?:-\d{4})?)$", s)
    if m: return {"city": "", "state": clean(m.group(1)), "zip": m.group(2)}
    return {"city": s, "state": "", "zip": ""}

def split_before_st_zip(before: str, fallback_state: str = ""):
    """
    Given 'before' (everything before 'ST ZIP'), try to split into street + city
    using last street suffix as a boundary.
    """
    s = clean(before)
    if not s: return "", "", to_full_state(fallback_state)
    tokens = s.split()
    # If the last token is a 2-letter state stuck here, peel it
    if tokens and len(tokens[-1]) == 2 and tokens[-1].isalpha():
        st = tokens.pop()
        fallback_state = st
    idx = -1
    for i, t in enumerate(tokens):
        if t.lower().strip(".,") in STREET_SUFFIXES:
            idx = i
    if idx != -1:
        street = " ".join(tokens[:idx+1]).strip()
        city = " ".join(tokens[idx+1:]).strip()
        return street, city, to_full_state(fallback_state)
    # fallback: treat all as street
    return s, "", to_full_state(fallback_state)

def parse_address_structured(raw):
    """
    Return a structured dict: {street, city, state (full name), zip, country}
    Supports:
      - "Street\nCity, ST ZIP"
      - "Street City ST\nZIP" or "Street City\nST, ZIP"
      - Single-line "Street City ST ZIP"
      - Single-line "Street, City, ST ZIP"
    """
    blanks = {"street": "", "city": "", "state": "", "zip": "", "country": ""}
    if isinstance(raw, dict):
        # already structured
        out = {
            "street": clean(raw.get("street")),
            "city": clean(raw.get("city")),
            "state": to_full_state(raw.get("state")),
            "zip": clean(raw.get("zip")),
            "country": fix_country(raw.get("country")),
        }
        return out

    s = clean(raw)
    if not s: return blanks

    # strip trailing country
    cm = re.search(r"\s*,\s*(USA|United States|United Sates|United State|Canada)$", s, re.I)
    country = ""
    if cm:
        country = fix_country(cm.group(1))
        s = re.sub(r"\s*,\s*(USA|United States|United Sates|United State|Canada)$", "", s, flags=re.I)

    lines = [clean(x).rstrip(",") for x in s.splitlines() if clean(x)]

    # --- single line ---
    if len(lines) == 1:
        single = lines[0]
        # "... ST ZIP" (with or without comma before ST)
        m = re.match(r"^(.*?)(?:,)?\s+([A-Za-z]{2})\s+(\d{5}(?:-\d{4})?)$", single)
        if m:
            before, st, z = m.group(1), m.group(2), m.group(3)
            street, city, state_full = split_before_st_zip(before, st)
            return {"street": street, "city": city, "state": state_full, "zip": z, "country": country}
        # "..., City, ST ZIP" — fall back to comma splitting
        parts = [p for p in [clean(p) for p in single.split(",")] if p]
        if len(parts) >= 3:
            street = ", ".join(parts[:-2])
            city = parts[-2]
            cs = parse_city_state_zip_flexible(f"{city}, {parts[-1]}")
            return {"street": street, "city": cs["city"] or city, "state": to_full_state(cs["state"]), "zip": cs["zip"], "country": country}
        # If it looks like just "City ST ZIP"
        cs2 = parse_city_state_zip_flexible(single)
        if cs2["state"] or cs2["zip"]:
            return {"street": "", "city": cs2["city"], "state": to_full_state(cs2["state"]), "zip": cs2["zip"], "country": country}
        # else treat as street only
        return {"street": single, "city": "", "state": "", "zip": "", "country": country}

    # --- 2+ lines ---
    first = lines[0]
    last = lines[-1]

    # "ST ZIP" or "ST, ZIP" on the last line
    m = re.match(r"^([A-Za-z]{2}|[A-Za-z .'-]+),?\s+(\d{5}(?:-\d{4})?)$", last)
    if m:
        st, z = m.group(1), m.group(2)
        street, city, state_full = split_before_st_zip(first, st)
        return {"street": street, "city": city, "state": state_full, "zip": z, "country": country}

    # "City, ST ZIP"
    m = re.match(r"^(.+?),\s*([A-Za-z]{2})\s+(\d{5}(?:-\d{4})?)$", last)
    if m:
        return {"street": first, "city": clean(m.group(1)), "state": to_full_state(m.group(2)), "zip": m.group(3), "country": country}

    # "City, ST"
    m = re.match(r"^(.+?),\s*([A-Za-z]{2})$", last)
    if m:
        return {"street": first, "city": clean(m.group(1)), "state": to_full_state(m.group(2)), "zip": "", "country": country}

    # "City ZIP"
    m = re.match(r"^(.+?)\s+(\d{5}(?:-\d{4})?)$", last)
    if m:
        return {"street": first, "city": clean(m.group(1)), "state": "", "zip": m.group(2), "country": country}

    # fallback
    return {"street": first, "city": last, "state": "", "zip": "", "country": country}

def normalize_book_to_structured(b):
    b = copy.deepcopy(b)

    # Author
    b["author_address"] = parse_address_structured(b.get("author_address",""))

    # Agent
    ag = b.get("author_agent") or {}
    if not isinstance(ag, dict): ag = {}
    ag_addr = parse_address_structured(ag.get("address",""))
    ag["address"] = ag_addr
    b["author_agent"] = ag

    # Illustrator
    ill = b.get("illustrator") or {}
    if not isinstance(ill, dict): ill = {}
    ill["address"] = parse_address_structured(ill.get("address",""))
    # Illustrator agent (if present)
    ill_ag = ill.get("agent") or {}
    if isinstance(ill_ag, dict):
        ill_ag["address"] = parse_address_structured(ill_ag.get("address",""))
        ill["agent"] = ill_ag
    b["illustrator"] = ill

    # Drop legacy per-row US School Grade
    for f in b.get("formats", []) or []:
        if isinstance(f, dict) and "US School Grade" in f:
            f.pop("US School Grade", None)

    return b

def locate_books_json():
    env = os.getenv("BOOKS_JSON")
    if env: return Path(env).expanduser()
    here = Path.cwd()
    candidates = [
        here / "Book_data" / "books.json",
        here / "data" / "books.json",
        here / "marble_app" / "routers" / "books.json",
    ]
    for p in candidates:
        if p.exists():
            return p
    return candidates[0]

def main():
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else locate_books_json()
    if not path.exists():
        print(f"Books file not found at {path}. Pass the path explicitly as an argument.")
        sys.exit(1)

    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    backup = path.with_suffix(path.suffix + ".bak")
    with backup.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    new = []
    changed = 0
    for b in data:
        b2 = normalize_book_to_structured(b)
        # Count if author/agent/illustrator were strings before
        def was_string(x): return isinstance(x, str)
        if was_string(b.get("author_address")) \
           or was_string((b.get("author_agent") or {}).get("address")) \
           or was_string((b.get("illustrator") or {}).get("address")) \
           or was_string(((b.get("illustrator") or {}).get("agent") or {}).get("address")):
            changed += 1
        new.append(b2)

    with path.open("w", encoding="utf-8") as f:
        json.dump(new, f, ensure_ascii=False, indent=2)

    print(f"Structured-normalized {changed}/{len(new)} books. Backup written to {backup}")

if __name__ == "__main__":
    main()
