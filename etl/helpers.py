# etl/helpers.py
import hashlib
import datetime

def sha256_hash(x: str):
    if x is None or x == '':
        return None
    return hashlib.sha256(x.encode('utf-8')).hexdigest()

def mask_id(s: str):
    if not s:
        return None
    s = str(s)
    if len(s) <= 4:
        return s
    return '*'*(len(s)-4) + s[-4:]

def parse_date(s: str):
    # dataset dates look like '28-03-2024' -> convert to YYYY-MM-DD
    try:
        return datetime.datetime.strptime(s.strip(), '%d-%m-%Y').date()
    except Exception:
        return None
