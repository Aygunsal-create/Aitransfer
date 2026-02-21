import os, re, json, uuid
from datetime import datetime
from typing import Dict, Any, List, Optional

from fastapi import FastAPI, Form, Cookie
from fastapi.responses import HTMLResponse, PlainTextResponse

app = FastAPI()
DB_PATH = "db.json"

# -----------------------------
# DB helpers
# -----------------------------
def _load_db() -> Dict[str, Any]:
    if not os.path.exists(DB_PATH):
        return {"sessions": {}}
    try:
        with open(DB_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"sessions": {}}

def _save_db(db: Dict[str, Any]) -> None:
    with open(DB_PATH, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)

def get_or_create_session(session_id: Optional[str]) -> str:
    db = _load_db()
    if not session_id or session_id not in db.get("sessions", {}):
        session_id = uuid.uuid4().hex
        db["sessions"][session_id] = {
            "buffer": "",
            "updated_at": datetime.utcnow().isoformat() + "Z",
            "last_result_tsv": ""
        }
        _save_db(db)
    return session_id

def append_to_buffer(session_id: str, text: str) -> None:
    db = _load_db()
    s = db["sessions"].setdefault(session_id, {"buffer": "", "updated_at": "", "last_result_tsv": ""})
    if text:
        if s["buffer"] and not s["buffer"].endswith("\n"):
            s["buffer"] += "\n"
        s["buffer"] += text
    s["updated_at"] = datetime.utcnow().isoformat() + "Z"
    _save_db(db)

def set_buffer(session_id: str, text: str) -> None:
    db = _load_db()
    prev = db.get("sessions", {}).get(session_id, {})
    db["sessions"][session_id] = {
        "buffer": text or "",
        "updated_at": datetime.utcnow().isoformat() + "Z",
        "last_result_tsv": prev.get("last_result_tsv", "")
    }
    _save_db(db)

def get_buffer(session_id: str) -> str:
    db = _load_db()
    return db.get("sessions", {}).get(session_id, {}).get("buffer", "")

def set_last_result(session_id: str, tsv: str) -> None:
    db = _load_db()
    s = db["sessions"].setdefault(session_id, {"buffer": "", "updated_at": "", "last_result_tsv": ""})
    s["last_result_tsv"] = tsv or ""
    s["updated_at"] = datetime.utcnow().isoformat() + "Z"
    _save_db(db)

def get_last_result(session_id: str) -> str:
    db = _load_db()
    return db.get("sessions", {}).get(session_id, {}).get("last_result_tsv", "")


# -----------------------------
# Parsing / cleaning
# -----------------------------
TIME_RE = re.compile(r"\b([01]?\d|2[0-3])[:\.]([0-5]\d)\b")
FLIGHT_RE = re.compile(r"\b([A-Z]{1,3}\s?\d{2,5})\b", re.IGNORECASE)

PHONE_RE = re.compile(r"\+?\d[\d\s\-\(\)]{7,}\d")
WA_HDR_RE = re.compile(r"^\s*\[\s*\d{1,2}\/\d{1,2}\s+\d{1,2}[:\.]\d{2}\s*\]\s*", re.UNICODE)

MONTH_WORDS_RE = re.compile(
    r"\b(şubat|subat|Åubat|ocak|mart|nisan|mayıs|mayis|haziran|temmuz|ağustos|agustos|"
    r"eylül|eylul|ekim|kasım|kasim|aralık|aralik|"
    r"january|february|march|april|may|june|july|august|september|october|november|december|"
    r"jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec)\b",
    re.IGNORECASE
)

# adres ipuçları (satır içinden de temizleyeceğiz)
ADDRESS_WORDS_RE = re.compile(
    r"\b(mah|mahalle|cad|caddesi|cd\.?|sok|sokak|sk\.?|no[:\.]?|apt|apartman|kat|daire|"
    r"istanbul|türkiye|turkiye|beyoğlu|beyoglu|fatih|şişli|sisli|beşiktaş|besiktas|"
    r"arnavutköy|arnavutkoy|karaköy|karakoy|"
    r"street|st\.|road|rd\.|avenue|ave\.|boulevard|blvd\.|"
    r"hotel|hotels|otel|residence|suite|suites)\b",
    re.IGNORECASE
)

POSTAL_RE = re.compile(r"\b\d{5}\b")
SLASH_LOC_RE = re.compile(r"/\s*(istanbul|türkiye|turkiye)\b", re.IGNORECASE)
JUST_SHORT_NUMBER_RE = re.compile(r"^\s*\d{1,2}\s*$")

def fix_mojibake(s: str) -> str:
    if not s:
        return s
    if any(ch in s for ch in ["Ã", "Å", "Ä", "Ð", "Þ", "�"]):
        try:
            return s.encode("latin1", errors="ignore").decode("utf-8", errors="ignore")
        except Exception:
            return s
    return s

def normalize_time_token(token: str) -> Optional[str]:
    m = TIME_RE.search(token or "")
    if not m:
        return None
    # only normalize separator '.' -> ':', keep digits
    return f"{int(m.group(1)):02d}:{m.group(2)}"

def strip_phones(s: str) -> str:
    s = PHONE_RE.sub(" ", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def remove_whatsapp_sender(line: str) -> str:
    """
    'Eyüp Abi BDR: Funda Kara' -> 'Funda Kara'
    But do NOT touch '11:50'
    """
    if ":" not in line:
        return line
    left, right = line.split(":", 1)
    if re.search(r"[A-Za-zÇĞİÖŞÜçğıöşü]{2,}", left):
        # looks like sender label
        return right.strip()
    return line

def clean_passenger_text(s: str) -> str:
    s = fix_mojibake(s or "")
    s = strip_phones(s)

    # remove months / dates words
    s = MONTH_WORDS_RE.sub(" ", s)

    # remove address-ish fragments (inside the passenger text)
    s = POSTAL_RE.sub(" ", s)
    s = SLASH_LOC_RE.sub(" ", s)
    s = ADDRESS_WORDS_RE.sub(" ", s)

    # remove leftover punctuation-heavy parts
    s = re.sub(r"[|]+", " ", s)
    s = re.sub(r"\s{2,}", " ", s).strip(' "\t')

    # if only small number (like '21') -> drop
    if JUST_SHORT_NUMBER_RE.match(s):
        return ""
    return s.strip()

def clean_line(raw_line: str) -> str:
    l = (raw_line or "").strip()
    if not l:
        return ""
    l = fix_mojibake(l)
    l = WA_HDR_RE.sub("", l)
    l = remove_whatsapp_sender(l)
    l = strip_phones(l)
    l = re.sub(r"\s{2,}", " ", l).strip()
    return l

def parse_records(raw: str) -> List[Dict[str, str]]:
    """
    No merging. Each time we have a completed (time+flight+name) we emit ONE row.
    Supports:
    - WhatsApp: name line, flight line, time line
    - Table: phone time flight name
    - Multi-line names in quotes
    """
    rows: List[Dict[str, str]] = []

    cur_time: Optional[str] = None
    cur_flight: Optional[str] = None
    cur_name_parts: List[str] = []

    def flush():
        nonlocal cur_time, cur_flight, cur_name_parts
        name = clean_passenger_text(" ".join(cur_name_parts).strip())
        if cur_time and cur_flight and name:
            rows.append({"saat": cur_time, "ucus": cur_flight, "yolcu": name})
        cur_time, cur_flight, cur_name_parts = None, None, []

    for raw_line in (raw or "").splitlines():
        l = clean_line(raw_line)
        if not l:
            continue

        # find time/flight in this line
        tm = TIME_RE.search(l)
        fm = FLIGHT_RE.search(l)

        found_time = normalize_time_token(tm.group(0)) if tm else None
        found_flight = fm.group(1).replace(" ", "").upper() if fm else None

        # remove time/flight from name candidate
        name_candidate = l
        if tm:
            name_candidate = TIME_RE.sub(" ", name_candidate)
        if fm:
            name_candidate = re.sub(re.escape(fm.group(1)), " ", name_candidate, flags=re.IGNORECASE)
        name_candidate = re.sub(r"\b(Name|İsim)\s*:\s*", " ", name_candidate, flags=re.IGNORECASE)
        name_candidate = name_candidate.strip()
        name_candidate = clean_passenger_text(name_candidate)

        # If we see time+flight together => new record starts, flush old first
        if found_time and found_flight:
            flush()
            cur_time = found_time
            cur_flight = found_flight
            if name_candidate:
                cur_name_parts.append(name_candidate)
            continue

        # If only flight
        if found_flight and not found_time:
            # if an unfinished record already has both time and flight and name -> flush before starting new
            if cur_time and cur_flight and cur_name_parts:
                flush()
            cur_flight = found_flight
            if name_candidate:
                cur_name_parts.append(name_candidate)
            continue

        # If only time
        if found_time and not found_flight:
            cur_time = found_time
            # if we now have time+flight+name -> flush
            if cur_time and cur_flight and cur_name_parts:
                flush()
            continue

        # plain name/continuation
        if name_candidate:
            cur_name_parts.append(name_candidate)
            # if we already have time+flight and enough name, keep waiting for time line? (no)
            # do nothing

    # final flush (if complete)
    flush()
    return rows

def to_tsv(rows: List[Dict[str, str]]) -> str:
    # IMPORTANT: NO MERGE. 1 row per record.
    lines = ["Saat\t\tUçuş\tYolcu"]
    for r in rows:
        saat = (r.get("saat") or "?").strip()
        ucus = (r.get("ucus") or "?").strip().upper()
        yolcu = (r.get("yolcu") or "?").strip()
        lines.append(f"{saat}\t\t{ucus}\t{yolcu}")
    return "\n".join(lines)

# -----------------------------
# UI
# -----------------------------
def render_home(buffer_text: str, message: str = "") -> str:
    buf_len = len((buffer_text or "").strip())
    return f"""<!doctype html>
<html lang="tr">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Transfer Parser</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; }}
    textarea {{ width: 100%; height: 240px; }}
    button {{ padding:10px 14px; cursor:pointer; }}
    .msg {{ color:#0a6; margin:10px 0; }}
    .small {{ font-size: 12px; color:#666; }}
  </style>
</head>
<body>
  <h2>Transfer Parser</h2>
  <p class="small">Metni parça parça ekle → <b>Ekle</b> / Bitince → <b>Bitti</b> → TSV üret.</p>
  {"<div class='msg'>" + message + "</div>" if message else ""}
  <p><b>Taslak:</b> {buf_len} karakter</p>

  <form method="post" action="/add">
    <textarea name="text" placeholder="Yeni parçayı buraya yapıştır..."></textarea><br/>
    <button type="submit">Ekle (Kaydet)</button>
  </form>

  <form method="post" action="/finish" style="margin-top:12px;">
    <button type="submit">Bitti (Çevir)</button>
    <button type="submit" formaction="/reset">Sıfırla</button>
  </form>

  <p class="small" style="margin-top:14px;">Test: <a href="/health">/health</a></p>
</body>
</html>"""

def render_result(tsv: str, found: int) -> str:
    safe = (tsv or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return f"""<!doctype html>
<html lang="tr">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Sonuç (TSV)</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; }}
    pre {{ white-space: pre; overflow-x:auto; background:#f7f7f7; padding:12px; }}
    a.button {{
      display:inline-block; padding:10px 14px; background:#6c2bd9; color:#fff;
      text-decoration:none; border-radius:6px;
    }}
  </style>
</head>
<body>
  <h3>Sonuç (TSV)</h3>
  <a class="button" href="/download">İndir (TSV)</a>
  &nbsp;&nbsp;<a href="/">Geri dön</a>
  <p><b>Kayıt:</b> {found}</p>
  <pre>{safe}</pre>
</body>
</html>"""

# -----------------------------
# Routes
# -----------------------------
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/", response_class=HTMLResponse)
def home(session_id: Optional[str] = Cookie(default=None)):
    sid = get_or_create_session(session_id)
    buf = get_buffer(sid)
    resp = HTMLResponse(render_home(buf))
    resp.set_cookie("session_id", sid, max_age=60*60*24*30)
    return resp

@app.post("/add", response_class=HTMLResponse)
def add_piece(text: str = Form(default=""), session_id: Optional[str] = Cookie(default=None)):
    sid = get_or_create_session(session_id)
    append_to_buffer(sid, (text or "").strip())
    buf = get_buffer(sid)
    resp = HTMLResponse(render_home(buf, message="Kaydedildi. Yeni parça ekleyebilirsin."))
    resp.set_cookie("session_id", sid, max_age=60*60*24*30)
    return resp

@app.post("/finish", response_class=HTMLResponse)
def finish(session_id: Optional[str] = Cookie(default=None)):
    sid = get_or_create_session(session_id)
    raw = get_buffer(sid)

    rows = parse_records(raw)
    tsv = to_tsv(rows)
    set_last_result(sid, tsv)

    found = max(0, len(tsv.splitlines()) - 1)
    return HTMLResponse(render_result(tsv, found=found))

@app.get("/download")
def download(session_id: Optional[str] = Cookie(default=None)):
    sid = get_or_create_session(session_id)
    tsv = get_last_result(sid) or ""
    headers = {
        "Content-Disposition": "attachment; filename=result.tsv",
        "Content-Type": "text/tab-separated-values; charset=utf-8",
    }
    return PlainTextResponse(tsv, headers=headers)

@app.post("/reset", response_class=HTMLResponse)
def reset(session_id: Optional[str] = Cookie(default=None)):
    sid = get_or_create_session(session_id)
    set_buffer(sid, "")
    resp = HTMLResponse(render_home("", message="Taslak sıfırlandı."))
    resp.set_cookie("session_id", sid, max_age=60*60*24*30)
    return resp
