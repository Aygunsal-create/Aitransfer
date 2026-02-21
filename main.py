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
# PARSE FIRST (DO NOT BREAK TIMES)
# -----------------------------
TIME_RE = re.compile(r"\b([01]?\d|2[0-3])[:\.]([0-5]\d)\b")  # 18:05 or 18.05
FLIGHT_RE = re.compile(r"\b([A-Z]{1,3}\s?\d{2,5})\b", re.IGNORECASE)

PHONE_RE = re.compile(r"\+?\d[\d\s\-\(\)]{7,}\d")
WA_TS_RE = re.compile(r"^\s*\[\s*\d{1,2}\/\d{1,2}\s+\d{1,2}[:\.]\d{2}\s*\]\s*")  # [21/2 06:48]

# Tarihi "kalıp halinde" sil (gün tek başına kalmasın!)
DATE_PHRASE_RE = re.compile(
    r"\b(\d{1,2})\s*(Şubat|Subat|Ocak|Mart|Nisan|Mayıs|Mayis|Haziran|Temmuz|Ağustos|Agustos|Eylül|Eylul|Ekim|Kasım|Kasim|Aralık|Aralik|"
    r"JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC|"
    r"January|February|March|April|May|June|July|August|September|October|November|December)\b",
    re.IGNORECASE
)
DATE_LABEL_RE = re.compile(r"\b(Date|Tarih)\s*:\s*.*$", re.IGNORECASE)

ADDRESS_HINT_RE = re.compile(
    r"\b(mah|mahalle|cad|caddesi|cd\.?|sok|sokak|sk\.?|no[:\.]?|apt|apartman|kat|daire|"
    r"istanbul|türkiye|turkiye|beyoğlu|beyoglu|fatih|şişli|sisli|beşiktaş|besiktas|"
    r"street|st\.|road|rd\.|avenue|ave\.|boulevard|blvd\.|hotel|hotels|otel|residence|suite|suites|"
    r"\b\d{5}\b|/\s*istanbul\b|/\s*türkiye\b|/\s*turkiye\b)\b",
    re.IGNORECASE
)

def normalize_time_token(token: str) -> Optional[str]:
    m = TIME_RE.search(token or "")
    if not m:
        return None
    # only normalize '.' -> ':' and pad hour, keep digits
    return f"{int(m.group(1)):02d}:{m.group(2)}"

def strip_phones(s: str) -> str:
    s = PHONE_RE.sub(" ", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def remove_whatsapp_sender_safely(line: str) -> str:
    """
    WhatsApp: '[21/2 06:48] Eyüp Abi BDR: Funda Kara' -> 'Funda Kara'
    IMPORTANT: Do NOT remove "11:50" because that's not a sender label.
    Rule: Only remove sender if line originally had WhatsApp timestamp OR starts with a sender-ish text then ':' then space.
    """
    original = line.strip()

    # remove leading [.. ..]
    line = WA_TS_RE.sub("", original).strip()

    # If remaining has "NAME: ..." pattern, remove only if left side has letters (not time)
    if ":" in line:
        left, right = line.split(":", 1)
        # left side must contain at least 2 letters and NOT be a time like 11:50
        if re.search(r"[A-Za-zÇĞİÖŞÜçğıöşü]{2,}", left) and not TIME_RE.search(left):
            return right.strip()

    return line

def clean_passenger_text_only(s: str) -> str:
    """
    Only applied to passenger/name field AFTER time/flight are already decided.
    Removes: phones, date phrases, address-y fragments.
    Does NOT do any risky "remove all small numbers" stuff.
    """
    s = (s or "").strip()
    if not s:
        return ""

    s = strip_phones(s)
    s = DATE_LABEL_RE.sub("", s)
    s = DATE_PHRASE_RE.sub(" ", s)   # removes "21 Şubat" together
    s = re.sub(r"\s{2,}", " ", s).strip()

    # If passenger text contains address-ish junk, drop those words/fragments
    # but keep remaining name.
    parts = []
    for chunk in re.split(r"[,\|]", s):
        c = chunk.strip()
        if not c:
            continue
        if ADDRESS_HINT_RE.search(c) and len(c) > 18:
            continue
        parts.append(c)

    s = ", ".join(parts).strip()
    s = re.sub(r"\s{2,}", " ", s).strip(' "\t')

    return s

def parse_records(raw: str) -> List[Dict[str, str]]:
    """
    NO MERGE.
    Record closes only when we have both time + flight + some name.
    We ALWAYS read time/flight first; cleaning happens after.
    """
    rows: List[Dict[str, str]] = []

    cur_time: Optional[str] = None
    cur_flight: Optional[str] = None
    cur_names: List[str] = []

    def flush_if_complete():
        nonlocal cur_time, cur_flight, cur_names
        name = clean_passenger_text_only(" ".join(cur_names).strip())
        if cur_time and cur_flight and name:
            rows.append({"saat": cur_time, "ucus": cur_flight, "yolcu": name})
            cur_time, cur_flight, cur_names = None, None, []
            return True
        return False

    for raw_line in (raw or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue

        # Remove WhatsApp prefix/sender WITHOUT touching times
        line = remove_whatsapp_sender_safely(line)
        line = strip_phones(line)
        line = re.sub(r"\s{2,}", " ", line).strip()

        if not line:
            continue

        # 1) READ TIME/FLIGHT FIRST (from current line)
        tm = TIME_RE.search(line)
        fm = FLIGHT_RE.search(line)

        found_time = normalize_time_token(tm.group(0)) if tm else None
        found_flight = fm.group(1).replace(" ", "").upper() if fm else None

        # Build a "name candidate" by removing time/flight tokens only
        name_candidate = line
        if tm:
            name_candidate = TIME_RE.sub(" ", name_candidate)
        if fm:
            name_candidate = re.sub(re.escape(fm.group(1)), " ", name_candidate, flags=re.IGNORECASE)
        name_candidate = re.sub(r"\b(Name|İsim)\s*:\s*", " ", name_candidate, flags=re.IGNORECASE).strip()

        # If this line is clearly an address line AND has no time/flight, skip it
        if not found_time and not found_flight and ADDRESS_HINT_RE.search(name_candidate) and len(name_candidate) > 25:
            continue

        # 2) STATE MACHINE (no merging)
        if found_time and found_flight:
            # new record start => flush previous if complete, then start fresh
            flush_if_complete()
            cur_time = found_time
            cur_flight = found_flight
            if name_candidate:
                cur_names.append(name_candidate)
            flush_if_complete()
            continue

        if found_flight and not found_time:
            # set flight for current pending record
            cur_flight = found_flight
            if name_candidate:
                cur_names.append(name_candidate)
            flush_if_complete()
            continue

        if found_time and not found_flight:
            cur_time = found_time
            if name_candidate:
                cur_names.append(name_candidate)
            flush_if_complete()
            continue

        # no time/flight -> treat as name continuation
        if name_candidate:
            cur_names.append(name_candidate)
            flush_if_complete()

    # final
    flush_if_complete()
    return rows

def to_tsv(rows: List[Dict[str, str]]) -> str:
    lines = ["Saat\t\tUçuş\tYolcu"]
    for r in rows:
        lines.append(f"{r.get('saat','?')}\t\t{r.get('ucus','?')}\t{r.get('yolcu','?')}")
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
