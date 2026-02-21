import os, re, json, uuid
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

from fastapi import FastAPI, Form, Cookie
from fastapi.responses import HTMLResponse, PlainTextResponse, JSONResponse

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
            "last_result_tsv": "",
            "last_debug": {}
        }
        _save_db(db)
    return session_id

def append_to_buffer(session_id: str, text: str) -> None:
    db = _load_db()
    s = db["sessions"].setdefault(session_id, {"buffer": "", "updated_at": "", "last_result_tsv": "", "last_debug": {}})
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
        "last_result_tsv": prev.get("last_result_tsv", ""),
        "last_debug": prev.get("last_debug", {})
    }
    _save_db(db)

def get_buffer(session_id: str) -> str:
    db = _load_db()
    return db.get("sessions", {}).get(session_id, {}).get("buffer", "")

def set_last(session_id: str, tsv: str, debug: Dict[str, Any]) -> None:
    db = _load_db()
    s = db["sessions"].setdefault(session_id, {"buffer": "", "updated_at": "", "last_result_tsv": "", "last_debug": {}})
    s["last_result_tsv"] = tsv or ""
    s["last_debug"] = debug or {}
    s["updated_at"] = datetime.utcnow().isoformat() + "Z"
    _save_db(db)

def get_last_result(session_id: str) -> str:
    db = _load_db()
    return db.get("sessions", {}).get(session_id, {}).get("last_result_tsv", "")

def get_last_debug(session_id: str) -> Dict[str, Any]:
    db = _load_db()
    return db.get("sessions", {}).get(session_id, {}).get("last_debug", {})


# -----------------------------
# RULE-BASED PARSER (NO AI)
# -----------------------------
# Job time: 21:00 or 18.05
JOB_TIME_RE = re.compile(r"\b([01]?\d|2[0-3])[:\.]([0-5]\d)\b")
FLIGHT_RE   = re.compile(r"\b([A-Z]{1,3}\s?\d{2,5})\b", re.IGNORECASE)
PHONE_RE    = re.compile(r"\+?\d[\d\s\-\(\)]{7,}\d")

# WhatsApp prefix: [21/2 20:35] Eyüp Abi BDR:
WA_PREFIX_RE = re.compile(
    r"^\s*\[\s*\d{1,2}\/\d{1,2}\s+\d{1,2}[:\.]\d{2}\s*\]\s*[^:]{0,160}:\s*",
    re.UNICODE
)

DATE_PHRASE_RE = re.compile(
    r"\b\d{1,2}\s*(Şubat|Subat|Ocak|Mart|Nisan|Mayıs|Mayis|Haziran|Temmuz|Ağustos|Agustos|Eylül|Eylul|Ekim|Kasım|Kasim|Aralık|Aralik|"
    r"JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC|"
    r"January|February|March|April|May|June|July|August|September|October|November|December)\b",
    re.IGNORECASE
)

IGNORE_LINE_RE = re.compile(
    r"^(uçak\s*inmiş|ucak\s*inmis|air\s*transfer|logo\b.*|operasyon\b.*|"
    r"pick\s*up\s*time.*|pick\s*up\s*from.*|drop\s*at.*|contact.*|passengers.*|pax.*|arrival.*|departure.*)$",
    re.IGNORECASE
)

ADDRESS_HINT_RE = re.compile(
    r"\b(mah|mahalle|cad|caddesi|cd\.?|sok|sokak|sk\.?|no[:\.]?|apt|apartman|kat|daire|"
    r"istanbul|türkiye|turkiye|beyoğlu|beyoglu|fatih|şişli|sisli|beşiktaş|besiktas|"
    r"arnavutköy|arnavutkoy|karaköy|karakoy|"
    r"street|st\.|road|rd\.|avenue|ave\.|boulevard|blvd\.|"
    r"hotel|hotels|otel|residence|suite|suites|"
    r"\d{5}\b)\b",
    re.IGNORECASE
)

def normalize_time_token(token: str) -> Optional[str]:
    m = JOB_TIME_RE.search(token or "")
    if not m:
        return None
    return f"{int(m.group(1)):02d}:{m.group(2)}"

def clean_line(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""

    # Remove WhatsApp prefix completely (20:35 here is NOT job time)
    s = WA_PREFIX_RE.sub("", s).strip()

    # Remove phones
    s = PHONE_RE.sub(" ", s)

    # Remove date phrase as whole ("21 Şubat" etc.)
    s = DATE_PHRASE_RE.sub(" ", s)

    # Remove numbering "1. NAME"
    s = re.sub(r"^\s*\d+\s*[\.\)\-]\s*", "", s).strip()

    s = re.sub(r"\s{2,}", " ", s).strip()

    # Drop status lines
    if IGNORE_LINE_RE.fullmatch(s):
        return ""

    # Drop address-like long lines (unless they contain a job time)
    if ADDRESS_HINT_RE.search(s) and len(s) > 30 and not JOB_TIME_RE.search(s):
        return ""

    return s

def clean_passenger(text: str) -> str:
    t = (text or "").strip()
    t = PHONE_RE.sub(" ", t)
    t = DATE_PHRASE_RE.sub(" ", t)
    t = re.sub(r"\s{2,}", " ", t).strip(' "\t')

    # remove obvious address leftovers inside passenger
    if ADDRESS_HINT_RE.search(t) and len(t) > 25:
        t2 = t.split(",")[0].strip()
        if ADDRESS_HINT_RE.search(t2) and len(t2) > 20:
            return ""
        t = t2

    return t

def parse_hour_row(raw: str) -> Tuple[List[Dict[str, str]], Dict[str, Any]]:
    """
    HARD RULE: Each JOB_TIME occurrence => 1 output row.
    Everything collected before that time belongs to that row.
    Flight = last flight seen in that block.
    """
    rows: List[Dict[str, str]] = []
    debug_times: List[str] = []
    debug_time_lines: List[str] = []

    block: List[str] = []
    last_flight: Optional[str] = None

    def flush(job_time: str):
        nonlocal block, last_flight
        passenger_raw = " ".join(block).strip()
        passenger = clean_passenger(passenger_raw) or "?"
        flight = (last_flight or "?").replace(" ", "").upper()
        rows.append({"saat": job_time, "ucus": flight, "yolcu": passenger})
        block = []
        last_flight = None

    for raw_line in (raw or "").splitlines():
        line = clean_line(raw_line)
        if not line:
            continue

        # capture flight
        fm = FLIGHT_RE.search(line)
        if fm:
            last_flight = fm.group(1)

        # capture job time -> flush immediately
        tm = JOB_TIME_RE.search(line)
        if tm:
            jt = normalize_time_token(tm.group(0))
            if jt:
                debug_times.append(jt)
                debug_time_lines.append(line)

            # remove the time token from remainder, and remove flight token from remainder
            rest = JOB_TIME_RE.sub(" ", line).strip()
            if fm:
                rest = re.sub(re.escape(fm.group(1)), " ", rest, flags=re.IGNORECASE).strip()
            if rest:
                block.append(rest)

            if jt:
                flush(jt)
            continue

        # ignore pure flight-only line in passenger
        if fm and re.sub(re.escape(fm.group(1)), "", line, flags=re.IGNORECASE).strip() == "":
            continue

        block.append(line)

    # DO NOT flush without time (hour/row rule)
    debug = {
        "job_time_count": len(debug_times),
        "job_times": debug_times[:50],
        "sample_lines_with_time": debug_time_lines[:20],
    }
    return rows, debug

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
    code {{ background:#f3f3f3; padding:2px 4px; }}
  </style>
</head>
<body>
  <h2>Transfer Parser</h2>
  <p class="small">Metni parça parça ekle → <b>Ekle</b> / Bitince → <b>Bitti</b> → TSV üret.</p>
  <p class="small">Debug: <code>/debug</code> (saatleri kaç tane yakaladı gösterecek)</p>
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

def render_result(tsv: str, found: int, time_count: int) -> str:
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
    .small {{ font-size: 12px; color:#666; }}
  </style>
</head>
<body>
  <h3>Sonuç (TSV)</h3>
  <a class="button" href="/download">İndir (TSV)</a>
  &nbsp;&nbsp;<a href="/">Geri dön</a>
  <p><b>Kayıt:</b> {found} | <b>Yakalanan saat:</b> {time_count}</p>
  <p class="small">Eğer “yakalanan saat” düşükse, metinde saatler bozulmuş demektir (örn <code>:20</code> gibi).</p>
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

    rows, debug = parse_hour_row(raw)
    tsv = to_tsv(rows)
    set_last(sid, tsv, debug)

    found = max(0, len(tsv.splitlines()) - 1)
    return HTMLResponse(render_result(tsv, found=found, time_count=debug.get("job_time_count", 0)))

@app.get("/debug")
def debug(session_id: Optional[str] = Cookie(default=None)):
    sid = get_or_create_session(session_id)
    return JSONResponse(get_last_debug(sid) or {})

@app.get("/download")
def download(session_id: Optional[str] = Cookie(default=None)):
    sid = get_or_create_session(session_id)
    tsv = get_last_result(sid) or ""
    # UTF-8 BOM so Turkish chars keep correct in Excel/Sheets sometimes
    tsv = "\ufeff" + tsv
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
