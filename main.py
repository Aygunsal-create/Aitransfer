import os, re, json, uuid
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional

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
        db["sessions"][session_id] = {"buffer": "", "updated_at": datetime.utcnow().isoformat() + "Z"}
        _save_db(db)
    return session_id

def append_to_buffer(session_id: str, text: str) -> None:
    db = _load_db()
    s = db["sessions"].setdefault(session_id, {"buffer": "", "updated_at": ""})
    if text:
        if s["buffer"] and not s["buffer"].endswith("\n"):
            s["buffer"] += "\n"
        s["buffer"] += text
    s["updated_at"] = datetime.utcnow().isoformat() + "Z"
    _save_db(db)

def set_buffer(session_id: str, text: str) -> None:
    db = _load_db()
    db["sessions"][session_id] = {"buffer": text or "", "updated_at": datetime.utcnow().isoformat() + "Z"}
    _save_db(db)

def get_buffer(session_id: str) -> str:
    db = _load_db()
    return db.get("sessions", {}).get(session_id, {}).get("buffer", "")

# -----------------------------
# Cleaning helpers
# -----------------------------
# time: keep EXACT, but normalize '.' to ':' (18.05 -> 18:05). Do NOT invent/change minutes.
TIME_RE = re.compile(r"\b([01]?\d|2[0-3])[:\.]([0-5]\d)\b")

# flights: allow 1-3 letters + 2-5 digits (TK16, SU2138, W9A5327, A3994, G9321, KL1959 etc.)
FLIGHT_RE = re.compile(r"\b([A-Z]{1,3}\s?\d{2,5})\b", re.IGNORECASE)

# phone-like tokens
PHONE_RE = re.compile(r"\+?\d[\d\s\-\(\)]{7,}\d")

# whatsapp header: [21/2 06:48] Name:
WA_HDR_RE = re.compile(r"^\s*\[\s*\d{1,2}\/\d{1,2}\s+\d{1,2}:\d{2}\s*\]\s*[^:]{1,80}:\s*", re.UNICODE)

# "21 Şubat" / "21 Åubat" / "21 February" / "Feb 21" etc. (we remove date tokens)
DATE_TOKEN_RE = re.compile(
    r"\b(\d{1,2}\s*(Şubat|Subat|Åubat|Mart|Nisan|Mayıs|Mayis|Haziran|Temmuz|Ağustos|Agustos|Eylül|Eylul|Ekim|Kasım|Kasim|Aralık|Aralik|"
    r"January|February|March|April|May|June|July|August|September|October|November|December|"
    r"Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec))\b",
    re.IGNORECASE
)

# address-ish keywords
ADDRESS_HINT_RE = re.compile(
    r"\b(Mah\.?|Mahallesi|Cad\.?|Caddesi|Sk\.?|Sokak|No:|No\.|Apt|Apartman|Blok|Kat|Daire|"
    r"Street|St\.|Avenue|Ave\.|Road|Rd\.|Boulevard|Blvd\.|"
    r"Fatih|Beyoğlu|Besiktas|Beşiktaş|Şişli|Sisli|Arnavutkoy|Arnavutköy|Karakoy|Karaköy|"
    r"İstanbul|Istanbul|Türkiye|Turkey|TR|"
    r"\d{5}\b|/\s*İstanbul\b|/\s*Istanbul\b)\b",
    re.IGNORECASE
)

# remove weird leading dots in copied chats
LEADING_DOTS_RE = re.compile(r"^\s*[.\u2022]+")  # ., .., bullets etc.

def normalize_time_token(t: str) -> str:
    # only normalize separator '.' -> ':' and pad hour, keep minutes as-is
    m = TIME_RE.search(t)
    if not m:
        return t
    hh, mm = m.group(1), m.group(2)
    return f"{int(hh):02d}:{mm}"

def strip_phones(s: str) -> str:
    s = PHONE_RE.sub("", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def clean_line(line: str) -> str:
    l = line.strip()
    if not l:
        return ""

    # remove WhatsApp header
    l = WA_HDR_RE.sub("", l)

    # remove leading dots/bullets
    l = LEADING_DOTS_RE.sub("", l).strip()

    # remove date tokens like "21 Şubat"
    l = DATE_TOKEN_RE.sub("", l)

    # remove phones
    l = strip_phones(l)

    # collapse spaces
    l = re.sub(r"[ \t]+", " ", l).strip()

    return l

def is_address_line(line: str) -> bool:
    # If line contains strong address hints, drop
    if ADDRESS_HINT_RE.search(line):
        return True
    # Very long comma-heavy line often address
    if line.count(",") >= 3 and len(line) > 60:
        return True
    return False

def should_drop_saw(text: str) -> bool:
    return "SAW" in (text or "").upper()

# -----------------------------
# Parsing
# -----------------------------
def parse_records(raw: str, drop_saw: bool) -> List[Dict[str, str]]:
    """
    Supports:
    - WhatsApp blocks: [date time] Sender: Name ... Flight ... HH:MM ...
    - Copied table-ish: +phone 15:20 B2783 Name (with line breaks inside quotes)
    Strategy:
    - Work line by line, but allow "multi-line name" accumulation.
    - A record starts when we can see BOTH a time and a flight nearby.
    - Subsequent lines without time/flight are treated as "name continuation" if we have an open record.
    """
    rows: List[Dict[str, str]] = []

    cur_time: Optional[str] = None
    cur_flight: Optional[str] = None
    cur_name_parts: List[str] = []

    def flush():
        nonlocal cur_time, cur_flight, cur_name_parts
        if cur_time and cur_flight and cur_name_parts:
            name = " ".join([p for p in cur_name_parts if p]).strip()
            name = re.sub(r'\s{2,}', ' ', name).strip(' "“”')
            if name:
                rows.append({"saat": cur_time, "ucus": cur_flight, "yolcu": name})
        cur_time, cur_flight, cur_name_parts = None, None, []

    for raw_line in (raw or "").splitlines():
        if not raw_line.strip():
            continue

        # keep original line for SAW detection
        if drop_saw and should_drop_saw(raw_line):
            continue

        l = clean_line(raw_line)
        if not l:
            continue

        # drop address-only lines
        if is_address_line(l):
            continue

        # extract time + flight from this line if present
        tm = TIME_RE.search(l)
        fm = FLIGHT_RE.search(l)

        # Normalize time token if exists (18.05->18:05). Do not invent time.
        found_time = normalize_time_token(tm.group(0)) if tm else None
        found_flight = fm.group(1).replace(" ", "").upper() if fm else None

        # Remove time/flight tokens from text to get name candidate
        name_candidate = l
        if tm:
            name_candidate = TIME_RE.sub("", name_candidate)
        if fm:
            name_candidate = re.sub(re.escape(fm.group(1)), "", name_candidate, flags=re.IGNORECASE)

        # also remove common labels
        name_candidate = re.sub(r"\b(Name|İsim)\s*:\s*", "", name_candidate, flags=re.IGNORECASE)
        name_candidate = re.sub(r"\b(yolcu|passengers?|pax)\b", "", name_candidate, flags=re.IGNORECASE)
        name_candidate = name_candidate.strip(" -:\t\"“”").strip()
        name_candidate = strip_phones(name_candidate)

        # RECORD LOGIC
        # Case A: line has time+flight => start NEW record (flush previous)
        if found_time and found_flight:
            flush()
            cur_time = found_time
            cur_flight = found_flight
            if name_candidate:
                cur_name_parts.append(name_candidate)
            continue

        # Case B: line has ONLY flight (common in WhatsApp where flight is separate line)
        if found_flight and not found_time:
            # If we already have a record without flight, set it. Else open a "pending record"
            if cur_time and not cur_flight:
                cur_flight = found_flight
            else:
                # If previous record exists but was complete, start a new pending flight-only record
                # (We wait for time)
                if cur_time and cur_flight and cur_name_parts:
                    flush()
                cur_flight = found_flight
            if name_candidate:
                cur_name_parts.append(name_candidate)
            continue

        # Case C: line has ONLY time (common in WhatsApp where time is separate line)
        if found_time and not found_flight:
            if cur_flight and not cur_time:
                cur_time = found_time
            else:
                # If previous record exists but was complete, start new pending time-only record
                if cur_time and cur_flight and cur_name_parts:
                    flush()
                cur_time = found_time
            if name_candidate:
                cur_name_parts.append(name_candidate)
            continue

        # Case D: line has no time/flight => continuation name, or standalone name before flight/time
        if name_candidate:
            cur_name_parts.append(name_candidate)

    flush()
    return rows

def to_tsv(rows: List[Dict[str, str]]) -> str:
    # group by (time, flight) – this matches your “same time same job” expectation when same flight repeats
    grouped: Dict[Tuple[str, str], List[str]] = {}
    order: List[Tuple[str, str]] = []

    for r in rows:
        saat = (r.get("saat") or "?").strip()
        ucus = (r.get("ucus") or "?").strip().upper()
        yolcu = (r.get("yolcu") or "").strip()
        if not yolcu:
            continue
        key = (saat, ucus)
        if key not in grouped:
            grouped[key] = []
            order.append(key)
        if yolcu not in grouped[key]:
            grouped[key].append(yolcu)

    # sort by time (stable), then flight
    def time_sort_key(t: str):
        m = TIME_RE.search(t)
        if not m:
            return (99, 99)
        return (int(m.group(1)), int(m.group(2)))

    order_sorted = sorted(order, key=lambda k: (time_sort_key(k[0]), k[1]))

    lines = ["Saat\t\tUçuş\tYolcu"]
    for (saat, ucus) in order_sorted:
        yolcular = ", ".join(grouped[(saat, ucus)])
        lines.append(f"{saat}\t\t{ucus}\t{yolcular}")
    return "\n".join(lines)

# -----------------------------
# UI
# -----------------------------
def render_home(buffer_text: str, message: str = "", drop_saw: bool = True) -> str:
    checked = "checked" if drop_saw else ""
    buf_len = len(buffer_text.strip())
    return f"""<!doctype html>
<html lang="tr">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>AI Transfer Bot</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; }}
    .box {{ max-width: 900px; }}
    textarea {{ width: 100%; height: 220px; }}
    .row {{ display:flex; gap:12px; flex-wrap: wrap; margin-top:12px; }}
    button {{ padding:10px 14px; cursor:pointer; }}
    .hint {{ color:#444; margin:10px 0 18px; }}
    .msg {{ color:#0a6; margin:10px 0; }}
    .small {{ font-size: 12px; color:#666; }}
  </style>
</head>
<body>
  <div class="box">
    <h2>AI Transfer Bot</h2>
    <div class="hint">
      WhatsApp metnini parça parça ekle → <b>Ekle</b><br/>
      Bitince → <b>Bitti</b> → TSV üret.
    </div>

    {"<div class='msg'>" + message + "</div>" if message else ""}

    <form method="post" action="/add">
      <label><input type="checkbox" name="drop_saw" value="1" {checked}/> SAW satırlarını çıkar</label>
      <p><b>Taslak metin:</b> {buf_len} karakter</p>
      <p class="small">Aşağıya yeni parçayı yapıştır (sadece yeni gelen kısmı). “Ekle” deyince taslağa ekler.</p>
      <textarea name="text" placeholder="Yeni parçayı buraya yapıştır..."></textarea>
      <div class="row">
        <button type="submit">Ekle (Kaydet)</button>
      </div>
    </form>

    <form method="post" action="/finish" style="margin-top:14px;">
      <input type="hidden" name="drop_saw" value="{1 if drop_saw else 0}">
      <div class="row">
        <button type="submit">Bitti (Çevir)</button>
        <button type="submit" formaction="/reset">Sıfırla</button>
      </div>
    </form>

    <p class="small" style="margin-top:18px;">Test: <a href="/health">/health</a></p>
  </div>
</body>
</html>"""

def render_result(tsv: str, count: int) -> str:
    safe = (tsv or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return f"""<!doctype html>
<html lang="tr">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Sonuç (TSV)</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; }}
    .box {{ max-width: 900px; }}
    pre {{ white-space: pre; overflow-x:auto; background:#f7f7f7; padding:12px; }}
    .row {{ display:flex; gap:12px; flex-wrap: wrap; margin-top:12px; }}
    a.button {{
      display:inline-block; padding:10px 14px; background:#6c2bd9; color:#fff;
      text-decoration:none; border-radius:6px;
    }}
  </style>
</head>
<body>
  <div class="box">
    <h3>Sonuç (TSV)</h3>
    <div class="row">
      <a class="button" href="/download">İndir (TSV)</a>
      <a href="/" style="padding:10px 14px;">Geri dön</a>
    </div>
    <p>Bulunan kayıt: <b>{count}</b></p>
    <p>WhatsApp / Sheets'e direkt yapıştırabilirsin.</p>
    <pre>{safe}</pre>
  </div>
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
    html = render_home(buf, message="", drop_saw=True)
    resp = HTMLResponse(html)
    resp.set_cookie("session_id", sid, max_age=60*60*24*30)
    return resp

@app.post("/add", response_class=HTMLResponse)
def add_piece(
    text: str = Form(default=""),
    drop_saw: Optional[str] = Form(default=None),
    session_id: Optional[str] = Cookie(default=None),
):
    sid = get_or_create_session(session_id)
    append_to_buffer(sid, text.strip())
    buf = get_buffer(sid)
    drop = bool(drop_saw)
    html = render_home(buf, message="Kaydedildi. Yeni parça ekleyebilirsin.", drop_saw=drop)
    resp = HTMLResponse(html)
    resp.set_cookie("session_id", sid, max_age=60*60*24*30)
    return resp

@app.post("/finish", response_class=HTMLResponse)
def finish(
    drop_saw: int = Form(default=1),
    session_id: Optional[str] = Cookie(default=None),
):
    sid = get_or_create_session(session_id)
    raw = get_buffer(sid)

    rows = parse_records(raw, drop_saw=bool(drop_saw))
    tsv = to_tsv(rows)

    db = _load_db()
    sess = db["sessions"].setdefault(sid, {"buffer": raw, "updated_at": ""})
    sess["last_result_tsv"] = tsv
    sess["updated_at"] = datetime.utcnow().isoformat() + "Z"
    _save_db(db)

    return HTMLResponse(render_result(tsv, count=max(0, len(tsv.splitlines()) - 1)))

@app.get("/download")
def download(session_id: Optional[str] = Cookie(default=None)):
    sid = get_or_create_session(session_id)
    db = _load_db()
    tsv = db.get("sessions", {}).get(sid, {}).get("last_result_tsv", "")
    headers = {
        "Content-Disposition": "attachment; filename=result.tsv",
        "Content-Type": "text/tab-separated-values; charset=utf-8",
    }
    return PlainTextResponse(tsv or "", headers=headers)

@app.post("/reset", response_class=HTMLResponse)
def reset(session_id: Optional[str] = Cookie(default=None)):
    sid = get_or_create_session(session_id)
    set_buffer(sid, "")
    html = render_home("", message="Taslak sıfırlandı.", drop_saw=True)
    resp = HTMLResponse(html)
    resp.set_cookie("session_id", sid, max_age=60*60*24*30)
    return resp
