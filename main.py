import os, re, json, uuid
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

from fastapi import FastAPI, Form, Cookie
from fastapi.responses import HTMLResponse, PlainTextResponse, Response

app = FastAPI()
DB_PATH = "db.json"

# -----------------------------
# DB helpers (simple session buffer)
# -----------------------------
def _load_db() -> Dict[str, Any]:
    if not os.path.exists(DB_PATH):
        return {"sessions": {}}
    try:
        with open(DB_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            data.setdefault("sessions", {})
            return data
    except Exception:
        pass
    return {"sessions": {}}

def _save_db(db: Dict[str, Any]) -> None:
    with open(DB_PATH, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)

def get_or_create_session(session_id: Optional[str]) -> str:
    db = _load_db()
    sessions = db.setdefault("sessions", {})
    if not session_id or session_id not in sessions:
        session_id = uuid.uuid4().hex
        sessions[session_id] = {
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
# Clean / Rules
# -----------------------------
PURE_TIME_RE = re.compile(r"^\s*([01]?\d|2[0-3])([:\.])([0-5]\d)\s*$")     # "6:48" / "21.05"
INLINE_TIME_RE = re.compile(r"\b([01]?\d|2[0-3])([:\.])([0-5]\d)\b")     # "... 13:10 ..."
SPLIT_TIME_RE = re.compile(r"(^|[\t ])([01]?\d|2[0-3])[\t ]+([0-5]\d)(?=($|[\t ]))")  # "13 <tab> 50"

MINUTE_ONLY_RE = re.compile(r"^\s*([0-5]?\d)\s*$")  # 0..59 tek satır
DATE_LINE_RE = re.compile(r"^\s*(date\s*:)?\s*\d{1,2}\s*(şubat|subat|feb|february)\b", re.IGNORECASE)

WA_BRACKET_RE = re.compile(r"^\s*\[\s*\d{1,2}\s*/\s*\d{1,2}.*?\]\s*")
SENDER_COLON_RE = re.compile(r"^\s*[^:\n]{1,60}:\s*")

URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
PHONE_RE = re.compile(r"\+?\d[\d\s\-\(\)]{7,}\d")

FLIGHT_RE = re.compile(r"\b([A-Z]{1,3}\s?\d{2,5}[A-Z]?)\b", re.IGNORECASE)

ADDRESS_HINT_RE = re.compile(
    r"\b("
    r"mah\.?|mahalle|cad\.?|caddesi|cd\.?|sok\.?|sokak|sk\.?|no\s*:|numara|"
    r"posta|zip|postcode|"
    r"istanbul|beyoğlu|fatih|şişli|besiktas|beşiktaş|arnavutkoy|arnavutköy|türkiye|turkiye|\btr\b|"
    r"hotel|otel|residence|suite|apart|"
    r"google|maps|waze"
    r")\b",
    re.IGNORECASE
)

BAD_SINGLE_WORDS = {"OK", "TEST", "LOGO", "OPERASYON", "AIR", "TRANSFER"}

def try_fix_mojibake(s: str) -> str:
    # EyÃ¼p -> Eyüp, Åubat -> Şubat vb. (en yaygınları)
    if not s:
        return s
    if "Ã" in s or "Å" in s or "Ä" in s or "Ð" in s or "Þ" in s:
        for enc in ("latin-1", "cp1252"):
            try:
                fixed = s.encode(enc, errors="ignore").decode("utf-8", errors="ignore")
                if fixed:
                    return fixed
            except Exception:
                pass
    return s

def strip_phones(s: str) -> str:
    s = PHONE_RE.sub("", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def looks_like_address(line: str) -> bool:
    if not line:
        return False
    if ADDRESS_HINT_RE.search(line):
        return True
    commas = line.count(",")
    digits = sum(ch.isdigit() for ch in line)
    if commas >= 3 and len(line) >= 30:
        return True
    if digits >= 8 and len(line) >= 25:
        return True
    if re.search(r"\b-?\d{1,3}\.\d{3,}\b", line):
        return True
    return False

def clean_line(raw_line: str) -> str:
    s = (raw_line or "").strip()
    if not s:
        return ""
    s = try_fix_mojibake(s)
    s = s.replace("\u200e", "").replace("\u200f", "").replace("\ufeff", "")

    s = URL_RE.sub("", s).strip()
    if not s:
        return ""

    s = WA_BRACKET_RE.sub("", s).strip()
    s = SENDER_COLON_RE.sub("", s).strip()
    s = re.sub(r"^\s*(name|i̇sim|isim)\s*:\s*", "", s, flags=re.IGNORECASE).strip()

    s = strip_phones(s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def should_drop_saw(line: str) -> bool:
    return "SAW" in (line or "").upper()

def as_time_text(hh: str, sep: str, mm: str) -> str:
    # Rakamları oynatma: hh nasıl geldiyse öyle kalsın. (06 geldiyse 06, 6 geldiyse 6)
    return f"{hh}{sep}{mm}"

# -----------------------------
# Block Parser (Saat = blok)
# -----------------------------
def parse_to_jobs(raw: str, drop_saw: bool) -> List[Dict[str, str]]:
    """
    Blok mantığı:
    - Saat satırı görünce yeni blok başlar (hour/row)
    - Blok içinde uçuş(lar) ve yolcu(lar) toplanır
    - Bir blokta birden fazla uçuş varsa: aynı saat için ayrı satırlar
    - Saatler asla kaymaz, sıralama korunur (as encountered)
    """
    lines = []
    for rl in (raw or "").splitlines():
        if not rl.strip():
            continue
        if drop_saw and should_drop_saw(rl):
            continue
        cl = clean_line(rl)
        if not cl:
            continue

        # gürültü
        if DATE_LINE_RE.match(cl):
            continue
        if MINUTE_ONLY_RE.match(cl):
            continue
        if cl.strip() == "?":
            continue
        if cl.strip().upper() in BAD_SINGLE_WORDS:
            continue
        if looks_like_address(cl):
            continue

        lines.append(cl)

    # blocks: each is (time_text, content_lines)
    blocks: List[Tuple[str, List[str]]] = []
    current_time: Optional[str] = None
    current_content: List[str] = []

    def flush_block():
        nonlocal current_time, current_content
        if current_time is None:
            return
        blocks.append((current_time, current_content))
        current_content = []

    for cl in lines:
        # Pure time line starts a new block
        m_pure = PURE_TIME_RE.match(cl)
        if m_pure:
            flush_block()
            current_time = as_time_text(m_pure.group(1), m_pure.group(2), m_pure.group(3))
            continue

        # split time (13 <tab> 50) often appears when pasting tables
        m_split = SPLIT_TIME_RE.search(cl)
        if m_split:
            # treat as a new block time, and remove it from content
            flush_block()
            current_time = f"{m_split.group(2)}:{m_split.group(3)}"
            # remaining content after removing split time
            rest = SPLIT_TIME_RE.sub(" ", cl).strip()
            if rest:
                current_content.append(rest)
            continue

        # inline time in same line: start a new block too
        m_in = INLINE_TIME_RE.search(cl)
        if m_in:
            flush_block()
            current_time = as_time_text(m_in.group(1), m_in.group(2), m_in.group(3))
            rest = INLINE_TIME_RE.sub(" ", cl).strip()
            if rest:
                current_content.append(rest)
            continue

        # if we don't have a time yet, we still collect, but it will go under "?"
        if current_time is None:
            current_time = "?"
        current_content.append(cl)

    flush_block()

    # Convert blocks -> jobs
    jobs: List[Dict[str, str]] = []

    for time_text, content in blocks:
        # Always create at least 1 row for each time block (hour/row rule)
        # We'll fill it after extracting. If nothing found, remains ? ?.
        # But if multiple flights found, create multiple rows.
        flights_in_order: List[str] = []
        passengers_by_flight: Dict[str, List[str]] = {}

        def push_flight(f: str):
            if f not in flights_in_order:
                flights_in_order.append(f)
            passengers_by_flight.setdefault(f, [])

        current_flight = "?"

        for line in content:
            # extract flight if exists
            fm = FLIGHT_RE.search(line)
            if fm:
                flight = fm.group(1).replace(" ", "").upper().strip(".,;:")
                # avoid false positives like "NO19" if you want: treat as ? unless known airline codes.
                # For now keep; if it becomes problem we can blacklist.
                current_flight = flight
                push_flight(current_flight)
                # remove flight token from name candidate line
                line = re.sub(re.escape(fm.group(1)), " ", line, flags=re.IGNORECASE).strip()

            # remove generic keywords
            line = re.sub(r"\b(IHL|IST|AIRPORT|HAVALIMANI|ARRIVAL|DEPARTURE|PICK\s*UP|DROP|DATE|FLIGHT)\b", " ", line, flags=re.IGNORECASE).strip()
            line = re.sub(r"\b\d+\s*(yolcu|pax|passengers?)\b", " ", line, flags=re.IGNORECASE).strip()

            # after cleaning, if looks like address/date/noise, skip
            if not line:
                continue
            if DATE_LINE_RE.match(line) or MINUTE_ONLY_RE.match(line) or looks_like_address(line):
                continue

            # Remove leading minute shards: "45 Kristina ..." -> "Kristina ..."
            line = re.sub(r"^\s*([0-5]?\d)\b\s*", "", line).strip()

            # very short lines not a passenger
            if len(line) < 2:
                continue

            # if it's still only uppercase noise
            if line.strip().upper() in BAD_SINGLE_WORDS:
                continue

            # Accept as passenger name (keep line as-is)
            # If no flight seen in block yet, attach to "?"
            push_flight(current_flight)
            passengers_by_flight[current_flight].append(line)

        # If we never saw a flight, keep one bucket "?"
        if not flights_in_order:
            flights_in_order = ["?"]
            passengers_by_flight.setdefault("?", [])

        # Emit jobs in order of flights encountered
        for fl in flights_in_order:
            pax = passengers_by_flight.get(fl, [])
            # uniq preserve order
            seen = set()
            uniq = []
            for p in pax:
                if p not in seen:
                    seen.add(p)
                    uniq.append(p)

            yolcu_cell = ", ".join(uniq) if uniq else "?"
            jobs.append({"saat": time_text, "ucus": fl if fl else "?", "yolcu": yolcu_cell})

    return jobs

def jobs_to_tsv(jobs: List[Dict[str, str]]) -> str:
    lines = ["Saat\t\tUçuş\tYolcu"]
    for j in jobs:
        lines.append(f"{j.get('saat','?')}\t\t{j.get('ucus','?')}\t{j.get('yolcu','?')}")
    return "\n".join(lines)

# -----------------------------
# UI
# -----------------------------
def render_home(buffer_text: str, message: str = "", drop_saw: bool = False) -> str:
    checked = "checked" if drop_saw else ""
    buf_len = len(buffer_text.strip())
    return f"""<!doctype html>
<html lang="tr">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Transfer Parser</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; }}
    .box {{ max-width: 900px; }}
    textarea {{ width: 100%; height: 220px; }}
    .row {{ display:flex; gap:12px; flex-wrap: wrap; margin-top:12px; }}
    button {{ padding:10px 14px; cursor:pointer; }}
    .hint {{ color:#444; margin:10px 0 18px; }}
    .msg {{ color:#0a6; margin:10px 0; }}
    .small {{ font-size: 12px; color:#666; }}
    .counter {{ margin-top: 10px; font-weight: bold; }}
  </style>
</head>
<body>
  <div class="box">
    <h2>Transfer Parser</h2>
    <div class="hint">
      Parça parça yapıştır → <b>Ekle (Kaydet)</b><br/>
      Bittiğinde → <b>Bitti (Çevir)</b> → TSV.
    </div>

    {"<div class='msg'>" + message + "</div>" if message else ""}

    <form method="post" action="/add">
      <label><input type="checkbox" name="drop_saw" value="1" {checked}/> SAW satırlarını çıkar</label>
      <div class="counter">Taslak metin: {buf_len} karakter</div>
      <p class="small">Aşağıya sadece yeni gelen parçayı yapıştır.</p>
      <textarea name="text" placeholder="Yeni parçayı buraya yapıştır..."></textarea>
      <div class="row">
        <button type="submit">Ekle (Kaydet)</button>
      </div>
    </form>

    <form method="post" action="/finish" style="margin-top:14px;">
      <input type="hidden" name="drop_saw" value="{1 if drop_saw else 0}">
      <div class="row">
        <button type="submit">Bitti (Çevir)</button>
        <button type="submit" formaction="/reset" style="background:#eee;">Sıfırla</button>
      </div>
    </form>

    <p class="small" style="margin-top:18px;">Test: <a href="/health">/health</a></p>
  </div>
</body>
</html>"""

def render_result(tsv: str) -> str:
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
    html = render_home(buf, message="", drop_saw=False)
    resp = HTMLResponse(html)
    resp.set_cookie("session_id", sid, max_age=60*60*24*30, samesite="lax")
    return resp

@app.post("/add", response_class=HTMLResponse)
def add_piece(
    text: str = Form(default=""),
    drop_saw: Optional[str] = Form(default=None),
    session_id: Optional[str] = Cookie(default=None),
):
    sid = get_or_create_session(session_id)
    append_to_buffer(sid, (text or "").strip())
    buf = get_buffer(sid)
    drop = bool(drop_saw)
    msg = "Kaydedildi. Yeni parça ekleyebilirsin."
    html = render_home(buf, message=msg, drop_saw=drop)
    resp = HTMLResponse(html)
    resp.set_cookie("session_id", sid, max_age=60*60*24*30, samesite="lax")
    return resp

@app.post("/finish", response_class=HTMLResponse)
def finish(
    drop_saw: int = Form(default=0),
    session_id: Optional[str] = Cookie(default=None),
):
    sid = get_or_create_session(session_id)
    raw = get_buffer(sid)

    jobs = parse_to_jobs(raw, drop_saw=bool(drop_saw))
    tsv = jobs_to_tsv(jobs)

    set_last_result(sid, tsv)
    return HTMLResponse(render_result(tsv))

@app.get("/download")
def download(session_id: Optional[str] = Cookie(default=None)):
    sid = get_or_create_session(session_id)
    tsv = get_last_result(sid)
    headers = {
        "Content-Disposition": "attachment; filename=result.tsv",
        "Content-Type": "text/tab-separated-values; charset=utf-8",
    }
    # BOM: Excel/Sheets Türkçe karakteri daha düzgün açsın
    return Response(content=("\ufeff" + (tsv or "")).encode("utf-8"), headers=headers)

@app.post("/reset", response_class=HTMLResponse)
def reset(session_id: Optional[str] = Cookie(default=None)):
    sid = get_or_create_session(session_id)
    set_buffer(sid, "")
    html = render_home("", message="Taslak sıfırlandı.", drop_saw=False)
    resp = HTMLResponse(html)
    resp.set_cookie("session_id", sid, max_age=60*60*24*30, samesite="lax")
    return resp
