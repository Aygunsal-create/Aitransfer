import os, re, json, uuid
from datetime import datetime
from typing import Dict, Any, List, Optional

from fastapi import FastAPI, Form, Cookie
from fastapi.responses import HTMLResponse, Response

app = FastAPI()
DB_PATH = "db.json"

# -----------------------------
# DB (session buffer)
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
# Cleaning / Regex
# -----------------------------
# Pickup time token: 0:00-23:59 or 14.05 (KEEP original separator)
PICKUP_TIME_TOKEN_RE = re.compile(r"\b([01]?\d|2[0-3])([:\.])([0-5]\d)\b")

# WhatsApp header "[21/2 06:48]" -> remove
WA_BRACKET_RE = re.compile(r"\[\s*\d{1,2}\s*/\s*\d{1,2}\s+\d{1,2}:\d{2}\s*\]")

# Sender prefix "Eyüp Abi BDR:" (or any "something:")
SENDER_PREFIX_RE = re.compile(r"(^|\n)\s*[^:\n]{1,70}:\s*")

# Flight line (often alone)
FLIGHT_LINE_RE = re.compile(r"^\s*([A-Z]{1,3}\s?\d{2,5}[A-Z]?)\s*$", re.IGNORECASE)

# URL / phone
URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
PHONE_RE = re.compile(r"\+?\d[\d\s\-\(\)]{7,}\d")

# Date-like junk (21 Şubat etc)
DATE_JUNK_RE = re.compile(r"\b\d{1,2}\s*(şubat|subat|mart|nisan|mayıs|mayis|haziran|temmuz|ağustos|agustos|eylül|eylul|ekim|kasım|kasim|aralık|aralik)\b", re.IGNORECASE)

# Minute-only junk lines: "50", "15"
MINUTE_ONLY_RE = re.compile(r"^\s*([0-5]?\d)\s*$")

# Address hints
ADDRESS_HINT_RE = re.compile(
    r"\b("
    r"mah\.?|mahalle|cad\.?|caddesi|cd\.?|sok\.?|sokak|sk\.?|no\s*:|numara|"
    r"istanbul|beyoğlu|fatih|şişli|beşiktaş|besiktas|arnavutköy|arnavutkoy|türkiye|turkiye|\btr\b|"
    r"hotel|otel|residence|suite|apart|"
    r"google|maps|waze|"
    r"refik|saydam|kemeraltı|kemeralti|"
    r"mah\s"
    r")\b",
    re.IGNORECASE
)

# Mixed code to remove from names e.g. A4*3069
MIXED_CODE_RE = re.compile(r"\b[A-Z]\d[\*\-]\d{3,6}\b", re.IGNORECASE)

def try_fix_mojibake(s: str) -> str:
    """Fix common WhatsApp encoding mojibake like 'Å' -> 'Ş'."""
    if not s:
        return s
    if "Ã" in s or "Å" in s or "Ä" in s:
        for enc in ("latin-1", "cp1252"):
            try:
                fixed = s.encode(enc, errors="ignore").decode("utf-8", errors="ignore")
                if fixed and (("Ã" not in fixed) and ("Å" not in fixed) and ("Ä" not in fixed)):
                    return fixed
            except Exception:
                pass
    return s

def looks_like_address(s: str) -> bool:
    if not s:
        return False
    if ADDRESS_HINT_RE.search(s):
        return True
    # heuristic: many commas + digits
    commas = s.count(",")
    digits = sum(ch.isdigit() for ch in s)
    if commas >= 3 and len(s) >= 30:
        return True
    if digits >= 8 and len(s) >= 25:
        return True
    return False

def clean_line(line: str) -> str:
    s = (line or "").strip()
    if not s:
        return ""
    s = try_fix_mojibake(s)
    s = s.replace("\u200e", "").replace("\u200f", "").replace("\ufeff", "")
    s = URL_RE.sub(" ", s)
    s = PHONE_RE.sub(" ", s)
    s = MIXED_CODE_RE.sub(" ", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def normalize_whatsapp_text(raw: str) -> str:
    """
    WhatsApp sometimes comes as a single paragraph. We force line breaks around:
    - WhatsApp [dd/mm hh:mm]
    - Flight codes (TK1710 etc)
    - Pickup times (11:50 / 14.05)
    Then we can parse reliably.
    """
    s = raw or ""
    s = try_fix_mojibake(s)

    # make sure bracket timestamps are separated
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"\s+\n", "\n", s)

    # put newlines around [21/2 06:48]
    s = re.sub(r"(\[\s*\d{1,2}\s*/\s*\d{1,2}\s+\d{1,2}:\d{2}\s*\])", r"\n\1\n", s)

    # put newlines around flight-like tokens anywhere
    s = re.sub(r"\b([A-Z]{1,3}\s?\d{2,5}[A-Z]?)\b", r"\n\1\n", s)

    # put newlines around pickup times
    s = re.sub(r"\b([01]?\d|2[0-3])([:\.])([0-5]\d)\b", r"\n\1\2\3\n", s)

    # collapse too many blank lines
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

# -----------------------------
# Parser: Name(s) -> Flight -> Pickup Time commits record
# -----------------------------
def parse_records(raw: str) -> List[Dict[str, str]]:
    # Remove WhatsApp timestamps globally (not pickup time)
    txt = normalize_whatsapp_text(raw)
    txt = WA_BRACKET_RE.sub("\n", txt)
    txt = SENDER_PREFIX_RE.sub("\n", txt)

    rows: List[Dict[str, str]] = []
    pending_names: List[str] = []
    pending_flight: str = "?"

    for raw_line in txt.splitlines():
        cl = clean_line(raw_line)
        if not cl:
            continue

        # drop pure dates ("21 Şubat") and address-ish lines
        if DATE_JUNK_RE.search(cl):
            continue
        if looks_like_address(cl):
            continue
        if MINUTE_ONLY_RE.match(cl):
            continue
        if cl.strip() == "?":
            continue

        # flight line
        fm = FLIGHT_LINE_RE.match(cl)
        if fm:
            pending_flight = fm.group(1).replace(" ", "").upper().strip(".,;:")
            continue

        # pickup time: if line contains time token -> commit
        tmatch = PICKUP_TIME_TOKEN_RE.search(cl)
        if tmatch and cl.strip() == tmatch.group(0):
            saat = tmatch.group(0)  # keep original separator
            yolcu = ", ".join(dict.fromkeys([n for n in pending_names if n])) if pending_names else "?"
            ucus = pending_flight if pending_flight else "?"
            rows.append({"saat": saat, "ucus": ucus, "yolcu": yolcu})
            pending_names = []
            pending_flight = "?"
            continue

        # otherwise treat as name line (but skip obvious hotel/address words already)
        name = re.sub(r"\b\d+\s*(yolcu|pax|passengers?)\b", " ", cl, flags=re.IGNORECASE).strip()
        name = re.sub(r"\s{2,}", " ", name).strip()
        if len(name) < 2:
            continue
        if looks_like_address(name) or DATE_JUNK_RE.search(name) or MINUTE_ONLY_RE.match(name):
            continue

        pending_names.append(name)

    return rows

def rows_to_tsv(rows: List[Dict[str, str]]) -> str:
    lines = ["Saat\t\tUçuş\tYolcu"]
    for r in rows:
        lines.append(f"{r.get('saat','?')}\t\t{r.get('ucus','?')}\t{r.get('yolcu','?')}")
    # BOM helps Turkish chars in Excel/Sheets
    return "\ufeff" + "\n".join(lines)

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
  <title>AI Transfer Bot</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; }}
    .box {{ max-width: 900px; }}
    textarea {{ width: 100%; height: 240px; }}
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
    <h2>AI Transfer Bot</h2>
    <div class="hint">
      WhatsApp metnini parça parça ekle → <b>Ekle</b><br/>
      Bitince → <b>Bitti</b> → TSV üret.
    </div>

    {"<div class='msg'>" + message + "</div>" if message else ""}

    <form method="post" action="/add">
      <div class="counter">Taslak metin: {buf_len} karakter</div>
      <textarea name="text" placeholder="Yeni parçayı buraya yapıştır..."></textarea>
      <div class="row">
        <button type="submit">Ekle (Kaydet)</button>
      </div>
    </form>

    <form method="post" action="/finish" style="margin-top:14px;">
      <div class="row">
        <button type="submit">Bitti (Çevir)</button>
        <button type="submit" formaction="/reset" style="background:#eee;">Sıfırla</button>
      </div>
    </form>

    <p class="small" style="margin-top:18px;">Test: <a href="/health">/health</a></p>
  </div>
</body>
</html>"""

def render_result(tsv: str, row_count: int) -> str:
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
    .small {{ font-size: 12px; color:#666; }}
  </style>
</head>
<body>
  <div class="box">
    <h3>Sonuç (TSV)</h3>
    <div class="row">
      <a class="button" href="/download">İndir (TSV)</a>
      <a href="/" style="padding:10px 14px;">Geri dön</a>
    </div>
    <p class="small">Bulunan kayıt: <b>{row_count}</b></p>
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
    resp = HTMLResponse(render_home(buf))
    resp.set_cookie("session_id", sid, max_age=60*60*24*30, samesite="lax")
    return resp

@app.post("/add", response_class=HTMLResponse)
def add_piece(
    text: str = Form(default=""),
    session_id: Optional[str] = Cookie(default=None),
):
    sid = get_or_create_session(session_id)
    append_to_buffer(sid, (text or "").strip())
    buf = get_buffer(sid)
    resp = HTMLResponse(render_home(buf, message="Kaydedildi. Yeni parça ekleyebilirsin."))
    resp.set_cookie("session_id", sid, max_age=60*60*24*30, samesite="lax")
    return resp

@app.post("/finish", response_class=HTMLResponse)
def finish(session_id: Optional[str] = Cookie(default=None)):
    sid = get_or_create_session(session_id)
    raw = get_buffer(sid)

    rows = parse_records(raw)
    tsv = rows_to_tsv(rows)
    set_last_result(sid, tsv)

    return HTMLResponse(render_result(tsv, row_count=len(rows)))

@app.get("/download")
def download(session_id: Optional[str] = Cookie(default=None)):
    sid = get_or_create_session(session_id)
    tsv = get_last_result(sid)
    headers = {
        "Content-Disposition": "attachment; filename=result.tsv",
        "Content-Type": "text/tab-separated-values; charset=utf-8",
    }
    return Response(content=(tsv or "").encode("utf-8"), headers=headers)

@app.post("/reset", response_class=HTMLResponse)
def reset(session_id: Optional[str] = Cookie(default=None)):
    sid = get_or_create_session(session_id)
    set_buffer(sid, "")
    resp = HTMLResponse(render_home("", message="Taslak sıfırlandı."))
    resp.set_cookie("session_id", sid, max_age=60*60*24*30, samesite="lax")
    return resp
