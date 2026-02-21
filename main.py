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
# Cleaning rules
# -----------------------------
# pickup time token: 0:00-23:59 or 14.05 (we keep original separator)
PICKUP_TIME_TOKEN_RE = re.compile(r"\b([01]?\d|2[0-3])([:\.])([0-5]\d)\b")

# WhatsApp header "[21/2 06:48]" (message timestamp) -> remove entirely
WA_BRACKET_RE = re.compile(r"^\s*\[\s*\d{1,2}\s*/\s*\d{1,2}\s+\d{1,2}:\d{2}\s*\]\s*")

# "Eyüp Abi BDR:" sender prefix
SENDER_COLON_RE = re.compile(r"^\s*[^:\n]{1,60}:\s*")

# URL / phone
URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
PHONE_RE = re.compile(r"\+?\d[\d\s\-\(\)]{7,}\d")

# flight line usually alone
FLIGHT_LINE_RE = re.compile(r"^\s*([A-Z]{1,3}\s?\d{2,5}[A-Z]?)\s*$", re.IGNORECASE)

# date-like junk
DATE_LINE_RE = re.compile(r"^\s*(date\s*:)?\s*\d{1,2}\s*(şubat|subat|feb|february)\b", re.IGNORECASE)

# minute-only junk lines: "50", "15"
MINUTE_ONLY_RE = re.compile(r"^\s*([0-5]?\d)\s*$")

# address hints
ADDRESS_HINT_RE = re.compile(
    r"\b("
    r"mah\.?|mahalle|cad\.?|caddesi|cd\.?|sok\.?|sokak|sk\.?|no\s*:|numara|"
    r"istanbul|beyoğlu|fatih|şişli|beşiktaş|besiktas|arnavutköy|arnavutkoy|türkiye|turkiye|\btr\b|"
    r"hotel|otel|residence|suite|apart|"
    r"google|maps|waze"
    r")\b",
    re.IGNORECASE
)

# mixed codes to remove from names e.g. A4*3069
MIXED_CODE_RE = re.compile(r"\b[A-Z]\d[\*\-]\d{3,6}\b", re.IGNORECASE)

BAD_SINGLE_WORDS = {"OK", "TEST", "LOGO", "OPERASYON", "AIR", "TRANSFER"}

def try_fix_mojibake(s: str) -> str:
    if not s:
        return s
    if "Ã" in s or "Å" in s or "Ä" in s:
        for enc in ("latin-1", "cp1252"):
            try:
                fixed = s.encode(enc, errors="ignore").decode("utf-8", errors="ignore")
                if fixed:
                    return fixed
            except Exception:
                pass
    return s

def looks_like_address(s: str) -> bool:
    if not s:
        return False
    if ADDRESS_HINT_RE.search(s):
        return True
    commas = s.count(",")
    digits = sum(ch.isdigit() for ch in s)
    if commas >= 3 and len(s) >= 30:
        return True
    if digits >= 8 and len(s) >= 25:
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

    # Remove whatsapp bracket timestamp then sender prefix
    s = WA_BRACKET_RE.sub("", s).strip()
    s = SENDER_COLON_RE.sub("", s).strip()

    # Remove phones
    s = PHONE_RE.sub("", s).strip()

    # Remove mixed codes from names
    s = MIXED_CODE_RE.sub(" ", s).strip()

    # normalize spaces
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

# -----------------------------
# Parser (Name -> Flight -> Pickup Time commits record)
# -----------------------------
def parse_whatsapp_records(raw: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    pending_names: List[str] = []
    pending_flight: str = "?"

    for raw_line in (raw or "").splitlines():
        cl = clean_line(raw_line)
        if not cl:
            continue

        up = cl.upper().strip()

        # skip junk
        if cl.strip() == "?":
            continue
        if up in BAD_SINGLE_WORDS:
            continue
        if DATE_LINE_RE.match(cl):
            continue
        if MINUTE_ONLY_RE.match(cl):
            continue
        if looks_like_address(cl):
            continue

        # flight line
        fm = FLIGHT_LINE_RE.match(cl)
        if fm:
            pending_flight = fm.group(1).replace(" ", "").upper().strip(".,;:")
            continue

        # pickup time: find token anywhere in the line
        tmatch = PICKUP_TIME_TOKEN_RE.search(cl)
        if tmatch:
            saat = f"{tmatch.group(1)}{tmatch.group(2)}{tmatch.group(3)}"  # keep ":" or "."
            yolcu = ", ".join(dict.fromkeys([n for n in pending_names if n])) if pending_names else "?"
            ucus = pending_flight if pending_flight else "?"

            rows.append({"saat": saat, "ucus": ucus, "yolcu": yolcu})

            # reset for next record
            pending_names = []
            pending_flight = "?"
            continue

        # name line
        cl2 = re.sub(r"\b\d+\s*(yolcu|pax|passengers?)\b", " ", cl, flags=re.IGNORECASE).strip()
        cl2 = re.sub(r"\s{2,}", " ", cl2).strip()
        if len(cl2) < 2:
            continue
        if looks_like_address(cl2) or DATE_LINE_RE.match(cl2) or MINUTE_ONLY_RE.match(cl2):
            continue

        pending_names.append(cl2)

    return rows

def rows_to_tsv(rows: List[Dict[str, str]]) -> str:
    lines = ["Saat\t\tUçuş\tYolcu"]
    for r in rows:
        lines.append(f"{r.get('saat','?')}\t\t{r.get('ucus','?')}\t{r.get('yolcu','?')}")
    # UTF-8 BOM for Turkish chars in Excel/Sheets
    return "\ufeff" + "\n".join(lines)

# -----------------------------
# UI
# -----------------------------
def render_home(buffer_text: str, message: str = "") -> str:
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
    <p>Sheets'e en düzgün: <b>İndir (TSV)</b>.</p>
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
    html = render_home(buf)
    resp = HTMLResponse(html)
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
    html = render_home(buf, message="Kaydedildi. Yeni parça ekleyebilirsin.")
    resp = HTMLResponse(html)
    resp.set_cookie("session_id", sid, max_age=60*60*24*30, samesite="lax")
    return resp

@app.post("/finish", response_class=HTMLResponse)
def finish(session_id: Optional[str] = Cookie(default=None)):
    sid = get_or_create_session(session_id)
    raw = get_buffer(sid)

    rows = parse_whatsapp_records(raw)
    tsv = rows_to_tsv(rows)

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
    return Response(content=(tsv or "").encode("utf-8"), headers=headers)

@app.post("/reset", response_class=HTMLResponse)
def reset(session_id: Optional[str] = Cookie(default=None)):
    sid = get_or_create_session(session_id)
    set_buffer(sid, "")
    html = render_home("", message="Taslak sıfırlandı.")
    resp = HTMLResponse(html)
    resp.set_cookie("session_id", sid, max_age=60*60*24*30, samesite="lax")
    return resp
