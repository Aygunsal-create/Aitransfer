import os, re, json, uuid
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional

from fastapi import FastAPI, Form, Cookie
from fastapi.responses import HTMLResponse, PlainTextResponse

app = FastAPI()

DB_PATH = "db.json"

# -----------------------------
# DB helpers (very simple)
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
    db["sessions"][session_id] = {
        "buffer": text or "",
        "updated_at": datetime.utcnow().isoformat() + "Z",
        "last_result_tsv": db.get("sessions", {}).get(session_id, {}).get("last_result_tsv", "")
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
# Parsing / Rules
# -----------------------------
TIME_RE = re.compile(r"^\s*([01]?\d|2[0-3])[:\.]([0-5]\d)\s*$")
TIME_INLINE_RE = re.compile(r"\b([01]?\d|2[0-3])[:\.]([0-5]\d)\b")
FLIGHT_RE = re.compile(r"\b([A-Z]{1,3}\s?\d{2,5})\b", re.IGNORECASE)

# WhatsApp style headers: [21/2 ] Name:
WAPP_PREFIX_RE = re.compile(r"^\s*\[\s*\d{1,2}\s*\/\s*\d{1,2}.*?\]\s*")
SENDER_COLON_RE = re.compile(r"^\s*[^:]{2,40}:\s*")  # "Eyüp Abi BDR: ..." / "Lord transfer: ..."

# address-ish patterns (heuristic)
ADDRESS_HINT_RE = re.compile(
    r"\b("
    r"mah\.?|mahalle|cad\.?|caddesi|sok\.?|sokak|bulvar|blvd|no\s*:|kat\s*:|daire|"
    r"ilçe|il\s*:|istanbul|beyoğlu|fatih|şişli|beşiktaş|arnavutköy|türkiye|turkiye|"
    r"\btr\b|zip|posta|postcode|"
    r"street|st\.|road|rd\.|avenue|ave\.|"
    r"hotel|otel|suite|apart|residence|"
    r"google|maps|http|www\."
    r")\b",
    re.IGNORECASE,
)

PHONE_RE = re.compile(r"\+?\d[\d\s\-\(\)]{7,}\d")

def normalize_time(hh: str, mm: str) -> str:
    return f"{int(hh):02d}:{int(mm):02d}"

def try_fix_mojibake(s: str) -> str:
    """
    Çok yaygın WhatsApp/encoding bozulması:
    'EyÃ¼p' -> 'Eyüp', 'Åubat' -> 'Şubat' gibi.
    %100 garanti değil ama çoğu vakayı düzeltir.
    """
    if not s:
        return s
    if "Ã" in s or "Å" in s or "Ä" in s or "Ð" in s or "Þ" in s:
        for enc in ("latin-1", "cp1252"):
            try:
                return s.encode(enc, errors="ignore").decode("utf-8", errors="ignore") or s
            except Exception:
                pass
    return s

def strip_phones(s: str) -> str:
    s = PHONE_RE.sub("", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def should_drop_saw(line: str) -> bool:
    return "SAW" in line.upper()

def clean_line(raw_line: str) -> str:
    l = raw_line.strip()
    if not l:
        return ""

    l = try_fix_mojibake(l)
    l = l.replace("\u200e", "").replace("\u200f", "").replace("\ufeff", "")

    # remove WhatsApp date prefix
    l = WAPP_PREFIX_RE.sub("", l)

    # remove sender prefix like "Eyüp Abi BDR:" / "Lord transfer:"
    # BUT: only if it looks like a sender tag and not part of a real name
    # (we keep if line is just a name without whatsapp structure)
    if ":" in l and len(l.split(":", 1)[0]) <= 40:
        # Many WhatsApp exports include "Name: message"
        # Remove sender tag
        l = SENDER_COLON_RE.sub("", l)

    # remove "Name:" / "İsim:" tokens
    l = re.sub(r"^\s*(name|i̇sim|isim)\s*:\s*", "", l, flags=re.IGNORECASE)

    # remove phones
    l = strip_phones(l)

    # normalize spaces
    l = re.sub(r"\s{2,}", " ", l).strip()
    return l

def is_address_line(line: str) -> bool:
    if not line:
        return False
    # strong hint: contains a lot of commas and digits and address keywords
    commas = line.count(",")
    digits = sum(ch.isdigit() for ch in line)
    if ADDRESS_HINT_RE.search(line):
        return True
    if commas >= 3 and digits >= 5:
        return True
    # GPS-ish / coordinates
    if re.search(r"\b-?\d{1,3}\.\d{3,}\b", line):
        return True
    return False

def is_noise_line(line: str) -> bool:
    if not line:
        return True
    # pure '?'
    if line.strip() == "?":
        return True
    # hotel lines, map url, etc (address heuristic)
    if is_address_line(line):
        return True
    # "Radisson Blu Hotel Istanbul..." vs. passenger - treat as address/venue -> noise
    # If line contains "Hotel/Otel" and also has commas or full address structure -> noise
    if re.search(r"\b(hotel|otel)\b", line, re.IGNORECASE) and ("," in line or any(ch.isdigit() for ch in line)):
        return True
    return False

def extract_rows_from_text(raw: str, drop_saw: bool) -> List[Dict[str, str]]:
    """
    SAAT KAYMASINI ÖNLEYEN parser:
    - Saat satırı (sadece "13:10" gibi tek başına) yeni blok başlatır.
    - O blok altında gelen isimler o saate aittir.
    - Yeni saat gelirse önceki blok kapanır.
    - Saat inline olsa bile (ör. "13:10 TK192 John") yine blok saatini günceller.
    - WhatsApp imza / adres / telefon / gereksiz satırlar filtrelenir.
    """
    rows: List[Dict[str, str]] = []

    current_time: str = "?"
    current_flight: str = "?"

    for raw_line in (raw or "").splitlines():
        line0 = raw_line.strip()
        if not line0:
            continue

        # SAW filter should be applied early
        if drop_saw and should_drop_saw(line0):
            continue

        line = clean_line(line0)
        if not line:
            continue

        # If after cleaning line becomes address/noise -> skip
        if is_noise_line(line):
            continue

        # 1) if line is a pure time line => start new block time
        m_pure_time = TIME_RE.match(line)
        if m_pure_time:
            current_time = normalize_time(m_pure_time.group(1), m_pure_time.group(2))
            # Do not reset flight automatically; flight may come next line as "TK192"
            current_flight = "?"
            continue

        # 2) If line contains an inline time, update current_time (but keep it exact)
        m_inline_time = TIME_INLINE_RE.search(line)
        if m_inline_time:
            current_time = normalize_time(m_inline_time.group(1), m_inline_time.group(2))
            # remove the inline time from the content to avoid polluting names
            line = TIME_INLINE_RE.sub("", line).strip()

        # 3) Find flight code
        fm = FLIGHT_RE.search(line)
        flight = fm.group(1).replace(" ", "").upper() if fm else "?"
        # remove flight token from line for name extraction
        if fm:
            line = re.sub(re.escape(fm.group(1)), "", line, flags=re.IGNORECASE).strip()

        # common keywords to remove
        line = re.sub(r"\b(IHL|IST|AIRPORT|HAVALIMANI|ARRIVAL|DEPARTURE)\b", "", line, flags=re.IGNORECASE).strip()

        # passenger count text
        line = re.sub(r"/\s*\d+\s*(YOLCU|YOLCULAR|PASSENGER|PASSENGERS)\b", "", line, flags=re.IGNORECASE).strip()

        # final phone cleanup
        name = strip_phones(line)

        # if still looks like address -> drop
        if is_address_line(name):
            continue

        # if name too short, ignore
        if not name or len(name) < 2:
            continue

        rows.append({
            "saat": current_time or "?",
            "ucus": flight if flight and flight != "?" else (current_flight or "?"),
            "yolcu": name
        })

        # update current_flight if we found one
        if flight and flight != "?":
            current_flight = flight

    return rows

def rows_to_tsv_grouped(rows: List[Dict[str, str]]) -> str:
    """
    Çıktı: 4 sütun (Saat, boş, Uçuş, Yolcu)
    - Aynı saat + aynı uçuş => yolcular aynı hücrede virgülle.
    - Saat/rakam KESİNLİKLE değişmez; sadece yakalanan saat yazılır.
    """
    grouped: Dict[Tuple[str, str], List[str]] = {}

    for r in rows:
        saat = (r.get("saat") or "?").strip()
        ucus = (r.get("ucus") or "?").strip().upper()
        yolcu = (r.get("yolcu") or "").strip()

        if not yolcu:
            continue

        yolcu = strip_phones(yolcu)
        if not yolcu:
            continue

        key = (saat, ucus)
        grouped.setdefault(key, []).append(yolcu)

    def time_key(t: str) -> Tuple[int, int, str]:
        m = re.match(r"^\s*(\d{2}):(\d{2})\s*$", t)
        if not m:
            return (99, 99, t)
        return (int(m.group(1)), int(m.group(2)), t)

    items = sorted(grouped.items(), key=lambda kv: (time_key(kv[0][0]), kv[0][1]))

    lines = []
    for (saat, ucus), yolcular in items:
        seen = set()
        uniq = []
        for y in yolcular:
            if y not in seen:
                seen.add(y)
                uniq.append(y)

        yolcu_cell = ", ".join(uniq)
        lines.append(f"{saat}\t\t{ucus}\t{yolcu_cell}")

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
    .counter {{ margin-top: 10px; font-weight: bold; }}
  </style>
</head>
<body>
  <div class="box">
    <h2>AI Transfer Bot</h2>
    <div class="hint">
      Parça parça liste ekle → <b>Ekle (Kaydet)</b> <br/>
      Her şey bittiğinde → <b>Bitti (Çevir)</b> ile TSV üret.
    </div>

    {"<div class='msg'>" + message + "</div>" if message else ""}

    <form method="post" action="/add">
      <label><input type="checkbox" name="drop_saw" value="1" {checked}/> SAW satırlarını çıkar</label>
      <div class="counter">Taslakta kayıtlı metin: {buf_len} karakter</div>
      <p class="small">Aşağıya sadece yeni gelen parçayı yapıştır. "Ekle" deyince taslağa ekler.</p>
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
    html = render_home(buf, message="", drop_saw=True)
    resp = HTMLResponse(html)
    resp.set_cookie("session_id", sid, max_age=60*60*24*30)  # 30 days
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
    msg = "Kaydedildi. Yeni parça ekleyebilirsin."
    html = render_home(buf, message=msg, drop_saw=drop)
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

    rows = extract_rows_from_text(raw, drop_saw=bool(drop_saw))
    tsv = rows_to_tsv_grouped(rows)

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
    return PlainTextResponse(tsv or "", headers=headers)

@app.post("/reset", response_class=HTMLResponse)
def reset(session_id: Optional[str] = Cookie(default=None)):
    sid = get_or_create_session(session_id)
    set_buffer(sid, "")
    html = render_home("", message="Taslak sıfırlandı.", drop_saw=True)
    resp = HTMLResponse(html)
    resp.set_cookie("session_id", sid, max_age=60*60*24*30)
    return resp
