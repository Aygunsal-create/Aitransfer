from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, PlainTextResponse, Response
import re, os, json, uuid
from typing import List, Dict, Optional, Tuple

app = FastAPI()

DB_PATH = "db.json"

# -----------------------
# Storage (session-based)
# -----------------------

def _read_db() -> Dict[str, List[Dict[str, str]]]:
    if not os.path.exists(DB_PATH):
        return {}
    try:
        with open(DB_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {}

def _write_db(data: Dict[str, List[Dict[str, str]]]) -> None:
    tmp = DB_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, DB_PATH)

def get_session_id(request: Request) -> str:
    sid = request.cookies.get("sid")
    if sid and isinstance(sid, str) and 10 <= len(sid) <= 80:
        return sid
    return str(uuid.uuid4())

def get_rows(sid: str) -> List[Dict[str, str]]:
    db = _read_db()
    return db.get(sid, [])

def set_rows(sid: str, rows: List[Dict[str, str]]) -> None:
    db = _read_db()
    db[sid] = rows
    _write_db(db)

def append_rows(sid: str, new_rows: List[Dict[str, str]]) -> None:
    rows = get_rows(sid)
    rows.extend(new_rows)
    set_rows(sid, rows)

# -----------------------
# Text cleanup (WhatsApp)
# -----------------------

WA_BRACKET = re.compile(r'^\s*\[\d{1,2}/\d{1,2}.*?\]\s*')  # [21/2 ...]
WA_SENDER  = re.compile(r'^\s*[^:\n]{1,50}:\s*')          # Eyüp Abi BDR: ...
NAME_LINE  = re.compile(r'^\s*(name|isim)\s*:\s*', re.I)
PHONE_LINE = re.compile(r'^\s*(phone|telefon)\s*(number|no)?\s*:\s*', re.I)
URL_LINE   = re.compile(r'https?://\S+', re.I)

def fix_mojibake(s: str) -> str:
    # EyÃ¼p -> Eyüp gibi durumları toparlar (zararsızsa dokunmaz)
    try:
        b = s.encode("latin-1", "ignore")
        u = b.decode("utf-8", "ignore")
        if "Ã" in s and ("Ã" not in u):
            return u
    except Exception:
        pass
    return s

def clean_whatsapp_text(raw: str) -> str:
    raw = fix_mojibake(raw)
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")

    cleaned: List[str] = []
    for line in raw.split("\n"):
        s = line.strip()
        if not s:
            continue

        # remove urls
        s = URL_LINE.sub("", s).strip()
        if not s:
            continue

        # remove [..] timestamp prefix
        s = WA_BRACKET.sub("", s).strip()

        # drop "Name:" / "Phone:" lines entirely
        if NAME_LINE.match(s) or PHONE_LINE.match(s):
            continue

        # remove "Sender:" prefix (WhatsApp group copy)
        s = WA_SENDER.sub("", s).strip()

        # normalize weird bullets
        s = s.replace("•", " ").replace("  ", " ").strip()

        if not s:
            continue
        cleaned.append(s)

    return "\n".join(cleaned)

# -----------------------
# Parsing logic (time -> rows)
# -----------------------

TIME_RE = re.compile(r'\b(\d{1,2})[:.](\d{2})\b')

def norm_time(h: str, m: str) -> str:
    hh = int(h)
    mm = int(m)
    if hh < 0 or hh > 23 or mm < 0 or mm > 59:
        return "?"
    return f"{hh:02d}:{mm:02d}"

def split_into_time_blocks(text: str) -> List[Tuple[str, str]]:
    """
    Returns list of (time, block_text).
    Rule: number of rows == number of time occurrences we detect.
    We DO NOT auto-merge same time values.
    """
    lines = text.split("\n")
    blocks: List[Tuple[str, List[str]]] = []
    current_time: Optional[str] = None
    current_lines: List[str] = []

    def flush():
        nonlocal current_time, current_lines
        if current_time is not None:
            blocks.append((current_time, current_lines[:]))
        current_time = None
        current_lines = []

    for line in lines:
        m = TIME_RE.search(line)
        if m:
            # new block starts at first time in the line
            flush()
            current_time = norm_time(m.group(1), m.group(2))
            # keep the rest of the line content (after that time token) too
            # but also keep whole line because sometimes flight/name sits before time in pasted tables
            current_lines.append(line)
        else:
            # if we haven't started yet but line exists, keep it as context
            if current_time is None:
                # ignore leading noise without time
                continue
            current_lines.append(line)

    flush()
    return [(t, "\n".join(ls).strip()) for t, ls in blocks]

FLIGHT_RE = re.compile(r'\b([A-Z]{1,3}\s?\d{1,4}[A-Z]?)\b')  # TK192, SU2138, W9A5327, etc.
PHONE_RE  = re.compile(r'\+?\d[\d\s\-()]{6,}\d')             # phone-ish
PASS_RE   = re.compile(r'\b(\d+)\s*(yolcu|passengers?)\b', re.I)

def normalize_flight(token: str) -> str:
    t = token.strip().replace(" ", "").upper()
    t = t.strip(".,;:()[]{}")
    return t

def extract_flight(block: str) -> str:
    # try to find best flight-like token
    candidates = [normalize_flight(x) for x in FLIGHT_RE.findall(block)]
    if not candidates:
        return "?"
    # filter out obvious non-flight short tokens if needed
    # keep first (most common in your input)
    return candidates[0] if candidates[0] else "?"

BAD_WORDS = {
    "TR", "TÜRKİYE", "TURKIYE", "ISTANBUL", "İSTANBUL", "BEYOĞLU", "FATIH", "ŞİŞLİ",
    "MAH", "MAH.", "CAD", "CAD.", "SK", "SK.", "NO", "NO:", "HOTEL", "HOTELS",
    "TRANSFER", "LORD", "RADISSON", "MARRIOTT", "SUITES", "GARDEN", "INTERCONTINENTAL",
}

def clean_name_fragment(s: str) -> str:
    s = s.strip().strip('"').strip("'").strip()
    s = re.sub(r'\s+', ' ', s)
    return s

def extract_passenger_note(block: str) -> str:
    m = PASS_RE.search(block)
    if not m:
        return ""
    n = m.group(1)
    return f"/ {n} Yolcu"

def extract_names(block: str) -> str:
    """
    Goal: keep passenger names, avoid WhatsApp metadata / phones / addresses.
    Heuristic-based (works well for your copy/paste patterns).
    """
    # remove phones
    s = PHONE_RE.sub(" ", block)
    # remove urls already removed, but safe
    s = URL_LINE.sub(" ", s)
    # remove bracket timestamps like [21/2 ...]
    s = re.sub(r'\[.*?\]', ' ', s)

    # remove flight token(s) to avoid mixing in name field
    for tok in set(FLIGHT_RE.findall(s)):
        s = s.replace(tok, " ")

    # remove time tokens
    s = TIME_RE.sub(" ", s)

    # split by commas and newlines
    parts = re.split(r'[,;\n]+', s)

    names: List[str] = []
    for p in parts:
        p = clean_name_fragment(p)
        if not p:
            continue

        # skip address-ish lines with lots of digits
        if len(re.findall(r'\d', p)) >= 4:
            continue

        # skip very long address-like fragments
        if len(p) > 80 and ("cad" in p.lower() or "mah" in p.lower() or "no" in p.lower()):
            continue

        upper = p.upper().replace("İ", "I")
        words = [w for w in re.split(r'\s+', upper) if w]
        if any(w in BAD_WORDS for w in words):
            continue

        # must contain at least 2 letters
        if len(re.findall(r'[A-Za-zÀ-ÖØ-öø-ÿĞğİıŞşÇçÜüÖö]', p)) < 2:
            continue

        # ignore generic phrases
        low = p.lower()
        if low in {"ok", "test", "health", "favicon.ico"}:
            continue

        names.append(p)

    # de-dup while keeping order
    out: List[str] = []
    seen = set()
    for n in names:
        key = n.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(n)

    if not out:
        return "?"

    return ", ".join(out)

def build_rows_from_text(raw_text: str, remove_saw: bool) -> List[Dict[str, str]]:
    cleaned = clean_whatsapp_text(raw_text)
    blocks = split_into_time_blocks(cleaned)

    rows: List[Dict[str, str]] = []
    for t, block in blocks:
        if remove_saw and "SAW" in block.upper():
            continue

        flight = extract_flight(block)
        passenger_note = extract_passenger_note(block)
        names = extract_names(block)
        yolcu = (names + (" " + passenger_note if passenger_note else "")).strip()

        rows.append({
            "saat": t if t else "?",
            "ucus": flight if flight else "?",
            "yolcu": yolcu if yolcu else "?"
        })

    return rows

# -----------------------
# TSV output
# -----------------------

def rows_to_tsv(rows: List[Dict[str, str]]) -> str:
    lines = ["Saat\t\tUçuş\tYolcu"]
    for r in rows:
        saat = str(r.get("saat", "?") or "?")
        ucus = str(r.get("ucus", "?") or "?")
        yolcu = str(r.get("yolcu", "?") or "?")
        lines.append(f"{saat}\t\t{ucus}\t{yolcu}")
    return "\n".join(lines)

# -----------------------
# UI
# -----------------------

def page_html(message: str = "", result: str = "", count: int = 0) -> str:
    msg_html = f"<div class='msg'>{message}</div>" if message else ""
    result_html = ""
    if result:
        # copy-friendly
        result_html = f"""
        <div class="card">
          <h3>Sonuç (TSV)</h3>
          <div class="hint">WhatsApp/Sheets’e direkt yapıştırabilirsin.</div>
          <textarea readonly class="out">{result}</textarea>
          <div class="row">
            <a class="btn" href="/download">İndir (TSV)</a>
            <form method="post" action="/reset" style="display:inline;">
              <button class="btn danger" type="submit">Sıfırla</button>
            </form>
          </div>
        </div>
        """
    return f"""
<!doctype html>
<html lang="tr">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>AI Transfer Bot</title>
<style>
  body {{ font-family: Arial, sans-serif; margin: 18px; }}
  .title {{ font-size: 20px; font-weight: 700; margin-bottom: 6px; }}
  .sub {{ color:#444; margin-bottom: 10px; }}
  .card {{ border:1px solid #ddd; border-radius: 10px; padding: 12px; margin-top: 12px; }}
  textarea {{ width:100%; min-height: 180px; font-family: ui-monospace, Menlo, Consolas, monospace; }}
  .out {{ min-height: 220px; }}
  .row {{ display:flex; gap:10px; flex-wrap:wrap; margin-top:10px; }}
  .btn {{ display:inline-block; padding:10px 12px; border-radius:10px; border:1px solid #333; background:#fff; cursor:pointer; text-decoration:none; color:#111; }}
  .btn.primary {{ background:#111; color:#fff; border-color:#111; }}
  .btn.danger {{ background:#b00020; color:#fff; border-color:#b00020; }}
  .hint {{ color:#666; font-size: 12px; margin: 6px 0; }}
  .msg {{ padding:10px 12px; border-radius:10px; background:#f3f3f3; margin: 10px 0; }}
  label {{ user-select:none; }}
</style>
</head>
<body>
  <div class="title">AI Transfer Bot</div>
  <div class="sub">Metni yapıştır → TAB ayrımlı 4 sütun üretir (Saat / boş / Uçuş / Yolcu). Parça parça ekleyebilirsin.</div>

  {msg_html}

  <div class="card">
    <form method="post" action="/append">
      <label>
        <input type="checkbox" name="remove_saw" value="1" checked>
        SAW satırlarını çıkar
      </label>
      <div class="hint">Not: WhatsApp kopya formatındaki “[21/2] …”, “Gönderen:” vb. otomatik temizlenir.</div>
      <textarea name="text" placeholder="Listeyi buraya yapıştır..."></textarea>
      <div class="row">
        <button class="btn primary" type="submit">Ekle (Kaydet)</button>
    </form>

    <form method="post" action="/finish">
        <input type="hidden" name="remove_saw" value="1">
        <button class="btn" type="submit">Bitir (Çıktı üret)</button>
    </form>

    <a class="btn" href="/download">İndir (TSV)</a>

    <form method="post" action="/reset">
      <button class="btn danger" type="submit">Sıfırla</button>
    </form>
      </div>
      <div class="hint">Şu an kayıtlı satır sayısı: <b>{count}</b></div>
  </div>

  {result_html}

  <div class="hint">Test: <a href="/health">/health</a></div>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    sid = get_session_id(request)
    rows = get_rows(sid)
    html = page_html(count=len(rows))
    resp = HTMLResponse(html)
    if request.cookies.get("sid") != sid:
        resp.set_cookie("sid", sid, httponly=True, samesite="lax")
    return resp

@app.post("/append", response_class=HTMLResponse)
def append_endpoint(request: Request, text: str = Form(""), remove_saw: Optional[str] = Form(None)):
    sid = get_session_id(request)
    remove = bool(remove_saw)

    new_rows = build_rows_from_text(text or "", remove_saw=remove)
    if not new_rows:
        rows = get_rows(sid)
        html = page_html(message="⚠️ Saat (HH:MM) bulunamadı. Metinde saat var mı kontrol et.", count=len(rows))
        resp = HTMLResponse(html)
        if request.cookies.get("sid") != sid:
            resp.set_cookie("sid", sid, httponly=True, samesite="lax")
        return resp

    append_rows(sid, new_rows)
    rows = get_rows(sid)
    msg = f"✅ {len(new_rows)} satır eklendi. Toplam: {len(rows)}"
    html = page_html(message=msg, count=len(rows))
    resp = HTMLResponse(html)
    if request.cookies.get("sid") != sid:
        resp.set_cookie("sid", sid, httponly=True, samesite="lax")
    return resp

@app.post("/finish", response_class=HTMLResponse)
def finish_endpoint(request: Request, remove_saw: Optional[str] = Form(None)):
    sid = get_session_id(request)
    rows = get_rows(sid)
    if not rows:
        html = page_html(message="⚠️ Kayıtlı satır yok. Önce metin ekle.", count=0)
        resp = HTMLResponse(html)
        if request.cookies.get("sid") != sid:
            resp.set_cookie("sid", sid, httponly=True, samesite="lax")
        return resp

    tsv = rows_to_tsv(rows)
    html = page_html(message="✅ Bitti. Çıktı hazır.", result=tsv, count=len(rows))
    resp = HTMLResponse(html)
    if request.cookies.get("sid") != sid:
        resp.set_cookie("sid", sid, httponly=True, samesite="lax")
    return resp

@app.get("/download")
def download(request: Request):
    sid = get_session_id(request)
    rows = get_rows(sid)
    tsv = rows_to_tsv(rows)

    # Excel/Sheets Türkçe karakter için UTF-8 BOM
    data = ("\ufeff" + tsv).encode("utf-8")
    headers = {
        "Content-Disposition": 'attachment; filename="transfer.tsv"',
        "Content-Type": "text/tab-separated-values; charset=utf-8"
    }
    resp = Response(content=data, headers=headers)
    if request.cookies.get("sid") != sid:
        resp.set_cookie("sid", sid, httponly=True, samesite="lax")
    return resp

@app.post("/reset", response_class=HTMLResponse)
def reset(request: Request):
    sid = get_session_id(request)
    set_rows(sid, [])
    html = page_html(message="🧹 Sıfırlandı. Yeni liste ekleyebilirsin.", count=0)
    resp = HTMLResponse(html)
    if request.cookies.get("sid") != sid:
        resp.set_cookie("sid", sid, httponly=True, samesite="lax")
    return resp

@app.get("/health", response_class=PlainTextResponse)
def health():
    return "ok"
