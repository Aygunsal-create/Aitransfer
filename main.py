from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, PlainTextResponse, Response
import re, os, json, uuid
from typing import List, Dict, Optional, Tuple

app = FastAPI()
DB_PATH = "db.json"

# -----------------------
# Storage
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

def get_session_id(request: Request, sid_form: Optional[str] = None) -> str:
    # 1) Form/URL’den gelen sid (cookie yoksa da çalışsın)
    if sid_form and isinstance(sid_form, str) and 10 <= len(sid_form) <= 80:
        return sid_form.strip()

    # 2) Cookie
    sid_cookie = request.cookies.get("sid")
    if sid_cookie and isinstance(sid_cookie, str) and 10 <= len(sid_cookie) <= 80:
        return sid_cookie

    # 3) Yeni oluştur
    return str(uuid.uuid4())

def get_rows(sid: str) -> List[Dict[str, str]]:
    db = _read_db()
    rows = db.get(sid, [])
    return rows if isinstance(rows, list) else []

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
    # EyÃ¼p -> Eyüp gibi bozulmayı toparlar
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

        s = URL_LINE.sub("", s).strip()
        if not s:
            continue

        s = WA_BRACKET.sub("", s).strip()

        if NAME_LINE.match(s) or PHONE_LINE.match(s):
            continue

        s = WA_SENDER.sub("", s).strip()

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
    try:
        hh = int(h); mm = int(m)
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            return f"{hh:02d}:{mm:02d}"
    except Exception:
        pass
    return "?"

def split_into_time_blocks(text: str) -> List[Tuple[str, str]]:
    """
    Saat kaç kere geçiyorsa o kadar satır.
    Aynı saat olsa bile birleştirme YOK.
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
            flush()
            current_time = norm_time(m.group(1), m.group(2))
            current_lines.append(line)
        else:
            if current_time is None:
                continue
            current_lines.append(line)

    flush()
    return [(t, "\n".join(ls).strip()) for t, ls in blocks]

FLIGHT_RE = re.compile(r'\b([A-Z]{1,3}\s?\d{1,4}[A-Z]?)\b')
PHONE_RE  = re.compile(r'\+?\d[\d\s\-()]{6,}\d')
PASS_RE   = re.compile(r'\b(\d+)\s*(yolcu|passengers?)\b', re.I)

def normalize_flight(token: str) -> str:
    return token.strip().replace(" ", "").upper().strip(".,;:()[]{}")

def extract_flight(block: str) -> str:
    cands = [normalize_flight(x) for x in FLIGHT_RE.findall(block)]
    return cands[0] if cands else "?"

BAD_WORDS = {
    "TR", "TÜRKİYE", "TURKIYE", "ISTANBUL", "İSTANBUL",
    "MAH", "MAH.", "CAD", "CAD.", "SK", "SK.", "NO", "NO:",
    "HOTEL", "HOTELS", "TRANSFER", "RADISSON", "MARRIOTT",
    "SUITES", "GARDEN", "INTERCONTINENTAL"
}

def extract_passenger_note(block: str) -> str:
    m = PASS_RE.search(block)
    return f"/ {m.group(1)} Yolcu" if m else ""

def extract_names(block: str) -> str:
    s = PHONE_RE.sub(" ", block)
    s = re.sub(r'\[.*?\]', ' ', s)
    s = TIME_RE.sub(" ", s)

    for tok in set(FLIGHT_RE.findall(s)):
        s = s.replace(tok, " ")

    parts = re.split(r'[,;\n]+', s)
    out: List[str] = []
    seen = set()

    for p in parts:
        p = re.sub(r'\s+', ' ', p).strip().strip('"').strip("'")
        if not p:
            continue

        if len(re.findall(r'\d', p)) >= 4:
            continue

        up = p.upper().replace("İ", "I")
        words = [w for w in re.split(r'\s+', up) if w]
        if any(w in BAD_WORDS for w in words):
            continue

        if len(re.findall(r'[A-Za-zÀ-ÖØ-öø-ÿĞğİıŞşÇçÜüÖö]', p)) < 2:
            continue

        key = p.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(p)

    return ", ".join(out) if out else "?"

def build_rows_from_text(raw_text: str, remove_saw: bool) -> List[Dict[str, str]]:
    cleaned = clean_whatsapp_text(raw_text or "")
    blocks = split_into_time_blocks(cleaned)

    rows: List[Dict[str, str]] = []
    for t, block in blocks:
        if remove_saw and "SAW" in block.upper():
            continue
        flight = extract_flight(block)
        note = extract_passenger_note(block)
        names = extract_names(block)
        yolcu = (names + (" " + note if note else "")).strip()
        rows.append({"saat": t or "?", "ucus": flight or "?", "yolcu": yolcu or "?"})
    return rows

# -----------------------
# TSV output
# -----------------------

def rows_to_tsv(rows: List[Dict[str, str]]) -> str:
    lines = ["Saat\t\tUçuş\tYolcu"]
    for r in rows:
        lines.append(f"{r.get('saat','?')}\t\t{r.get('ucus','?')}\t{r.get('yolcu','?')}")
    return "\n".join(lines)

# -----------------------
# UI
# -----------------------

def page_html(sid: str, message: str = "", result: str = "", count: int = 0, remove_saw_checked: bool = True) -> str:
    msg_html = f"<div class='msg'>{message}</div>" if message else ""
    checked = "checked" if remove_saw_checked else ""
    result_html = ""
    if result:
        result_html = f"""
        <div class="card">
          <h3>Sonuç (TSV)</h3>
          <div class="hint">WhatsApp/Sheets’e direkt yapıştırabilirsin.</div>
          <textarea readonly class="out">{result}</textarea>
          <div class="row">
            <a class="btn" href="/download?sid={sid}">İndir (TSV)</a>
            <form method="post" action="/reset" style="display:inline;">
              <input type="hidden" name="sid" value="{sid}">
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
  .row {{ display:flex; gap:10px; flex-wrap:wrap; margin-top:10px; align-items:center; }}
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
  <div class="sub">Parça parça yapıştır → <b>Ekle</b> ile kaydet → en sonunda <b>Bitir</b> ile tek çıktı al.</div>

  {msg_html}

  <div class="card">
    <form method="post" action="/action">
      <input type="hidden" name="sid" value="{sid}">
      <label>
        <input type="checkbox" name="remove_saw" value="1" {checked}>
        SAW satırlarını çıkar
      </label>
      <div class="hint">WhatsApp kopya formatındaki “[21/2] …”, “Gönderen:” vb. otomatik temizlenir.</div>
      <textarea name="text" placeholder="Listeyi buraya yapıştır..."></textarea>

      <div class="row">
        <button class="btn primary" type="submit" name="do" value="append">Ekle (Kaydet)</button>
        <button class="btn" type="submit" name="do" value="finish">Bitir (Çıktı üret)</button>
        <a class="btn" href="/download?sid={sid}">İndir (TSV)</a>
      </div>

      <div class="hint">Kayıtlı satır: <b>{count}</b></div>
    </form>

    <form method="post" action="/reset" style="margin-top:10px;">
      <input type="hidden" name="sid" value="{sid}">
      <button class="btn danger" type="submit">Sıfırla</button>
    </form>
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
    html = page_html(sid=sid, count=len(rows))
    resp = HTMLResponse(html)
    # cookie çalışırsa iyi, çalışmazsa zaten URL sid ile gidecek
    resp.set_cookie("sid", sid, httponly=True, samesite="lax")
    return resp

@app.post("/action", response_class=HTMLResponse)
def action(request: Request,
           sid: str = Form(""),
           do: str = Form("append"),
           text: str = Form(""),
           remove_saw: Optional[str] = Form(None)):
    sid = get_session_id(request, sid_form=sid)
    remove = bool(remove_saw)
    existing = get_rows(sid)

    if do == "append":
        new_rows = build_rows_from_text(text, remove_saw=remove)
        if not new_rows:
            html = page_html(sid=sid, message="⚠️ Saat (HH:MM) bulunamadı. Metinde saat var mı kontrol et.", count=len(existing), remove_saw_checked=remove)
            resp = HTMLResponse(html)
            resp.set_cookie("sid", sid, httponly=True, samesite="lax")
            return resp

        append_rows(sid, new_rows)
        rows = get_rows(sid)
        html = page_html(sid=sid, message=f"✅ {len(new_rows)} satır eklendi. Toplam: {len(rows)}", count=len(rows), remove_saw_checked=remove)
        resp = HTMLResponse(html)
        resp.set_cookie("sid", sid, httponly=True, samesite="lax")
        return resp

    # finish
    rows = get_rows(sid)
    if not rows:
        html = page_html(sid=sid, message="⚠️ Kayıtlı satır yok. Önce Ekle (Kaydet) yap.", count=0, remove_saw_checked=remove)
        resp = HTMLResponse(html)
        resp.set_cookie("sid", sid, httponly=True, samesite="lax")
        return resp

    tsv = rows_to_tsv(rows)
    html = page_html(sid=sid, message="✅ Bitti. Çıktı hazır.", result=tsv, count=len(rows), remove_saw_checked=remove)
    resp = HTMLResponse(html)
    resp.set_cookie("sid", sid, httponly=True, samesite="lax")
    return resp

@app.get("/download")
def download(request: Request, sid: str = ""):
    sid = get_session_id(request, sid_form=sid)
    rows = get_rows(sid)
    tsv = rows_to_tsv(rows)

    data = ("\ufeff" + tsv).encode("utf-8")  # UTF-8 BOM
    headers = {
        "Content-Disposition": 'attachment; filename="transfer.tsv"',
        "Content-Type": "text/tab-separated-values; charset=utf-8"
    }
    resp = Response(content=data, headers=headers)
    resp.set_cookie("sid", sid, httponly=True, samesite="lax")
    return resp

@app.post("/reset", response_class=HTMLResponse)
def reset(request: Request, sid: str = Form("")):
    sid = get_session_id(request, sid_form=sid)
    set_rows(sid, [])
    html = page_html(sid=sid, message="🧹 Sıfırlandı. Yeni liste ekleyebilirsin.", count=0)
    resp = HTMLResponse(html)
    resp.set_cookie("sid", sid, httponly=True, samesite="lax")
    return resp

@app.get("/health", response_class=PlainTextResponse)
def health():
    return "ok"
