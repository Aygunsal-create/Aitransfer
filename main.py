import os, re, json, uuid
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

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
# Text cleaning / parsing
# -----------------------------

# 06:48 or 6:48 or 14.05
TIME_RE = re.compile(r"\b([01]?\d|2[0-3])[:\.]([0-5]\d)\b")
FLIGHT_RE = re.compile(r"\b([A-Z]{1,3}\s?\d{2,5})\b", re.IGNORECASE)

def normalize_time(token: str) -> Optional[str]:
    m = TIME_RE.search(token)
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2))
    return f"{hh:02d}:{mm:02d}"

def fix_mojibake(s: str) -> str:
    """
    WhatsApp / kopyalama bazen UTF-8 metni latin1 gibi bozuyor:
    'Åubat' -> 'Şubat' gibi.
    Tam garanti değil ama çoğu durumda düzeltir.
    """
    if not s:
        return s
    # sadece garip karakterler varsa dene
    if any(ch in s for ch in ["Ã", "Å", "Ä", "Ð", "Þ", "�"]):
        try:
            return s.encode("latin1", errors="ignore").decode("utf-8", errors="ignore")
        except Exception:
            return s
    return s

ADDRESS_HINTS = [
    "mah", "mahalle", "cad", "cadd", "sok", "sk", "no:", "no.", "apt", "kat",
    "istanbul", "türkiye", "turkiye", "beyoğlu", "besiktas", "beşiktaş",
    "fatih", "şişli", "arnavutköy", "avrupa", "asya", "street", "road", "avenue"
]

def looks_like_address(line: str) -> bool:
    l = line.lower()
    # uzun ve virgüllü satırlar çoğunlukla adres
    if l.count(",") >= 2 and len(l) > 40:
        return True
    for k in ADDRESS_HINTS:
        if k in l:
            # tek kelime “istanbul” diye isim silmesin diye uzunluk şartı
            if len(l) > 35:
                return True
    return False

def strip_phones(s: str) -> str:
    s = re.sub(r"\+?\d[\d\s\-\(\)]{7,}\d", " ", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def remove_whatsapp_prefixes(text: str) -> str:
    """
    WhatsApp satır başı örnekleri:
    [21/2 06:48] Eyüp Abi BDR: Funda Kara
    -> Funda Kara
    """
    out_lines = []
    for raw_line in (text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue

        line = fix_mojibake(line)

        # [dd/mm hh:mm] ...: prefix
        line = re.sub(r"^\[\d{1,2}/\d{1,2}\s+\d{1,2}[:\.]\d{2}\]\s*", "", line)
        # [dd/mm] ...: prefix (bazı kopyalarda saat yok)
        line = re.sub(r"^\[\d{1,2}/\d{1,2}\]\s*", "", line)

        # gönderen ismi: SADECE harf içeriyorsa kaldır (saat 11:50 gibi değil)
        # örn "Eyüp Abi BDR: Funda Kara"
        if ":" in line:
            left, right = line.split(":", 1)
            # sol tarafın içinde en az 2 harf varsa "gönderen" say
            if re.search(r"[A-Za-zÇĞİÖŞÜçğıöşü]{2,}", left):
                line = right.strip()

        # phone temizle
        line = strip_phones(line)

        if line:
            out_lines.append(line)
    return "\n".join(out_lines)

def extract_rows(raw: str) -> List[Tuple[str, str, str]]:
    """
    Akış parser:
    - isim satırları birikir
    - uçuş yakalanır (TK1710)
    - saat yakalanınca kayıt kapanır: (saat, uçuş, isimler)
    """
    cleaned = remove_whatsapp_prefixes(raw)

    # satır bazlı ilerleyelim (WhatsApp bloklarında daha sağlam)
    lines = [fix_mojibake(l.strip()) for l in cleaned.splitlines() if l.strip()]

    rows: List[Tuple[str, str, str]] = []
    name_parts: List[str] = []
    flight: Optional[str] = None

    for line in lines:
        if looks_like_address(line):
            # adres/hotel satırı tamamen silinsin
            continue

        # line içinde saat varsa kayıt kapanır
        t = normalize_time(line)
        if t:
            yolcu = " ".join(name_parts).strip() or "?"
            ucus = (flight or "?").strip().upper()

            # gereksiz kelimeleri temizle
            yolcu = re.sub(r"\b(Şubat|Subat|Feb|February|Mart|March|Nisan|April)\b", "", yolcu, flags=re.IGNORECASE).strip()
            yolcu = re.sub(r"\s{2,}", " ", yolcu)

            rows.append((t, ucus, yolcu))
            name_parts = []
            flight = None
            continue

        # uçuş kodu yakala (satırın herhangi yerinden)
        fm = FLIGHT_RE.search(line)
        if fm:
            flight = fm.group(1).replace(" ", "").upper()
            # uçuş satırında başka kelime varsa isim olarak eklemeyelim
            only = re.sub(re.escape(fm.group(1)), "", line, flags=re.IGNORECASE).strip()
            if only:
                # bazen aynı satırda isim de olur, ekle
                name_parts.append(only)
            continue

        # geriye kalan isim satırı
        # sayılar tek başına ise (50, 15 gibi) at
        if line.isdigit():
            continue

        # "3 yolcu" gibi çöpleri at
        if re.fullmatch(r"\d+\s*(yolcu|passengers?|pax)", line, flags=re.IGNORECASE):
            continue

        name_parts.append(line)

    return rows

def make_tsv(rows: List[Tuple[str, str, str]]) -> str:
    # 4 sütun: Saat, boş, Uçuş, Yolcu
    lines = ["Saat\t\tUçuş\tYolcu"]
    for saat, ucus, yolcu in rows:
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
  <title>AI Transfer Bot</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; }}
    textarea {{ width: 100%; height: 240px; }}
    button {{ padding:10px 14px; cursor:pointer; }}
    .msg {{ color:#0a6; margin:10px 0; }}
    .small {{ font-size: 12px; color:#666; }}
  </style>
</head>
<body>
  <h2>AI Transfer Bot</h2>
  <p class="small">WhatsApp metnini parça parça ekle → <b>Ekle</b> / Bitince → <b>Bitti</b> → TSV üret.</p>
  {"<div class='msg'>" + message + "</div>" if message else ""}
  <p><b>Taslak metin:</b> {buf_len} karakter</p>

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
  <p><b>Bulunan kayıt:</b> {found}</p>
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
    html = render_home(buf, "")
    resp = HTMLResponse(html)
    resp.set_cookie("session_id", sid, max_age=60*60*24*30)
    return resp

@app.post("/add", response_class=HTMLResponse)
def add_piece(
    text: str = Form(default=""),
    session_id: Optional[str] = Cookie(default=None),
):
    sid = get_or_create_session(session_id)
    append_to_buffer(sid, (text or "").strip())
    buf = get_buffer(sid)
    html = render_home(buf, "Kaydedildi. Yeni parça ekleyebilirsin.")
    resp = HTMLResponse(html)
    resp.set_cookie("session_id", sid, max_age=60*60*24*30)
    return resp

@app.post("/finish", response_class=HTMLResponse)
def finish(session_id: Optional[str] = Cookie(default=None)):
    sid = get_or_create_session(session_id)
    raw = get_buffer(sid)

    rows = extract_rows(raw)
    tsv = make_tsv(rows)

    set_last_result(sid, tsv)
    return HTMLResponse(render_result(tsv, found=len(rows)))

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
    html = render_home("", "Taslak sıfırlandı.")
    resp = HTMLResponse(html)
    resp.set_cookie("session_id", sid, max_age=60*60*24*30)
    return resp
