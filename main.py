import os, re, json, uuid
from datetime import datetime
from typing import Dict, Any, List, Tuple

from fastapi import FastAPI, Form, Cookie
from fastapi.responses import HTMLResponse, PlainTextResponse, Response

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

def get_or_create_session(session_id: str | None) -> str:
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

# -----------------------------
# WhatsApp / Turkish cleanup
# -----------------------------

# [21/2 17:10] gibi prefix
WA_BRACKET = re.compile(r"^\s*\[\d{1,2}/\d{1,2}.*?\]\s*")

# "Eyüp Abi BDR:" gibi gönderici
WA_SENDER = re.compile(r"^\s*[^:\n]{1,60}:\s*")

# Name/Phone satırları
NAME_LINE = re.compile(r"^\s*(name|isim)\s*:\s*", re.I)
PHONE_LINE = re.compile(r"^\s*(phone|telefon)\s*(number|no)?\s*:\s*", re.I)

# URL / maps
URL_RE = re.compile(r"https?://\S+|www\.\S+", re.I)

ADDRESS_KEYWORDS = [
    # TR / İstanbul / Türkiye
    "TÜRKİYE", "TURKIYE", "TR", "ISTANBUL", "İSTANBUL",
    # adres kısaltmaları
    "MAH", "MAH.", "MAHALLE", "CAD", "CAD.", "CADDESİ", "CADDESI",
    "SOK", "SOK.", "SK", "SK.", "SOKAK",
    "NO:", "NO", "NUMARA", "APT", "APARTMAN", "KAT", "DAIRE", "DAİRE",
    "BLOK", "POSTA", "KODU", "ZIP", "ZIPCODE",
    # otel / lokasyon
    "HOTEL", "HOTELS", "SUITES", "RESIDENCE", "APART", "APARTMENT",
    "MARRIOTT", "RADISSON", "HILTON", "HYATT", "INTERCONTINENTAL", "IHG",
    # harita
    "MAPS", "GOOGLE", "WAZE"
]

def fix_mojibake(s: str) -> str:
    """
    WhatsApp kopyasında sık görülen bozulmayı düzeltir:
    EyÃ¼p -> Eyüp, Ä°stanbul -> İstanbul, JiÅ™i -> Jiři vb.
    """
    if not s:
        return s
    # sadece belirti varsa dene
    if ("Ã" in s) or ("Ä" in s) or ("Å" in s) or ("Ð" in s):
        try:
            return s.encode("latin-1", "ignore").decode("utf-8", "ignore")
        except Exception:
            return s
    return s

def looks_like_address(line: str) -> bool:
    """
    Adresleri silmek için heuristik:
    - çok fazla virgül / rakam
    - adres anahtar kelimeleri
    - uzun ve lokasyon ağırlıklı satırlar
    """
    if not line:
        return False

    up = line.upper().replace("İ", "I")
    # keyword
    for kw in ADDRESS_KEYWORDS:
        if kw in up:
            return True

    # çok virgül + uzunluk adres ihtimali
    comma_count = line.count(",")
    digit_count = sum(ch.isdigit() for ch in line)

    if comma_count >= 3 and len(line) >= 35:
        return True
    if digit_count >= 8 and len(line) >= 25:
        return True

    # "Mah Refik Saydam Cad" gibi kelime paterni
    if re.search(r"\b(MAH|CAD|SK|SOK)\b", up):
        return True

    return False

def clean_whatsapp_line(line: str) -> str:
    s = fix_mojibake(line).strip()
    if not s:
        return ""

    # URL sil
    s = URL_RE.sub("", s).strip()
    if not s:
        return ""

    # [21/2 ...] sil
    s = WA_BRACKET.sub("", s).strip()

    # Name/Phone satırlarını direkt yok say
    if NAME_LINE.match(s) or PHONE_LINE.match(s):
        return ""

    # Gönderen adını sil: "Eyüp Abi BDR: ..."
    s = WA_SENDER.sub("", s).strip()

    # fazla boşlukları toparla
    s = re.sub(r"\s{2,}", " ", s).strip()

    return s

def clean_whatsapp_text(raw: str) -> str:
    raw = fix_mojibake(raw or "")
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")
    out: List[str] = []
    for line in raw.split("\n"):
        s = clean_whatsapp_line(line)
        if not s:
            continue
        # adres satırıysa komple at
        if looks_like_address(s):
            continue
        out.append(s)
    return "\n".join(out)

# -----------------------------
# Parsing / Rules
# -----------------------------
TIME_RE = re.compile(r"\b([01]?\d|2[0-3])[:\.]([0-5]\d)\b")
FLIGHT_RE = re.compile(r"\b([A-Z]{1,3}\s?\d{2,5})\b", re.IGNORECASE)

def normalize_time(hh: str, mm: str) -> str:
    return f"{int(hh):02d}:{int(mm):02d}"

def strip_phones(s: str) -> str:
    # +90 5xx xxx xx xx / 05xx... / 10+ digits etc.
    s = re.sub(r"\+?\d[\d\s\-\(\)]{7,}\d", "", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def should_drop_saw(line: str) -> bool:
    return "SAW" in line.upper()

def extract_rows_from_text(raw: str, drop_saw: bool) -> List[Dict[str, str]]:
    """
    Toleranslı parser + WhatsApp temizliği + adres silme.
    """
    cleaned = clean_whatsapp_text(raw or "")
    rows: List[Dict[str, str]] = []
    last_time = None

    for line in cleaned.splitlines():
        l = line.strip()
        if not l:
            continue
        if drop_saw and should_drop_saw(l):
            continue

        # adres gibi görünüyorsa zaten temizlemede atıldı; ama ekstra güvenlik:
        if looks_like_address(l):
            continue

        # time
        tm = TIME_RE.search(l)
        if tm:
            last_time = normalize_time(tm.group(1), tm.group(2))

        # flight
        fm = FLIGHT_RE.search(l)
        flight = fm.group(1).replace(" ", "").upper() if fm else "?"

        # passenger/name: remove known tokens
        l2 = TIME_RE.sub("", l)
        if fm:
            l2 = re.sub(re.escape(fm.group(1)), "", l2, flags=re.IGNORECASE)

        l2 = re.sub(r"\b(IHL|IST|AIRPORT|HAVALIMANI|ARRIVAL|DEPARTURE)\b", "", l2, flags=re.IGNORECASE)
        l2 = re.sub(r"/\s*\d+\s*(YOLCU|YOLCULAR|PASSENGER|PASSENGERS)\b", "", l2, flags=re.IGNORECASE)

        name = strip_phones(l2)
        name = re.sub(r"^\s*(Name|İsim)\s*:\s*", "", name, flags=re.IGNORECASE).strip()
        name = name.strip(" ,.-—|")

        # yine adres gibi kaldıysa at
        if looks_like_address(name):
            continue

        if not name or len(name) < 2:
            continue

        rows.append({
            "saat": last_time or "?",
            "ucus": flight or "?",
            "yolcu": name
        })

    return rows

def rows_to_tsv_grouped(rows: List[Dict[str, str]]) -> str:
    """
    Senin iş akışına uygun:
    - Aynı saat + aynı uçuş => tek satır, yolcular virgülle
    - 4 sütun: Saat, boş, Uçuş, Yolcu(lar)
    """
    grouped: Dict[Tuple[str, str], List[str]] = {}

    for r in rows:
        saat = (r.get("saat") or "?").strip()
        ucus = (r.get("ucus") or "?").strip().upper()
        yolcu = (r.get("yolcu") or "").strip()

        if not yolcu:
            continue

        yolcu = strip_phones(yolcu)
        if looks_like_address(yolcu):
            continue

        key = (saat, ucus)
        grouped.setdefault(key, []).append(yolcu)

    def time_key(t: str) -> Tuple[int, int, str]:
        m = TIME_RE.search(t)
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
      <p class="small">Aşağıya yeni parçayı yapıştır (sadece yeni gelen kısmı). "Ekle" deyince taslağa ekler.</p>
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
def home(session_id: str | None = Cookie(default=None)):
    sid = get_or_create_session(session_id)
    buf = get_buffer(sid)
    html = render_home(buf, message="", drop_saw=True)
    resp = HTMLResponse(html)
    resp.set_cookie("session_id", sid, max_age=60*60*24*30, samesite="lax")
    return resp

@app.post("/add", response_class=HTMLResponse)
def add_piece(
    text: str = Form(default=""),
    drop_saw: str | None = Form(default=None),
    session_id: str | None = Cookie(default=None),
):
    sid = get_or_create_session(session_id)

    # ham ekliyoruz (temizliği finish'te zaten yapıyoruz) ama istersen burada da temizleyebilirsin:
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
    drop_saw: int = Form(default=1),
    session_id: str | None = Cookie(default=None),
):
    sid = get_or_create_session(session_id)
    raw = get_buffer(sid)

    rows = extract_rows_from_text(raw, drop_saw=bool(drop_saw))
    tsv = rows_to_tsv_grouped(rows)

    db = _load_db()
    sess = db["sessions"].setdefault(sid, {"buffer": raw, "updated_at": "", "last_result_tsv": ""})
    sess["last_result_tsv"] = tsv
    sess["updated_at"] = datetime.utcnow().isoformat() + "Z"
    _save_db(db)

    return HTMLResponse(render_result(tsv))

@app.get("/download")
def download(session_id: str | None = Cookie(default=None)):
    sid = get_or_create_session(session_id)
    db = _load_db()
    tsv = db.get("sessions", {}).get(sid, {}).get("last_result_tsv", "") or ""

    # Türkçe karakterler için BOM (Excel/Sheets daha sorunsuz açar)
    data = ("\ufeff" + tsv).encode("utf-8")

    headers = {
        "Content-Disposition": "attachment; filename=result.tsv",
        "Content-Type": "text/tab-separated-values; charset=utf-8",
    }
    return Response(content=data, headers=headers)

@app.post("/reset", response_class=HTMLResponse)
def reset(session_id: str | None = Cookie(default=None)):
    sid = get_or_create_session(session_id)
    set_buffer(sid, "")
    html = render_home("", message="Taslak sıfırlandı.", drop_saw=True)
    resp = HTMLResponse(html)
    resp.set_cookie("session_id", sid, max_age=60*60*24*30, samesite="lax")
    return resp
