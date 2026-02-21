import os, re, json, uuid
from datetime import datetime
from typing import Dict, Any, List

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
# Regex / cleaning
# -----------------------------
TIME_RE = re.compile(r"\b([01]?\d|2[0-3])[:\.]([0-5]\d)\b")
FLIGHT_RE = re.compile(r"\b([A-Z]{1,3}\s?\d{2,5}|[A-Z]\d[A-Z]?\d{3,5}|\d[A-Z]{1,2}\d{3,5})\b", re.IGNORECASE)

WA_PREFIX_RE = re.compile(r"^\s*\[\d{1,2}/\d{1,2}\s+\d{1,2}:\d{2}\]\s*[^:]{1,60}:\s*", re.UNICODE)
PHONE_RE = re.compile(r"\+?\d[\d\s\-\(\)]{7,}\d")

ADDRESS_HINT_RE = re.compile(
    r"(\bmah\b|\bmahalle\b|\bcad\b|\bcadde\b|\bcd\.\b|\bsk\b|\bsok\b|\bsokak\b|\bno\b[:\.]|\bkat\b|\bd:i?re\b|\bapt\b|\bblok\b|"
    r"\bistanbul\b|\btürkiye\b|\bturkiye\b|\btr\b|\bpostcode\b|\bzip\b|\b\d{5}\b|"
    r"fatih/|beyoğlu/|beşiktaş/|şişli/|arnavutköy/|cankurtaran|sirkeci|harbiye|gümüşsuyu)",
    re.IGNORECASE | re.UNICODE
)

NOISE_LINE_RE = re.compile(r"\b(uçak inmiş|ucak inmis|landed|arrived|inmiş|inmis)\b", re.IGNORECASE)

# "Alış saat:" yakalama (TR + farklı yazımlar)
PICKUP_TIME_RE = re.compile(r"\b(alış\s*saat|alis\s*saat)\s*:\s*([01]?\d|2[0-3])[:\.]([0-5]\d)\b", re.IGNORECASE | re.UNICODE)
FLIGHT_LINE_RE = re.compile(r"\b(uçak\s*kod|ucak\s*kod)\s*:\s*([A-Z]{1,3}\s?\d{2,5})\b", re.IGNORECASE | re.UNICODE)
NAME_LINE_RE = re.compile(r"\b(isim\s*listesi|i̇sim\s*listesi|isim\s*list|i̇si̇m\s*li̇stesi|i̇si̇m\s*li̇stesi)\s*:\s*(.+)$", re.IGNORECASE | re.UNICODE)

def normalize_time(hh: str, mm: str) -> str:
    return f"{int(hh):02d}:{int(mm):02d}"

def fix_mojibake(s: str) -> str:
    if not s:
        return s
    if not any(x in s for x in ("Ã", "Â", "Å", "Ä", "Ð", "Þ", "Ý", "�")):
        return s
    try:
        candidate = s.encode("latin1", errors="strict").decode("utf-8", errors="strict")
        if candidate.count("�") <= s.count("�"):
            return candidate
    except Exception:
        pass
    return s

def clean_line(line: str) -> str:
    line = fix_mojibake(line)
    line = WA_PREFIX_RE.sub("", line)
    line = line.replace("\u200e", "").replace("\u200f", "")
    return line.strip()

def strip_phones(s: str) -> str:
    s = PHONE_RE.sub("", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def remove_quotes_noise(s: str) -> str:
    s = s.replace('"', " ").replace("“", " ").replace("”", " ")
    s = s.replace("：", ":")  # full-width colon
    s = re.sub(r"\s*\.\s*", " ", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def should_drop_saw(text: str) -> bool:
    return "SAW" in (text or "").upper()

def is_address_line(line: str) -> bool:
    if ADDRESS_HINT_RE.search(line):
        digits = sum(ch.isdigit() for ch in line)
        letters = sum(ch.isalpha() for ch in line)
        if digits >= 6 or (digits >= 3 and letters >= 10):
            return True
    return False

# -----------------------------
# Mode 1: "Alış saat:" blokları (bu varsa bunu kullan!)
# -----------------------------
def parse_pickup_style(raw: str, drop_saw: bool) -> List[Dict[str, str]]:
    lines = [clean_line(x) for x in (raw or "").splitlines()]
    # filtre
    filtered: List[str] = []
    for ln in lines:
        if not ln:
            continue
        if NOISE_LINE_RE.search(ln):
            continue
        if drop_saw and should_drop_saw(ln):
            continue
        if is_address_line(ln):
            continue
        filtered.append(fix_mojibake(ln))

    rows: List[Dict[str, str]] = []
    current = None  # {"saat":..., "ucus":..., "names":[...]}

    def flush():
        nonlocal current
        if not current:
            return
        names = [n.strip() for n in current.get("names", []) if n.strip()]
        # uniq preserve order
        seen = set()
        uniq = []
        for n in names:
            if n not in seen:
                seen.add(n)
                uniq.append(n)
        yolcu = ", ".join(uniq) if uniq else "?"
        rows.append({"saat": current.get("saat", "?"), "ucus": current.get("ucus", "?"), "yolcu": yolcu})
        current = None

    for ln in filtered:
        # Yeni iş başlangıcı: Alış saat
        pm = PICKUP_TIME_RE.search(ln)
        if pm:
            flush()
            saat = normalize_time(pm.group(2), pm.group(3))
            current = {"saat": saat, "ucus": "?", "names": []}
            continue

        if current is None:
            continue  # alış saat görmeden başlamayız

        # Uçak kod satırı
        fm = FLIGHT_LINE_RE.search(ln.replace("İ", "I").replace("ı", "i"))
        if fm:
            current["ucus"] = fm.group(2).replace(" ", "").upper()
            continue

        # İsim listesi satırı (ve devam satırları)
        nm = NAME_LINE_RE.search(ln)
        if nm:
            tail = nm.group(2).strip()
            tail = remove_quotes_noise(strip_phones(tail))
            # Çin ayırıcı 、 ve / ve , ile böl
            parts = re.split(r"[、,]+", tail)
            for p in parts:
                p = p.strip()
                if p:
                    current["names"].append(p)
            continue

        # İsim listesi bazen bir alt satıra taşar: "İsim listesi：" satırından sonra isimler tek satır gelebilir
        # Bu yüzden, current varsa ve satır "XIE/..." gibi isim formatındaysa ekle.
        # Adres/otel/araç tipi vs. olanları alma
        low = ln.lower()
        if any(k in low for k in ("otel", "araç", "arac", "transfer", "pax", "uçak", "ucak", "kodu", "can-ist", "hkg", "ist", "alış", "alis")):
            continue

        # isim gibi duruyorsa ekle
        candidate = remove_quotes_noise(strip_phones(ln))
        if candidate and len(candidate) >= 2:
            # sadece tarih ise alma
            if re.fullmatch(r"\d{1,2}\.\d{1,2}\.\d{4}", candidate):
                continue
            # tamamen saatler gibi ise alma
            if TIME_RE.fullmatch(candidate):
                continue
            current["names"].append(candidate)

    flush()
    return rows

# -----------------------------
# Mode 2: generic time blocks (fallback)
# -----------------------------
def split_into_time_blocks(raw: str, drop_saw: bool) -> List[Dict[str, str]]:
    lines = [clean_line(x) for x in (raw or "").splitlines()]
    filtered: List[str] = []
    for ln in lines:
        if not ln:
            continue
        if NOISE_LINE_RE.search(ln):
            continue
        if drop_saw and should_drop_saw(ln):
            continue
        if is_address_line(ln):
            continue
        filtered.append(ln)

    text = fix_mojibake("\n".join(filtered))
    matches = list(TIME_RE.finditer(text))
    blocks: List[Dict[str, str]] = []
    if not matches:
        return blocks

    for i, m in enumerate(matches):
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        time_str = normalize_time(m.group(1), m.group(2))
        chunk = text[start:end].strip()
        blocks.append({"saat": time_str, "chunk": chunk})
    return blocks

def extract_flight_and_names(chunk: str) -> Dict[str, str]:
    c = chunk
    c = TIME_RE.sub(" ", c)
    fm = FLIGHT_RE.search(c)
    flight = fm.group(1).replace(" ", "").upper() if fm else "?"
    if fm:
        c = re.sub(re.escape(fm.group(1)), " ", c, flags=re.IGNORECASE)
    c = strip_phones(c)
    c = re.sub(r"\[\d{1,2}/\d{1,2}\s+\d{1,2}:\d{2}\]", " ", c)
    c = re.sub(r"\b\d+\.\s*", " ", c)
    c = re.sub(r"/\s*\d+\s*(yolcu|yolcular|passenger|passengers)\b", " ", c, flags=re.IGNORECASE)
    c = remove_quotes_noise(c)
    parts = [p.strip() for p in re.split(r"[\n,]+", c) if p.strip()]
    cleaned: List[str] = []
    for p in parts:
        if len(p) < 2:
            continue
        if FLIGHT_RE.fullmatch(p.replace(" ", ""), re.IGNORECASE):
            continue
        if re.fullmatch(r"[\d\W_]+", p):
            continue
        cleaned.append(p)
    seen = set()
    uniq = []
    for n in cleaned:
        if n not in seen:
            seen.add(n)
            uniq.append(n)
    names = ", ".join(uniq) if uniq else "?"
    return {"ucus": flight, "yolcu": names}

def to_tsv(rows: List[Dict[str, str]]) -> str:
    out = []
    for r in rows:
        out.append(f"{r.get('saat','?')}\t\t{r.get('ucus','?')}\t{r.get('yolcu','?')}")
    return "\n".join(out)

# -----------------------------
# UI
# -----------------------------
def render_home(buffer_text: str, message: str = "", drop_saw: bool = True) -> str:
    checked = "checked" if drop_saw else ""
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
      WhatsApp metnini parça parça ekle → <b>Ekle</b><br/>
      Bitince → <b>Bitti</b> → TSV üret.
    </div>

    {"<div class='msg'>" + message + "</div>" if message else ""}

    <form method="post" action="/add">
      <label><input type="checkbox" name="drop_saw" value="1" {checked}/> SAW satırlarını çıkar</label>
      <div class="counter">Taslak metin: {buf_len} karakter</div>
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
def home(session_id: str | None = Cookie(default=None)):
    sid = get_or_create_session(session_id)
    buf = get_buffer(sid)
    html = render_home(buf, message="", drop_saw=True)
    resp = HTMLResponse(html)
    resp.set_cookie("session_id", sid, max_age=60*60*24*30)
    return resp

@app.post("/add", response_class=HTMLResponse)
def add_piece(
    text: str = Form(default=""),
    drop_saw: str | None = Form(default=None),
    session_id: str | None = Cookie(default=None),
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
    session_id: str | None = Cookie(default=None),
):
    sid = get_or_create_session(session_id)
    raw = get_buffer(sid)

    # Önce "Alış saat:" var mı? varsa o moda geç
    if PICKUP_TIME_RE.search(raw or ""):
        rows = parse_pickup_style(raw, drop_saw=bool(drop_saw))
    else:
        blocks = split_into_time_blocks(raw, drop_saw=bool(drop_saw))
        rows = []
        for b in blocks:
            info = extract_flight_and_names(b["chunk"])
            rows.append({"saat": b["saat"], "ucus": info["ucus"], "yolcu": info["yolcu"]})

    tsv = to_tsv(rows)
    set_last_result(sid, tsv)

    return HTMLResponse(render_result(tsv, count=len(rows)))

@app.get("/download")
def download(session_id: str | None = Cookie(default=None)):
    sid = get_or_create_session(session_id)
    tsv = get_last_result(sid) or ""
    bom_tsv = "\ufeff" + tsv  # UTF-8 BOM (Sheets/Excel TR karakter için)
    headers = {
        "Content-Disposition": "attachment; filename=result.tsv",
        "Content-Type": "text/tab-separated-values; charset=utf-8",
    }
    return PlainTextResponse(bom_tsv, headers=headers)

@app.post("/reset", response_class=HTMLResponse)
def reset(session_id: str | None = Cookie(default=None)):
    sid = get_or_create_session(session_id)
    set_buffer(sid, "")
    html = render_home("", message="Taslak sıfırlandı.", drop_saw=True)
    resp = HTMLResponse(html)
    resp.set_cookie("session_id", sid, max_age=60*60*24*30)
    return resp
