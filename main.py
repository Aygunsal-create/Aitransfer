import os, re, json, uuid
from datetime import datetime
from typing import Dict, Any, List, Tuple

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

def get_or_create_session(session_id: str | None) -> str:
    db = _load_db()
    if not session_id or session_id not in db.get("sessions", {}):
        session_id = uuid.uuid4().hex
        db["sessions"][session_id] = {
            "buffer": "",
            "updated_at": datetime.utcnow().isoformat() + "Z"
        }
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
    db["sessions"][session_id] = {
        "buffer": text or "",
        "updated_at": datetime.utcnow().isoformat() + "Z"
    }
    _save_db(db)

def get_buffer(session_id: str) -> str:
    db = _load_db()
    return db.get("sessions", {}).get(session_id, {}).get("buffer", "")

# -----------------------------
# Parsing / Rules
# -----------------------------
TIME_RE = re.compile(r"\b([01]?\d|2[0-3])[:\.]([0-5]\d)\b")
FLIGHT_RE = re.compile(r"\b([A-Z]{1,3}\s?\d{2,5})\b", re.IGNORECASE)

# WhatsApp header: [21/2 06:48] Eyüp Abi BDR: ...
WA_HDR_RE = re.compile(r"^\s*\[\d{1,2}/\d{1,2}\s+\d{1,2}:\d{2}\]\s*[^:]{1,60}:\s*", re.UNICODE)

# "Alış saat: 5:50" / "Alis saat: 05:50"
PICKUP_LABEL_RE = re.compile(r"(alış|alis)\s*saat\s*:\s*([01]?\d|2[0-3])[:\.]([0-5]\d)", re.IGNORECASE)

# "Uçak kod:" / "Ucus kod:" / "Uçuş kod:" / "Ucus numarası:"
FLIGHT_LABEL_RE = re.compile(r"(uçak|ucak|uçuş|ucus)\s*(kod|kodu|no|numarası|numarasi)\s*:\s*([A-Z]{1,3}\s?\d{2,5})", re.IGNORECASE)

# Address-like / location junk
ADDR_HINT_RE = re.compile(
    r"(mah(alle)?|cad(de)?|sk(\.|ak)?|no\s*:|sokak|street|st\.|avenue|ave\.|blvd|"
    r"fatih/|beyoğlu/|şişli/|beşiktaş/|arnavutköy/|istanbul|türkiye|turkiye|"
    r"\b\d{5}\b|\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b)",
    re.IGNORECASE
)

def normalize_time(hh: str, mm: str) -> str:
    return f"{int(hh):02d}:{int(mm):02d}"

def strip_phones(s: str) -> str:
    # +90 5xx xxx xx xx / 05xx... / 10+ digits etc.
    s = re.sub(r"\+?\d[\d\s\-\(\)]{7,}\d", "", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def fix_mojibake(s: str) -> str:
    """
    WhatsApp kopyasında bazen 'Ã¼' gibi bozuk Türkçe çıkar.
    Basit düzeltme denemesi: UTF-8 bytes -> Latin-1 decode edilmesi gibi durumları toparlar.
    Güvenli: olmazsa aynı döner.
    """
    if not s:
        return s
    try:
        if "Ã" in s or "Å" in s or "Ä" in s:
            return s.encode("latin-1", errors="ignore").decode("utf-8", errors="ignore")
    except Exception:
        pass
    return s

def clean_line(line: str) -> str:
    line = fix_mojibake(line)

    # WhatsApp imzası / header temizle
    line = WA_HDR_RE.sub("", line).strip()

    # "21 Şubat" gibi tek başına tarih satırlarını at
    if re.fullmatch(
        r"\d{1,2}\s*(şubat|subat|ocak|mart|nisan|mayıs|mayis|haziran|temmuz|ağustos|agustos|"
        r"eylül|eylul|ekim|kasım|kasim|aralık|aralik)\s*\d{0,4}",
        line,
        flags=re.I
    ):
        return ""

    # "Yarın için", "Uçak inmiş" gibi bilgi satırlarını at
    if re.search(r"\b(uçak inmiş|yarın için|bugün|today|tomorrow)\b", line, flags=re.I):
        return ""

    # Adres satırı gibi görünüyorsa komple at
    if ADDR_HINT_RE.search(line):
        return ""

    return line.strip()

def should_drop_saw(line: str) -> bool:
    return "SAW" in line.upper()

def extract_rows_from_text(raw: str, drop_saw: bool) -> List[Dict[str, str]]:
    """
    Yeni mantık:
    - Satırları temizle (WhatsApp header, tarih, adres, bozuk TR karakter)
    - 'Alış saat:' varsa onu JOB saati kabul et
    - Yoksa satır tek başına saat ise JOB saati kabul et
    - Uçuş kodunu mümkünse 'Uçak kod:' etiketinden, yoksa genel FLIGHT_RE'den al
    - Uçuş içindeki 23:15-05:50 gibi saatler JOB saati yapılmaz
    """
    rows: List[Dict[str, str]] = []
    current_time: str | None = None
    pending_names: List[str] = []
    pending_flight: str | None = None

    def flush_pending():
        nonlocal pending_names, pending_flight, current_time, rows
        if not current_time:
            return
        if not pending_names and not pending_flight:
            return

        yolcu = ", ".join([n for n in pending_names if n]) if pending_names else "?"
        ucus = (pending_flight or "?").strip().replace(" ", "").upper() if pending_flight else "?"
        rows.append({"saat": current_time, "ucus": ucus, "yolcu": yolcu})

        pending_names = []
        pending_flight = None

    for raw_line in (raw or "").splitlines():
        line0 = raw_line.strip()
        if not line0:
            continue

        if drop_saw and should_drop_saw(line0):
            continue

        line = clean_line(line0)
        if not line:
            continue

        # 1) "Alış saat: 5:50"  → yeni JOB
        m_pick = PICKUP_LABEL_RE.search(line)
        if m_pick:
            flush_pending()
            current_time = normalize_time(m_pick.group(2), m_pick.group(3))

            rest = PICKUP_LABEL_RE.sub("", line).strip(" -–:")
            rest = strip_phones(rest)
            if rest and len(rest) > 2:
                pending_names.append(rest)
            continue

        # 2) Satır tek başına saat ise → yeni JOB (ör: "21:00" veya "18.05")
        m_time_only = re.fullmatch(r"\s*([01]?\d|2[0-3])[:\.]([0-5]\d)\s*", line)
        if m_time_only:
            flush_pending()
            current_time = normalize_time(m_time_only.group(1), m_time_only.group(2))
            continue

        # current_time yoksa hiçbir şey toplama (saat kesinleşmeden veri alma)
        if not current_time:
            continue

        # 3) Uçuş etiketi: "Uçak kod: TK073 ..."
        m_flabel = FLIGHT_LABEL_RE.search(line)
        if m_flabel:
            pending_flight = m_flabel.group(3)
            continue

        # 4) Genel uçuş kodu
        fm = FLIGHT_RE.search(line)
        if fm:
            pending_flight = fm.group(1)

        # 5) İsim listesi / name satırları
        # "İsim listesi: ..." veya "Name:" vb.
        if re.search(r"\b(isim|name)\b", line, flags=re.I):
            tail = re.split(r":", line, maxsplit=1)[-1].strip()
            tail = strip_phones(tail)
            parts = re.split(r"[、,;/]|\\n", tail)
            for p in parts:
                p = p.strip().strip('"').strip("'")
                if len(p) >= 2:
                    pending_names.append(p)
            continue

        # Numaralı isim listesi: "1. VLACHOS ... 2. ..."
        if re.match(r"^\s*\d+\.\s*\S+", line):
            t = re.sub(r"^\s*\d+\.\s*", "", line).strip()
            t = strip_phones(t)
            if len(t) >= 2:
                pending_names.append(t)
            continue

        # Normal isim satırı
        candidate = strip_phones(line).strip().strip('"').strip("'")

        # Açıklama satırlarını isim sanma
        if re.search(r"\b(transfer|araç|arac|otel|pax|ist alış|ist alis)\b", candidate, flags=re.I):
            continue
        if re.fullmatch(r"[A-Z]{3}\s*-\s*[A-Z]{3}", candidate):
            continue

        if len(candidate) >= 2:
            pending_names.append(candidate)

    flush_pending()
    return rows

def rows_to_tsv_grouped(rows: List[Dict[str, str]]) -> str:
    """
    Senin kurala göre:
    - Aynı saat = 1 iş/araç
    - Aynı saatte birden çok yolcu varsa 4. sütunda aynı hücrede virgülle
    - (Pratikte: saat bazlı gruplayıp birleştiriyoruz)
    - 4 sütun: Saat, boş, Uçuş, Yolcu(lar)
    NOT: Saatte birden fazla farklı uçuş varsa -> aynı saatte farklı satır olur (mantıklı)
    """
    grouped: Dict[Tuple[str, str], List[str]] = {}

    for r in rows:
        saat = (r.get("saat") or "?").strip()
        ucus = (r.get("ucus") or "?").strip().upper()
        yolcu = (r.get("yolcu") or "").strip()

        if not yolcu:
            continue

        # kesin telefon temizliği
        yolcu = strip_phones(yolcu)

        key = (saat, ucus)
        grouped.setdefault(key, []).append(yolcu)

    # sort by time then flight
    def time_key(t: str) -> Tuple[int, int, str]:
        m = TIME_RE.search(t)
        if not m:
            return (99, 99, t)
        return (int(m.group(1)), int(m.group(2)), t)

    items = sorted(grouped.items(), key=lambda kv: (time_key(kv[0][0]), kv[0][1]))

    lines = []
    for (saat, ucus), yolcular in items:
        # unique preserve order
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
    .warn {{ color:#b00; margin:10px 0; }}
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

def render_result(tsv: str, found_count: int) -> str:
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
    <p>Bulunan kayıt: {found_count}</p>
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
    resp.set_cookie("session_id", sid, max_age=60*60*24*30)  # 30 days
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

    rows = extract_rows_from_text(raw, drop_saw=bool(drop_saw))
    tsv = rows_to_tsv_grouped(rows)

    db = _load_db()
    sess = db["sessions"].setdefault(sid, {"buffer": raw, "updated_at": ""})
    sess["last_result_tsv"] = tsv
    sess["updated_at"] = datetime.utcnow().isoformat() + "Z"
    _save_db(db)

    found = len([ln for ln in tsv.splitlines() if ln.strip()]) if tsv else 0
    return HTMLResponse(render_result(tsv, found))

@app.get("/download")
def download(session_id: str | None = Cookie(default=None)):
    sid = get_or_create_session(session_id)
    db = _load_db()
    tsv = db.get("sessions", {}).get(sid, {}).get("last_result_tsv", "")

    # UTF-8 BOM ekleyelim (Excel/Sheets TR karakteri doğru görsün)
    bom_tsv = "\ufeff" + (tsv or "")

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
```0
