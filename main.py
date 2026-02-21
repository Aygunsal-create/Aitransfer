import os, re, json, uuid
from datetime import datetime
from typing import Dict, Any, List, Optional

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
# Parsing / Cleaning
# -----------------------------
TIME_RE = re.compile(r"\b([01]?\d|2[0-3])[:\.]([0-5]\d)\b")
DATE_RE = re.compile(r"\b(\d{1,2}[./]\d{1,2}[./]\d{2,4}|\d{1,2}\s*(Şubat|Ocak|Mart|Nisan|Mayıs|Haziran|Temmuz|Ağustos|Eylül|Ekim|Kasım|Aralık))\b", re.IGNORECASE)

# Flight codes we want:
# TK1710, SU2130, BA678, CZ8065, W95771, A3994, etc.
FLIGHT_RE = re.compile(r"\b([A-Z]{1,3}\d{2,5}|[A-Z]\d{1}[A-Z]\d{3,4}|[A-Z]{1,2}\d{4,5})\b", re.IGNORECASE)

WHATSAPP_PREFIX_RE = re.compile(
    r"^\s*\d{1,2}[./]\d{1,2}(?:[./]\d{2,4})?\s+\d{1,2}[:\.]\d{2}\s*[^:]{1,80}:\s*"
)

ONLY_TIME_LINE_RE = re.compile(r"^\s*([01]?\d|2[0-3])[:\.]([0-5]\d)\s*$")


def normalize_time(hh: str, mm: str) -> str:
    return f"{int(hh):02d}:{int(mm):02d}"


def fix_mojibake(s: str) -> str:
    """
    WhatsApp/Sheets bazen UTF-8'i Latin-1 gibi gösteriyor:
    'Åubat', 'Ãimen' vb. Bu tip bozulmaları mümkünse düzelt.
    """
    if not s:
        return s
    # Heuristic: if looks mojibake-ish
    if "Ã" in s or "Å" in s or "Ä" in s:
        try:
            return s.encode("latin1", errors="ignore").decode("utf-8", errors="ignore")
        except Exception:
            return s
    return s


def strip_phones(s: str) -> str:
    # Remove phone-ish tokens: +90..., 10+ digits with spaces, etc.
    s = re.sub(r"\+?\d[\d\s\-]{7,}\d", "", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s


def looks_like_address(line: str) -> bool:
    u = line.upper()
    if "HTTP" in u or "WWW." in u:
        return True
    if re.search(r"\b\d{5}\b", line):  # postal code
        return True
    # Address keywords (TR)
    keywords = ["MAH", "MAHALLE", "CAD", "CADDESI", "CD.", "SOK", "SK.", "NO:", "KAT", "DAIRE", "FATIH/", "BEYOĞLU/", "İSTANBUL", "TÜRKİYE", "TURKIYE"]
    if any(k in u for k in keywords):
        return True
    # Many commas often indicates full address
    if line.count(",") >= 3:
        return True
    return False


def clean_line(line: str) -> str:
    line = fix_mojibake(line)
    line = line.replace("\u00a0", " ").strip()

    # Remove WhatsApp signature prefix
    line = WHATSAPP_PREFIX_RE.sub("", line)

    # Remove common noise
    line = line.replace("Uçak inmiş", "").strip()
    line = re.sub(r"\b(Yarın için|Bugün için)\b", "", line, flags=re.IGNORECASE).strip()

    # Remove emoji (keep it simple)
    line = re.sub(r"[\U0001F300-\U0001FAFF]", "", line).strip()

    # Normalize multiple spaces
    line = re.sub(r"\s{2,}", " ", line).strip()
    return line


def is_noise_line(line: str) -> bool:
    if not line:
        return True

    # Dates like 16.02.2026 or "21 Şubat" -> drop (do NOT touch times)
    if DATE_RE.search(line):
        # but if the line is ONLY a time, keep it
        if ONLY_TIME_LINE_RE.match(line):
            return False
        return True

    u = line.upper()

    # Drop address lines
    if looks_like_address(line):
        return True

    # Drop logistical lines we don't want inside "yolcu"
    bad_starts = [
        "ALIŞ SAAT", "ALIS SAAT", "TRANSFER", "ARAÇ", "ARAC", "OTEL",
        "UÇAK KOD", "UCAK KOD", "UÇUŞ", "UCUS", "UÇUŞ KOD", "UCUS KOD",
        "PAX", "IST ALIŞ", "IST ALIS"
    ]
    if any(u.startswith(x) for x in bad_starts):
        # BUT: sometimes flight code is on this line too. We'll handle flight extraction separately,
        # so we can drop it from passenger text.
        return False

    # Lines that are only separators / quotes
    if u in {"?", "-", "—", "–", "''", '""'}:
        return True

    return False


def extract_pickup_time_from_line(line: str) -> Optional[str]:
    """
    Return time if line indicates pickup time (or is only a time).
    Avoid taking times inside flight range "23:15-05:50" as job time.
    """
    m_only = ONLY_TIME_LINE_RE.match(line)
    if m_only:
        return normalize_time(m_only.group(1), m_only.group(2))

    # "Alış saat: 5:50"
    m = re.search(r"(ALIŞ|ALIS)\s*SAAT[:\s]*([01]?\d|2[0-3])[:\.]([0-5]\d)", line, flags=re.IGNORECASE)
    if m:
        return normalize_time(m.group(2), m.group(3))

    return None


def extract_flight_from_line(line: str) -> Optional[str]:
    """
    Extract first flight-ish code.
    Avoid grabbing things like "A4*3069" (has *).
    """
    if "*" in line:
        # A4*3069 -> not flight code
        line2 = line.replace("*", "")
    else:
        line2 = line

    fm = FLIGHT_RE.search(line2)
    if not fm:
        return None
    code = fm.group(1).replace(" ", "").upper()

    # Exclude very short accidental matches
    if len(code) < 4:
        return None

    return code


def extract_names_from_line(line: str) -> List[str]:
    """
    Extract passenger names from various formats:
    - "İsim listesi：A, B, C"  / Chinese comma "、"
    - "1. NAME" "2. NAME"
    - Plain "Funda Kara"
    """
    s = line.strip()

    # If contains "İsim listesi"
    m = re.search(r"(İSİM|ISIM)\s*LİSTESİ\s*[:：]\s*(.+)$", s, flags=re.IGNORECASE)
    if m:
        tail = m.group(2).strip()
        tail = tail.replace("、", ",")
        parts = [p.strip().strip('"').strip("'") for p in tail.split(",")]
        return [p for p in parts if p]

    # Numbered lines "1. VLACHOS SPYRIDON"
    s = re.sub(r"^\s*\d+\.\s*", "", s).strip()

    # Remove phones
    s = strip_phones(s)

    # If line still contains obvious non-name tokens, drop them
    u = s.upper()
    if any(k in u for k in ["OTEL:", "OTEL", "TRANSFER", "ARAÇ", "ARAC", "PAX", "IST", "ALIŞ", "ALIS", "UÇAK", "UCAK", "UÇUŞ", "UCUS"]):
        # but line can still be a name; be conservative:
        # if it has 2+ words and mostly letters, allow.
        pass

    # A very simple name heuristic: at least 2 letters
    if len(re.sub(r"[^A-Za-zÇĞİÖŞÜçğıöşüÀ-ÿ\s\-']", "", s)) < 2:
        return []

    # If this is only a flight code, ignore
    if ONLY_TIME_LINE_RE.match(s):
        return []

    if FLIGHT_RE.fullmatch(s.replace(" ", "")):
        return []

    # Clean quotes
    s = s.strip().strip('"').strip("'").strip()
    if not s:
        return []

    return [s]


def parse_jobs(raw: str, drop_saw: bool) -> List[Dict[str, str]]:
    """
    State-machine parser.
    We build a pending record by collecting:
      - names
      - flight
      - pickup time
    For WhatsApp format, typically: Name -> Flight -> Time
    When we encounter a pickup time and pending has a name/flight, we finalize one job.
    """
    raw = fix_mojibake(raw or "")
    lines0 = raw.splitlines()

    # Pre-clean lines
    lines: List[str] = []
    saw_flag = False
    for ln in lines0:
        cl = clean_line(ln)
        if not cl:
            continue
        if "SAW" in cl.upper():
            saw_flag = True
        lines.append(cl)

    jobs: List[Dict[str, str]] = []
    pending_names: List[str] = []
    pending_flight: str = "?"
    pending_drop: bool = False

    def finalize(time_str: str):
        nonlocal pending_names, pending_flight, pending_drop
        if drop_saw and pending_drop:
            pending_names = []
            pending_flight = "?"
            pending_drop = False
            return

        # If no names, don't create a record
        if not pending_names:
            pending_names = []
            pending_flight = "?"
            pending_drop = False
            return

        # De-dup preserve order
        seen = set()
        uniq = []
        for n in pending_names:
            n2 = n.strip()
            if not n2:
                continue
            if n2 not in seen:
                seen.add(n2)
                uniq.append(n2)

        jobs.append({
            "saat": time_str or "?",
            "ucus": pending_flight or "?",
            "yolcu": ", ".join(uniq)
        })

        pending_names = []
        pending_flight = "?"
        pending_drop = False

    for cl in lines:
        if drop_saw and "SAW" in cl.upper():
            pending_drop = True
            continue

        # If address line, skip
        if looks_like_address(cl):
            continue

        # Flight extraction (even if the line is otherwise "noise")
        flight = extract_flight_from_line(cl)
        if flight:
            pending_flight = flight

        # Pickup time?
        pickup_time = extract_pickup_time_from_line(cl)
        if pickup_time:
            # Finalize only if we already collected something meaningful
            if pending_names or (pending_flight and pending_flight != "?"):
                finalize(pickup_time)
            else:
                # time exists but no names/flight yet -> do nothing
                pass
            continue

        # Drop pure date lines (do not touch other lines)
        if DATE_RE.search(cl) and not ONLY_TIME_LINE_RE.match(cl):
            continue

        # If it's noise-like line but could contain names list, let extract_names try
        if is_noise_line(cl):
            # still try extract names from "İsim listesi"
            ns = extract_names_from_line(cl)
            if ns:
                pending_names.extend(ns)
            continue

        # Names
        ns = extract_names_from_line(cl)
        if ns:
            pending_names.extend(ns)

    return jobs


def jobs_to_tsv(jobs: List[Dict[str, str]]) -> str:
    """
    4 columns TSV:
    Saat, (blank), Uçuş, Yolcu
    Do NOT auto-merge different jobs.
    """
    out_lines = []
    for j in jobs:
        saat = (j.get("saat") or "?").strip()
        ucus = (j.get("ucus") or "?").strip().upper()
        yolcu = (j.get("yolcu") or "?").strip()
        out_lines.append(f"{saat}\t\t{ucus}\t{yolcu}")
    return "\n".join(out_lines)


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
      Parça parça liste ekle → <b>Ekle (Kaydet)</b><br/>
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
    <p>Bulunan kayıt: <b>{found}</b></p>
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
    resp.set_cookie("session_id", sid, max_age=60 * 60 * 24 * 30)  # 30 days
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
    resp.set_cookie("session_id", sid, max_age=60 * 60 * 24 * 30)
    return resp


@app.post("/finish", response_class=HTMLResponse)
def finish(
    drop_saw: int = Form(default=1),
    session_id: Optional[str] = Cookie(default=None),
):
    sid = get_or_create_session(session_id)
    raw = get_buffer(sid)

    jobs = parse_jobs(raw, drop_saw=bool(drop_saw))
    tsv = jobs_to_tsv(jobs)

    set_last_result(sid, tsv)
    return HTMLResponse(render_result(tsv, found=len(jobs)))


@app.get("/download")
def download(session_id: Optional[str] = Cookie(default=None)):
    sid = get_or_create_session(session_id)
    tsv = get_last_result(sid) or ""
    # UTF-8 BOM (Excel/Sheets TR için)
    bom_tsv = "\ufeff" + tsv
    headers = {
        "Content-Disposition": "attachment; filename=result.tsv",
        "Content-Type": "text/tab-separated-values; charset=utf-8",
    }
    return PlainTextResponse(bom_tsv, headers=headers)


@app.post("/reset", response_class=HTMLResponse)
def reset(session_id: Optional[str] = Cookie(default=None)):
    sid = get_or_create_session(session_id)
    set_buffer(sid, "")
    html = render_home("", message="Taslak sıfırlandı.", drop_saw=True)
    resp = HTMLResponse(html)
    resp.set_cookie("session_id", sid, max_age=60 * 60 * 24 * 30)
    return resp
