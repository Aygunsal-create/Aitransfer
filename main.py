import os
import re
import json
import uuid
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

from fastapi import FastAPI, Form, Cookie
from fastapi.responses import HTMLResponse, PlainTextResponse

app = FastAPI()

# -----------------------------
# Storage (file-based buffer to survive multiple Railway workers)
# -----------------------------
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

DB_PATH = "db.json"


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


def session_file(sid: str) -> Path:
    return DATA_DIR / f"{sid}.txt"


def get_or_create_session(session_id: Optional[str]) -> str:
    db = _load_db()
    sessions = db.setdefault("sessions", {})
    if not session_id or session_id not in sessions:
        session_id = uuid.uuid4().hex
        sessions[session_id] = {"updated_at": datetime.utcnow().isoformat() + "Z"}
        _save_db(db)
    return session_id


def get_buffer(sid: str) -> str:
    f = session_file(sid)
    if not f.exists():
        return ""
    return f.read_text(encoding="utf-8", errors="replace")


def set_buffer(sid: str, text: str) -> None:
    session_file(sid).write_text(text or "", encoding="utf-8")


def append_to_buffer(sid: str, text: str) -> None:
    if not text:
        return
    cur = get_buffer(sid)
    if cur and not cur.endswith("\n"):
        cur += "\n"
    cur += text
    set_buffer(sid, cur)

    db = _load_db()
    sess = db.setdefault("sessions", {}).setdefault(sid, {})
    sess["updated_at"] = datetime.utcnow().isoformat() + "Z"
    _save_db(db)


# -----------------------------
# Normalization & Cleaning
# -----------------------------
TIME_RE = re.compile(r"(?<!\d)([01]?\d|2[0-3])[:\.]([0-5]\d)(?!\d)")
# Standard airline-ish codes: TK1710, SU2130, BA678, GQ670, LH1300, KL1959, W95771 etc.
FLIGHT_STD_RE = re.compile(r"\b([A-Z]{1,3}\s?\d{2,5})\b", re.IGNORECASE)
# Misc codes like W9A5327, A4*3069
MISC_CODE_RE = re.compile(r"\b([A-Z]\d[A-Z]\d{3,4}|\bA\d\*\d{4}\b)\b", re.IGNORECASE)

WHATSAPP_META_RE = re.compile(r"^\s*\d{1,2}/\d{1,2}\s+\d{1,2}[:\.]\d{2}\s*[^:]{1,40}:\s*")


MONTHS_TR = (
    "Ocak", "Şubat", "Mart", "Nisan", "Mayıs", "Haziran",
    "Temmuz", "Ağustos", "Eylül", "Ekim", "Kasım", "Aralık"
)
DATE_DOT_RE = re.compile(r"\b\d{1,2}\.\d{1,2}\.\d{4}\b")
DATE_MONTH_RE = re.compile(r"\b\d{1,2}\s+(%s)\b" % "|".join(MONTHS_TR), re.IGNORECASE)

# Address-ish keywords (TR + EN)
ADDRESS_HINT_RE = re.compile(
    r"\b(Mah|Mah\.|Caddesi|Cad\.|Cad|Sokak|Sk\.|Sk|No:|No\.|Daire|Kat|Blok|"
    r"Apt|Apartman|Street|St\.|Road|Rd\.|Ave|Avenue|Boulevard|Blvd|"
    r"Beyoğlu|Fatih|Şişli|Beşiktaş|Kadıköy|Üsküdar|Arnavutköy|İstanbul|Turkiye|Türkiye)\b",
    re.IGNORECASE
)
POSTAL_TR_RE = re.compile(r"\b\d{5}\b")
MANY_DIGITS_RE = re.compile(r"\d.*\d.*\d")  # line contains multiple digits somewhere


def normalize_time(hh: str, mm: str) -> str:
    return f"{int(hh):02d}:{int(mm):02d}"


def fix_mojibake(s: str) -> str:
    """
    Fix common WhatsApp mojibake like 'Å', 'Ã¼' when UTF-8 got decoded as latin-1.
    Safe: if it doesn't look like mojibake, returns original.
    """
    if not s:
        return s
    if ("Ã" in s) or ("Å" in s) or ("Ä" in s) or ("Ð" in s):
        try:
            return s.encode("latin-1", errors="strict").decode("utf-8", errors="strict")
        except Exception:
            return s
    return s


def strip_phones(s: str) -> str:
    # removes +90 5xx..., 05xx..., long digit sequences
    s = re.sub(r"\+?\d[\d\s\-]{7,}\d", "", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s


def is_noise_line(line: str) -> bool:
    l = line.strip()
    if not l:
        return True
    # WhatsApp meta like [21/2 06:48] Name:
    if WHATSAPP_META_RE.search(l):
        return False  # we'll strip meta but keep the remaining content
    # status lines
    if l.lower() in {"uçak inmiş", "ucak inmis", "ok", "tamam"}:
        return True
    # pure question mark / separators
    if l in {"?", "-", "—", "–"}:
        return True
    # a bare day number coming from "21 Şubat" cleanup
    if re.fullmatch(r"\d{1,2}", l):
        return True
    # pure phone-ish
    if re.fullmatch(r"\+?\d[\d\s\-]{7,}\d", l):
        return True
    return False


def looks_like_address(line: str) -> bool:
    l = line.strip()
    if not l:
        return False
    # if strong hints or postal code present
    if ADDRESS_HINT_RE.search(l):
        return True
    if POSTAL_TR_RE.search(l) and ("istan" in l.lower() or "türk" in l.lower() or "turk" in l.lower()):
        return True
    # many digits + comma structure often an address
    if l.count(",") >= 2 and MANY_DIGITS_RE.search(l):
        return True
    return False


def clean_line_keep_content(line: str) -> str:
    l = fix_mojibake(line)

    # Remove WhatsApp meta prefix, keep content after "Name:"
    l = WHATSAPP_META_RE.sub("", l).strip()

    # Remove dates (whole)
    l = DATE_DOT_RE.sub("", l)
    l = DATE_MONTH_RE.sub("", l)

    # Clean leftover double spaces
    l = re.sub(r"\s{2,}", " ", l).strip()
    return l


def extract_flight(text: str) -> str:
    t = text or ""
    m = FLIGHT_STD_RE.search(t)
    if m:
        return m.group(1).replace(" ", "").upper()
    m2 = MISC_CODE_RE.search(t)
    if m2:
        return m2.group(1).replace(" ", "").upper()
    return "?"


def extract_names(text_lines: List[str]) -> str:
    """
    Prefer explicit 'İsim listesi' lines, else use cleaned meaningful lines.
    """
    joined = "\n".join(text_lines)

    # Try explicit name list markers
    markers = [
        r"İsim listesi\s*[:：]\s*(.*)",
        r"ISIM listesi\s*[:：]\s*(.*)",
        r"İSİM LİSTESİ\s*[:：]\s*(.*)",
        r"NAME\s*[:：]\s*(.*)",
        r"İSİM\s*[:：]\s*(.*)",
    ]
    for pat in markers:
        m = re.search(pat, joined, flags=re.IGNORECASE)
        if m:
            tail = m.group(1).strip()
            tail = tail.replace("、", ",")
            tail = re.sub(r"[\u201c\u201d\"“”]", "", tail)
            tail = strip_phones(tail)
            tail = re.sub(r"\s{2,}", " ", tail).strip()
            if tail:
                # split by comma and re-join to normalize
                parts = [p.strip() for p in re.split(r"[,\n]+", tail) if p.strip()]
                return ", ".join(parts)

    # Numbered list like "1. NAME" "2. NAME"
    numbered = []
    for ln in text_lines:
        mm = re.match(r"^\s*\d+\.\s*(.+)$", ln.strip())
        if mm:
            val = mm.group(1).strip()
            val = re.sub(r"[\u201c\u201d\"“”]", "", val)
            val = strip_phones(val)
            if val:
                numbered.append(val)
    if numbered:
        return ", ".join(numbered)

    # Fallback: keep lines that look like person names (not addresses, not flight-like, not time-only)
    keep = []
    for ln in text_lines:
        x = ln.strip()
        if not x:
            continue
        if looks_like_address(x):
            continue
        # remove flights and times from candidate
        x2 = FLIGHT_STD_RE.sub("", x)
        x2 = MISC_CODE_RE.sub("", x2)
        x2 = TIME_RE.sub("", x2)
        x2 = strip_phones(x2)
        x2 = re.sub(r"\b(Transfer|Araç|Arac|Otel|Uçak|Ucak|Kod|pax|IST|IHL|Airport|Havalimanı)\b", "", x2, flags=re.IGNORECASE)
        x2 = re.sub(r"\s{2,}", " ", x2).strip()
        if len(x2) >= 2:
            keep.append(x2)

    # Remove duplicates preserve order
    seen = set()
    uniq = []
    for k in keep:
        if k not in seen:
            seen.add(k)
            uniq.append(k)

    return ", ".join(uniq) if uniq else "?"


def should_drop_saw(line: str) -> bool:
    return "SAW" in (line or "").upper()


# -----------------------------
# Core parsing: "time anchors" -> each time starts a new job/row
# -----------------------------
def parse_jobs(raw: str, drop_saw: bool) -> List[Dict[str, str]]:
    lines_in = (raw or "").splitlines()
    lines: List[str] = []

    for ln in lines_in:
        ln = clean_line_keep_content(ln)
        if not ln:
            continue
        if drop_saw and should_drop_saw(ln):
            continue
        # remove pure noise
        if is_noise_line(ln):
            continue
        # drop addresses early
        if looks_like_address(ln):
            continue
        lines.append(ln)

    jobs: List[Dict[str, Any]] = []
    current: Optional[Dict[str, Any]] = None

    def start_job(t: str):
        nonlocal current
        if current is not None:
            jobs.append(current)
        current = {"time": t, "lines": []}

    for ln in lines:
        tm = TIME_RE.search(ln)
        if tm:
            # THIS time is the job time anchor
            tnorm = normalize_time(tm.group(1), tm.group(2))
            start_job(tnorm)
            # keep rest of line content (minus the time)
            rest = TIME_RE.sub("", ln).strip()
            rest = rest.strip(" -–—")
            rest = re.sub(r"\s{2,}", " ", rest).strip()
            if rest and (not looks_like_address(rest)):
                current["lines"].append(rest)
        else:
            if current is None:
                # no time yet -> ignore (we only build rows from time anchors)
                continue
            current["lines"].append(ln)

    if current is not None:
        jobs.append(current)

    # build output rows (one row per job/time)
    out: List[Dict[str, str]] = []
    for j in jobs:
        t = j.get("time", "?")
        jlines = j.get("lines", [])

        # Remove residual date words that could appear alone
        cleaned_lines = []
        for x in jlines:
            x = x.strip()
            if not x:
                continue
            # remove standalone "21 Şubat" remnants safely
            x = DATE_DOT_RE.sub("", x)
            x = DATE_MONTH_RE.sub("", x)
            x = re.sub(r"\s{2,}", " ", x).strip()
            if not x or re.fullmatch(r"\d{1,2}", x):
                continue
            if looks_like_address(x):
                continue
            cleaned_lines.append(x)

        block_text = " ".join(cleaned_lines)
        flight = extract_flight(block_text)
        passenger = extract_names(cleaned_lines)

        out.append({"saat": t, "ucus": flight, "yolcu": passenger})

    return out


def rows_to_tsv(rows: List[Dict[str, str]]) -> str:
    """
    4 columns:
    Saat \t (blank) \t Uçuş \t Yolcu
    No grouping. One row per detected job time.
    """
    out_lines = []
    for r in rows:
        saat = (r.get("saat") or "?").strip()
        ucus = (r.get("ucus") or "?").strip().upper()
        yolcu = (r.get("yolcu") or "?").strip()
        out_lines.append(f"{saat}\t\t{ucus}\t{yolcu}")
    return "\n".join(out_lines)


# -----------------------------
# UI
# -----------------------------
def render_home(buffer_text: str, message: str = "", drop_saw: bool = True) -> str:
    checked = "checked" if drop_saw else ""
    buf_len = len(buffer_text or "")
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
    <p><b>Bulunan kayıt:</b> {found}</p>
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

    rows = parse_jobs(raw, drop_saw=bool(drop_saw))
    tsv = rows_to_tsv(rows)

    # store last result in db (small)
    db = _load_db()
    sess = db.setdefault("sessions", {}).setdefault(sid, {})
    sess["last_result_tsv"] = tsv
    sess["updated_at"] = datetime.utcnow().isoformat() + "Z"
    _save_db(db)

    return HTMLResponse(render_result(tsv, found=len(rows)))


@app.get("/download")
def download(session_id: Optional[str] = Cookie(default=None)):
    sid = get_or_create_session(session_id)
    db = _load_db()
    tsv = db.get("sessions", {}).get(sid, {}).get("last_result_tsv", "")

    # Excel/Sheets TR için UTF-8 BOM ekleyelim
    bom_tsv = "\ufeff" + (tsv or "")

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
