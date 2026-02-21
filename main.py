from fastapi import FastAPI, Form
from fastapi.responses import HTMLResponse, PlainTextResponse
import re
from typing import List, Dict, Optional

app = FastAPI()

# -----------------------------
# Helpers
# -----------------------------
TIME_RE = re.compile(r"\b([01]?\d|2[0-3])[:\.]([0-5]?\d)\b")
# Flight examples: TK192, BA676, A3992, W64321, QR239, etc.
FLIGHT_RE = re.compile(r"\b([A-Z0-9]{1,4}\d{1,4}[A-Z]?)\b")

def normalize_time(raw: str) -> str:
    m = TIME_RE.search(raw.strip())
    if not m:
        return "?"
    hh = int(m.group(1))
    mm = int(m.group(2))
    return f"{hh:02d}:{mm:02d}"

def looks_like_flight(token: str) -> bool:
    # Must contain at least one digit and at least one letter
    if not re.search(r"\d", token):
        return False
    if not re.search(r"[A-Z]", token):
        return False
    # Basic shape
    return bool(re.fullmatch(r"[A-Z0-9]{1,4}\d{1,4}[A-Z]?", token))

def extract_flight(line: str) -> str:
    line_up = line.upper()
    candidates = FLIGHT_RE.findall(line_up)
    # prefer ones that look like real flight codes
    for c in candidates:
        if looks_like_flight(c):
            return c
    return "?"

def extract_names_from_text(block: str) -> List[str]:
    """
    Tries to extract passenger names from a block.
    Priority:
      1) Lines like "Name: John Doe"
      2) Lines that look like person names (2+ words, mostly letters)
    """
    names: List[str] = []

    # 1) Name: ....
    for m in re.finditer(r"(?im)^\s*name\s*:\s*(.+?)\s*$", block):
        cand = m.group(1).strip()
        if cand:
            names.append(cand)

    # 2) Fallback: scan lines for "Firstname Lastname" style
    if not names:
        for ln in block.splitlines():
            s = ln.strip()
            if not s:
                continue
            # skip obvious non-name lines
            if any(k in s.lower() for k in ["phone", "number", "uçuş", "flight", "ihl", "ist", "saw", "reservation", "rezervasyon"]):
                continue
            # must contain at least two words
            parts = s.split()
            if len(parts) < 2:
                continue
            # mostly letters (allow accents)
            letters = sum(ch.isalpha() for ch in s)
            if letters >= max(6, int(len(s) * 0.5)):
                names.append(s)

    # Clean duplicates while preserving order
    seen = set()
    out = []
    for n in names:
        key = n.strip().lower()
        if key and key not in seen:
            seen.add(key)
            out.append(n.strip())
    return out

def to_tsv(rows: List[Dict[str, str]]) -> str:
    # 4 columns: Saat, (blank), Uçuş, Yolcu
    lines = ["Saat\t\tUçuş\tYolcu"]
    for r in rows:
        lines.append(f"{r['saat']}\t\t{r['ucus']}\t{r['yolcu']}")
    return "\n".join(lines)

def parse_input_to_rows(text: str) -> List[Dict[str, str]]:
    """
    Strategy:
    - Split input into lines
    - Each time occurrence creates one "row context" based on that line + nearby lines
    - Apply SAW filter: if that line (or nearby) contains SAW -> skip
    - If no times exist: create one row from whole text with saat='?'
    """
    raw = (text or "").strip()
    if not raw:
        return [{"saat": "?", "ucus": "?", "yolcu": "?"}]

    lines = raw.splitlines()

    # Find all line indexes that contain times
    time_hits = []
    for i, ln in enumerate(lines):
        if TIME_RE.search(ln):
            time_hits.append(i)

    rows: List[Dict[str, str]] = []

    def build_context(i: int) -> str:
        # small window around the time line to capture Name: lines etc.
        start = max(0, i - 4)
        end = min(len(lines), i + 6)
        return "\n".join(lines[start:end])

    if time_hits:
        for idx in time_hits:
            ctx = build_context(idx)
            if "SAW" in ctx.upper():
                # rule: drop SAW rows
                continue

            saat = normalize_time(lines[idx])
            ucus = extract_flight(ctx)

            names = extract_names_from_text(ctx)
            yolcu = ", ".join(names) if names else "?"

            rows.append({"saat": saat, "ucus": ucus, "yolcu": yolcu})
    else:
        # No times found -> put '?' for time and try extract flight + name from whole text
        if "SAW" in raw.upper():
            # If everything is SAW-like, return empty -> still show header + one row
            pass
        ucus = extract_flight(raw)
        names = extract_names_from_text(raw)
        yolcu = ", ".join(names) if names else "?"
        rows.append({"saat": "?", "ucus": ucus, "yolcu": yolcu})

    # If everything got filtered out, return one placeholder row
    if not rows:
        rows = [{"saat": "?", "ucus": "?", "yolcu": "?"}]

    return rows

# -----------------------------
# Routes
# -----------------------------
@app.get("/health", response_class=PlainTextResponse)
def health():
    return "ok"

@app.get("/", response_class=HTMLResponse)
def home():
    return """
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>AI Transfer Bot</title>
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial; margin: 24px; }
    textarea { width: 100%; min-height: 260px; font-size: 14px; padding: 12px; }
    button { padding: 10px 14px; font-size: 14px; cursor: pointer; }
    pre { background: #111; color: #eee; padding: 12px; overflow: auto; }
    .row { display: flex; gap: 12px; flex-wrap: wrap; }
    .card { flex: 1 1 420px; }
  </style>
</head>
<body>
  <h2>AI Transfer Bot</h2>
  <p>Metni yapıştır → TAB-separated tablo üretir (Saat / boş / Uçuş / Yolcu). SAW satırları otomatik atlanır.</p>

  <div class="row">
    <div class="card">
      <form method="post" action="/convert">
        <textarea name="text" placeholder="Listeyi buraya yapıştır..."></textarea><br/><br/>
        <button type="submit">Convert</button>
      </form>
    </div>
    <div class="card">
      <p><b>Not:</b> Çıktıyı WhatsApp’a direkt yapıştırabilirsin.</p>
      <p><b>Test:</b> <code>/health</code> → ok</p>
    </div>
  </div>
</body>
</html>
"""

@app.post("/convert", response_class=HTMLResponse)
def convert(text: str = Form(...)):
    rows = parse_input_to_rows(text)
    tsv = to_tsv(rows)

    # HTML shows output + also plain TSV in a textarea for easy copy
    return f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Converted</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial; margin: 24px; }}
    textarea {{ width: 100%; min-height: 220px; font-size: 14px; padding: 12px; }}
    a {{ display: inline-block; margin-top: 12px; }}
  </style>
</head>
<body>
  <h3>Output (TSV)</h3>
  <textarea readonly>{tsv}</textarea>
  <br/>
  <a href="/">← Back</a>
</body>
</html>
"""

# Optional: If you want pure text output for integrations
@app.post("/convert.txt", response_class=PlainTextResponse)
def convert_txt(text: str = Form(...)):
    rows = parse_input_to_rows(text)
    return to_tsv(rows)
