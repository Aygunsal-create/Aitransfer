import os
import re
from fastapi import FastAPI, Form
from fastapi.responses import HTMLResponse, PlainTextResponse

app = FastAPI()


def norm_time(t: str) -> str:
    # 7:5 -> 07:05, 7.05 -> 07:05, 07:05 -> 07:05
    t = t.strip().replace(".", ":")
    m = re.match(r"^(\d{1,2}):(\d{1,2})$", t)
    if not m:
        return "?"
    h = int(m.group(1))
    mi = int(m.group(2))
    if h < 0 or h > 23 or mi < 0 or mi > 59:
        return "?"
    return f"{h:02d}:{mi:02d}"


def extract_flight(line: str) -> str:
    # TK192, BA676, A3990, QR123 etc.
    # (çok sıkı doğrulama yapmıyorum; yoksa ? döner)
    m = re.search(r"\b([A-Z]{1,3}\s?\d{2,4})\b", line.upper())
    if not m:
        return "?"
    return m.group(1).replace(" ", "")


def extract_names(line: str) -> str:
    # "Name: X" varsa onu al, yoksa satırın kalanını isim gibi bırak
    m = re.search(r"\bName:\s*(.+)$", line, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip() or "?"
    # saat ve uçuşu çıkarıp kalan metni isim gibi kullan
    line2 = re.sub(r"\b(\d{1,2})[:.](\d{1,2})\b", " ", line)
    line2 = re.sub(r"\b([A-Z]{1,3}\s?\d{2,4})\b", " ", line2, flags=re.IGNORECASE)
    line2 = re.sub(r"\s+", " ", line2).strip()
    return line2 if line2 else "?"


def convert_to_tsv(raw: str, drop_saw: bool = True) -> str:
    lines = [l.strip() for l in raw.splitlines() if l.strip()]
    out = []
    for line in lines:
        if drop_saw and "SAW" in line.upper():
            continue

        # saat ara
        tm = re.search(r"\b(\d{1,2})[:.](\d{1,2})\b", line)
        saat = norm_time(f"{tm.group(1)}:{tm.group(2)}") if tm else "?"

        ucus = extract_flight(line)
        yolcu = extract_names(line)

        # 4 sütun: Saat | boş | Uçuş | Yolcu
        out.append(f"{saat}\t\t{ucus}\t{yolcu}")

    if not out:
        return "?\t\t?\t?"
    return "\n".join(out)


@app.get("/health", response_class=PlainTextResponse)
def health():
    return "ok"


@app.get("/", response_class=HTMLResponse)
def home():
    return """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>AI Transfer Bot</title>
  <style>
    body { font-family: Arial, sans-serif; max-width: 900px; margin: 24px auto; padding: 0 12px; }
    textarea { width: 100%; height: 260px; }
    pre { background:#111; color:#eee; padding:12px; overflow:auto; }
    button { padding:10px 14px; }
  </style>
</head>
<body>
  <h2>AI Transfer Bot</h2>
  <p>Metni yapıştır → TAB ayrımlı 4 sütun üretir (Saat / boş / Uçuş / Yolcu).</p>
  <form method="post" action="/convert">
    <label><input type="checkbox" name="drop_saw" checked> SAW satırlarını çıkar</label><br><br>
    <textarea name="text" placeholder="Listeyi buraya yapıştır..."></textarea><br><br>
    <button type="submit">Çevir</button>
  </form>
  <p><small>Test: <code>/health</code> → ok</small></p>
</body>
</html>
"""


@app.post("/convert", response_class=HTMLResponse)
def convert(text: str = Form(...), drop_saw: str = Form(None)):
    drop = drop_saw is not None
    tsv = convert_to_tsv(text, drop_saw=drop)
    # Sayfada da göster + kopyalanabilir
    return f"""
<!doctype html>
<html><head><meta charset="utf-8"><title>Sonuç</title></head>
<body style="font-family:Arial;max-width:900px;margin:24px auto;padding:0 12px;">
  <h3>Sonuç (TSV)</h3>
  <p>WhatsApp/Sheets'e direkt yapıştırabilirsin.</p>
  <pre>{tsv}</pre>
  <p><a href="/">Geri dön</a></p>
</body></html>
"""
