from fastapi import FastAPI, Form
from fastapi.responses import HTMLResponse, PlainTextResponse, Response, JSONResponse
import json, os, re
from typing import List, Dict, Optional

app = FastAPI()

DB_PATH = "db.json"

# -------------------- DB helpers --------------------
def load_db() -> List[Dict[str, str]]:
    if not os.path.exists(DB_PATH):
        return []
    try:
        with open(DB_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def save_db(rows: List[Dict[str, str]]) -> None:
    with open(DB_PATH, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

# -------------------- Output TSV (ONLY 4 columns) --------------------
def rows_to_tsv(rows: List[Dict[str, str]]) -> str:
    # 4 sütun: Saat, boş, Uçuş, Yolcu
    lines = ["Saat\t\tUçuş\tYolcu"]
    for r in rows:
        saat = str(r.get("saat", "?") or "?")
        ucus = str(r.get("ucus", "?") or "?")
        yolcu = str(r.get("yolcu", "?") or "?")
        lines.append(f"{saat}\t\t{ucus}\t{yolcu}")
    # Türkçe karakterler için UTF-8 BOM
    return "\ufeff" + "\n".join(lines)

# -------------------- Cleaning / parsing rules --------------------
RE_TIME = re.compile(r"(?<!\d)([01]?\d|2[0-3])[:\. ]([0-5]\d)(?!\d)")
RE_FLIGHT = re.compile(r"\b([A-Z0-9]{1,3})\s?(\d{1,5})\b")
RE_PHONE_IN_TEXT = re.compile(r"\+?\d[\d\-\s\(\)]{7,}\d")  # telefon gibi

# Bu kelimeler isim değildir -> satırı/ekleri kırp
BAD_WORDS = [
    "pax", "passenger", "passengers", "kişi", "kisi", "yolcu",
    "tel", "phone", "contact", "arrival", "departure",
    "date", "time", "airport", "hotel", "transfer", "operasyon",
    "drop at", "pick up", "pickup", "pick up from", "pick up time",
    "logo", "pasaport", "passport", "rez", "reservation", "booking",
    "cod", "code", "pnr"
]

def norm_time_from_text(s: str) -> Optional[str]:
    m = RE_TIME.search(s)
    if not m:
        return None
    hh = int(m.group(1)); mm = int(m.group(2))
    return f"{hh:02d}:{mm:02d}"

def norm_flight_from_text(s: str) -> Optional[str]:
    if "?" in s:
        return "?"
    m = RE_FLIGHT.search(s.upper())
    if not m:
        return None
    return f"{m.group(1)}{m.group(2)}"

def strip_noise(name: str) -> str:
    """İsim hücresinde isim dışında her şeyi siler."""
    t = name.strip()

    # tırnak, tire vs
    t = t.strip('"').strip("'").strip()
    t = t.lstrip("-").strip()

    # telefonları sil
    t = RE_PHONE_IN_TEXT.sub("", t)

    # parantez içi (rez kodu vs) sil
    t = re.sub(r"\([^)]*\)", "", t)

    # " / 3 Yolcu" gibi ekleri sil
    t = re.sub(r"\s*/\s*\d+\s*(yolcu|kisi|kişi|pax|passengers?)\b.*", "", t, flags=re.I)

    # " 3 Yolcu" gibi ekleri sil
    t = re.sub(r"\b\d+\s*(yolcu|kisi|kişi|pax|passengers?)\b.*", "", t, flags=re.I)

    # rez/booking/pnr vb kelimeden sonrası sil
    t = re.sub(r"\b(rez|reservation|booking|pnr|code|cod)\b.*", "", t, flags=re.I)

    # fazla boşlukları düzelt
    t = re.sub(r"\s{2,}", " ", t).strip()

    # baştaki/sondaki noktalama temizliği
    t = t.strip(" ,;:\t")

    return t

def looks_like_name_line(line: str) -> bool:
    """İsim satırı mı? (telefon/ucus/saat vb değil)"""
    t = line.strip()
    if not t:
        return False

    low = t.lower()
    for w in BAD_WORDS:
        if w in low:
            # ama bazı isimlerde "noor" gibi şeyler var; yine de badwords listesi genelde güvenli
            return False

    # sadece saat veya uçuşsa isim değil
    if RE_TIME.fullmatch(t):
        return False
    if RE_FLIGHT.fullmatch(t.replace(" ", "").upper()):
        return False

    # çok uzun satır (adres vb) isim olmasın
    if len(t) > 60:
        return False

    # telefon içeriyorsa isim değil
    if RE_PHONE_IN_TEXT.search(t):
        return False

    # en az 2 kelime (ad soyad) kuralı
    words = [w for w in re.split(r"\s+", t) if len(w) > 1]
    if len(words) < 2:
        return False

    # harf ağırlığı kontrolü
    letters = sum(ch.isalpha() for ch in t)
    digits = sum(ch.isdigit() for ch in t)
    if letters < 3 or digits > letters:
        return False

    return True

def parse_text_to_rows(text: str) -> List[Dict[str, str]]:
    """
    Kural: Her saat 1 satır.
    Aynı saat içinde birden fazla yolcu varsa aynı hücreye virgülle.
    İsim hücresinde telefon/rez/kişi/pax vb temizlenir.
    """
    lines = text.splitlines()

    rows: List[Dict[str, str]] = []
    cur_time: Optional[str] = None
    cur_flight: Optional[str] = None
    cur_names: List[str] = []

    def flush():
        nonlocal cur_time, cur_flight, cur_names, rows
        if not (cur_time or cur_flight or cur_names):
            return
        saat = cur_time or "?"
        ucus = cur_flight or "?"
        cleaned = []
        for n in cur_names:
            cn = strip_noise(n)
            if cn:
                cleaned.append(cn)
        yolcu = ", ".join(cleaned) if cleaned else "?"
        rows.append({"saat": saat, "ucus": ucus, "yolcu": yolcu})
        cur_time = None
        cur_flight = None
        cur_names = []

    for raw in lines:
        t = raw.strip()
        if not t:
            continue

        # Satır içinde saat/ucus arayalım
        found_time = norm_time_from_text(t)
        found_flight = norm_flight_from_text(t)

        # Yeni saat geldiyse -> yeni iş/satır başlıyor
        if found_time:
            if cur_time and found_time != cur_time:
                flush()
            if not cur_time:
                cur_time = found_time

        # Uçuşu ilk bulduğumuzda al, değişirse flush (yeni iş gibi)
        if found_flight:
            if cur_flight and found_flight != cur_flight:
                flush()
            if not cur_flight:
                cur_flight = found_flight

        # Aynı satırda isimler tırnak içinde listelenmiş olabilir
        if '"' in raw:
            parts = re.findall(r'"([^"]+)"', raw)
            for p in parts:
                p = p.strip()
                if looks_like_name_line(p):
                    cur_names.append(p)

        # Satır isim gibi ise ekle
        if looks_like_name_line(t):
            cur_names.append(t)

    # son blok
    flush()
    return rows

# -------------------- UI --------------------
@app.get("/")
def home():
    return {"ok": True, "endpoints": ["/yapistir", "/metin", "/tablo", "/tablo_temizle", "/list", "/temizle"]}

@app.get("/yapistir")
def yapistir():
    return HTMLResponse("""
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Transfer AI</title>
</head>
<body style="font-family:Arial; padding:16px;">
  <h2>Transfer Listesi Yapıştır</h2>
  <form action="/metin" method="post">
    <textarea name="text" rows="18" style="width:100%; font-size:16px;"></textarea><br/>
    <button type="submit" style="padding:14px 18px; font-size:18px;">Kaydet</button>
  </form>

  <hr/>
  <p>
    <a href="/tablo">Tablo indir</a><br/>
    <a href="/tablo_temizle">Tablo indir + Temizle</a><br/>
    <a href="/list">Liste (JSON)</a><br/>
    <a href="/temizle" onclick="return confirm('Tüm kayıtlar silinsin mi?');">Tümünü sil</a>
  </p>
</body>
</html>
""")

@app.post("/metin")
def metin_form(text: str = Form("")):
    text = (text or "").strip()
    if not text:
        return PlainTextResponse("Boş metin geldi.")

    rows = parse_text_to_rows(text)
    db = load_db()
    for r in rows:
        db.append({"saat": r["saat"], "ucus": r["ucus"], "yolcu": r["yolcu"]})
    save_db(db)

    return PlainTextResponse(f"Kaydedildi. Eklenen satır: {len(rows)}")

@app.get("/list")
def list_json():
    return JSONResponse(load_db())

@app.get("/tablo")
def tablo():
    rows = load_db()
    output = rows_to_tsv(rows)
    return Response(
        content=output,
        media_type="text/tab-separated-values; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=tablo.tsv"},
    )

@app.get("/tablo_temizle")
def tablo_temizle():
    # önce dosyayı üret
    rows = load_db()
    output = rows_to_tsv(rows)
    # sonra DB temizle (indirilen dosya etkilenmez)
    save_db([])
    return Response(
        content=output,
        media_type="text/tab-separated-values; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=tablo.tsv"},
    )

@app.get("/temizle")
def temizle():
    save_db([])
    return {"ok": True, "cleared": True}
