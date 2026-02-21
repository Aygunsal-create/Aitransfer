import os, re, json, uuid
from datetime import datetime
from typing import Dict, Any, List, Optional

from fastapi import FastAPI, Form, Cookie
from fastapi.responses import HTMLResponse, Response

app = FastAPI()
DB_PATH = "db.json"

# ---------------- DB ----------------
def load_db():
    if not os.path.exists(DB_PATH):
        return {"sessions": {}}
    try:
        with open(DB_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {"sessions": {}}

def save_db(db):
    with open(DB_PATH, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)

def get_session(sid):
    db = load_db()
    if not sid or sid not in db["sessions"]:
        sid = uuid.uuid4().hex
        db["sessions"][sid] = {"buffer": "", "result": ""}
        save_db(db)
    return sid

def append_buffer(sid, text):
    db = load_db()
    db["sessions"][sid]["buffer"] += "\n" + text
    save_db(db)

def get_buffer(sid):
    return load_db()["sessions"][sid]["buffer"]

def set_result(sid, tsv):
    db = load_db()
    db["sessions"][sid]["result"] = tsv
    save_db(db)

def get_result(sid):
    return load_db()["sessions"][sid].get("result","")

# ---------------- PARSER ----------------

TIME = re.compile(r"\b([01]?\d|2[0-3])[:\.]([0-5]\d)\b")
FLIGHT = re.compile(r"\b([A-Z]{1,3}\d{2,5})\b")

def clean_whatsapp(text: str) -> str:
    text = re.sub(r"\[\d{1,2}/\d{1,2}\s\d{2}:\d{2}\]", " ", text)  # [21/2 06:48]
    text = re.sub(r"[^\n]{0,40}:", " ", text)  # Eyüp Abi BDR:
    text = re.sub(r"\+?\d[\d\s]{8,}", " ", text)  # phone
    return text

def parse(text: str):

    text = clean_whatsapp(text)
    words = text.split()

    rows=[]
    name=[]
    flight=None

    for w in words:

        # saat
        if TIME.match(w):
            saat=w
            yolcu=" ".join(name) if name else "?"
            ucus=flight if flight else "?"
            rows.append((saat,ucus,yolcu))
            name=[]
            flight=None
            continue

        # uçuş
        if FLIGHT.match(w):
            flight=w
            continue

        # isim
        if not w.isdigit():
            name.append(w)

    return rows

def make_tsv(rows):
    lines=["Saat\t\tUçuş\tYolcu"]
    for r in rows:
        lines.append(f"{r[0]}\t\t{r[1]}\t{r[2]}")
    return "\n".join(lines)

# ---------------- UI ----------------

@app.get("/", response_class=HTMLResponse)
def home(session_id: Optional[str] = Cookie(default=None)):
    sid=get_session(session_id)
    buf=get_buffer(sid)
    html=f"""
    <h2>AI Transfer Bot</h2>
    <p>Kopyala → Ekle → Bitir</p>
    <form method=post action=/add>
    <textarea name=text rows=12 style='width:100%'></textarea><br>
    <button>Ekle</button>
    </form>
    <form method=post action=/finish>
    <button>Bitir</button>
    </form>
    <p>taslak: {len(buf)} karakter</p>
    """
    resp=HTMLResponse(html)
    resp.set_cookie("session_id",sid)
    return resp

@app.post("/add", response_class=HTMLResponse)
def add(text:str=Form(""),session_id:Optional[str]=Cookie(default=None)):
    sid=get_session(session_id)
    append_buffer(sid,text)
    return home(sid)

@app.post("/finish", response_class=HTMLResponse)
def finish(session_id:Optional[str]=Cookie(default=None)):
    sid=get_session(session_id)
    raw=get_buffer(sid)
    rows=parse(raw)
    tsv=make_tsv(rows)
    set_result(sid,tsv)
    return HTMLResponse(f"<pre>{tsv}</pre><br><a href=/download>İndir</a>")

@app.get("/download")
def download(session_id:Optional[str]=Cookie(default=None)):
    sid=get_session(session_id)
    tsv=get_result(sid)
    return Response(tsv,media_type="text/tab-separated-values",
    headers={"Content-Disposition":"attachment; filename=result.tsv"})
