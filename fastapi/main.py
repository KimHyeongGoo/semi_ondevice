from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from db import get_latest_data
import yaml
import os

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# 사용할 칼럼 정의
predict_columns = [
    'MFC26_F.PWR', 'MFC27_L.POS', 'MFC28_R.POS', 'MFC7_DCS', 'MFC8_NH3',
    'VG11 Press value', 'VG12 Press value', 'VG13 Press value',
    'Temp_Act_U', 'Temp_Act_CU', 'Temp_Act_C', 'Temp_Act_CL', 'Temp_Act_L'
]

LIMIT_PATH = "limits.yaml"

@app.get("/", response_class=HTMLResponse)
async def get_page(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "columns": predict_columns})

@app.get("/api/data")
async def get_data(duration: int = 300):
    data = get_latest_data(predict_columns, duration)
    limits = {}
    if os.path.exists(LIMIT_PATH):
        with open(LIMIT_PATH, 'r') as f:
            limits = yaml.safe_load(f)
    data["limits"] = limits
    return JSONResponse(data)

@app.post("/api/save_limits")
async def save_limits(request: Request):
    body = await request.json()
    with open(LIMIT_PATH, "w") as f:
        yaml.dump(body, f)
    return JSONResponse({"status": "saved"})

@app.get("/api/logs")
async def get_logs():
    import psycopg2
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS realtime_violation_log (
            "Timestamp" TIMESTAMP NOT NULL,
            parameter TEXT NOT NULL,
            message TEXT NOT NULL,
            UNIQUE ("Timestamp", parameter)
        );
    """)
    conn.commit()
    
    cur.execute("""
        SELECT "Timestamp", message FROM realtime_violation_log
        ORDER BY "Timestamp" DESC
        LIMIT 10
    """)
    logs = cur.fetchall()
    cur.close()
    conn.close()

    return JSONResponse([
        f"{msg}" for ts, msg in logs
    ])