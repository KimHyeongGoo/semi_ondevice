from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from datetime import datetime
from dateutil import parser
import yaml
import os
import psycopg2
from db import get_latest_data

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# main.py
PREDICT_STEPS = [10, 20, 30]

# 사용할 칼럼 정의
predict_columns = [      
    #'PPExecStepID',
    'MFC7_DCS',           ## MFC Dichlorosilane(DCS) 유량 모니터링 값
    'MFC8_NH3',           ## MFC 암모니아(NH3) 유량 모니터링 값
    #'MFC9_F2',
    'MFC1_N2-1',  # MFC(Mass Flow Controller) N2-1 모니터링 값
    'MFC2_N2-2',          # MFC N2-2 모니터링 값
    'MFC3_N2-3',  # MFC N2-3 모니터링 값
    'MFC4_N2-4',          
    'VG11 Press value',                 ## Baratron Gauge(의 압력 모니터링 값 (프로세스중 작용)
    'VG12 Press value',                 # Baratron Gauge(의 압력 모니터링 값 (프로세스외 작용)
    'VG13 Press value',                 # Baratron Gauge(의 압력 모니터링 값 (프로세스외 작용)
    'MFC26_F.PWR',
    'MFC27_L.POS',         # MFC Left Position 위치 모니터링 값
    'MFC28_R.POS',         # MFC P.POS 위치 모니터링 값
    'Temp_Act_U',            # 상부 위치 실제 온도
    'Temp_Act_CU',           # 중앙 상부 위치 실제 온도
    'Temp_Act_C',            # 중앙 위치 실제 온도
    'Temp_Act_CL',           # 중앙 하부 위치 실제 온도
    'Temp_Act_L'              
]

LIMIT_PATH = "limits.yaml"

@app.get("/", response_class=HTMLResponse)
async def get_page(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "columns": predict_columns, "active_tab": "index"}
    )

@app.get("/index2.html", response_class=HTMLResponse)
async def get_page2(request: Request):
    return templates.TemplateResponse(
        "index2.html",
        {"request": request, "active_tab": "index2"}
    )

@app.get("/index3.html", response_class=HTMLResponse)
async def get_page3(request: Request):
    return templates.TemplateResponse(
        "index3.html",
        {"request": request, "active_tab": "index3"}
    )
    
@app.get("/api/data")
async def get_data(duration: int = 300, step: int = 10):
    data = get_latest_data(predict_columns, duration, step)
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
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432
    )
    cur = conn.cursor()
    
    cur.execute("""
        SELECT "Timestamp", parameter, message FROM realtime_violation_log
        ORDER BY "Timestamp" DESC
        LIMIT 10
    """)
    logs = cur.fetchall()
    cur.close()
    conn.close()

    return JSONResponse([
        {
            "timestamp": ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # millisecond 포함
            "parameter": param,
            "message": msg
        }
        for ts, param, msg in logs
    ])
    
@app.get("/api/event_chart")
async def event_chart(param: str, start: str = Query(...), end: str = Query(...), step: int = 10):
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432
    )
    cur = conn.cursor()

    from_ts = parser.parse(start)
    to_ts = parser.parse(end)
    #date_suffix = from_ts.strftime("%d%H") 
    date_suffix = from_ts.strftime("%Y%m%d") 

    raw_table = f"rawdata{date_suffix}"
    param_modified = param.replace(' ', '_').replace('.', '_').replace('-', '_')
    pred_table = f"pred_{step}_{param_modified}"

    if len(str(from_ts)) >= 26:
        from_ts = str(from_ts)[:23]
        to_ts = str(to_ts)[:23]
    try:
        # 실제값
        cur.execute(f"""
            SELECT DATE_TRUNC('second', "Timestamp") AS ts, "{param}"
            FROM "{raw_table}"
            WHERE "Timestamp" BETWEEN %s::timestamp AND %s::timestamp
            ORDER BY ts ASC
        """, (from_ts, to_ts))
        actuals = [{"x": str(ts), "y": val} for ts, val in cur.fetchall()]
        # 예측값
        cur.execute(f"""
            SELECT DATE_TRUNC('second', "Timestamp") AS ts, "Parameter"
            FROM "{pred_table}"
            WHERE "Timestamp" BETWEEN %s::timestamp AND %s::timestamp
            ORDER BY ts ASC
        """, (from_ts, to_ts))
        preds = [{"x": str(ts), "y": val} for ts, val in cur.fetchall()]
    except Exception as e:
        actuals, preds = [], []
        print("[event_chart ERROR]", e)

    cur.close()
    conn.close()

    return JSONResponse({
        "actual": actuals,
        "predicted": preds
    })

@app.get("/logview.html", response_class=HTMLResponse)
async def view_log_chart():
    return templates.TemplateResponse("logview.html", {"request": {}})


@app.get("/api/log_detail")
async def get_log_detail(time: str = Query(...), parameter: str = Query(...)):
    import psycopg2
    from dateutil import parser

    ts = parser.parse(time)

    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT message FROM realtime_violation_log
        WHERE "Timestamp" = %s AND parameter = %s
        LIMIT 1
    """, (ts, parameter))
    row = cur.fetchone()
    cur.close()
    conn.close()

    return {"message": row[0] if row else "(메시지 없음)"}
