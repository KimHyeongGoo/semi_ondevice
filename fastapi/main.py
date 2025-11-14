from fastapi import Body, FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from datetime import datetime
from dateutil import parser
import yaml
import os
import psycopg2
import json
from pathlib import Path
import numpy as np
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.tri as tri

from db import (
    get_latest_data,
    get_trace_info,
    get_event_chart_data,
    get_trace_pred_chart_data,
    get_process_range,
    get_latest_pvd_stream_data,
    get_current_step,
    get_recent_pvd_violence_logs,
)

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

HEATMAP_DIR = Path("static/heatmaps")
HEATMAP_DIR.mkdir(parents=True, exist_ok=True)

WAFER_LABELS = ["U", "CU", "C", "CL", "L"]
POINTER_COORDS = np.array(
    [
        (0.0, 0.0),   # 1 center
        (0.0, 0.5),   # 2 inner top
        (-0.5, 0.0),  # 3 inner left
        (0.0, -0.5),  # 4 inner bottom
        (0.5, 0.0),   # 5 inner right
        (0.0, 1.0),   # 6 outer top
        (-1.0, 0.0),  # 7 outer left
        (0.0, -1.0),  # 8 outer bottom
        (1.0, 0.0),   # 9 outer right
    ]
)


def _format_identifier(proc):
    row_num = proc.get("row_num")
    start_time = proc.get("start_time", "")
    safe_time = start_time.replace(":", "-").replace(" ", "_")
    return f"proc_{row_num}_{safe_time}"


def _generate_heatmap(values, output_path):
    triang = tri.Triangulation(POINTER_COORDS[:, 0], POINTER_COORDS[:, 1])
    refiner = tri.UniformTriRefiner(triang)
    tri_refined, values_refined = refiner.refine_field(values, subdiv=4)

    fig, ax = plt.subplots(figsize=(3.2, 3.2))
    contour = ax.tricontourf(
        tri_refined,
        values_refined,
        levels=20,
        cmap="viridis",
    )
    circle = plt.Circle((0, 0), 1.05, color="black", fill=False, linewidth=1)
    ax.add_artist(circle)
    ax.set_aspect("equal")
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_xlim(-1.1, 1.1)
    ax.set_ylim(-1.1, 1.1)
    fig.colorbar(contour, ax=ax, fraction=0.046, pad=0.04)
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def ensure_heatmaps(process_rows):
    for proc in process_rows:
        thicknesses = proc.get("thicknesses", [])
        if len(thicknesses) != 45:
            proc["heatmap_images"] = {label: None for label in WAFER_LABELS}
            continue

        base_name = _format_identifier(proc)
        heatmaps = {}
        for wafer_idx, label in enumerate(WAFER_LABELS):
            wafer_values = [
                thicknesses[pointer * 5 + wafer_idx]
                for pointer in range(9)
            ]
            if any(v is None for v in wafer_values):
                heatmaps[label] = None
                continue

            arr = np.array(wafer_values, dtype=float)
            file_name = f"{base_name}_{label}.png"
            file_path = HEATMAP_DIR / file_name
            if not file_path.exists() or file_path.stat().st_size == 0:
                _generate_heatmap(arr, file_path)
            heatmaps[label] = f"/static/heatmaps/{file_name}"

        proc["heatmap_images"] = heatmaps



def ensure_realtime_log_table(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS realtime_violation_log (
            "Timestamp" TIMESTAMP  PRIMARY KEY,
            parameter TEXT NOT NULL,
            message TEXT NOT NULL,
            UNIQUE ("Timestamp", parameter)
        );
        """
    )

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
SETTINGS_PATH = "settings.yaml"

def _parse_range_timestamp(value: str):
    try:
        return datetime.fromisoformat(value.replace(' ', 'T'))
    except ValueError:
        return None

@app.get("/", response_class=HTMLResponse)
async def get_page(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "columns": predict_columns,
            "active_tab": "index",
            "active_side": "ald",
        },
    )


@app.get("/pvd", response_class=HTMLResponse)
async def get_pvd_page(request: Request):
    return templates.TemplateResponse(
        "pvd.html",
        {
            "request": request,
            "columns": ["ion_gauge_i", "baratron_gauge_i", "ar_mfc_i"],
            "active_tab": "index",
            "active_side": "pvd",
        },
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

@app.get("/index4.html", response_class=HTMLResponse)
async def get_page4(request: Request):
    return templates.TemplateResponse(
        "index4.html",
        {"request": request, "columns": predict_columns, "active_tab": "index4"}
    )
    
@app.get("/index5.html", response_class=HTMLResponse)
async def get_assistant(request: Request):
    return templates.TemplateResponse(
        "index5.html",
        {"request": request, "active_tab": "index5"}
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


@app.get("/api/settings")
async def get_settings():
    if os.path.exists(SETTINGS_PATH):
        with open(SETTINGS_PATH, 'r') as f:
            settings = yaml.safe_load(f) or {}
    else:
        settings = {}
    return JSONResponse(settings)


@app.post("/api/settings")
async def save_settings(request: Request):
    body = await request.json()
    with open(SETTINGS_PATH, "w") as f:
        yaml.dump(body, f)
    return JSONResponse({"status": "saved"})

@app.post("/api/save_limits")
async def save_limits(request: Request):
    body = await request.json()
    with open(LIMIT_PATH, "w") as f:
        yaml.dump(body, f)
    return JSONResponse({"status": "saved"})

@app.get("/api/limits")
async def api_limits():
    if os.path.exists(LIMIT_PATH):
        with open(LIMIT_PATH, 'r') as f:
            lim = yaml.safe_load(f) or {}
    else:
        lim = {}
    return JSONResponse(lim)

@app.get("/api/process_range")
async def api_process_range(time: str = Query(...)):
    data = get_process_range(time)
    return JSONResponse(data)

@app.get("/api/trace_info")
async def api_trace_info(limit: int = 10):
    data = get_trace_info(limit)
    ensure_heatmaps(data)
    return JSONResponse(data)


@app.get("/api/current_step")
async def api_current_step():
    data = get_current_step()
    return JSONResponse(data)


@app.get("/api/pvd/latest")
async def api_latest_pvd(last_table: str | None = None, since: str | None = None):
    data = get_latest_pvd_stream_data(last_table=last_table or None, since=since or None)
    return JSONResponse(data)


@app.get("/api/pvd/logs")
async def api_latest_pvd_logs(limit: int = 50):
    capped_limit = max(1, min(int(limit), 200))
    data = get_recent_pvd_violence_logs(capped_limit)
    return JSONResponse(data)

@app.get("/api/model_columns")
async def api_model_columns():
    cols = []
    for fname in os.listdir("../model"):
        if fname.startswith("192_patchtst_") and fname.endswith(".keras"):
            col = fname[len("192_patchtst_"):-6]
            cols.append(col)
    cols.sort()
    new_cols = []
    for col in predict_columns:
        if col in cols:
            new_cols.append(col)
    return JSONResponse(new_cols)
    return JSONResponse(cols)

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

    ensure_realtime_log_table(cur)
    conn.commit()
    
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

@app.get("/api/history/logs")
async def get_history_logs(
    start: str = Query(...),
    end: str = Query(...),
    parameter: str | None = Query(default=None),
):
    start_dt = _parse_range_timestamp(start)
    end_dt = _parse_range_timestamp(end)
    if start_dt is None or end_dt is None:
        raise HTTPException(status_code=400, detail="Invalid timestamp format")
    if start_dt > end_dt:
        raise HTTPException(status_code=400, detail="Start time must be before end time")

    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432,
    )
    cur = conn.cursor()

    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS realtime_violation_log (
                "Timestamp" TIMESTAMP  PRIMARY KEY,
                parameter TEXT NOT NULL,
                message TEXT NOT NULL,
                UNIQUE ("Timestamp", parameter)
            );
        """)
        conn.commit()

        query = """
            SELECT "Timestamp", parameter, message
            FROM realtime_violation_log
            WHERE "Timestamp" BETWEEN %s::timestamp AND %s::timestamp
        """
        params = [start_dt, end_dt]
        if parameter:
            query += " AND parameter = %s"
            params.append(parameter)
        query += " ORDER BY \"Timestamp\" DESC"

        cur.execute(query, params)
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    return JSONResponse([
        {
            "timestamp": ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            "parameter": param,
            "message": msg,
        }
        for ts, param, msg in rows
    ])

@app.post("/api/logs")
async def create_log(entry: dict = Body(...)):
    parameter = entry.get("parameter")
    if not parameter:
        raise HTTPException(status_code=400, detail="parameter is required")

    def _parse_time(value):
        if not value:
            return None
        try:
            return parser.parse(value)
        except (ValueError, TypeError):
            return None

    start_dt = _parse_time(entry.get("start"))
    end_dt = _parse_time(entry.get("end"))
    peak_dt = _parse_time(entry.get("peak_time"))

    message_payload = {
        "parameter": parameter,
        "start": start_dt.isoformat() if start_dt else None,
        "end": end_dt.isoformat() if end_dt else None,
        "duration_seconds": entry.get("duration_seconds"),
        "diff_percent": entry.get("diff"),
        "step_id": entry.get("step_id") or [],
        "step_name": entry.get("step_name") or [],
        "peak_time": peak_dt.isoformat() if peak_dt else None,
        "actual_value": entry.get("actual_value"),
        "predicted_value": entry.get("predicted_value"),
    }

    message_text = json.dumps(message_payload, ensure_ascii=False)
    log_timestamp = datetime.utcnow()

    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432
    )
    cur = conn.cursor()

    try:
        ensure_realtime_log_table(cur)
        cur.execute(
            """
            INSERT INTO realtime_violation_log ("Timestamp", parameter, message)
            VALUES (%s, %s, %s)
            ON CONFLICT ("Timestamp", parameter) DO NOTHING
            """,
            (log_timestamp, parameter, message_text),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

    return JSONResponse({"status": "stored"})
    
@app.get("/api/event_chart")
async def event_chart(param: str, start: str = Query(...), end: str = Query(...), step: int = 10):
    data = get_event_chart_data(param, start, end, step)
    return JSONResponse(data)

@app.get("/api/trace_pred_chart")
async def trace_pred_chart(param: str, start: str = Query(...), end: str = Query(...)):
    data = get_trace_pred_chart_data(param, start, end)
    return JSONResponse(data)

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
