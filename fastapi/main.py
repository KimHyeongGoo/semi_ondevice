from fastapi import Body, FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
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
# generator writes to ../realtimedata from semi_ondevice; read the same file
GEN_HEALTH_FILE = Path(__file__).resolve().parents[1] / "generator_health.json"

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
    probe_coords = POINTER_COORDS
    probe_values = np.asarray(values, dtype=float)

    grid_x, grid_y = np.meshgrid(
        np.linspace(-1.05, 1.05, 200), np.linspace(-1.05, 1.05, 200)
    )

    # Inverse distance weighting (IDW) interpolation so the entire wafer
    # interior is filled based on the 9 probe positions laid out as in the
    # provided diagram.
    flat_x = grid_x.ravel()
    flat_y = grid_y.ravel()
    diff_x = flat_x[:, None] - probe_coords[None, :, 0]
    diff_y = flat_y[:, None] - probe_coords[None, :, 1]
    distances = np.sqrt(diff_x**2 + diff_y**2)

    # Avoid division by zero and ensure exact probe locations keep their value.
    zero_dist = distances == 0
    distances[zero_dist] = 1e-12

    weights = 1.0 / (distances**2)
    weighted_sum = np.sum(weights * probe_values[None, :], axis=1)
    weight_total = np.sum(weights, axis=1)
    interpolated = weighted_sum / weight_total

    # If the grid point coincides with a probe location, overwrite with the
    # exact measurement to prevent tiny numerical artifacts.
    if np.any(zero_dist):
        zero_rows = zero_dist.any(axis=1)
        exact_indices = np.argmax(zero_dist[zero_rows], axis=1)
        interpolated[zero_rows] = probe_values[exact_indices]

    grid_z = interpolated.reshape(grid_x.shape)
    circle_mask = grid_x**2 + grid_y**2 > 1.0**2
    grid_z_masked = np.ma.array(grid_z, mask=circle_mask)

    fig, ax = plt.subplots(figsize=(3.2, 3.2))
    contour = ax.contourf(
        grid_x,
        grid_y,
        grid_z_masked,
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


def ensure_realtime_abnormal_log_table(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS realtime_abnormal_log (
            id SERIAL PRIMARY KEY,
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP NOT NULL,
            parameter TEXT NOT NULL,
            duration_seconds DOUBLE PRECISION,
            avg_diff_percent DOUBLE PRECISION,
            max_diff_percent DOUBLE PRECISION,
            peak_time TIMESTAMP,
            actual_value DOUBLE PRECISION,
            predicted_value DOUBLE PRECISION,
            violation_type INT,
            message TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            UNIQUE (parameter, start_time)
        );
        """
    )

def ensure_realtime_violation_log_table(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS realtime_violation_log (
            "Timestamp" TIMESTAMP PRIMARY KEY,
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


def _read_generator_status():
    if not GEN_HEALTH_FILE.exists():
        return {"status": "DOWN", "last_tick": None}
    try:
        data = json.loads(GEN_HEALTH_FILE.read_text(encoding="utf-8"))
        last_tick_raw = data.get("last_tick")
        if not last_tick_raw:
            return {"status": "DOWN", "last_tick": None}
        try:
            last_dt = datetime.fromisoformat(last_tick_raw)
        except Exception:
            return {"status": "DOWN", "last_tick": None}
        now = datetime.now()
        delta = now - last_dt
        status = "RUN" if delta <= timedelta(seconds=5) else "DOWN"
        return {"status": status, "last_tick": last_dt.isoformat()}
    except Exception:
        return {"status": "DOWN", "last_tick": None}

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
        {"request": request, "active_tab": "assistant"}
    )

@app.get("/index6.html", response_class=HTMLResponse)
async def get_page6(request: Request):
    return templates.TemplateResponse(
        "index6.html",
        {"request": request, "active_tab": "index6"}
    )

@app.get("/api/equipment_history")
async def get_equipment_history(process_recipe: str | None = Query(default=None)):
    """equipment_history 테이블에서 데이터 조회"""
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432
    )
    cur = conn.cursor()
    
    try:
        query = """
            SELECT PJOB_ID, ProcessRecipe, ProcessStartTime, ProcessEndTime, EndStatus
            FROM equipment_history
        """
        params = []
        
        if process_recipe and process_recipe != "전체":
            query += " WHERE ProcessRecipe = %s"
            params.append(process_recipe)
        
        query += " ORDER BY ProcessStartTime DESC"
        
        cur.execute(query, params)
        rows = cur.fetchall()
        
        result = []
        for idx, (pjob_id, process_recipe_val, start_time, end_time, end_status) in enumerate(rows, 1):
            result.append({
                "no": idx,
                "pjob_id": pjob_id or "",
                "process_recipe": process_recipe_val or "",
                "process_start_time": start_time.strftime("%Y-%m-%d %H:%M:%S") if start_time else "",
                "process_end_time": end_time.strftime("%Y-%m-%d %H:%M:%S") if end_time else "",
                "end_status": end_status or ""
            })
    except Exception as e:
        print(f"[get_equipment_history ERROR] {e}")
        result = []
    finally:
        cur.close()
        conn.close()
    
    return JSONResponse(result)

@app.get("/api/equipment_history/recipes")
async def get_equipment_history_recipes():
    """equipment_history 테이블에서 ProcessRecipe 목록 조회"""
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432
    )
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT DISTINCT ProcessRecipe
            FROM equipment_history
            WHERE ProcessRecipe IS NOT NULL
            ORDER BY ProcessRecipe
        """)
        rows = cur.fetchall()
        recipes = ["전체"] + [row[0] for row in rows if row[0]]
    except Exception as e:
        print(f"[get_equipment_history_recipes ERROR] {e}")
        recipes = ["전체"]
    finally:
        cur.close()
        conn.close()
    
    return JSONResponse(recipes)

@app.get("/api/csv_files")
async def get_csv_files(pjob_id: str = Query(...)):
    """KE-PJ000000000XX 디렉토리의 CSV 파일 목록 조회"""
    import glob
    from pathlib import Path
    
    base_path = Path("/home/goo4168/semi_platform/traceData/2025/11")
    pjob_folder = base_path / pjob_id
    
    if not pjob_folder.exists() or not pjob_folder.is_dir():
        return JSONResponse([])
    
    csv_files = []
    csv_paths = sorted(glob.glob(str(pjob_folder / "*.csv")))
    
    for csv_path in csv_paths:
        csv_file = Path(csv_path)
        try:
            # 파일 수정 시간 가져오기
            edit_time = datetime.fromtimestamp(csv_file.stat().st_mtime)
            edit_datetime = edit_time.strftime("%Y-%m-%d %H:%M:%S")
            
            # Recipe Name은 DB에서 가져온 ProcessRecipe와 동일하므로
            # equipment_history에서 조회
            conn = psycopg2.connect(
                dbname="postgres",
                user="keti",
                password="keti1234!",
                host="localhost",
                port=5432
            )
            cur = conn.cursor()
            cur.execute("""
                SELECT ProcessRecipe
                FROM equipment_history
                WHERE PJOB_ID = %s
                LIMIT 1
            """, (pjob_id,))
            row = cur.fetchone()
            recipe_name = row[0] if row else ""
            cur.close()
            conn.close()
            
            csv_files.append({
                "file_name": csv_file.name,
                "edit_datetime": edit_datetime,
                "recipe_name": recipe_name
            })
        except Exception as e:
            print(f"[get_csv_files ERROR] {e}")
            continue
    
    return JSONResponse(csv_files)

@app.get("/api/csv_data")
async def get_csv_data(pjob_id: str = Query(...), file_name: str = Query(...)):
    """CSV 파일의 데이터를 읽어서 반환"""
    import csv
    from pathlib import Path
    
    base_path = Path("/home/goo4168/semi_platform/traceData/2025/11")
    csv_path = base_path / pjob_id / file_name
    
    if not csv_path.exists() or not csv_path.is_file():
        return JSONResponse({"error": "CSV 파일을 찾을 수 없습니다."}, status_code=404)
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            
            if len(lines) < 6:
                return JSONResponse({"error": "CSV 파일 형식이 올바르지 않습니다."}, status_code=400)
            
            # 헤더는 5번째 줄 (인덱스 4), 데이터는 6번째 줄부터 (인덱스 5)
            header_line = lines[4].strip()
            headers = [h.strip() for h in header_line.split(',')]
            
            # 데이터 파싱
            data_rows = []
            start_time = None
            end_time = None
            
            for i in range(5, len(lines)):
                line = lines[i].strip()
                if not line:
                    continue
                
                # CSV 파싱 (쉼표로 분리)
                parts = line.split(',')
                if len(parts) < 6:
                    continue
                
                no = parts[0].strip()
                step_name = parts[3].strip() if len(parts) > 3 else ""
                date = parts[4].strip() if len(parts) > 4 else ""
                time = parts[5].strip() if len(parts) > 5 else ""
                
                # 첫 번째와 마지막 시간 저장
                if i == 5:
                    start_time = f"{date} {time}"
                end_time = f"{date} {time}"
                
                # numeric 데이터 추출 (6번째 컬럼부터)
                numeric_data = []
                for j in range(6, len(parts)):
                    if j < len(headers):
                        value = parts[j].strip()
                        numeric_data.append(value)
                
                data_rows.append({
                    "no": no,
                    "step_name": step_name,
                    "date": date,
                    "time": time,
                    "numeric_data": numeric_data
                })
            
            # 헤더에서 numeric 컬럼명 추출
            numeric_headers = []
            if len(headers) > 6:
                numeric_headers = headers[6:]
            
            return JSONResponse({
                "start_time": start_time or "",
                "end_time": end_time or "",
                "headers": numeric_headers,
                "data": data_rows
            })
    except Exception as e:
        print(f"[get_csv_data ERROR] {e}")
        return JSONResponse({"error": str(e)}, status_code=500)
    
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


@app.get("/api/generator_status")
async def api_generator_status():
    return JSONResponse(_read_generator_status())

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
    ensure_realtime_violation_log_table(cur)
    conn.commit()

    cur.execute(
        """
        SELECT "Timestamp", parameter, message
        FROM realtime_violation_log
        ORDER BY "Timestamp" DESC
        LIMIT 50
        """
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    result = []
    for ts, param, msg in rows:
        result.append({
            "timestamp": ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            "parameter": param,
            "message": msg,
            "start_time": ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            "end_time": ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            "violation_type": None,
        })

    return JSONResponse(result)

@app.get("/api/anomaly_logs")
async def get_anomaly_logs():
    """실시간 이상감지 화면(index4)용: realtime_abnormal_log에서 최근 로그 조회"""
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432
    )
    cur = conn.cursor()
    ensure_realtime_abnormal_log_table(cur)
    conn.commit()

    cur.execute(
        """
        SELECT start_time, end_time, parameter, message, violation_type
        FROM realtime_abnormal_log
        ORDER BY end_time DESC
        LIMIT 50
        """
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    result = []
    for start_ts, end_ts, param, msg, vtype in rows:
        result.append({
            "timestamp": end_ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            "parameter": param,
            "message": msg,
            "start_time": start_ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            "end_time": end_ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            "violation_type": vtype,
        })

    return JSONResponse(result)

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
        ensure_realtime_abnormal_log_table(cur)
        conn.commit()

        query = """
            SELECT start_time, end_time, parameter, message, violation_type
            FROM realtime_abnormal_log
            WHERE start_time BETWEEN %s::timestamp AND %s::timestamp
        """
        params = [start_dt, end_dt]
        if parameter:
            query += " AND parameter = %s"
            params.append(parameter)
        query += " ORDER BY end_time DESC"

        cur.execute(query, params)
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    return JSONResponse([
        {
            "timestamp": end_ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            "start_time": start_ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            "end_time": end_ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            "parameter": param,
            "message": msg,
            "violation_type": vtype,
        }
        for start_ts, end_ts, param, msg, vtype in rows
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
    violation_type = entry.get("violation_type")

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
        "violation_type": violation_type,
    }

    message_text = json.dumps(message_payload, ensure_ascii=False)
    if not start_dt or not end_dt:
        raise HTTPException(status_code=400, detail="start and end times are required")

    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432
    )
    cur = conn.cursor()

    try:
        ensure_realtime_abnormal_log_table(cur)
        duration_seconds = (end_dt - start_dt).total_seconds()
        cur.execute(
            """
            INSERT INTO realtime_abnormal_log (
                start_time, end_time, parameter,
                duration_seconds, avg_diff_percent, message, violation_type,
                created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (parameter, start_time) DO UPDATE
            SET end_time = EXCLUDED.end_time,
                duration_seconds = EXCLUDED.duration_seconds,
                avg_diff_percent = EXCLUDED.avg_diff_percent,
                message = EXCLUDED.message,
                violation_type = EXCLUDED.violation_type,
                updated_at = NOW()
            """,
            (start_dt, end_dt, parameter, duration_seconds, entry.get("diff"), message_text, violation_type),
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
    cur.execute(
        """
        SELECT message
        FROM realtime_abnormal_log
        WHERE end_time = %s AND parameter = %s
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        (ts, parameter),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    return {"message": row[0] if row else "(메시지 없음)"}
