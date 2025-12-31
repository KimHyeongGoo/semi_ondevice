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

def ensure_realtime_abnormal_log2_table(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS realtime_abnormal_log2 (
            id SERIAL PRIMARY KEY,
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP NOT NULL,
            parameter TEXT NOT NULL,
            duration_seconds DOUBLE PRECISION,
            avg_diff_percent DOUBLE PRECISION,
            max_diff_percent DOUBLE PRECISION,
            peak_time TIMESTAMP,
            actual_value DOUBLE PRECISION,
            limit_type TEXT,
            upper_value DOUBLE PRECISION,
            lower_value DOUBLE PRECISION,
            is_interrupted INT,
            violation_type INT,
            message TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            UNIQUE (parameter, start_time)
        );
        """
    )
    # 기존 테이블에 violation_type 칼럼이 없으면 추가
    cur.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'realtime_abnormal_log2' 
                AND column_name = 'violation_type'
            ) THEN
                ALTER TABLE realtime_abnormal_log2 ADD COLUMN violation_type INT;
            END IF;
        END $$;
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
LIMIT_PATH2 = "limits2.yaml"
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
        status = "RUN" if delta <= timedelta(seconds=3) else "DOWN"
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

@app.get("/api/alarm_history")
async def get_alarm_history(
    process_start_time: str = Query(...),
    process_end_time: str = Query(...)
):
    """realtime_abnormal_log2 테이블에서 ProcessStartTime과 ProcessEndTime 사이의 알람 이력 조회"""
    # 서버 재시작을 위한 주석
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432
    )
    cur = conn.cursor()
    
    try:
        ensure_realtime_abnormal_log2_table(cur)
        conn.commit()
        
        # 시간 문자열을 datetime으로 변환
        try:
            start_dt = datetime.strptime(process_start_time, "%Y-%m-%d %H:%M:%S")
            end_dt = datetime.strptime(process_end_time, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            # 다른 형식 시도
            try:
                start_dt = parser.parse(process_start_time)
                end_dt = parser.parse(process_end_time)
            except Exception:
                return JSONResponse({"error": "Invalid timestamp format"}, status_code=400)
        
        # realtime_abnormal_log2에서 start_time이 ProcessStartTime과 ProcessEndTime 사이에 있는 데이터 조회
        query = """
            SELECT parameter, start_time, end_time, avg_diff_percent, violation_type, message, duration_seconds,
                   limit_type, upper_value, lower_value, is_interrupted, actual_value
            FROM realtime_abnormal_log2
            WHERE start_time >= %s AND start_time <= %s
            ORDER BY start_time DESC
        """
        
        cur.execute(query, (start_dt, end_dt))
        rows = cur.fetchall()
        
        result = []
        for idx, (param, start_ts, end_ts, diff_percent, violation_type, message, duration_seconds,
                  limit_type, upper_value, lower_value, is_interrupted, actual_value) in enumerate(rows, 1):
            result.append({
                "no": idx,
                "parameter": param or "",
                "start_time": start_ts.strftime("%Y-%m-%d %H:%M:%S") if start_ts else "",
                "end_time": end_ts.strftime("%Y-%m-%d %H:%M:%S") if end_ts else "",
                "diff_percent": round(diff_percent, 2) if diff_percent is not None else 0.0,
                "violation_type": violation_type if violation_type is not None else None,
                "message": message or "",
                "duration_seconds": duration_seconds if duration_seconds is not None else None,
                "limit_type": limit_type,
                "upper_value": round(upper_value, 3) if upper_value is not None else None,
                "lower_value": round(lower_value, 3) if lower_value is not None else None,
                "is_interrupted": is_interrupted if is_interrupted is not None else 2,
                "actual_value": round(actual_value, 3) if actual_value is not None else None
            })
    except Exception as e:
        print(f"[get_alarm_history ERROR] {e}")
        import traceback
        traceback.print_exc()
        result = []
    finally:
        cur.close()
        conn.close()
    
    # 캐시 방지 헤더 추가
    response = JSONResponse(result)
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

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
    return JSONResponse({"limits": lim})

@app.get("/api/interlock_limits")
async def api_interlock_limits():
    if os.path.exists(LIMIT_PATH2):
        with open(LIMIT_PATH2, 'r') as f:
            lim = yaml.safe_load(f) or {}
    else:
        lim = {}
    return JSONResponse({"limits": lim})

@app.post("/api/save_interlock_limits")
async def save_interlock_limits(request: Request):
    body = await request.json()
    with open(LIMIT_PATH2, "w") as f:
        yaml.dump(body, f)
    return JSONResponse({"status": "saved"})

@app.get("/api/process_range")
async def api_process_range(time: str = Query(...)):
    data = get_process_range(time)
    return JSONResponse(data)

@app.get("/api/trace_info")
async def api_trace_info(limit: int = 10):
    try:
        data = get_trace_info(limit)
        ensure_heatmaps(data)
        return JSONResponse(data)
    except Exception as e:
        print(f"[api_trace_info ERROR] {e}")
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/current_step")
async def api_current_step():
    try:
        data = get_current_step()
        return JSONResponse(data)
    except Exception as e:
        print(f"[api_current_step ERROR] {e}")
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)


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
    """실시간 이상감지 화면(index2)용: realtime_abnormal_log2에서 최근 로그 조회"""
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432
    )
    cur = conn.cursor()
    ensure_realtime_abnormal_log2_table(cur)
    conn.commit()

    cur.execute(
        """
        SELECT start_time, end_time, parameter, message, limit_type, is_interrupted, violation_type
        FROM realtime_abnormal_log2
        ORDER BY end_time DESC
        LIMIT 50
        """
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    result = []
    for start_ts, end_ts, param, msg, limit_type, is_interrupted, violation_type in rows:
        result.append({
            "timestamp": end_ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            "parameter": param,
            "message": msg,
            "start_time": start_ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            "end_time": end_ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            "limit_type": limit_type,
            "is_interrupted": is_interrupted,
            "violation_type": violation_type,
        })

    return JSONResponse(result)

@app.get("/api/anomaly_logs")
async def get_anomaly_logs():
    """실시간 이상감지 화면(index4)용: realtime_abnormal_log2에서 최근 로그 조회"""
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432
    )
    cur = conn.cursor()
    ensure_realtime_abnormal_log2_table(cur)
    conn.commit()

    cur.execute(
        """
        SELECT start_time, end_time, parameter, message, limit_type, is_interrupted, violation_type
        FROM realtime_abnormal_log2
        ORDER BY end_time DESC
        LIMIT 50
        """
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    result = []
    for start_ts, end_ts, param, msg, limit_type, is_interrupted, violation_type in rows:
        result.append({
            "timestamp": end_ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            "parameter": param,
            "message": msg,
            "start_time": start_ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            "end_time": end_ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            "limit_type": limit_type,
            "is_interrupted": is_interrupted,
            "violation_type": violation_type,
        })

    return JSONResponse(result)

@app.get("/api/prediction_logs")
async def get_prediction_logs():
    """예측 이상감지 로그: realtime_abnormal_log에서 최근 로그 조회"""
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
    for start_ts, end_ts, param, msg, violation_type in rows:
        result.append({
            "timestamp": end_ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            "parameter": param,
            "message": msg,
            "start_time": start_ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            "end_time": end_ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            "limit_type": None,
            "is_interrupted": None,
            "violation_type": violation_type,
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
        ensure_realtime_abnormal_log2_table(cur)
        conn.commit()

        query = """
            SELECT start_time, end_time, parameter, message, violation_type, limit_type, is_interrupted
            FROM realtime_abnormal_log2
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
            "limit_type": limit_type,
            "is_interrupted": is_interrupted,
        }
        for start_ts, end_ts, param, msg, vtype, limit_type, is_interrupted in rows
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

@app.post("/api/equipment/stop")
async def stop_equipment():
    """generate_data.py와 insert_real_time.py 프로세스 종료 - 강화된 버전"""
    import subprocess
    import signal
    import time
    import traceback
    
    try:
        # 현재 실행 중인 프로세스 확인
        generate_running_initial, insert_running_initial = check_processes_running()
        
        if not generate_running_initial and not insert_running_initial:
            print("INFO: Both processes are already stopped.")
            return JSONResponse({
                "status": "already_stopped",
                "message": "장비가 이미 DOWN되어 있습니다.",
                "killed_count": 0
            })
        
        killed_pids = []
        script_names = ["generate_data.py", "insert_real_time.py"]
        
        # 방법 1: 즉시 SIGKILL로 강제 종료 (대기 시간 최소화)
        for script_name in script_names:
            # SIGKILL로 즉시 강제 종료
            subprocess.run(
                ["pkill", "-9", "-f", script_name],
                capture_output=True,
                text=True
            )
        
        # 방법 2: pgrep으로 모든 PID 찾아서 프로세스 그룹과 자식 프로세스까지 종료
        time.sleep(0.1)  # 최소 대기
        for script_name in script_names:
            result = subprocess.run(
                ["pgrep", "-f", script_name],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                pids = [pid.strip() for pid in result.stdout.strip().split('\n') if pid.strip()]
                for pid_str in pids:
                    try:
                        pid = int(pid_str)
                        # 프로세스 그룹 ID 가져오기
                        try:
                            pgid = os.getpgid(pid)
                            # 프로세스 그룹 전체에 SIGTERM 전송
                            os.killpg(pgid, signal.SIGTERM)
                            print(f"Sent SIGTERM to process group {pgid} (PID {pid} for {script_name})")
                        except (ProcessLookupError, OSError):
                            # 프로세스 그룹을 찾을 수 없으면 개별 프로세스만 종료
                            try:
                                os.kill(pid, signal.SIGTERM)
                                print(f"Sent SIGTERM to {script_name} (PID {pid})")
                            except ProcessLookupError:
                                pass
                        killed_pids.append((script_name, pid))
                    except (ValueError, ProcessLookupError, OSError) as e:
                        print(f"WARN: Could not kill PID {pid_str} for {script_name}: {e}")
        
        # 방법 3: 남은 프로세스 강제 종료 (SIGKILL) - 즉시 실행
        for script_name in script_names:
            result = subprocess.run(
                ["pgrep", "-f", script_name],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                pids = [pid.strip() for pid in result.stdout.strip().split('\n') if pid.strip()]
                for pid_str in pids:
                    try:
                        pid = int(pid_str)
                        # 프로세스 그룹 ID 가져오기
                        try:
                            pgid = os.getpgid(pid)
                            # 프로세스 그룹 전체에 SIGKILL 전송
                            os.killpg(pgid, signal.SIGKILL)
                            print(f"Force killed process group {pgid} (PID {pid} for {script_name})")
                        except (ProcessLookupError, OSError):
                            # 프로세스 그룹을 찾을 수 없으면 개별 프로세스만 종료
                            try:
                                os.kill(pid, signal.SIGKILL)
                                print(f"Force killed {script_name} (PID {pid})")
                            except ProcessLookupError:
                                pass
                        if (script_name, pid) not in killed_pids:
                            killed_pids.append((script_name, pid))
                    except (ValueError, ProcessLookupError, OSError) as e:
                        print(f"WARN: Could not force kill PID {pid_str} for {script_name}: {e}")
        
        # 방법 4: pkill로 최종 확인 및 강제 종료
        for script_name in script_names:
            # 최종 강제 종료 시도
            subprocess.run(
                ["pkill", "-9", "-f", script_name],
                capture_output=True,
                text=True
            )
        
        # 방법 5: ps와 awk를 사용하여 모든 관련 프로세스 찾아서 종료
        for script_name in script_names:
            # ps aux | grep script_name | grep -v grep | awk '{print $2}' | xargs kill -9
            result = subprocess.run(
                ["sh", "-c", f"ps aux | grep '{script_name}' | grep -v grep | awk '{{print $2}}' | xargs -r kill -9"],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                print(f"Killed remaining processes for {script_name} using ps/awk/kill")
        
        # 방법 6: 프로세스 트리 전체 종료 (pkill -P로 자식 프로세스까지 종료)
        for script_name in script_names:
            result = subprocess.run(
                ["pgrep", "-f", script_name],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                pids = [pid.strip() for pid in result.stdout.strip().split('\n') if pid.strip()]
                for pid_str in pids:
                    try:
                        pid = int(pid_str)
                        # pkill로 프로세스와 모든 자식 프로세스 종료
                        subprocess.run(
                            ["pkill", "-9", "-P", pid_str],
                            capture_output=True,
                            text=True
                        )
                        # 부모 프로세스도 종료
                        try:
                            os.kill(pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                    except (ValueError, ProcessLookupError, OSError):
                        pass
        
        # 방법 7: 최종 강제 종료 (pkill -9 반복)
        for _ in range(3):  # 3회 반복
            for script_name in script_names:
                subprocess.run(
                    ["pkill", "-9", "-f", script_name],
                    capture_output=True,
                    text=True
                )
        
        # 최종 확인 (최소 대기)
        time.sleep(0.2)
        generate_running_final, insert_running_final = check_processes_running()
        
        if not generate_running_final and not insert_running_final:
            msg = f"장비가 DOWN되었습니다. (종료된 프로세스: {len(killed_pids)}개)" if killed_pids else "장비가 DOWN되었습니다."
            print(f"SUCCESS: {msg}")
            
            # 텔레그램 알림 전송
            try:
                import requests
                TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8426533458:AAFw6pNm5xGa7ponvmasrYs_i8AicT66tIg")
                TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "-1003454160562")
                TELEGRAM_ENABLED = os.getenv("TELEGRAM_ENABLED", "true").lower() == "true"
                
                if TELEGRAM_ENABLED:
                    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
                    payload = {
                        "chat_id": TELEGRAM_CHAT_ID,
                        "text": "🔴 장비가 DOWN 되었습니다.",
                        "parse_mode": "HTML"
                    }
                    requests.post(url, json=payload, timeout=5)
            except Exception as e:
                print(f"[텔레그램 알림 전송 실패] {e}")
            
            return JSONResponse({
                "status": "stopped",
                "message": msg,
                "killed_count": len(killed_pids)
            })
        else:
            remaining_scripts = []
            if generate_running_final:
                remaining_scripts.append("generate_data.py")
            if insert_running_final:
                remaining_scripts.append("insert_real_time.py")
            msg = f"장비 DOWN 실패: {', '.join(remaining_scripts)}가(이) 여전히 실행 중입니다."
            print(f"ERROR: {msg}")
            return JSONResponse({
                "status": "partial_stop_failure",
                "message": msg,
                "killed_count": len(killed_pids)
            }, status_code=500)
            
    except Exception as e:
        print(f"CRITICAL ERROR in stop_equipment: {e}")
        traceback.print_exc()
        return JSONResponse({"status": "error", "message": f"장비 DOWN 중 오류 발생: {str(e)}"}, status_code=500)

def check_processes_running():
    """프로세스 실행 상태 확인"""
    import subprocess
    generate_running = False
    insert_running = False
    
    result = subprocess.run(
        ["pgrep", "-f", "generate_data.py"],
        capture_output=True,
        text=True
    )
    if result.returncode == 0 and result.stdout.strip():
        generate_running = True
    
    result = subprocess.run(
        ["pgrep", "-f", "insert_real_time.py"],
        capture_output=True,
        text=True
    )
    if result.returncode == 0 and result.stdout.strip():
        insert_running = True
    
    return generate_running, insert_running

@app.get("/api/equipment/status")
async def get_equipment_status():
    """프로세스 실행 상태만 확인 (종료하지 않음)"""
    generate_running, insert_running = check_processes_running()
    return JSONResponse({
        "generate_running": generate_running,
        "insert_running": insert_running,
        "status": "running" if (generate_running or insert_running) else "stopped"
    })

@app.post("/api/equipment/start")
async def start_equipment():
    """generate_data.py와 insert_real_time.py 프로세스 시작"""
    import subprocess
    
    try:
        # 프로세스가 이미 실행 중인지 확인
        generate_running, insert_running = check_processes_running()
        
        if generate_running and insert_running:
            return JSONResponse({
                "status": "already_running", 
                "message": "프로세스가 이미 실행 중입니다.",
                "generate_running": True,
                "insert_running": True
            })
        elif generate_running:
            return JSONResponse({
                "status": "partially_running",
                "message": "generate_data.py는 이미 실행 중입니다. insert_real_time.py만 시작합니다.",
                "generate_running": True,
                "insert_running": False
            })
        elif insert_running:
            return JSONResponse({
                "status": "partially_running",
                "message": "insert_real_time.py는 이미 실행 중입니다. generate_data.py만 시작합니다.",
                "generate_running": False,
                "insert_running": True
            })
        
        # 절대 경로로 스크립트 실행
        base_dir = Path(__file__).resolve().parents[1]
        generate_script = base_dir / "generate_data.py"
        insert_script = base_dir / "insert_real_time.py"
        
        started = []
        
        # generate_data.py 시작
        if not generate_running:
            subprocess.Popen(
                ["python3", str(generate_script)],
                cwd=str(base_dir),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            started.append("generate_data.py")
        
        # insert_real_time.py 시작
        if not insert_running:
            subprocess.Popen(
                ["python3", str(insert_script)],
                cwd=str(base_dir),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            started.append("insert_real_time.py")
        
        # 장비 시작 시간을 generator_health.json에 기록
        try:
            equipment_start_time = datetime.now().isoformat()
            if GEN_HEALTH_FILE.exists():
                data = json.loads(GEN_HEALTH_FILE.read_text(encoding="utf-8"))
            else:
                data = {}
            data["equipment_start_time"] = equipment_start_time
            GEN_HEALTH_FILE.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
            print(f"[장비 시작 시간 기록] {equipment_start_time}")
        except Exception as e:
            print(f"[장비 시작 시간 기록 실패] {e}")
        
        return JSONResponse({
            "status": "started", 
            "message": f"프로세스가 시작되었습니다. ({', '.join(started)})",
            "started": started
        })
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

@app.post("/api/telegram/notify")
async def send_telegram_notification(request: Request):
    """텔레그램 알림 전송"""
    try:
        import requests
    except ImportError:
        return JSONResponse({"status": "error", "message": "requests 모듈이 설치되지 않았습니다."}, status_code=500)
    
    try:
        body = await request.json()
        message = body.get("message", "")
        reply_markup = body.get("reply_markup")
        
        if not message:
            return JSONResponse({"status": "error", "message": "메시지가 없습니다."}, status_code=400)
        
        # 텔레그램 설정
        TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8426533458:AAFw6pNm5xGa7ponvmasrYs_i8AicT66tIg")
        TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "-1003454160562")
        TELEGRAM_ENABLED = os.getenv("TELEGRAM_ENABLED", "true").lower() == "true"
        
        if not TELEGRAM_ENABLED:
            return JSONResponse({"status": "disabled", "message": "텔레그램 알림이 비활성화되어 있습니다."})
        
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }
        if reply_markup:
            payload["reply_markup"] = json.dumps(reply_markup)
        
        response = requests.post(url, json=payload, timeout=5)
        
        if response.status_code == 200:
            return JSONResponse({"status": "sent", "message": "텔레그램 메시지가 전송되었습니다."})
        else:
            return JSONResponse({"status": "error", "message": f"텔레그램 전송 실패: {response.status_code}"}, status_code=500)
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

@app.post("/api/telegram/webhook")
async def telegram_webhook(request: Request):
    """텔레그램 봇 웹훅 - callback_query 처리"""
    try:
        import requests
    except ImportError:
        return JSONResponse({"status": "error", "message": "requests 모듈이 설치되지 않았습니다."}, status_code=500)
    
    try:
        body = await request.json()
        print(f"[텔레그램 웹훅] 수신된 데이터: {body}")  # 디버깅용
        
        # callback_query가 최상위에 있는 경우 (웹훅 형식)
        callback_query = body.get("callback_query")
        
        # message 안에 callback_query가 있는 경우도 처리
        if not callback_query and "message" in body:
            callback_query = body.get("message", {}).get("callback_query")
        
        if not callback_query:
            print("[텔레그램 웹훅] callback_query가 없습니다.")
            return JSONResponse({"status": "ok"})
        
        callback_data = callback_query.get("data")
        message = callback_query.get("message", {})
        chat_id = message.get("chat", {}).get("id")
        message_id = message.get("message_id")
        
        print(f"[텔레그램 웹훅] callback_data: {callback_data}, chat_id: {chat_id}")
        
        TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8426533458:AAFw6pNm5xGa7ponvmasrYs_i8AicT66tIg")
        
        # callback_query에 대한 응답 전송
        answer_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
        try:
            requests.post(answer_url, json={
                "callback_query_id": callback_query.get("id")
            }, timeout=5)
        except Exception as e:
            print(f"[텔레그램 웹훅] answerCallbackQuery 실패: {e}")
        
        # 장비 DOWN 버튼 클릭 시
        if callback_data == "equipment_down":
            print("[텔레그램 웹훅] 장비 DOWN 버튼 클릭됨")
            # 장비 중지 API 직접 호출 (동기 방식으로 즉시 실행)
            try:
                # stop_equipment 함수를 직접 호출
                stop_result = await stop_equipment()
                stop_data = json.loads(stop_result.body.decode())
                
                # 결과를 텔레그램으로 전송
                status = stop_data.get("status", "unknown")
                if status == "already_stopped":
                    result_message = "⚠️ " + stop_data.get("message", "장비가 이미 DOWN되어 있습니다.")
                elif status == "stopped":
                    killed_count = stop_data.get("killed_count", 0)
                    result_message = f"✅ 장비가 정지되었습니다.\n종료된 프로세스: {killed_count}개"
                else:
                    result_message = "❌ " + stop_data.get("message", "장비 중지 요청이 처리되었습니다.")
                
                send_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
                requests.post(send_url, json={
                    "chat_id": chat_id,
                    "text": result_message,
                    "parse_mode": "HTML"
                }, timeout=5)
                print(f"[텔레그램 웹훅] 장비 중지 완료: {result_message}")
            except Exception as e:
                import traceback
                traceback.print_exc()
                # 에러 발생 시 텔레그램으로 알림
                send_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
                requests.post(send_url, json={
                    "chat_id": chat_id,
                    "text": f"❌ 장비 중지 중 오류 발생: {str(e)}",
                    "parse_mode": "HTML"
                }, timeout=5)
                print(f"[텔레그램 웹훅] 장비 중지 오류: {e}")
        
        # Cancel 버튼 클릭 시
        elif callback_data == "equipment_cancel" or callback_data == "cancel_down":
            send_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            requests.post(send_url, json={
                "chat_id": chat_id,
                "text": "✅ 장비 Down이 취소되었습니다.",
                "parse_mode": "HTML"
            }, timeout=5)
        
        return JSONResponse({"status": "ok"})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)
