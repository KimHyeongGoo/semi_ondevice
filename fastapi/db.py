import psycopg2
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import yaml
import os

def insert_violation(conn, cur, timestamp, col, step_id, val, limit_type, threshold):

    symbol = "<=" if limit_type == "min" else ">="
    if symbol == '<=':
        msg = f"[{timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] 하한선 침범\n[파라미터 {col}] val({val:.3f}) {symbol} {limit_type}({threshold})"
    else:
        msg = f"[{timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] 상한선 침범\n[파라미터 {col}] val({val:.3f}) {symbol} {limit_type}({threshold})"
        
    cur.execute("""
        INSERT INTO realtime_violation_log ("Timestamp",  parameter, message)
        VALUES (%s, %s, %s)
        ON CONFLICT ("Timestamp", parameter) DO NOTHING
    """, (timestamp, col, msg))
    conn.commit()

def get_latest_data(columns, duration=300):
    tz = ZoneInfo("Asia/Seoul")
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432
    )
    cur = conn.cursor()
    now = datetime.now(ZoneInfo("Asia/Seoul"))
    from_time = now - timedelta(seconds=duration)

    result = {}
    date_suffix = now.strftime("%d%H")
    raw_table = f"rawdata{date_suffix}"

    # Step ID 데이터 미리 가져오기 (Timestamp → Step ID 매핑)+ 남은 시간 정보 가져오기
    cur.execute(f"""
        SELECT DATE_TRUNC('second', "Timestamp") AS ts,
            "ProcessRecipeStepRemainTime",
            "ProcessRecipeStepID"
        FROM "{raw_table}"
        WHERE "Timestamp" >= %s
        ORDER BY "Timestamp" ASC
    """, (from_time,))

    step_raw = []
    seen_timestamps = set()
    last_ts = None
    last_step_id = None
    last_remain_sec = 0

    # 1️⃣ 원본 row 추가
    for ts, remain_str, step_id in cur.fetchall():
        if step_id is None:
            continue
        step_raw.append((ts, step_id))
        seen_timestamps.add(ts)
        try: 
            if remain_str == "00:00:00":
                remain_sec = 10
            else:
                h, m, s = map(int, remain_str.split(":"))
                remain_sec = h * 3600 + m * 60 + s
            last_ts = ts
            last_step_id = step_id
            last_remain_sec = remain_sec
        except:
            continue

    # 2️⃣ 마지막 StepID에 대해 복제 보간
    if last_ts is not None and last_remain_sec > 0:
        for i in range(1, last_remain_sec):  # +1초부터 복제 (0초는 원본에 이미 있음)
            new_ts = (last_ts + timedelta(seconds=i))
            if new_ts not in seen_timestamps:
                step_raw.append((new_ts, last_step_id))

    # 3️⃣ 정렬 + 필터링
    step_map = {
        ts: step_id for ts, step_id in sorted(step_raw)
    }

    # limits.yaml 읽기
    limits = {}
    if os.path.exists("limits.yaml"):
        with open("limits.yaml", "r", encoding="utf-8") as f:
            limits = yaml.safe_load(f)

    for col in columns:
        col_modified = col.replace(' ', '_').replace('.', '_')
        pred_table = f"pred_{col_modified}"

        # 실제값
        cur.execute(f"""
            SELECT DATE_TRUNC('second', "Timestamp") AS ts, "{col}" FROM "{raw_table}"
            WHERE "Timestamp" >= %s
            ORDER BY "Timestamp" ASC
        """, (from_time,))
        actuals = [{"time": str(r[0]), "value": r[1]} for r in cur.fetchall()]
        
        # 예측값 + Step ID 포함
        cur.execute(f"""
            SELECT DATE_TRUNC('second', "Timestamp") AS ts, "{col}" FROM "{pred_table}"
            WHERE "Timestamp" >= %s
            ORDER BY "Timestamp" ASC
        """, (from_time,))
        preds = []
        for row in cur.fetchall():
            ts, val = row
            step_id = step_map.get(ts)
            preds.append({
                "time": str(ts),
                "value": val,
                "step_id": int(step_id) if step_id is not None else None
            })    
            #if col == 'Temp_Act_U' and val >= 302.8:
            #    insert_violation(conn, cur, ts, col, 1, val, 'min', 302.8)
            if step_id is not None and val is not None:
                step_limits = limits.get(col, {}).get(str(step_id))
                if step_limits:
                    if "min" in step_limits and val <= step_limits["min"]:
                        insert_violation(conn, cur, ts, col, step_id, val, 'min', step_limits["min"])
                    elif "max" in step_limits and val >= step_limits["max"]:
                        insert_violation(conn, cur, ts, col, step_id, val, 'max', step_limits["max"])
     

        result[col] = {
            "actual": actuals,
            "predicted": preds
        }

    result["limits"] = limits  # JS로 전달
    cur.close()
    conn.close()
    return result
