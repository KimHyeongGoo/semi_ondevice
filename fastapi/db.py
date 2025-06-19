import psycopg2
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os

def get_latest_data(columns, duration=300, step=10):
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
    #date_suffix = now.strftime("%d%H")"
    date_suffix = now.strftime("%Y%m%d")
    raw_table = f"rawdata{date_suffix}"

    for col in columns:
        col_modified = col.replace(' ', '_').replace('.', '_').replace('-', '_')
        pred_table = f"pred_{step}_{col_modified}"

        # 실제값
        cur.execute(f"""
            SELECT DATE_TRUNC('second', "Timestamp") AS ts, "{col}" FROM "{raw_table}"
            WHERE "Timestamp" >= %s
            ORDER BY "Timestamp" ASC
        """, (from_time,))
        actuals = [{"time": str(r[0]), "value": r[1]} for r in cur.fetchall()]
        
        # 예측값 + Step ID 포함
        cur.execute(f"""
            SELECT DATE_TRUNC('second', "Timestamp") AS ts, "Parameter", "ProcessRecipeStepID", "ProcessRecipeStepName"
            FROM "{pred_table}"
            WHERE "Timestamp" >= %s
            ORDER BY "Timestamp" ASC
        """, (from_time,))
        preds = []
        for row in cur.fetchall():
            ts, val, step_id, step_name = row
            preds.append({
                "time": str(ts),
                "value": val,
                "step_id": int(step_id) if step_id is not None else None,
                "step_name": str(step_name) if step_name is not None else None
            })
        result[col] = {
            "actual": actuals,
            "predicted": preds
        }

    cur.close()
    conn.close()
    return result

def get_trace_info(limit=10):
    """Return recent rows from trace_info ordered by start_time descending."""
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432,
    )
    cur = conn.cursor()

    thickness_cols = [f"thickness_{i+1}" for i in range(45)]
    col_sql = ", ".join(thickness_cols)

    # Assign row numbers by start_time (oldest -> newest)
    query = f"""
        SELECT row_num, start_time, end_time, {col_sql}
        FROM (
            SELECT *, ROW_NUMBER() OVER (ORDER BY start_time) AS row_num
            FROM trace_info
        ) t
        ORDER BY start_time DESC
        LIMIT %s
    """

    cur.execute(query, (limit,))
    rows = cur.fetchall()

    result = []
    for row in rows:
        row_num = row[0]
        start_time = row[1]
        end_time = row[2]
        thicknesses = list(row[3:])
        result.append({
            "row_num": int(row_num) if row_num is not None else None,
            "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S"),
            "thicknesses": [float(t) if t is not None else None for t in thicknesses],
        })

    cur.close()
    conn.close()
    return result