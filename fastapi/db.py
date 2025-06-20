import psycopg2
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os
from dateutil import parser

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

def get_event_chart_data(param, start, end, step=10):
    """Return actual and predicted data for a parameter between two timestamps."""
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432,
    )
    cur = conn.cursor()

    from_ts = parser.parse(start)
    to_ts = parser.parse(end)
    date_suffix = from_ts.strftime("%Y%m%d")

    raw_table = f"rawdata{date_suffix}"
    param_modified = param.replace(' ', '_').replace('.', '_').replace('-', '_')
    pred_table = f"pred_{step}_{param_modified}"

    if len(str(from_ts)) >= 26:
        from_ts = str(from_ts)[:23]
        to_ts = str(to_ts)[:23]

    try:
        cur.execute(
            f"""
            SELECT DATE_TRUNC('second', "Timestamp") AS ts, "{param}"
            FROM "{raw_table}"
            WHERE "Timestamp" BETWEEN %s::timestamp AND %s::timestamp
            ORDER BY ts ASC
            """,
            (from_ts, to_ts),
        )
        actuals = [{"x": str(ts), "y": val} for ts, val in cur.fetchall()]

        cur.execute(
            f"""
            SELECT DATE_TRUNC('second', "Timestamp") AS ts, "Parameter"
            FROM "{pred_table}"
            WHERE "Timestamp" BETWEEN %s::timestamp AND %s::timestamp
            ORDER BY ts ASC
            """,
            (from_ts, to_ts),
        )
        preds = [{"x": str(ts), "y": val} for ts, val in cur.fetchall()]
    except Exception as e:
        actuals, preds = [], []
        print("[get_event_chart_data ERROR]", e)

    cur.close()
    conn.close()

    return {"actual": actuals, "predicted": preds}

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