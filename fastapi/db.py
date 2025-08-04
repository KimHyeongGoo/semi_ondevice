import psycopg2
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os
from dateutil import parser

tasks = [
    ['MFC7_DCS','MFC8_NH3','MFC26_F.PWR'],
    ['MFC1_N2-1','MFC2_N2-2','MFC3_N2-3'],
    ['MFC4_N2-4','MFC27_L.POS','MFC28_R.POS'],
    ['VG11 Press value','VG12 Press value','VG13 Press value'],
    ['Temp_Act_U','Temp_Act_CU','Temp_Act_C','Temp_Act_CL','Temp_Act_L'],
]

param_table_map = {}
for idx, cols in enumerate(tasks):
    for col in cols:
        param_table_map[col] = f"pred_proc{idx}"
        
        
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
        table_name = param_table_map.get(col)

        # 실제값
        cur.execute(f"""
            SELECT DATE_TRUNC('second', "Timestamp") AS ts, "{col}" FROM "{raw_table}"
            WHERE "Timestamp" >= %s
            ORDER BY "Timestamp" ASC
        """, (from_time,))
        actuals = [{"time": str(r[0]), "value": r[1]} for r in cur.fetchall()]
        
        # 예측값 + Step ID 포함
        preds = []
        if table_name:
            try:
                cur.execute(f"""
                    SELECT DATE_TRUNC('second', "Timestamp") AS ts, "{col_modified}", "ProcessRecipeStepID", "ProcessRecipeStepName"
                    FROM "{table_name}"
                    WHERE "PredictStep" = %s AND "Timestamp" >= %s
                    ORDER BY "Timestamp" ASC
                """, (step, from_time))

                for row in cur.fetchall():
                    ts, val, step_id, step_name = row
                    preds.append({
                        "time": str(ts),
                        "value": val,
                        "step_id": int(step_id) if step_id is not None else None,
                        "step_name": str(step_name) if step_name is not None else None
                    })
            except Exception:
                conn.rollback()
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
    table_name = param_table_map.get(param)

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

        if table_name:
            cur.execute(
                f"""
                SELECT DATE_TRUNC('second', "Timestamp") AS ts, "{param_modified}"
                FROM "{table_name}"
                WHERE "PredictStep" = %s AND "Timestamp" BETWEEN %s::timestamp AND %s::timestamp
                ORDER BY ts ASC
                """,
                (step, from_ts, to_ts),
            )
            preds = [{"x": str(ts), "y": val} for ts, val in cur.fetchall()]
        else:
            preds = []
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

def get_trace_pred_chart_data(param, start, end, step=10):
    """Return actual and predicted data from trace_pred_data table."""
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
    table_name = param_table_map.get(param)

    if len(str(from_ts)) >= 26:
        from_ts = str(from_ts)[:23]
        to_ts = str(to_ts)[:23]

    try:
        cur.execute(
            f"""
            SELECT DATE_TRUNC('second', "Timestamp") AS ts, "{param}"
            FROM "{raw_table}"
            WHERE "Timestamp" BETWEEN %s::timestamp AND %s::timestamp
            AND "ProcessRecipeStepID" >= 100
            AND "ProcessRecipeStepID" < 160
            ORDER BY ts ASC
            """,
            (from_ts, to_ts),
        )
        actuals = [{"x": str(ts), "y": val} for ts, val in cur.fetchall()]

        cur.execute(
            f"""
            SELECT MIN("Timestamp"), MAX("Timestamp")
            FROM "{raw_table}"
            WHERE "Timestamp" BETWEEN %s::timestamp AND %s::timestamp
            AND "ProcessRecipeStepID" >= 100 AND "ProcessRecipeStepID" < 160
            """,
            (from_ts, to_ts),
        )
        filtered_from_ts, filtered_to_ts = cur.fetchone()

        
        preds = []
        if table_name:
            cur.execute(
                f"""
                SELECT DATE_TRUNC('second', "Timestamp") AS ts, "{param_modified}"
                FROM "{table_name}"
                WHERE "PredictStep" = %s AND "Timestamp" BETWEEN %s::timestamp AND %s::timestamp
                ORDER BY ts ASC
                """,
                (step, filtered_from_ts, filtered_to_ts),
            )
            preds = [{"x": str(ts), "y": val} for ts, val in cur.fetchall()]
    except Exception as e:
        actuals, preds = [], []
        print("[get_trace_pred_chart_data ERROR]", e)

    cur.close()
    conn.close()

    return {"actual": actuals, "predicted": preds}


def get_process_range(target_time):
    """Return process start and end timestamps around the given time."""
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432,
    )
    cur = conn.cursor()

    ts = parser.parse(target_time)
    date_suffix = ts.strftime("%Y%m%d")
    raw_table = f"rawdata{date_suffix}"

    start_time = ts
    end_time = ts

    try:
        # 검색 범위: 주어진 시점 이전 데이터 중 최근 START 단계
        cur.execute(
            f"""
            SELECT "Timestamp", "ProcessRecipeStepName"
            FROM "{raw_table}"
            WHERE "Timestamp" <= %s AND "ProcessRecipeStepName" IS NOT NULL
            ORDER BY "Timestamp" DESC
            LIMIT 20000
            """,
            (ts,)
        )
        rows = cur.fetchall()
        for t, step in rows:
            if (step or "").strip().upper() == "START":
                start_time = t
                break
            else:
                if rows:
                    start_time = rows[-1][0]

        # 주어진 시점 이후 데이터 중 종료로 판단되는 단계 탐색
        cur.execute(
            f"""
            SELECT "Timestamp", "ProcessRecipeStepName"
            FROM "{raw_table}"
            WHERE "Timestamp" >= %s AND "ProcessRecipeStepName" IS NOT NULL
            ORDER BY "Timestamp" ASC
            LIMIT 20000
            """,
            (ts,)
        )
        rows = cur.fetchall()
        for t, step in rows:
            step = (step or "").strip().upper()
            if step in ("END", "", "NAN", "NULL", "NONE", "IDLE", "NA", "NA", "NONE"):
                end_time = t
                break
            else:
                if rows:
                    end_time = rows[-1][0]
        #print( str(start_time), str(end_time))
    except Exception as e:
        print("[get_process_range ERROR]", e)

    cur.close()
    conn.close()
    
    return {"start": str(start_time), "end": str(end_time)}