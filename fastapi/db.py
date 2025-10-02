import psycopg2
from psycopg2 import errors
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import os
from dateutil import parser


# PVD abnormal tolerance configuration (must stay in sync with pvd_detect.py)
AR_TOL = 0.01
ION_TOL = 0.10
BAR_TOL = 0.10
ION_MIN_ABS = 0.0
BAR_MIN_ABS = 0.0


def _percent_dev(val, ref):
    if ref in (None,):
        return float("inf") if val not in (0, None) else 0.0
    try:
        return abs((float(val) - float(ref)) / float(ref))
    except ZeroDivisionError:
        return float("inf") if val not in (0, None) else 0.0
    except (TypeError, ValueError):
        return 0.0


def _is_ar_abnormal(value, mu_value):
    if value is None or mu_value is None:
        return False
    return _percent_dev(value, mu_value) > AR_TOL


def _is_ion_abnormal(value, mu_value):
    if value is None or mu_value is None:
        return False
    try:
        value_f = float(value)
        mu_f = float(mu_value)
    except (TypeError, ValueError):
        return False
    tolerance = max(ION_TOL * abs(mu_f), ION_MIN_ABS)
    return abs(value_f - mu_f) > tolerance


def _is_baratron_abnormal(value, mu_value):
    if value is None or mu_value is None:
        return False
    try:
        value_f = float(value)
        mu_f = float(mu_value)
    except (TypeError, ValueError):
        return False
    tolerance = max(BAR_TOL * abs(mu_f), BAR_MIN_ABS)
    return abs(value_f - mu_f) > tolerance

tasks = [
    ['MFC7_DCS','MFC8_NH3','MFC26_F.PWR'],
    ['MFC1_N2-1','MFC2_N2-2','MFC3_N2-3'],
    ['MFC4_N2-4','MFC27_L.POS','MFC28_R.POS'],
    ['VG11 Press value','VG12 Press value','VG13 Press value'],
    ['Temp_Act_U','Temp_Act_CU','Temp_Act_C','Temp_Act_CL','Temp_Act_L'],
]

KST = timezone(timedelta(hours=9))
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
            SELECT DATE_TRUNC('second', "Timestamp") AS ts,
                   "{col}",
                   "ProcessRecipeStepID",
                   "ProcessRecipeStepName"
            FROM "{raw_table}"
            WHERE "Timestamp" >= %s
            ORDER BY "Timestamp" ASC
        """, (from_time,))
        actual_rows = cur.fetchall()
        actuals = []
        for ts, val, step_id, step_name in actual_rows:
            actuals.append({
                "time": str(ts),
                "value": val,
                "step_id": int(step_id) if step_id is not None else None,
                "step_name": str(step_name) if step_name is not None else None,
            })
        
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


def get_current_step():
    """Return the latest step id and name from the current raw data table."""
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432,
    )
    cur = conn.cursor()

    now = datetime.now(ZoneInfo("Asia/Seoul"))
    date_suffix = now.strftime("%Y%m%d")
    raw_table = f"rawdata{date_suffix}"

    result = {"step_id": None, "step_name": None}

    try:
        cur.execute(
            f"""
            SELECT "ProcessRecipeStepID", "ProcessRecipeStepName"
            FROM "{raw_table}"
            WHERE "ProcessRecipeStepID" IS NOT NULL OR "ProcessRecipeStepName" IS NOT NULL
            ORDER BY "Timestamp" DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
        if row:
            step_id, step_name = row
            result = {
                "step_id": int(step_id) if step_id is not None else None,
                "step_name": str(step_name) if step_name is not None else None,
            }
    except Exception:
        conn.rollback()
    finally:
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

    #from_ts = parser.parse(start)
    #to_ts = parser.parse(end)
    
    to_ts = datetime.now(ZoneInfo("Asia/Seoul"))
    from_ts = to_ts - timedelta(seconds=300)
    #print(to_ts)
    date_suffix = from_ts.strftime("%Y%m%d")
    #print(date_suffix)
    raw_table = f"rawdata{date_suffix}"
    param_modified = param.replace(' ', '_').replace('.', '_').replace('-', '_')
    table_name = param_table_map.get(param)

    if len(str(from_ts)) >= 26:
        from_ts = str(from_ts)[:23]
        to_ts = str(to_ts)[:23]
    print(to_ts)
    '''
    from_ts = parser.parse(from_ts)
    to_ts = parser.parse(to_ts)

    if from_ts.tzinfo is None:
        from_ts = from_ts.replace(tzinfo=timezone.utc).astimezone(KST)
        to_ts = to_ts.replace(tzinfo=timezone.utc).astimezone(KST)
    else:
        from_ts = from_ts.astimezone(KST)
        to_ts = to_ts.astimezone(KST)
    ''' 
    try:
        cur.execute(
            f"""
            SELECT DATE_TRUNC('second', "Timestamp") AS ts,
                   "{param}",
                   "ProcessRecipeStepID",
                   "ProcessRecipeStepName"
            FROM "{raw_table}"
            WHERE "Timestamp" BETWEEN %s::timestamp AND %s::timestamp
            ORDER BY ts ASC
            """,
            (from_ts, to_ts),
        )
        actuals = [
            {
                "x": str(ts),
                "y": val,
                "step_id": step_id,
                "step_name": step_name,
            }
            for ts, val, step_id, step_name in cur.fetchall()
        ]
        if table_name:
            # Predicted 테이블 조회는 3시간 빼서 조회

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
            #print(adj_from_ts, adj_to_ts)
        else:
            preds = []
    except Exception as e:
        actuals, preds = [], []
        print("[get_event_chart_data ERROR]", e)

    cur.close()
    conn.close()

    return {"actual": actuals, "predicted": preds}

def _get_latest_pvd_table(cur):
    cur.execute(
        """
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public' AND tablename LIKE 'pvd4_new_%'
        ORDER BY tablename DESC
        LIMIT 1
        """
    )
    row = cur.fetchone()
    return row[0] if row else None


def get_latest_pvd_stream_data(last_table=None, since=None):
    """Return streaming data for the latest PVD table.

    Args:
        last_table: Table name that the client is currently showing.
        since: ISO formatted timestamp string of the last received row.

    Returns:
        Dict with current table name, whether the table changed, and rows.
    """

    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432,
    )
    cur = conn.cursor()

    latest_table = _get_latest_pvd_table(cur)
    if not latest_table:
        cur.close()
        conn.close()
        return {"table": None, "is_new_table": False, "rows": []}

    is_new_table = last_table != latest_table

    query = (
        f'SELECT "timer", "ion_gauge_i", "baratron_gauge_i", "ar_mfc_i" '
        f'FROM "{latest_table}"'
    )

    params = []
    if not is_new_table and since:
        try:
            since_dt = parser.parse(since)
        except (ValueError, TypeError):
            since_dt = None
        if since_dt is not None:
            query += " WHERE \"timer\" > %s"
            params.append(since_dt)

    query += " ORDER BY \"timer\" ASC"

    if params:
        cur.execute(query, tuple(params))
    else:
        cur.execute(query)

    fetched_rows = cur.fetchall()
    timer_values = [timer for timer, *_ in fetched_rows if timer is not None]

    abnormal_by_time = {}
    if timer_values:
        start_time = min(timer_values)
        end_time = max(timer_values)
        try:
            cur.execute(
                """
                SELECT timer, ion_gauge_i, baratron_gauge_i, ar_mfc_i,
                       mu_ion, mu_baratron, mu_ar
                FROM pvd4_abnormals
                WHERE source_table = %s
                  AND timer BETWEEN %s AND %s
                """,
                (latest_table, start_time, end_time),
            )
            for row in cur.fetchall():
                (
                    abnormal_time,
                    ion_val,
                    bar_val,
                    ar_val,
                    mu_ion,
                    mu_bar,
                    mu_ar,
                ) = row
                if abnormal_time is None:
                    continue
                key_time = abnormal_time.replace(microsecond=0)
                fields = []
                if _is_ion_abnormal(ion_val, mu_ion):
                    fields.append("ion_gauge_i")
                if _is_baratron_abnormal(bar_val, mu_bar):
                    fields.append("baratron_gauge_i")
                if _is_ar_abnormal(ar_val, mu_ar):
                    fields.append("ar_mfc_i")
                if fields:
                    abnormal_by_time.setdefault(key_time, set()).update(fields)
        except errors.UndefinedTable:
            conn.rollback()
            abnormal_by_time = {}

    rows = []
    for timer, ion, baratron, ar_mfc in fetched_rows:
        abnormal_fields = []
        if timer and abnormal_by_time:
            key_time = timer.replace(microsecond=0)
            fields = abnormal_by_time.get(key_time)
            if fields:
                abnormal_fields.extend(sorted(fields))
        row = {
            "timer": timer.isoformat() if timer else None,
            "ion_gauge_i": ion,
            "baratron_gauge_i": baratron,
            "ar_mfc_i": ar_mfc,
        }
        if abnormal_fields:
            row["abnormal_fields"] = abnormal_fields
            row["abnormal"] = True
        rows.append(row)

    cur.close()
    conn.close()

    return {"table": latest_table, "is_new_table": is_new_table, "rows": rows}


def get_recent_pvd_violence_logs(limit=10):
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432,
    )
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT id, created_at, timer, source_table, state, set_id, fields, log_text
            FROM pvd_violence
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (max(1, int(limit)),),
        )
        rows = cur.fetchall()
    except errors.UndefinedTable:
        conn.rollback()
        rows = []
    finally:
        cur.close()
        conn.close()

    tz = ZoneInfo("Asia/Seoul")

    def _to_iso(dt):
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(tz).isoformat()

    result = []
    for row in rows:
        (
            log_id,
            created_at,
            timer,
            source_table,
            state,
            set_id,
            fields,
            log_text,
        ) = row

        created_iso = _to_iso(created_at)
        timer_iso = _to_iso(timer)
        field_list = [
            f.strip()
            for f in (fields.split(",") if fields else [])
            if f.strip()
        ]

        result.append({
            "id": log_id,
            "created_at": created_iso,
            "timer": timer_iso,
            "source_table": source_table,
            "state": state,
            "set_id": set_id,
            "fields": field_list,
            "log_text": log_text,
        })

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
            #print(len(preds))
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