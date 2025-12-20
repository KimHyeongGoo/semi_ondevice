# realtime_on_anomaly_monitor.py
# -*- coding: utf-8 -*-
import logging, time, math, yaml
from collections import deque
from datetime import datetime, timedelta, timezone
from dateutil import parser
import numpy as np
import psycopg2

# ===== 사용자 설정 =====
SETPOINTS_PATH = "baco/setpoints.yaml"  # build 단계에서 만든 라이브러리
ABNORMAL_TABLE = "pvd4_abnormals"
VIOLENCE_TABLE = "pvd_violence"

FIELD_LABELS = {
    "ion_gauge_i": "Ion Gauge",
    "baratron_gauge_i": "Baratron Gauge",
    "ar_mfc_i": "Ar MFC",
}

# 톨러런스(퍼센트)
AR_TOL = 0.01        # Ar ±1%
ION_TOL = 0.10       # Ion ±10%
BAR_TOL = 0.10       # Baratron ±10%
ION_MIN_ABS = 0.0    # (선택) Ion 절대 tol
BAR_MIN_ABS = 0.0    # (선택) Bar 절대 tol

# ON 판정용
BASELINE_WINDOW = 5        # 최근 N초 버퍼(1Hz 가정)
UP_THR = 0.6               # 정규화 기준 상향 임계
DOWN_THR = 0.4             # 정규화 기준 하향 임계
SMOOTH_WIN = 3             # 간단 median smoothing에 대응(여기선 생략하여 최소화)

# 이상 구간 판정 기준
MIN_VIOLATION_SECONDS = 2.0  # ON 상태에서 2초 이상 연속 위반 시 이상

# ===== DB 연결 정보 =====
PG_CONN_KW = dict(
    dbname="postgres",
    user="keti",
    password="keti1234!",
    host="localhost",
    port=5432,
)

# =========================
# 네가 준 코드 (원형 유지)
# =========================
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
    conn = psycopg2.connect(**PG_CONN_KW)
    cur = conn.cursor()

    latest_table = _get_latest_pvd_table(cur)
    if not latest_table:
        cur.close(); conn.close()
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
            query += ' WHERE "timer" > %s'
            params.append(since_dt)

    query += ' ORDER BY "timer" ASC'
    if params:
        cur.execute(query, tuple(params))
    else:
        cur.execute(query)

    rows = []
    for timer, ion, baratron, ar_mfc in cur.fetchall():
        rows.append(
            {
                "timer": timer.isoformat() if timer else None,
                "ion_gauge_i": ion,
                "baratron_gauge_i": baratron,
                "ar_mfc_i": ar_mfc,
            }
        )

    cur.close(); conn.close()
    return {"table": latest_table, "is_new_table": is_new_table, "rows": rows}

# =========================
# 보조 유틸
# =========================
def robust_01(arr):
    """[5,95]% 분위수로 0..1 스케일 (값 적거나 상수면 0 반환)"""
    x = np.asarray(arr, dtype=float).ravel()
    if x.size < 2: return np.zeros_like(x)
    finite = np.isfinite(x)
    if finite.sum() < 2: return np.zeros_like(x)
    xx = x[finite]
    q05, q95 = np.percentile(xx, [5,95])
    if not np.isfinite(q95-q05) or (q95 - q05) < 1e-12:
        return np.zeros_like(x)
    y = (x - q05) / (q95 - q05)
    y = np.clip(y, 0, 1)
    y[~finite] = 0.0
    return y

def decide_on_off(ref_buf, up=UP_THR, down=DOWN_THR):
    """버퍼 값으로 ON/OFF (히스테리시스). 최신값 기준 반환."""
    if len(ref_buf) < 3:
        return False
    ref_n = robust_01(list(ref_buf))
    # 히스테리시스 누적 상태를 간단히 재생
    on = False
    for v in ref_n:
        if not on and v >= up: on = True
        elif on and v <= down: on = False
    return on

def load_setpoints(path=SETPOINTS_PATH):
    with open(path, "r", encoding="utf-8") as f:
        lib = yaml.safe_load(f)
    if not lib or "sets" not in lib or not lib["sets"]:
        raise RuntimeError("setpoints.yaml에 sets가 없습니다.")
    return lib

def pick_set_by_ar(lib, ar_value):
    """Ar 값이 가장 가까운 세트 선택."""
    sets = lib["sets"]
    best, best_d = sets[0], float("inf")
    for s in sets:
        mu_ar = s["mu"]["Ar.MFC.i"]
        d = abs((ar_value or 0.0) - mu_ar)
        if d < best_d:
            best, best_d = s, d
    return best

def percent_dev(val, ref):
    if ref == 0 or ref is None:
        return float("inf") if val not in (0, None) else 0.0
    return abs((val - ref) / ref)


def _format_value(value, digits=5):
    if value is None:
        return "N/A"
    try:
        if math.isfinite(value):
            return f"{value:.{digits}f}"
    except (TypeError, ValueError):
        return str(value)
    return str(value)


def _format_percent(value):
    if value is None:
        return "N/A"
    try:
        if math.isfinite(value):
            return f"{value * 100:.2f}%"
    except (TypeError, ValueError):
        return "N/A"
    return "∞%"


# =========================
# DB: 이상 로그 테이블
# =========================
def ensure_abnormal_table():
    sql = f"""
    CREATE TABLE IF NOT EXISTS {ABNORMAL_TABLE} (
        id BIGSERIAL PRIMARY KEY,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        source_table TEXT,
        timer TIMESTAMPTZ,
        state TEXT,
        set_id INT,
        ion_gauge_i DOUBLE PRECISION,
        baratron_gauge_i DOUBLE PRECISION,
        ar_mfc_i DOUBLE PRECISION,
        mu_ion DOUBLE PRECISION,
        mu_baratron DOUBLE PRECISION,
        mu_ar DOUBLE PRECISION,
        dev_ion DOUBLE PRECISION,
        dev_baratron DOUBLE PRECISION,
        dev_ar DOUBLE PRECISION,
        kofm TEXT
    );
    """
    conn = psycopg2.connect(**PG_CONN_KW)
    with conn, conn.cursor() as cur:
        cur.execute(sql)
    conn.close()

def ensure_violence_table():
    sql = f"""
    CREATE TABLE IF NOT EXISTS {VIOLENCE_TABLE} (
        id BIGSERIAL PRIMARY KEY,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        timer TIMESTAMPTZ,
        source_table TEXT,
        state TEXT,
        set_id INT,
        fields TEXT,
        log_text TEXT NOT NULL
    );
    """
    conn = psycopg2.connect(**PG_CONN_KW)
    with conn, conn.cursor() as cur:
        cur.execute(sql)
    conn.close()

def insert_abnormal(row, source_table, state, set_id, mu, devs, violation_window):
    sql = f"""
    INSERT INTO {ABNORMAL_TABLE}
    (source_table, timer, state, set_id,
     ion_gauge_i, baratron_gauge_i, ar_mfc_i,
     mu_ion, mu_baratron, mu_ar,
     dev_ion, dev_baratron, dev_ar, kofm)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """
    vals = (
        source_table,
        parser.parse(row["timer"]) if row["timer"] else None,
        state,
        set_id,
        row["ion_gauge_i"], row["baratron_gauge_i"], row["ar_mfc_i"],
        mu["Ion.Gauge.i"], mu["Baratron.Gauge.i"], mu["Ar.MFC.i"],
        devs["ion"], devs["bar"], devs["ar"],
        violation_window
    )
    conn = psycopg2.connect(**PG_CONN_KW)
    with conn, conn.cursor() as cur:
        cur.execute(sql, vals)
    conn.close()


def insert_violence_log(row, source_table, state, set_id, fields, violation_window):
    if not fields:
        return

    detail_parts = []
    field_names = []
    for field in fields:
        key = field["key"]
        value = field["value"]
        mu_value = field["mu"]
        dev_value = field["dev"]
        label = FIELD_LABELS.get(key, key)
        field_names.append(key)

        tol_text = None
        if key == "ar_mfc_i":
            tol_text = f"허용±{AR_TOL * 100:.2f}%"
        elif key == "ion_gauge_i":
            tol_text = f"허용±{ION_TOL * 100:.2f}%"
            if ION_MIN_ABS > 0:
                tol_text += f" 또는 ±{ION_MIN_ABS:.5f}"
        elif key == "baratron_gauge_i":
            tol_text = f"허용±{BAR_TOL * 100:.2f}%"
            if BAR_MIN_ABS > 0:
                tol_text += f" 또는 ±{BAR_MIN_ABS:.5f}"

        pieces = [
            f"{label}({key}) 값 {_format_value(value)}",
        ]
        if mu_value is not None:
            pieces.append(f"기준 {_format_value(mu_value)}")
        pieces.append(f"편차 {_format_percent(dev_value)}")
        if tol_text:
            pieces.append(tol_text)
        detail_parts.append(", ".join(pieces))

    summary = " | ".join(detail_parts)
    log_text = (
        f"상태={state}, 세트={set_id}, 위반지속={violation_window} :: "
        f"{summary}"
    )

    sql = f"""
    INSERT INTO {VIOLENCE_TABLE}
    (timer, source_table, state, set_id, fields, log_text)
    VALUES (%s, %s, %s, %s, %s, %s);
    """

    vals = (
        parser.parse(row["timer"]) if row.get("timer") else None,
        source_table,
        state,
        set_id,
        ", ".join(field_names),
        log_text,
    )

    conn = psycopg2.connect(**PG_CONN_KW)
    with conn, conn.cursor() as cur:
        cur.execute(sql, vals)
    conn.close()

# =========================
# 메인 루프
# =========================
def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )
    logging.info("Starting realtime ON/abnormal monitor.")
    ensure_abnormal_table()
    ensure_violence_table()
    lib = load_setpoints(SETPOINTS_PATH)
    logging.info("Loaded setpoints: %d sets", len(lib["sets"]))

    # 상태 변수
    last_table = None
    since_iso = None
    ref_buf = deque(maxlen=BASELINE_WINDOW)  # ON 판정용 Ar 버퍼
    violation_start = None                  # 연속 위반 시작 시각
    violation_duration = 0.0                # 현재 연속 위반 지속 시간(초)

    while True:
        try:
            res = get_latest_pvd_stream_data(last_table=last_table, since=since_iso)
            table = res["table"]
            if table is None:
                logging.warning("No PVD table found.")
                time.sleep(1); continue

            if res["is_new_table"]:
                logging.info("Switched to new source table: %s", table)
                ref_buf.clear()
                violation_start = None
                violation_duration = 0.0
                since_iso = None  # 새 테이블이면 처음부터 읽음

            rows = res["rows"]
            if rows:
                # 최신 타임스탬프 기억
                since_iso = rows[-1]["timer"]

            # 신규 행들을 순서대로 처리
            for row in rows:
                last_table = table
                ar = row["ar_mfc_i"]; ion = row["ion_gauge_i"]; bar = row["baratron_gauge_i"]

                # ON/OFF 판정용 버퍼 갱신
                ref_buf.append(ar)
                is_on = decide_on_off(ref_buf)

                state = "ON" if is_on else "OFF"

                timer_iso = row.get("timer")
                try:
                    timer_dt = parser.parse(timer_iso) if timer_iso else None
                except (ValueError, TypeError):
                    timer_dt = None

                if not is_on:
                    violation_start = None
                    violation_duration = 0.0
                    continue

                # 세트 선택 (Ar 값이 가장 가까운 세트)
                s = pick_set_by_ar(lib, ar)
                mu = s["mu"]

                # 편차 계산
                dev_ar  = percent_dev(ar,  mu["Ar.MFC.i"])
                dev_ion = percent_dev(ion, mu["Ion.Gauge.i"])
                dev_bar = percent_dev(bar, mu["Baratron.Gauge.i"])

                # 위반 여부
                vio_ar  = dev_ar  > AR_TOL
                vio_ion = abs(ion - mu["Ion.Gauge.i"]) > max(ION_TOL*abs(mu["Ion.Gauge.i"]), ION_MIN_ABS)
                vio_bar = abs(bar - mu["Baratron.Gauge.i"]) > max(BAR_TOL*abs(mu["Baratron.Gauge.i"]), BAR_MIN_ABS)

                violated = bool(vio_ar or vio_ion or vio_bar)

                if violated:
                    if timer_dt is None:
                        # 타임스탬프가 없으면 이전 지속 시간을 유지
                        pass
                    elif violation_start is None or timer_dt < violation_start:
                        violation_start = timer_dt
                        violation_duration = 0.0
                    else:
                        violation_duration = max(
                            0.0,
                            (timer_dt - violation_start).total_seconds(),
                        )
                else:
                    violation_start = None
                    violation_duration = 0.0

                # ON 상태에서 연속 위반 시간이 임계(2초) 이상이면 이상 판정
                stable_alarm = violated and (
                    violation_duration >= MIN_VIOLATION_SECONDS
                )
                violation_duration_str = f"{violation_duration:.2f}s"

                # 로그/DB
                if stable_alarm:
                    dev_map = {"ion": dev_ion, "bar": dev_bar, "ar": dev_ar}
                    insert_abnormal(
                        row=row,
                        source_table=table,
                        state=state,
                        set_id=s["id"],
                        mu=mu,
                        devs=dev_map,
                        violation_window=violation_duration_str
                    )

                    violated_fields = []
                    if vio_ion:
                        violated_fields.append({
                            "key": "ion_gauge_i",
                            "value": ion,
                            "mu": mu["Ion.Gauge.i"],
                            "dev": dev_ion,
                        })
                    if vio_bar:
                        violated_fields.append({
                            "key": "baratron_gauge_i",
                            "value": bar,
                            "mu": mu["Baratron.Gauge.i"],
                            "dev": dev_bar,
                        })
                    if vio_ar:
                        violated_fields.append({
                            "key": "ar_mfc_i",
                            "value": ar,
                            "mu": mu["Ar.MFC.i"],
                            "dev": dev_ar,
                        })

                    insert_violence_log(
                        row=row,
                        source_table=table,
                        state=state,
                        set_id=s["id"],
                        fields=violated_fields,
                        violation_window=violation_duration_str,
                    )
                    logging.warning(
                        "[ABNORMAL] %s set=%s Ar dev=%.3f%% Ion dev=%.3f%% Bar dev=%.3f%% 위반지속=%s",
                        row["timer"], s["id"], dev_ar*100, dev_ion*100, dev_bar*100, violation_duration_str
                    )
                else:
                    logging.info(
                        "ON ok? violated=%s | set=%s | dev%% (Ar=%.3f, Ion=%.3f, Bar=%.3f) | 위반지속=%s",
                        violated, s["id"], dev_ar*100, dev_ion*100, dev_bar*100, violation_duration_str
                    )

            time.sleep(0.3)

        except Exception as e:
            logging.exception("Loop error: %s", e)
            time.sleep(0.3)

if __name__ == "__main__":
    main()
