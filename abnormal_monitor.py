import json
import random
import time
from collections import defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.pool import SimpleConnectionPool


# ALD 파라미터 그룹 (predict_real_time.py와 동일한 매핑을 사용한다)
TASKS = [
    ['MFC7_DCS', 'MFC8_NH3', 'MFC26_F.PWR'],
    ['MFC1_N2-1', 'MFC2_N2-2', 'MFC3_N2-3'],
    ['MFC4_N2-4', 'MFC27_L.POS', 'MFC28_R.POS'],
    ['VG11 Press value', 'VG12 Press value', 'VG13 Press value'],
    ['Temp_Act_U', 'Temp_Act_CU', 'Temp_Act_C', 'Temp_Act_CL', 'Temp_Act_L'],
]

# 예측 테이블 매핑
PARAM_TABLE_MAP = {}
for idx, cols in enumerate(TASKS):
    for col in cols:
        PARAM_TABLE_MAP[col] = f"pred_proc{idx}"

# 이상감지 대상 파라미터만 필터링
ALLOWED_PARAMS = {
    'MFC7_DCS',
    'MFC8_NH3',
    'MFC1_N2-1',
    'MFC2_N2-2',
    'MFC3_N2-3',
    'MFC4_N2-4',
}


PREDICT_STEP = 10
THRESHOLD_PERCENT = 10.0  # 10% 이상
THRESHOLD_ABS = 0.25      # 절대값 0.25 이상
MIN_DURATION_SEC = 4.0    # 이상 구간 최소 지속시간
CLEAR_GAP_SEC = 2.0       # 2초 이상 정상 구간이면 종료
POLL_INTERVAL_SEC = 1.0

DB_CONF = {
    "dbname": "postgres",
    "user": "keti",
    "password": "keti1234!",
    "host": "localhost",
    "port": 5432,
}

KST = ZoneInfo("Asia/Seoul")


def ensure_abnormal_log_table(conn):
    with conn.cursor() as cur:
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
        conn.commit()


def weighted_violation_type():
    # 1:40%, 2:30%, 3:20%, 4:10%
    r = random.random()
    if r < 0.4:
        return 1
    if r < 0.7:
        return 2
    if r < 0.9:
        return 3
    return 4


class Monitor:
    def __init__(self):
        self.pool = SimpleConnectionPool(1, 5, **DB_CONF)
        self.state = defaultdict(dict)  # param -> event state
        self.last_ts = {}  # param -> last processed timestamp
        conn = self.pool.getconn()
        try:
            ensure_abnormal_log_table(conn)
        finally:
            self.pool.putconn(conn)

    def log(self, msg):
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{now}] {msg}")

    def fetch_rows(self, param, since):
        """Fetch actual and predicted rows newer than `since` for the parameter."""
        tz_now = datetime.now(KST)
        raw_table = f"rawdata{tz_now.strftime('%Y%m%d')}"
        pred_table = PARAM_TABLE_MAP.get(param)
        pred_col = param.replace(' ', '_').replace('.', '_').replace('-', '_')
        actual_rows = []
        pred_rows = []
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        f"""
                        SELECT date_trunc('second', "Timestamp") AS ts,
                               "{param}",
                               "PPExecStepStepID",
                               "PPExecStepStepName"
                        FROM "{raw_table}"
                        WHERE "Timestamp" > %s
                        ORDER BY "Timestamp" ASC
                        """,
                        (since,)
                    )
                    actual_rows = cur.fetchall()
                except Exception:
                    conn.rollback()

                if pred_table:
                    try:
                        cur.execute(
                            f"""
                            SELECT date_trunc('second', "Timestamp") AS ts, "{pred_col}"
                            FROM "{pred_table}"
                            WHERE "PredictStep" = %s AND "Timestamp" > %s
                            ORDER BY "Timestamp" ASC
                            """,
                            (PREDICT_STEP, since),
                        )
                        pred_rows = cur.fetchall()
                    except Exception:
                        conn.rollback()
        finally:
            self.pool.putconn(conn)
        return actual_rows, pred_rows

    def upsert_event(self, param, event):
        def _to_py(val):
            import numpy as np
            if isinstance(val, np.generic):
                return val.item()
            return val

        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                message_payload = {
                    "parameter": param,
                    "start": event["start"].isoformat(),
                    "end": event["end"].isoformat(),
                    "duration_seconds": event["duration"],
                    "diff_percent": _to_py(event["avg_diff"]),
                    "peak_time": event.get("peak_time").isoformat() if event.get("peak_time") else None,
                    "actual_value": _to_py(event.get("peak_actual")),
                    "predicted_value": _to_py(event.get("peak_pred")),
                    "step_id": sorted(event.get("step_ids", [])),
                    "step_name": sorted(event.get("step_names", [])),
                    "violation_type": _to_py(event.get("violation_type")),
                }
                message_text = json.dumps(message_payload, ensure_ascii=False)
                cur.execute(
                    """
                    INSERT INTO realtime_abnormal_log (
                        start_time, end_time, parameter,
                        duration_seconds, avg_diff_percent, max_diff_percent,
                        peak_time, actual_value, predicted_value, violation_type, message,
                        created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (parameter, start_time) DO UPDATE
                    SET end_time = EXCLUDED.end_time,
                        duration_seconds = EXCLUDED.duration_seconds,
                        avg_diff_percent = EXCLUDED.avg_diff_percent,
                        max_diff_percent = EXCLUDED.max_diff_percent,
                        peak_time = EXCLUDED.peak_time,
                        actual_value = EXCLUDED.actual_value,
                        predicted_value = EXCLUDED.predicted_value,
                        violation_type = EXCLUDED.violation_type,
                        message = EXCLUDED.message,
                        updated_at = NOW()
                    """,
                    (
                        event["start"],
                        event["end"],
                        param,
                        _to_py(event["duration"]),
                        _to_py(event["avg_diff"]),
                        _to_py(event["max_diff"]),
                        event.get("peak_time"),
                        _to_py(event.get("peak_actual")),
                        _to_py(event.get("peak_pred")),
                        _to_py(event.get("violation_type")),
                        message_text,
                    ),
                )
            conn.commit()
        finally:
            self.pool.putconn(conn)

    def finalize_event(self, param):
        state = self.state.get(param)
        if not state or not state.get("active"):
            return
        duration = (state["last_anomaly"] - state["start"]).total_seconds() + 1.0
        if duration < MIN_DURATION_SEC:
            self.state[param] = {}
            return
        avg_diff = state["diff_sum"] / max(1, state["diff_count"])
        event = {
            "start": state["start"],
            "end": state["last_anomaly"],
            "duration": duration,
            "avg_diff": avg_diff,
            "max_diff": state["max_diff"],
            "peak_time": state.get("peak_time"),
            "peak_actual": state.get("peak_actual"),
            "peak_pred": state.get("peak_pred"),
            "step_ids": state.get("step_ids", set()),
            "step_names": state.get("step_names", set()),
            "violation_type": state.get("violation_type"),
        }
        self.upsert_event(param, event)
        self.state[param] = {}

    def handle_sample(self, param, ts, actual_val, pred_val, step_id=None, step_name=None):
        state = self.state.setdefault(param, {})
        if pred_val is None or actual_val is None:
            # missing prediction or actual
            if state.get("active"):
                # check if should close
                if (ts - state["last_anomaly"]).total_seconds() >= CLEAR_GAP_SEC:
                    self.finalize_event(param)
            return

        try:
            diff_pct = abs(actual_val - pred_val) / (abs(actual_val) or 1.0) * 100.0
        except Exception:
            diff_pct = 0.0
        diff_abs = abs(actual_val - pred_val)
        is_anomaly = diff_pct > THRESHOLD_PERCENT and diff_abs > THRESHOLD_ABS

        if is_anomaly:
            if not state.get("active"):
                state["active"] = True
                state["start"] = ts
                state["violation_type"] = weighted_violation_type()
                state["diff_sum"] = diff_pct
                state["diff_count"] = 1
                state["max_diff"] = diff_pct
                state["peak_time"] = ts
                state["peak_actual"] = actual_val
                state["peak_pred"] = pred_val
                state["step_ids"] = set()
                state["step_names"] = set()
            else:
                state["diff_sum"] += diff_pct
                state["diff_count"] += 1
                if diff_pct > state.get("max_diff", 0):
                    state["max_diff"] = diff_pct
                    state["peak_time"] = ts
                    state["peak_actual"] = actual_val
                    state["peak_pred"] = pred_val
            state["last_anomaly"] = ts
            if step_id is not None:
                state.setdefault("step_ids", set()).add(step_id)
            if step_name:
                state.setdefault("step_names", set()).add(step_name)

            duration = (state["last_anomaly"] - state["start"]).total_seconds() + 1.0
            if duration >= MIN_DURATION_SEC:
                avg_diff = state["diff_sum"] / max(1, state["diff_count"])
                self.upsert_event(
                    param,
                    {
                        "start": state["start"],
                        "end": state["last_anomaly"],
                        "duration": duration,
                        "avg_diff": avg_diff,
                        "max_diff": state.get("max_diff"),
                        "peak_time": state.get("peak_time"),
                        "peak_actual": state.get("peak_actual"),
                        "peak_pred": state.get("peak_pred"),
                        "step_ids": state.get("step_ids", set()),
                        "step_names": state.get("step_names", set()),
                        "violation_type": state.get("violation_type"),
                    },
                )
        else:
            if state.get("active"):
                if (ts - state["last_anomaly"]).total_seconds() >= CLEAR_GAP_SEC:
                    self.finalize_event(param)

    def run(self):
        self.log("Starting abnormal monitor (backend diff detection)")
        while True:
            loop_start = datetime.now(KST)
            for param in PARAM_TABLE_MAP.keys():
                if param not in ALLOWED_PARAMS:
                    continue
                since = self.last_ts.get(param)
                if since is None:
                    since = loop_start - timedelta(seconds=10)
                actual_rows, pred_rows = self.fetch_rows(param, since)
                if not actual_rows and not pred_rows:
                    continue
                pred_map = {row[0]: row[1] for row in pred_rows}
                for ts, val, step_id, step_name in actual_rows:
                    self.last_ts[param] = ts
                    pred_val = pred_map.get(ts)
                    self.handle_sample(param, ts, val, pred_val, step_id, step_name)
            time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    monitor = Monitor()
    try:
        monitor.run()
    except KeyboardInterrupt:
        monitor.log("Monitor stopped")
