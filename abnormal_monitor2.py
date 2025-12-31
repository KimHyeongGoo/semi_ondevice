import json
import os
import random
import time
import subprocess
from collections import defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path

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
THRESHOLD_ABS = 0.4      # 절대값 0.25 이상
MIN_DURATION_SEC = 5.0    # 이상 구간 최소 지속시간
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
        self.start_time = time.time()  # 프로세스 시작 시간
        self.last_process_pids = {}  # generate_data.py, insert_real_time.py의 마지막 PID 추적
        self.equipment_start_grace_period_sec = 60  # 장비 시작 후 3분(180초) 동안 이상감지/DB저장 비활성화
        self.equipment_start_time = None  # 장비 시작 시간 (generator_health.json에서 읽음)
        os.makedirs("./log", exist_ok=True)
        conn = self.pool.getconn()
        try:
            ensure_abnormal_log_table(conn)
        finally:
            self.pool.putconn(conn)

    def log(self, msg):
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        line = f"[{now}] {msg}"
        print(line)
    
    def get_equipment_start_time(self):
        """generator_health.json에서 장비 시작 시간을 읽어옵니다."""
        try:
            health_file = Path(__file__).resolve().parent / "generator_health.json"
            if health_file.exists():
                data = json.loads(health_file.read_text(encoding="utf-8"))
                start_time_str = data.get("equipment_start_time")
                if start_time_str:
                    from dateutil import parser
                    return parser.parse(start_time_str).timestamp()
        except Exception as e:
            self.log(f"[장비 시작 시간 읽기 오류] {e}")
        return None
    
    def is_within_equipment_start_grace_period(self):
        """장비 시작 후 3분(180초) 이내인지 확인"""
        if self.equipment_start_time is None:
            self.equipment_start_time = self.get_equipment_start_time()
        
        if self.equipment_start_time is None:
            return False
        
        current_time = time.time()
        time_since_equipment_start = current_time - self.equipment_start_time
        
        # 장비 시작 시간이 3분 이내이고, 프로세스 시작 시간보다 최근이면 그레이스 기간 적용
        if time_since_equipment_start < self.equipment_start_grace_period_sec:
            # 장비 시작 시간이 프로세스 시작 시간보다 최근인지 확인
            if self.equipment_start_time > self.start_time:
                return True
        
        return False

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
                               "PPExecStepID",
                               "PPExecStepName"
                        FROM "{raw_table}"
                        WHERE "Timestamp" > %s
                        ORDER BY "Timestamp" ASC
                        """,
                        (since,)
                    )
                    actual_rows = cur.fetchall()
                except Exception as e:
                    conn.rollback()
                    self.log(f"[WARN] actual fetch 실패 param={param}, table={raw_table}, since={since}: {e}")

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
                    except Exception as e:
                        conn.rollback()
                        self.log(f"[WARN] predict fetch 실패 param={param}, table={pred_table}, since={since}: {e}")
        finally:
            self.pool.putconn(conn)
        return actual_rows, pred_rows

    def get_overlapping_violation_type(self, param: str, start_time: datetime, end_time: datetime):
        """realtime_abnormal_log2 테이블에서 기간이 겹치는 데이터의 violation_type을 조회"""
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                # 기간이 겹치는 조건: 
                # (new_start <= old_end) AND (new_end >= old_start)
                cur.execute(
                    """
                    SELECT violation_type
                    FROM realtime_abnormal_log2
                    WHERE parameter = %s
                      AND violation_type IS NOT NULL
                      AND start_time <= %s
                      AND end_time >= %s
                    ORDER BY start_time DESC
                    LIMIT 1
                    """,
                    (param, end_time, start_time)
                )
                row = cur.fetchone()
                if row and row[0] is not None:
                    return int(row[0])
        except Exception as e:
            self.log(f"[violation_type 조회 오류] param={param}: {e}")
        finally:
            self.pool.putconn(conn)
        return None

    def upsert_event(self, param, event):
        def _to_py(val):
            import numpy as np
            if isinstance(val, np.generic):
                return val.item()
            return val

        # realtime_abnormal_log2 테이블에서 기간이 겹치는 데이터의 violation_type 조회
        violation_type = self.get_overlapping_violation_type(param, event["start"], event["end"])
        
        # 겹치는 기간이 없으면 기존 방식대로 weighted_violation_type 사용
        if violation_type is None:
            violation_type = event.get("violation_type")
            if violation_type is None:
                violation_type = weighted_violation_type()
        else:
            # 겹치는 기간이 있으면 조회한 violation_type 사용
            self.log(f"[violation_type 적용] param={param}, start={event['start']}, end={event['end']}, violation_type={violation_type}")

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
                    "violation_type": violation_type,
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
                        violation_type,
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
        # 장비 시작 후 3분 이내면 이상감지 스킵
        if self.is_within_equipment_start_grace_period():
            return
        
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

        # 로그 출력 (초 단위)
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
        '''
        self.log(
            f"{param} | 실제값: [{ts_str}] [{actual_val}] | 예측값: [{ts_str}] [{pred_val}] | "
            f"diff_abs: {diff_abs:.4f}, diff_pct: {diff_pct:.2f}% | {'이상' if is_anomaly else '정상'}"
        )
        '''
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

    def check_process_restart(self):
        """generate_data.py와 insert_real_time.py의 재가동 감지 및 장비 시작 시간 확인"""
        scripts_to_check = ["generate_data.py", "insert_real_time.py"]
        restarted = False
        
        for script_name in scripts_to_check:
            try:
                # 현재 실행 중인 프로세스의 PID 확인
                result = subprocess.run(
                    ["pgrep", "-f", script_name],
                    capture_output=True,
                    text=True
                )
                if result.returncode == 0:
                    current_pids = set([pid.strip() for pid in result.stdout.strip().split('\n') if pid.strip()])
                    last_pids = self.last_process_pids.get(script_name, set())
                    
                    # PID가 변경되었거나 새로 시작된 경우 (재가동 감지)
                    if current_pids and (not last_pids or not current_pids.issubset(last_pids)):
                        if last_pids:  # 이전에 실행 중이었던 경우에만 재가동으로 간주
                            self.log(f"[프로세스 재가동 감지] {script_name} 재시작됨")
                            restarted = True
                        self.last_process_pids[script_name] = current_pids
                    elif not current_pids:
                        # 프로세스가 종료된 경우
                        if last_pids:
                            self.log(f"[프로세스 종료 감지] {script_name} 종료됨")
                        self.last_process_pids[script_name] = set()
                else:
                    # 프로세스가 실행 중이지 않은 경우
                    if self.last_process_pids.get(script_name):
                        self.log(f"[프로세스 종료 감지] {script_name} 종료됨")
                    self.last_process_pids[script_name] = set()
            except Exception as e:
                self.log(f"[프로세스 체크 오류] {script_name}: {e}")
        
        # 재가동이 감지되면 start_time 리셋
        if restarted:
            self.start_time = time.time()
            self.log(f"[프로세스 재가동] 프로세스가 재시작되었습니다.")
        
        # 장비 시작 시간 확인 (주기적으로 업데이트)
        equipment_start_time = self.get_equipment_start_time()
        if equipment_start_time is not None:
            # 장비 시작 시간이 업데이트되었거나 처음 읽는 경우
            if self.equipment_start_time is None or equipment_start_time > self.equipment_start_time:
                self.equipment_start_time = equipment_start_time
                current_time = time.time()
                time_since_equipment_start = current_time - equipment_start_time
                if time_since_equipment_start < self.equipment_start_grace_period_sec:
                    remaining_sec = self.equipment_start_grace_period_sec - time_since_equipment_start
                    self.log(f"[장비 시작 그레이스 기간] 장비 시작 후 {remaining_sec:.1f}초 동안 이상감지/DB저장/메시지 발송 비활성화")
    
    def run(self):
        self.log("Starting abnormal monitor (backend diff detection)")
        # 초기 프로세스 상태 확인
        self.check_process_restart()
        
        while True:
            # 프로세스 재가동 체크 (매 루프마다 확인)
            self.check_process_restart()
            
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
