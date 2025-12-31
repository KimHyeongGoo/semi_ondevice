import json
import os
import random
import time
import requests
import subprocess
import yaml
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

# FastAPI 서버 URL 설정 (환경 변수로 오버라이드 가능)
FASTAPI_BASE_URL = os.getenv("FASTAPI_BASE_URL", "http://bigsoft.iptime.org:9301")

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
MIN_DURATION_SEC_LIMITS = 3.0    # limits.yaml 이상 구간 최소 지속시간 (3초)
MIN_DURATION_SEC_INTERLOCK = 1.0  # limits2.yaml 이상 구간 최소 지속시간 (1초)
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

# 텔레그램 설정
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8426533458:AAFw6pNm5xGa7ponvmasrYs_i8AicT66tIg")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "-1003454160562")
TELEGRAM_ENABLED = os.getenv("TELEGRAM_ENABLED", "true").lower() == "true"

# 설정 파일 경로
SETTINGS_PATH = Path(__file__).parent / "fastapi" / "settings.yaml"
LIMITS_PATH = Path(__file__).parent / "fastapi" / "limits.yaml"
LIMITS2_PATH = Path(__file__).parent / "fastapi" / "limits2.yaml"

def load_warning_setting():
    """settings.yaml에서 warning_enabled 설정을 읽어옵니다."""
    try:
        if SETTINGS_PATH.exists():
            with open(SETTINGS_PATH, 'r', encoding='utf-8') as f:
                settings = yaml.safe_load(f)
                return settings.get('warning_enabled', True)  # 기본값은 True
        return True  # 파일이 없으면 기본값 True
    except Exception as e:
        print(f"[설정 파일 읽기 오류] {e}")
        return True  # 오류 발생 시 기본값 True

def send_telegram_message(text, reply_markup=None):
    """텔레그램 메시지 전송"""
    if not TELEGRAM_ENABLED:
        return False
    
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML"
        }
        if reply_markup:
            payload["reply_markup"] = json.dumps(reply_markup)
        
        response = requests.post(url, json=payload, timeout=5)
        if response.status_code == 200:
            # 메시지 전송 후 callback_query를 처리하기 위해 polling 시작
            # 별도 스레드에서 polling 실행
            import threading
            if not hasattr(send_telegram_message, '_polling_started'):
                send_telegram_message._polling_started = True
                polling_thread = threading.Thread(target=telegram_polling, daemon=True)
                polling_thread.start()
        return response.status_code == 200
    except Exception as e:
        print(f"[텔레그램 전송 실패] {e}")
        return False

def telegram_polling():
    """텔레그램 봇 polling으로 callback_query 처리"""
    import time
    last_update_id = 0
    
    while True:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
            params = {"offset": last_update_id + 1, "timeout": 10}
            response = requests.get(url, params=params, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                if data.get("ok") and data.get("result"):
                    for update in data["result"]:
                        last_update_id = update.get("update_id", last_update_id)
                        callback_query = update.get("callback_query")
                        
                        if callback_query:
                            callback_data = callback_query.get("data")
                            chat_id = callback_query.get("message", {}).get("chat", {}).get("id")
                            
                            # callback_query에 대한 응답 전송
                            answer_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
                            requests.post(answer_url, json={
                                "callback_query_id": callback_query.get("id")
                            }, timeout=5)
                            
                            # 장비 DOWN 버튼 클릭 시
                            if callback_data == "equipment_down":
                                print(f"[텔레그램 Polling] 장비 DOWN 버튼 클릭됨 (chat_id: {chat_id})")
                                # FastAPI 서버의 /api/equipment/stop 엔드포인트 호출
                                try:
                                    stop_response = requests.post(
                                        f"{FASTAPI_BASE_URL}/api/equipment/stop",
                                        timeout=10
                                    )
                                    stop_data = stop_response.json()
                                    
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
                                    print(f"[텔레그램 Polling] 장비 중지 완료: {result_message}")
                                except requests.exceptions.ConnectionError as e:
                                    # 서버 연결 실패 시 명확한 메시지 전송
                                    error_msg = f"❌ FastAPI 서버에 연결할 수 없습니다.\n\n서버가 실행 중인지 확인해주세요.\n({FASTAPI_BASE_URL})"
                                    send_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
                                    try:
                                        telegram_response = requests.post(send_url, json={
                                            "chat_id": chat_id,
                                            "text": error_msg,
                                            "parse_mode": "HTML"
                                        }, timeout=5)
                                        if telegram_response.status_code == 200:
                                            print(f"[텔레그램 Polling] 오류 메시지 전송 완료 (chat_id: {chat_id})")
                                        else:
                                            print(f"[텔레그램 Polling] 오류 메시지 전송 실패: {telegram_response.status_code}")
                                    except Exception as telegram_err:
                                        print(f"[텔레그램 Polling] 오류 메시지 전송 중 예외 발생: {telegram_err}")
                                    print(f"[텔레그램 Polling] 장비 중지 오류: {error_msg}")
                                except Exception as e:
                                    import traceback
                                    traceback.print_exc()
                                    # 일반적인 오류 메시지
                                    error_msg = f"❌ 장비 중지 중 오류 발생:\n{str(e)}"
                                    send_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
                                    try:
                                        telegram_response = requests.post(send_url, json={
                                            "chat_id": chat_id,
                                            "text": error_msg,
                                            "parse_mode": "HTML"
                                        }, timeout=5)
                                        if telegram_response.status_code == 200:
                                            print(f"[텔레그램 Polling] 오류 메시지 전송 완료 (chat_id: {chat_id})")
                                        else:
                                            print(f"[텔레그램 Polling] 오류 메시지 전송 실패: {telegram_response.status_code}")
                                    except Exception as telegram_err:
                                        print(f"[텔레그램 Polling] 오류 메시지 전송 중 예외 발생: {telegram_err}")
                                    print(f"[텔레그램 Polling] 장비 중지 오류: {e}")
                            
                            # Cancel 버튼 클릭 시
                            elif callback_data == "equipment_cancel" or callback_data == "cancel_down":
                                send_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
                                requests.post(send_url, json={
                                    "chat_id": chat_id,
                                    "text": "✅ 장비 Down이 취소되었습니다.",
                                    "parse_mode": "HTML"
                                }, timeout=5)
        except Exception as e:
            print(f"[텔레그램 Polling 오류] {e}")
            time.sleep(5)  # 오류 발생 시 5초 대기 후 재시도


def load_limits():
    """limits.yaml 파일을 읽어서 파라미터별 상한/하한값 정보를 반환"""
    limits_dict = {}
    if LIMITS_PATH.exists():
        try:
            with open(LIMITS_PATH, 'r', encoding='utf-8') as f:
                limits_data = yaml.safe_load(f) or {}
            for param, steps in limits_data.items():
                # 'all' 키의 값을 가져옴
                if 'all' in steps and isinstance(steps['all'], dict):
                    all_limits = steps['all']
                    limits_dict[param] = {
                        'upper': all_limits.get('max'),
                        'lower': all_limits.get('min')
                    }
                else:
                    limits_dict[param] = {
                        'upper': None,
                        'lower': None
                    }
        except Exception as e:
            print(f"[경고] limits.yaml 로드 실패: {e}")
    return limits_dict

def load_interlock_limits():
    """limits2.yaml 파일을 읽어서 파라미터별 Interlock 상한/하한값 정보를 반환"""
    limits_dict = {}
    if LIMITS2_PATH.exists():
        try:
            with open(LIMITS2_PATH, 'r', encoding='utf-8') as f:
                limits_data = yaml.safe_load(f) or {}
            for param, steps in limits_data.items():
                # 'all' 키의 값을 가져옴
                if 'all' in steps and isinstance(steps['all'], dict):
                    all_limits = steps['all']
                    limits_dict[param] = {
                        'upper': all_limits.get('max'),
                        'lower': all_limits.get('min')
                    }
                else:
                    limits_dict[param] = {
                        'upper': None,
                        'lower': None
                    }
        except Exception as e:
            print(f"[경고] limits2.yaml 로드 실패: {e}")
    return limits_dict

def ensure_abnormal_log_table(conn):
    with conn.cursor() as cur:
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
        conn.commit()


def weighted_violation_type():
    """
    이상감지마다 violation_type을 확률에 따라 지정
    1: 40% (0.0 ~ 0.4)
    2: 30% (0.4 ~ 0.7)
    3: 20% (0.7 ~ 0.9)
    4: 10% (0.9 ~ 1.0)
    """
    r = random.random()
    if r < 0.4:
        return 1  # 40%
    elif r < 0.7:
        return 2  # 30%
    elif r < 0.9:
        return 3  # 20%
    else:
        return 4  # 10%


class Monitor:
    def __init__(self):
        self.pool = SimpleConnectionPool(1, 5, **DB_CONF)
        self.state = defaultdict(dict)  # param -> event state
        self.last_ts = {}  # param -> last processed timestamp
        self.last_telegram_time = {}  # param -> last telegram notification time
        self.last_telegram_time_global = 0  # 전역 마지막 텔레그램 알림 시간
        self.telegram_cooldown_sec = 10  # 10초 쿨다운
        self.start_time = time.time()  # 프로세스 시작 시간
        self.last_process_pids = {}  # generate_data.py, insert_real_time.py의 마지막 PID 추적
        self.warning_enabled = load_warning_setting()  # 경고 팝업 토글 상태
        self.last_settings_check = time.time()  # 마지막 설정 파일 확인 시간
        self.settings_check_interval = 5.0  # 설정 파일 확인 간격 (5초)
        self.equipment_start_grace_period_sec = 10  # 장비 시작 후 10초 동안 이상감지/DB저장/메시지 발송 비활성화
        self.equipment_start_time = None  # 장비 시작 시간 (generator_health.json에서 읽음)
        self.limits = {}  # limits.yaml 데이터
        self.interlock_limits = {}  # limits2.yaml 데이터
        self.last_limits_check = time.time()  # 마지막 limits 파일 확인 시간
        self.limits_check_interval = 10.0  # limits 파일 확인 간격 (10초)
        os.makedirs("./log", exist_ok=True)
        conn = self.pool.getconn()
        try:
            ensure_abnormal_log_table(conn)
        finally:
            self.pool.putconn(conn)
        # 초기 limits 로드
        self.limits = load_limits()
        self.interlock_limits = load_interlock_limits()

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
        """장비 시작 후 10초 이내인지 확인"""
        if self.equipment_start_time is None:
            self.equipment_start_time = self.get_equipment_start_time()
        
        if self.equipment_start_time is None:
            return False
        
        current_time = time.time()
        time_since_equipment_start = current_time - self.equipment_start_time
        
        # 장비 시작 시간이 10초 이내이고, 프로세스 시작 시간보다 최근이면 그레이스 기간 적용
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
                    "limit_type": event.get("limit_type"),
                    "upper_value": _to_py(event.get("upper_value")),
                    "lower_value": _to_py(event.get("lower_value")),
                    "is_interrupted": event.get("is_interrupted"),
                    "violation_type": _to_py(event.get("violation_type")),
                    "step_id": sorted(event.get("step_ids", [])),
                    "step_name": sorted(event.get("step_names", [])),
                }
                message_text = json.dumps(message_payload, ensure_ascii=False)
                cur.execute(
                    """
                    INSERT INTO realtime_abnormal_log2 (
                        start_time, end_time, parameter,
                        duration_seconds, avg_diff_percent, max_diff_percent,
                        peak_time, actual_value, limit_type, upper_value, lower_value, is_interrupted, violation_type, message,
                        created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (parameter, start_time) DO UPDATE
                    SET end_time = EXCLUDED.end_time,
                        duration_seconds = EXCLUDED.duration_seconds,
                        avg_diff_percent = EXCLUDED.avg_diff_percent,
                        max_diff_percent = EXCLUDED.max_diff_percent,
                        peak_time = EXCLUDED.peak_time,
                        actual_value = EXCLUDED.actual_value,
                        limit_type = EXCLUDED.limit_type,
                        upper_value = EXCLUDED.upper_value,
                        lower_value = EXCLUDED.lower_value,
                        is_interrupted = EXCLUDED.is_interrupted,
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
                        event.get("limit_type"),
                        _to_py(event.get("upper_value")),
                        _to_py(event.get("lower_value")),
                        event.get("is_interrupted"),
                        _to_py(event.get("violation_type")),
                        message_text,
                    ),
                )
            conn.commit()
            
            # 장비 시작 후 10초 이내면 DB 저장과 메시지 발송 스킵
            if self.is_within_equipment_start_grace_period():
                self.log(f"[이상감지 스킵] 장비 시작 후 10초 그레이스 기간 중 (파라미터: {param})")
                return
            
            # 설정 파일에서 경고 팝업 토글 상태 확인 (주기적으로 업데이트)
            current_time = time.time()
            if current_time - self.last_settings_check >= self.settings_check_interval:
                self.warning_enabled = load_warning_setting()
                self.last_settings_check = current_time
            
            # 텔레그램 경고 메시지 전송 (경고 팝업 토글 ON일 때만, 30초 쿨다운 적용)
            last_notification_time_param = self.last_telegram_time.get(param, 0)
            last_notification_time_global = self.last_telegram_time_global
            
            # 쿨다운 체크: 
            # 1. 경고 팝업 토글: warning_enabled가 True일 때만 알림 전송
            # 2. is_interrupted: is_interrupted가 1일 때만 알림 전송
            # 3. 전역 쿨다운: 마지막 알림 후 30초 이내에는 어떤 파라미터든 알림을 보내지 않음
            # 4. 파라미터별 쿨다운: 같은 파라미터에 대해 30초 이내에는 알림을 보내지 않음
            time_since_global = current_time - last_notification_time_global
            time_since_param = current_time - last_notification_time_param
            is_interrupted = event.get("is_interrupted")
            
            if (self.warning_enabled and
                is_interrupted == 1 and
                time_since_global >= self.telegram_cooldown_sec and 
                time_since_param >= self.telegram_cooldown_sec):
                diff_pct = _to_py(event.get("avg_diff", 0))
                duration_sec = _to_py(event.get("duration", 0))
                start_time = event["start"].strftime("%Y-%m-%d %H:%M:%S")
                end_time = event["end"].strftime("%Y-%m-%d %H:%M:%S")
                
                warning_msg = f"""
⚠️ <b>이상 감지 알림</b>

파라미터: <b>{param}</b>
시작 시간: {start_time}
종료 시간: {end_time}
지속 시간: {duration_sec:.1f}초
편차율: {diff_pct:.2f}%
"""
                send_telegram_message(warning_msg)
                
                # 장비 Down 확인 메시지 전송
                reply_markup = {
                    "inline_keyboard": [
                        [
                            {"text": "장비 DOWN", "callback_data": "equipment_down"},
                            {"text": "Cancel", "callback_data": "equipment_cancel"}
                        ]
                    ]
                }
                confirm_msg = f"""
🔴 <b>장비 Down 확인</b>

파라미터 <b>{param}</b>에서 이상이 감지되었습니다.
장비를 Down 하시겠습니까?

⚠️ 주의: 장비 Down은 즉시 실행되며 되돌릴 수 없습니다.
"""
                send_telegram_message(confirm_msg, reply_markup)
                
                # 쿨다운 시간 업데이트 (전역 및 파라미터별)
                self.last_telegram_time[param] = current_time
                self.last_telegram_time_global = current_time
                self.log(f"[텔레그램 알림 전송] 파라미터: {param}, 전역 쿨다운: {time_since_global:.1f}초, 파라미터 쿨다운: {time_since_param:.1f}초")
            else:
                skip_reason = []
                if not self.warning_enabled:
                    skip_reason.append("경고 팝업 토글 OFF")
                if is_interrupted != 1:
                    skip_reason.append(f"is_interrupted={is_interrupted} (1이 아님)")
                if time_since_global < self.telegram_cooldown_sec:
                    skip_reason.append(f"전역 쿨다운 {time_since_global:.1f}초")
                if time_since_param < self.telegram_cooldown_sec:
                    skip_reason.append(f"파라미터 쿨다운 {time_since_param:.1f}초")
                self.log(f"[텔레그램 알림 스킵] 파라미터: {param}, 이유: {', '.join(skip_reason)}")
        finally:
            self.pool.putconn(conn)

    def finalize_event(self, param):
        state = self.state.get(param)
        if not state or not state.get("active"):
            return
        duration = (state["last_anomaly"] - state["start"]).total_seconds() + 1.0
        # limits2.yaml 침범이 있으면 1초, 없고 limits.yaml만 침범이면 3초
        min_duration = MIN_DURATION_SEC_INTERLOCK if state.get("is_interlock_violation", False) else MIN_DURATION_SEC_LIMITS
        if duration < min_duration:
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
            "limit_type": state.get("limit_type"),
            "upper_value": state.get("upper_value"),
            "lower_value": state.get("lower_value"),
            "is_interrupted": state.get("is_interrupted"),
            "violation_type": state.get("violation_type"),
            "step_ids": state.get("step_ids", set()),
            "step_names": state.get("step_names", set()),
        }
        self.upsert_event(param, event)
        self.state[param] = {}

    def handle_sample(self, param, ts, actual_val, pred_val, step_id=None, step_name=None):
        # 장비 시작 후 10초 이내면 이상감지 스킵
        if self.is_within_equipment_start_grace_period():
            return
        
        # limits 파일 주기적으로 다시 로드
        current_time = time.time()
        if current_time - self.last_limits_check >= self.limits_check_interval:
            self.limits = load_limits()
            self.interlock_limits = load_interlock_limits()
            self.last_limits_check = current_time
        
        state = self.state.setdefault(param, {})
        if actual_val is None:
            # missing actual value
            if state.get("active"):
                # check if should close
                if (ts - state["last_anomaly"]).total_seconds() >= CLEAR_GAP_SEC:
                    self.finalize_event(param)
            return

        # limits.yaml에서 해당 파라미터의 상한/하한값 가져오기
        param_limits = self.limits.get(param, {})
        upper_limit = param_limits.get('upper')
        lower_limit = param_limits.get('lower')
        
        # limits2.yaml에서 Interlock 상한/하한값 확인
        interlock_param_limits = self.interlock_limits.get(param, {})
        interlock_upper = interlock_param_limits.get('upper')
        interlock_lower = interlock_param_limits.get('lower')
        
        # limits.yaml 또는 limits2.yaml 중 하나라도 없으면 스킵
        if (upper_limit is None and lower_limit is None) and \
           (interlock_upper is None and interlock_lower is None):
            if state.get("active"):
                if (ts - state["last_anomaly"]).total_seconds() >= CLEAR_GAP_SEC:
                    self.finalize_event(param)
            return
        
        # limits.yaml 기준으로 상한/하한선 침범 여부 확인
        is_upper_violation = upper_limit is not None and actual_val > upper_limit
        is_lower_violation = lower_limit is not None and actual_val < lower_limit
        
        # limits2.yaml 기준으로 Interlock 상한/하한선 침범 여부 확인
        is_interlock_upper_violation = interlock_upper is not None and actual_val > interlock_upper
        is_interlock_lower_violation = interlock_lower is not None and actual_val < interlock_lower
        
        # limits.yaml 침범 여부
        is_limits_violation = is_upper_violation or is_lower_violation
        # limits2.yaml 침범 여부
        is_interlock_violation = is_interlock_upper_violation or is_interlock_lower_violation
        
        # limits.yaml 또는 limits2.yaml 중 하나라도 침범하면 이상감지
        is_anomaly = is_limits_violation or is_interlock_violation
        
        # limit_type 결정 (limits.yaml 기준)
        limit_type = None
        if is_upper_violation:
            limit_type = 'u'
        elif is_lower_violation:
            limit_type = 'l'
        elif is_interlock_upper_violation:
            limit_type = 'u'  # limits2.yaml만 침범한 경우도 상한으로 표시
        elif is_interlock_lower_violation:
            limit_type = 'l'  # limits2.yaml만 침범한 경우도 하한으로 표시
        
        # is_interrupted 결정 (limits2.yaml 기준)
        is_interrupted = 2  # 기본값: 2 (Interlock 침범 안함)
        if is_interlock_violation:
            is_interrupted = 1  # Interlock 침범
        
        # 실제값과 상한/하한값과의 차이 percentage 계산
        # limits.yaml이 있으면 limits.yaml 기준, 없으면 limits2.yaml 기준
        diff_pct = 0.0
        if is_upper_violation and upper_limit is not None:
            try:
                diff_pct = abs(actual_val - upper_limit) / (abs(upper_limit) or 1.0) * 100.0
            except Exception:
                diff_pct = 0.0
        elif is_lower_violation and lower_limit is not None:
            try:
                diff_pct = abs(actual_val - lower_limit) / (abs(lower_limit) or 1.0) * 100.0
            except Exception:
                diff_pct = 0.0
        elif is_interlock_upper_violation and interlock_upper is not None:
            # limits.yaml이 없고 limits2.yaml만 침범한 경우
            try:
                diff_pct = abs(actual_val - interlock_upper) / (abs(interlock_upper) or 1.0) * 100.0
            except Exception:
                diff_pct = 0.0
        elif is_interlock_lower_violation and interlock_lower is not None:
            # limits.yaml이 없고 limits2.yaml만 침범한 경우
            try:
                diff_pct = abs(actual_val - interlock_lower) / (abs(interlock_lower) or 1.0) * 100.0
            except Exception:
                diff_pct = 0.0
        
        if is_anomaly:
            # 저장할 상한/하한값 결정: limits.yaml이 있으면 limits.yaml 기준, 없으면 limits2.yaml 기준
            save_upper = upper_limit if upper_limit is not None else interlock_upper
            save_lower = lower_limit if lower_limit is not None else interlock_lower
            
            if not state.get("active"):
                state["active"] = True
                state["start"] = ts
                state["violation_type"] = weighted_violation_type()
                state["diff_sum"] = diff_pct
                state["diff_count"] = 1
                state["max_diff"] = diff_pct
                state["peak_time"] = ts
                state["peak_actual"] = actual_val
                state["limit_type"] = limit_type
                state["upper_value"] = save_upper
                state["lower_value"] = save_lower
                state["is_interrupted"] = is_interrupted
                state["is_limits_violation"] = is_limits_violation
                state["is_interlock_violation"] = is_interlock_violation
                state["step_ids"] = set()
                state["step_names"] = set()
            else:
                state["diff_sum"] += diff_pct
                state["diff_count"] += 1
                if diff_pct > state.get("max_diff", 0):
                    state["max_diff"] = diff_pct
                    state["peak_time"] = ts
                    state["peak_actual"] = actual_val
                    state["limit_type"] = limit_type
                    state["upper_value"] = save_upper
                    state["lower_value"] = save_lower
                # is_interrupted는 가장 최근 값으로 업데이트
                state["is_interrupted"] = is_interrupted
                # 침범 상태도 업데이트 (OR 연산으로 누적)
                state["is_limits_violation"] = state.get("is_limits_violation", False) or is_limits_violation
                state["is_interlock_violation"] = state.get("is_interlock_violation", False) or is_interlock_violation
            state["last_anomaly"] = ts
            if step_id is not None:
                state.setdefault("step_ids", set()).add(step_id)
            if step_name:
                state.setdefault("step_names", set()).add(step_name)

            duration = (state["last_anomaly"] - state["start"]).total_seconds() + 1.0
            # limits2.yaml 침범이 있으면 1초, 없고 limits.yaml만 침범이면 3초
            min_duration = MIN_DURATION_SEC_INTERLOCK if state.get("is_interlock_violation", False) else MIN_DURATION_SEC_LIMITS
            if duration >= min_duration:
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
                        "limit_type": state.get("limit_type"),
                        "upper_value": state.get("upper_value"),
                        "lower_value": state.get("lower_value"),
                        "is_interrupted": state.get("is_interrupted"),
                        "violation_type": state.get("violation_type"),
                        "step_ids": state.get("step_ids", set()),
                        "step_names": state.get("step_names", set()),
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
                if not actual_rows:
                    continue
                for ts, val, step_id, step_name in actual_rows:
                    self.last_ts[param] = ts
                    self.handle_sample(param, ts, val, None, step_id, step_name)
            time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    monitor = Monitor()
    try:
        monitor.run()
    except KeyboardInterrupt:
        monitor.log("Monitor stopped")
