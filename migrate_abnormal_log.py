#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
realtime_abnormal_log2 테이블의 모든 데이터를 realtime_abnormal_log 테이블로 마이그레이션하는 스크립트

- predicted_value: start_time과 end_time 사이의 예측값 평균 (없으면 NULL)
- avg_diff_percent, max_diff_percent: realtime_abnormal_log2의 값 그대로 사용
"""

import psycopg2
from datetime import datetime
from zoneinfo import ZoneInfo

# 데이터베이스 연결 정보
DB_CONF = {
    "dbname": "postgres",
    "user": "keti",
    "password": "keti1234!",
    "host": "localhost",
    "port": 5432,
}

# 예측 테이블 매핑 (abnormal_monitor2.py와 동일)
TASKS = [
    ['MFC7_DCS', 'MFC8_NH3', 'MFC26_F.PWR'],
    ['MFC1_N2-1', 'MFC2_N2-2', 'MFC3_N2-3'],
    ['MFC4_N2-4', 'MFC27_L.POS', 'MFC28_R.POS'],
    ['VG11 Press value', 'VG12 Press value', 'VG13 Press value'],
    ['Temp_Act_U', 'Temp_Act_CU', 'Temp_Act_C', 'Temp_Act_CL', 'Temp_Act_L'],
]

PARAM_TABLE_MAP = {}
for idx, cols in enumerate(TASKS):
    for col in cols:
        PARAM_TABLE_MAP[col] = f"pred_proc{idx}"

PREDICT_STEP = 10
KST = ZoneInfo("Asia/Seoul")


def get_predicted_values(conn, param, start_time, end_time):
    """
    start_time과 end_time 사이의 예측값들을 (timestamp, value) 튜플 리스트로 반환
    """
    pred_table = PARAM_TABLE_MAP.get(param)
    if not pred_table:
        return []
    
    pred_col = param.replace(' ', '_').replace('.', '_').replace('-', '_')
    
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT "Timestamp", "{pred_col}"
                FROM "{pred_table}"
                WHERE "PredictStep" = %s 
                  AND "Timestamp" >= %s 
                  AND "Timestamp" <= %s
                ORDER BY "Timestamp" ASC
                """,
                (PREDICT_STEP, start_time, end_time)
            )
            rows = cur.fetchall()
            # None이 아닌 값만 필터링
            values = [(row[0], row[1]) for row in rows if row[1] is not None]
            return values
    except Exception as e:
        print(f"[경고] 예측값 조회 실패 param={param}, table={pred_table}, start={start_time}, end={end_time}: {e}")
        return []


def get_actual_values(conn, param, start_time, end_time):
    """
    start_time과 end_time 사이의 실제값들을 (timestamp, value) 튜플 리스트로 반환
    """
    tz_now = datetime.now(KST)
    raw_table = f"rawdata{tz_now.strftime('%Y%m%d')}"
    
    # start_time의 날짜로 rawdata 테이블 결정
    date_suffix = start_time.strftime('%Y%m%d')
    raw_table = f"rawdata{date_suffix}"
    
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT DATE_TRUNC('second', "Timestamp") AS ts, "{param}"
                FROM "{raw_table}"
                WHERE "Timestamp" >= %s 
                  AND "Timestamp" <= %s
                ORDER BY "Timestamp" ASC
                """,
                (start_time, end_time)
            )
            rows = cur.fetchall()
            # None이 아닌 값만 필터링
            values = [(row[0], row[1]) for row in rows if row[1] is not None]
            return values
    except Exception as e:
        # 날짜별로 테이블이 다를 수 있으므로, 여러 날짜 시도
        try:
            # end_time의 날짜로도 시도
            date_suffix = end_time.strftime('%Y%m%d')
            raw_table = f"rawdata{date_suffix}"
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT DATE_TRUNC('second', "Timestamp") AS ts, "{param}"
                    FROM "{raw_table}"
                    WHERE "Timestamp" >= %s 
                      AND "Timestamp" <= %s
                    ORDER BY "Timestamp" ASC
                    """,
                    (start_time, end_time)
                )
                rows = cur.fetchall()
                values = [(row[0], row[1]) for row in rows if row[1] is not None]
                return values
        except Exception as e2:
            print(f"[경고] 실제값 조회 실패 param={param}, start={start_time}, end={end_time}: {e2}")
            return []


def calculate_diff_percent(actual_value, predicted_value):
    """
    실제값과 예측값의 차이를 %로 계산
    """
    if actual_value is None or predicted_value is None:
        return None
    
    try:
        if abs(actual_value) < 1e-10:  # 0에 가까운 경우
            if abs(predicted_value) < 1e-10:
                return 0.0
            return float('inf')  # 무한대
        diff_percent = abs(actual_value - predicted_value) / abs(actual_value) * 100.0
        return diff_percent
    except (ZeroDivisionError, TypeError, ValueError):
        return None


def migrate_data():
    """
    realtime_abnormal_log2의 모든 데이터를 realtime_abnormal_log로 마이그레이션
    """
    conn = psycopg2.connect(**DB_CONF)
    
    try:
        with conn.cursor() as cur:
            # realtime_abnormal_log2의 모든 데이터 조회
            cur.execute("""
                SELECT 
                    id, start_time, end_time, parameter,
                    duration_seconds, avg_diff_percent, max_diff_percent,
                    peak_time, actual_value,
                    violation_type, message, created_at, updated_at
                FROM realtime_abnormal_log2
                ORDER BY start_time ASC
            """)
            
            rows = cur.fetchall()
            total_rows = len(rows)
            print(f"총 {total_rows}개의 행을 마이그레이션합니다.")
            
            migrated_count = 0
            skipped_count = 0
            error_count = 0
            
            for idx, row in enumerate(rows, 1):
                (id, start_time, end_time, parameter,
                 duration_seconds, avg_diff_percent, max_diff_percent,
                 peak_time, actual_value,
                 violation_type, message, created_at, updated_at) = row
                
                try:
                    # 예측값 조회 (timestamp, value) 튜플 리스트
                    predicted_data = get_predicted_values(conn, parameter, start_time, end_time)
                    
                    # 예측값 평균 계산 (없으면 NULL)
                    if predicted_data:
                        predicted_values = [val for _, val in predicted_data]
                        predicted_value = sum(predicted_values) / len(predicted_values)
                    else:
                        predicted_value = None
                    
                    # avg_diff_percent, max_diff_percent는 realtime_abnormal_log2의 값 그대로 사용
                    
                    # realtime_abnormal_log에 삽입 (이미 존재하는 경우 건너뛰기)
                    cur.execute("""
                        INSERT INTO realtime_abnormal_log (
                            start_time, end_time, parameter,
                            duration_seconds, avg_diff_percent, max_diff_percent,
                            peak_time, actual_value, predicted_value,
                            violation_type, message, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (parameter, start_time) DO NOTHING
                    """, (
                        start_time, end_time, parameter,
                        duration_seconds, avg_diff_percent, max_diff_percent,
                        peak_time, actual_value, predicted_value,
                        violation_type, message, created_at, updated_at
                    ))
                    
                    # 실제로 INSERT된 경우에만 카운트 증가 (0이면 이미 존재하여 건너뛴 경우)
                    if cur.rowcount > 0:
                        migrated_count += 1
                    else:
                        skipped_count += 1
                    
                    if idx % 100 == 0:
                        print(f"[{idx}/{total_rows}] 진행 중... (마이그레이션: {migrated_count}, 건너뜀: {skipped_count}, 오류: {error_count})")
                    
                except Exception as e:
                    error_count += 1
                    print(f"[{idx}/{total_rows}] 오류 발생: {parameter} ({start_time} ~ {end_time}) - {e}")
                    continue
            
            conn.commit()
            print(f"\n마이그레이션 완료!")
            print(f"  - 총 행 수: {total_rows}")
            print(f"  - 마이그레이션 성공: {migrated_count}")
            print(f"  - 건너뜀 (이미 존재): {skipped_count}")
            print(f"  - 오류: {error_count}")
            
    except Exception as e:
        conn.rollback()
        print(f"마이그레이션 중 오류 발생: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    print("realtime_abnormal_log2 -> realtime_abnormal_log 마이그레이션 시작...")
    migrate_data()
    print("마이그레이션 스크립트 종료.")

