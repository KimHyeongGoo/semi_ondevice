"""
시작시간과 끝시간을 입력받아 해당 기간의 원본 데이터를 불러와 예측을 수행하고
pred_proc0 ~ pred_proc4 테이블에 저장하는 스크립트

사용법:
    python batch_predict_by_time_range.py '2025-11-02 00:00:00' '2025-11-02 23:59:59'
"""
from datetime import datetime, timedelta
from dateutil import parser

from psycopg2.pool import SimpleConnectionPool

from batch_process_abnormal2 import (
    BatchPredictor,
    DB_CONF,
    PREDICT_STEPS,
    TASKS,
    WINDOW_SIZE,
    densify,
    fetch_data_from_table,
    insert_pred_rows,
)


def log(msg: str) -> None:
    """로그 메시지 출력"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}")


def get_table_names_for_date_range(start_time: datetime, end_time: datetime) -> list[str]:
    """시작시간과 끝시간 사이의 모든 rawdata 테이블 이름을 반환"""
    table_names = []
    current_date = start_time.date()
    end_date = end_time.date()
    
    while current_date <= end_date:
        table_name = f"rawdata{current_date.strftime('%Y%m%d')}"
        table_names.append(table_name)
        current_date += timedelta(days=1)
    
    return table_names




def process_time_range(start_time_str: str, end_time_str: str):
    """시작시간과 끝시간을 받아 해당 기간의 데이터를 예측하고 저장"""
    # 시간 파싱
    try:
        start_time = parser.parse(start_time_str)
        end_time = parser.parse(end_time_str)
    except Exception as e:
        log(f"시간 파싱 오류: {e}")
        return
    
    if start_time >= end_time:
        log("시작시간이 끝시간보다 늦거나 같습니다.")
        return
    
    log(f"예측 시작: {start_time} ~ {end_time}")
    
    # DB 연결 풀 생성
    pool = SimpleConnectionPool(1, 5, **DB_CONF)
    predictor = BatchPredictor(pool)
    
    # 해당 기간의 테이블 목록 가져오기
    table_names = get_table_names_for_date_range(start_time, end_time)
    log(f"처리할 테이블 수: {len(table_names)}")
    
    total_processed = 0
    
    # 각 테이블별로 처리
    for table_idx, table_name in enumerate(table_names, 1):
        log(f"[{table_idx}/{len(table_names)}] 테이블 처리 중: {table_name}")
        
        # 테이블별 시간 범위 설정
        table_date = datetime.strptime(table_name.replace("rawdata", ""), "%Y%m%d")
        table_start = table_date.replace(hour=0, minute=0, second=0, microsecond=0)
        table_end = table_date.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        # 전체 기간과 테이블 기간의 교집합
        query_start = max(start_time, table_start)
        query_end = min(end_time, table_end)
        
        # 데이터 불러오기
        df = fetch_data_from_table(pool, table_name, query_start, query_end)
        
        if df.empty:
            log(f"  {table_name}: 데이터 없음, 건너뜀")
            continue
        
        log(f"  {table_name}: {len(df)} 행 불러옴")
        
        # 데이터 densify (1초 간격으로 채움)
        dense_df = densify(df)
        log(f"  {table_name}: densify 후 {len(dense_df)} 행")
        
        if len(dense_df) < WINDOW_SIZE:
            log(f"  {table_name}: 데이터가 너무 적음 (최소 {WINDOW_SIZE} 행 필요), 건너뜀")
            continue
        
        # 예측 수행
        log(f"  {table_name}: 예측 수행 중...")
        pred_rows, _ = predictor.predict(dense_df)
        
        # 각 pred_proc 테이블에 저장
        for proc_idx in range(len(TASKS)):
            table_name_pred = f"pred_proc{proc_idx}"
            rows = pred_rows.get(table_name_pred, [])
            
            if not rows:
                continue
            
            col_names = predictor.pred_col_names[proc_idx]
            
            # insert_pred_rows 함수가 ON CONFLICT DO NOTHING을 사용하므로
            # 중복된 행은 자동으로 건너뜀
            log(f"  {table_name_pred}: {len(rows)} 행 저장 시도 중...")
            insert_pred_rows(pool, col_names, rows, table_name_pred)
            total_processed += len(rows)
        
        log(f"  {table_name}: 처리 완료")
    
    pool.closeall()
    log(f"전체 처리 완료: {total_processed} 행 저장 시도 완료 (중복된 행은 자동으로 건너뜀)")


def main():
    """메인 함수 - 시작시간과 끝시간을 입력받아 처리"""
    start_time_str = '2026-01-01 01:00:00'
    end_time_str = '2026-01-11 12:59:59'
    
    process_time_range(start_time_str, end_time_str)


if __name__ == "__main__":
    main()
