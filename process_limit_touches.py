"""
realtime_abnormal_log 테이블의 데이터를 분석하여
상한값 또는 하한값을 터치한 경우를 limit_touch_log 테이블에 저장하는 스크립트
"""
import psycopg2
import yaml
import os
from pathlib import Path

# DB 연결 설정
DB_CONF = {
    "dbname": "postgres",
    "user": "keti",
    "password": "keti1234!",
    "host": "localhost",
    "port": 5432,
}

# limits.yaml 파일 경로
LIMITS_YAML_PATH = Path(__file__).parent / 'fastapi'  / "limits.yaml"


def load_limits():
    """limits.yaml 파일을 읽어서 파라미터별 상한/하한값 정보를 반환"""
    with open(LIMITS_YAML_PATH, 'r', encoding='utf-8') as f:
        limits_data = yaml.safe_load(f)
    
    limits_dict = {}
    for param, steps in limits_data.items():
        # 'all' 키의 값을 가져옴
        if 'all' in steps and isinstance(steps['all'], dict):
            all_limits = steps['all']
            limits_dict[param] = {
                'upper': all_limits.get('max'),
                'lower': all_limits.get('min')
            }
        else:
            # 'all'에 값이 없으면 None으로 설정
            limits_dict[param] = {
                'upper': None,
                'lower': None
            }
    
    return limits_dict


def ensure_limit_touch_log_table(cur):
    """limit_touch_log 테이블이 없으면 생성"""
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS limit_touch_log (
            id SERIAL PRIMARY KEY,
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP NOT NULL,
            parameter TEXT NOT NULL,
            limit_type TEXT NOT NULL,
            actual_value DOUBLE PRECISION NOT NULL,
            upper_value DOUBLE PRECISION,
            lower_value DOUBLE PRECISION,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """
    )


def process_limit_touches():
    """realtime_abnormal_log의 모든 행을 분석하여 limit_touch_log에 저장"""
    # limits.yaml 로드
    limits = load_limits()
    print(f"로드된 파라미터 수: {len(limits)}")
    
    # DB 연결
    conn = psycopg2.connect(**DB_CONF)
    cur = conn.cursor()
    
    try:
        # limit_touch_log 테이블 생성
        ensure_limit_touch_log_table(cur)
        conn.commit()
        
        # realtime_abnormal_log의 모든 행 가져오기
        cur.execute(
            """
            SELECT start_time, end_time, parameter, actual_value
            FROM realtime_abnormal_log
            WHERE actual_value IS NOT NULL
            ORDER BY start_time
            """
        )
        
        rows = cur.fetchall()
        print(f"처리할 행 수: {len(rows)}")
        
        inserted_count = 0
        
        for start_time, end_time, parameter, actual_value in rows:
            if parameter not in limits:
                # limits.yaml에 해당 파라미터가 없으면 스킵
                continue
            
            param_limits = limits[parameter]
            upper_value = param_limits['upper']
            lower_value = param_limits['lower']
            
            limit_type = None
            
            # 상한값 체크 (actual_value >= upper_value)
            if upper_value is not None and actual_value >= upper_value:
                limit_type = 'u'
            
            # 하한값 체크 (actual_value <= lower_value)
            if lower_value is not None and actual_value <= lower_value:
                # 상한과 하한 둘 다 터치한 경우, 상한을 우선
                if limit_type is None:
                    limit_type = 'l'
            
            # limit_type이 설정되었으면 (상한 또는 하한 터치) 테이블에 저장
            if limit_type:
                cur.execute(
                    """
                    INSERT INTO limit_touch_log 
                    (start_time, end_time, parameter, limit_type, actual_value, upper_value, lower_value)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (start_time, end_time, parameter, limit_type, actual_value, upper_value, lower_value)
                )
                inserted_count += 1
        
        conn.commit()
        print(f"저장된 행 수: {inserted_count}")
        
    except Exception as e:
        conn.rollback()
        print(f"오류 발생: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    process_limit_touches()
    print("처리 완료!")

