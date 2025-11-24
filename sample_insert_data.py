from datetime import datetime
import psycopg2


def insert_trace_info_with_thickness(start_time, end_time, start_table, end_table, thicknesses):
    assert len(thicknesses) == 45, "thicknesses must contain exactly 45 values"
    
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432,
    )
    cur = conn.cursor()
    # 컬럼명 동적 생성
    thickness_cols = [f"thickness_{i+1}" for i in range(45)]

    # 전체 컬럼
    columns = ["start_time", "end_time", "start_table", "end_table"] + thickness_cols
    placeholders = ', '.join(['%s'] * len(columns))
    colnames = ', '.join(columns)

    sql = f"""
        INSERT INTO trace_info ({colnames})
        VALUES ({placeholders})
        ON CONFLICT (start_time) DO NOTHING;
    """
    values = [start_time, end_time, start_table, end_table] + thicknesses
    cur.execute(sql, values)
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    # 1) 시간 설정
    start_time = datetime(2025, 11, 26, 10, 5, 25)
    end_time   = datetime(2025, 11, 26, 14, 21, 7)

    # 2) 테이블 이름 (rawdataYYYYMMDD 형식 그대로 사용)
    start_table = "rawdata20251126"
    end_table   = "rawdata20251126"

    # 3) thickness 값 45개 (앞 9줄×5개 사용 예시)
    thicknesses = [
        87.9,	88.6,	88.4,	88.3,	88,
        88.1,	88.7,	88.6,	88.5,	88.5,
        88.3,	88.9,	88.8,	88.6,	88.3,
        88.5,	89.1,	89.1,	88.8,	88.6,
        88.1,	88.8,	88.7,	88.5,	88.3,
        88.9,	89.4,	89.3,	89.1,	88.9,
        89.1,	89.6,	89.5,	89.3,	88.4,
        89.6,	90.1,	90.5,	89.8,	89,
        88.3,	88.8,	88.9,	88.7,	88

    ]

    # 4) 실제 INSERT 실행
    insert_trace_info_with_thickness(start_time, end_time, start_table, end_table, thicknesses)
    print("✅ 수동 thickness 데이터 1건 INSERT 완료")