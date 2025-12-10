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
    # 2025-07-30 19:55:05	2025-07-30 23:59:31	rawdata20250730	rawdata20250730
    # 1) 시간 설정
    start_time = datetime(2025, 7, 30, 19, 55, 5)
    end_time   = datetime(2025, 7, 30, 23, 59, 31)

    # 2) 테이블 이름 (rawdataYYYYMMDD 형식 그대로 사용)
    start_table = "rawdata20250730"
    end_table   = "rawdata20250730"

    # 3) thickness 값 45개 (앞 9줄×5개 사용 예시)
    
    ''' 
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
    '''
    thicknesses = [
    105.45, 104.76, 105.44, 105.14, 104.95, 105.67, 104.55, 105.11, 104.63, 104.74,
    104.88, 104.69, 105.97, 105.71, 104.77, 104.96, 106.82, 104.79, 105.53, 105.64,
    105.27, 105.63, 104.61, 105.99, 106.79, 106.54, 106.98, 106.32, 106.18, 105.77,
    105.56, 106.27, 106.66, 106.37, 105.37, 106.13, 106.72, 105.76, 104.99, 104.67,
    106.78, 105.66, 106.99, 105.19, 105.21
    ]
    '''  
    thicknesses = [
    91.32, 91.09, 92.79, 92.7, 91.16, 90.9, 92.24, 92.01, 92.55, 90.63,
    91.64, 91.54, 91.28, 92.79, 90.87, 90.8, 92.89, 92.3, 92.85, 90.55,
    91.06, 92.42, 92.01, 92.51, 91.07, 90.97, 91.93, 92.73, 92.85, 90.64,
    90.83, 92.24, 92.62, 92.43, 90.5, 91.33, 92.34, 92.66, 92.15, 90.77,
    91.48, 91.86, 92.7, 91.97, 91.07
    ]  
    '''   

    # 4) 실제 INSERT 실행
    insert_trace_info_with_thickness(start_time, end_time, start_table, end_table, thicknesses)
    print("✅ 수동 thickness 데이터 1건 INSERT 완료")