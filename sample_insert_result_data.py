from datetime import datetime
import psycopg2
import csv
import os
import glob


def get_db_connection():
    """DB 연결 반환"""
    return psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432,
    )


def ensure_trace_info_table():
    """trace_info 테이블이 없으면 생성"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # thickness_1 ~ thickness_45까지 REAL 컬럼 추가
        thickness_cols_sql = ',\n    '.join([f'thickness_{i+1} REAL' for i in range(45)])

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS trace_info (
                start_time TIMESTAMP PRIMARY KEY,
                end_time TIMESTAMP,
                start_table TEXT,
                end_table TEXT,
                {thickness_cols_sql}
            );
        """)
        conn.commit()
        print("✅ trace_info 테이블 확인/생성 완료")
    except Exception as e:
        print(f"❌ trace_info 테이블 생성 실패: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


def get_equipment_history_times():
    """equipment_history 테이블에서 ProcessStartTime, ProcessEndTime을 순서대로 가져오기"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT ProcessStartTime, ProcessEndTime
            FROM equipment_history
            ORDER BY ProcessStartTime ASC
        """)
        rows = cur.fetchall()
        return [(row[0], row[1]) for row in rows]
    except Exception as e:
        print(f"❌ equipment_history 조회 실패: {e}")
        return []
    finally:
        cur.close()
        conn.close()


def read_csv_thickness(csv_path):
    """CSV 파일에서 thickness 값 45개 추출 (9줄 × 5컬럼: U, CU, C, CL, L)"""
    thicknesses = []
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            data_rows = []
            
            for row in reader:
                # x, y가 모두 비어있거나 빈 문자열이 아닌 행만 처리 (실제 데이터 행)
                if row.get('x', '').strip() and row.get('y', '').strip():
                    data_rows.append(row)
            
            # 처음 9줄만 사용
            if len(data_rows) < 9:
                print(f"⚠️  {csv_path}: 데이터 행이 9줄 미만입니다 ({len(data_rows)}줄)")
                return None
            
            # 9줄에서 U, CU, C, CL, L 값을 순서대로 추출
            for i in range(9):
                row = data_rows[i]
                thicknesses.append(float(row.get('U', 0) or 0))
                thicknesses.append(float(row.get('CU', 0) or 0))
                thicknesses.append(float(row.get('C', 0) or 0))
                thicknesses.append(float(row.get('CL', 0) or 0))
                thicknesses.append(float(row.get('L', 0) or 0))
            
            if len(thicknesses) != 45:
                print(f"⚠️  {csv_path}: thickness 값이 45개가 아닙니다 ({len(thicknesses)}개)")
                return None
            
            return thicknesses
            
    except Exception as e:
        print(f"❌ CSV 파일 읽기 실패 ({csv_path}): {e}")
        return None


def insert_trace_info_with_thickness(start_time, end_time, thicknesses):
    """trace_info 테이블에 데이터 삽입"""
    assert len(thicknesses) == 45, "thicknesses must contain exactly 45 values"
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # 컬럼명 동적 생성
        thickness_cols = [f"thickness_{i+1}" for i in range(45)]

        # 전체 컬럼 (start_table, end_table은 NULL로 설정)
        columns = ["start_time", "end_time", "start_table", "end_table"] + thickness_cols
        placeholders = ', '.join(['%s'] * len(columns))
        colnames = ', '.join(columns)

        sql = f"""
            INSERT INTO trace_info ({colnames})
            VALUES ({placeholders})
            ON CONFLICT (start_time) DO NOTHING;
        """
        values = [start_time, end_time, None, None] + thicknesses
        cur.execute(sql, values)
        conn.commit()
        return True
    except Exception as e:
        print(f"❌ INSERT 실패: {e}")
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()


def process_csv_files(csv_dir_path):
    """특정 경로의 CSV 파일 목록을 읽어서 trace_info 테이블에 저장"""
    # trace_info 테이블 확인/생성
    ensure_trace_info_table()
    
    # CSV 파일 목록 가져오기
    csv_pattern = os.path.join(csv_dir_path, "*.csv")
    csv_files = sorted(glob.glob(csv_pattern))
    if not csv_files:
        print(f"❌ {csv_dir_path} 경로에 CSV 파일이 없습니다.")
        return
    
    print(f"📂 발견된 CSV 파일: {len(csv_files)}개")
    
    # equipment_history에서 시간 정보 가져오기
    time_pairs = get_equipment_history_times()
    
    if not time_pairs:
        print("❌ equipment_history 테이블에서 시간 정보를 가져올 수 없습니다.")
        return
    
    if len(time_pairs) < len(csv_files):
        print(f"⚠️  CSV 파일({len(csv_files)}개)보다 equipment_history 레코드({len(time_pairs)}개)가 적습니다.")
        print(f"   처음 {len(time_pairs)}개 CSV 파일만 처리합니다.")
    
    # 각 CSV 파일 처리
    success_count = 0
    skip_count = 0
    
    for idx, csv_file in enumerate(csv_files):
        if idx >= len(time_pairs):
            print(f"⏭️  {os.path.basename(csv_file)}: 시간 정보 부족으로 스킵")
            skip_count += 1
            continue
        
        print(f"\n📄 처리 중: {os.path.basename(csv_file)}")
        
        # CSV에서 thickness 값 추출
        thicknesses = read_csv_thickness(csv_file)
        
        if thicknesses is None:
            print(f"   ❌ thickness 값 추출 실패")
            skip_count += 1
            continue
        
        # equipment_history에서 시간 정보 가져오기
        start_time, end_time = time_pairs[idx]
        
        # trace_info에 삽입
        if insert_trace_info_with_thickness(start_time, end_time, thicknesses):
            print(f"   ✅ 저장 완료: {start_time} ~ {end_time}")
            success_count += 1
        else:
            print(f"   ⏭️  이미 존재하는 start_time이거나 저장 실패")
            skip_count += 1
    
    print(f"\n📊 처리 결과:")
    print(f"   ✅ 성공: {success_count}개")
    print(f"   ⏭️  스킵/실패: {skip_count}개")
    print(f"   📁 전체: {len(csv_files)}개")


if __name__ == "__main__":
    # CSV 파일이 있는 경로 설정 (필요에 따라 수정)
    csv_directory = '/data2/kcl_2025/data/surplus_result'
    
    if not csv_directory:
        print("❌ 경로가 입력되지 않았습니다.")
    elif not os.path.isdir(csv_directory):
        print(f"❌ 경로가 존재하지 않습니다: {csv_directory}")
    else:
        process_csv_files(csv_directory)