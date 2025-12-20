import psycopg2
import os
import glob
import pandas as pd
from datetime import datetime
from pathlib import Path

def get_db_connection():
    """PostgreSQL 데이터베이스 연결"""
    return psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432
    )

def create_equipment_history_table():
    """equipment_history 테이블 생성 (이미 존재하면 스킵)"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS equipment_history (
                ProcessStartTime TIMESTAMP PRIMARY KEY,
                PJOB_ID TEXT NOT NULL,
                ProcessRecipe TEXT,
                ProcessEndTime TIMESTAMP NOT NULL,
                EndStatus TEXT NOT NULL
            );
        """)
        
        # 기존 테이블에 ProcessRecipe 컬럼이 없으면 추가
        cur.execute("""
            DO $$ 
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'equipment_history' 
                    AND column_name = 'processrecipe'
                ) THEN
                    ALTER TABLE equipment_history ADD COLUMN ProcessRecipe TEXT;
                END IF;
            END $$;
        """)
        
        conn.commit()
        print("✅ equipment_history 테이블 생성 완료 (또는 이미 존재)")
    except Exception as e:
        print(f"❌ 테이블 생성 실패: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def parse_datetime(date_str, time_str):
    """Date(mm/dd/yyyy)와 Time(hh:mm:ss.0) 문자열을 TIMESTAMP로 변환"""
    try:
        # Time에서 .0 제거
        time_clean = time_str.split('.')[0] if '.' in time_str else time_str
        # Date와 Time을 결합하여 파싱
        datetime_str = f"{date_str} {time_clean}"
        return datetime.strptime(datetime_str, "%m/%d/%Y %H:%M:%S")
    except Exception as e:
        print(f"⚠️ 날짜 파싱 실패: {date_str} {time_str} → {e}")
        return None

def get_recipe_name(csv_path):
    """CSV 파일의 2번째 줄에서 Recipe Name 추출"""
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            # 2번째 줄은 인덱스 1
            if len(lines) < 2:
                return None
            
            recipe_line = lines[1].strip()
            # "Recipe Name:" 다음의 텍스트 추출
            if "Recipe Name:" in recipe_line:
                recipe_part = recipe_line.split("Recipe Name:")[1].strip()
                # 쉼표 제거
                recipe_part = recipe_part.rstrip(',').strip()
                return recipe_part
            return None
    except Exception as e:
        print(f"⚠️ Recipe Name 추출 실패: {csv_path} → {e}")
        return None

def get_first_data_line(csv_path):
    """CSV 파일의 첫 번째 데이터 라인(Date, Time) 반환"""
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            # 헤더는 5번째 라인(인덱스 4), 첫 번째 데이터는 6번째 라인(인덱스 5)
            if len(lines) < 6:
                return None, None
            
            data_line = lines[5].strip()
            parts = data_line.split(',')
            
            if len(parts) < 6:
                return None, None
            
            date = parts[4].strip()
            time = parts[5].strip()
            
            return date, time
    except Exception as e:
        print(f"⚠️ CSV 파일 읽기 실패: {csv_path} → {e}")
        return None, None

def get_csv_files_sorted(folder_path):
    """폴더 내 CSV 파일들을 정렬하여 반환"""
    csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
    csv_files.sort()  # 파일명으로 정렬
    return csv_files

def process_pjob_folder(pjob_folder_path):
    """각 KE-PJ000000000XX 폴더를 처리하여 데이터 추출"""
    pjob_id = os.path.basename(pjob_folder_path)
    
    # CSV 파일 목록 가져오기
    csv_files = get_csv_files_sorted(pjob_folder_path)
    
    if not csv_files:
        print(f"⚠️ {pjob_id}: CSV 파일이 없습니다.")
        return None
    
    # 첫 번째 CSV 파일에서 Recipe Name 추출
    first_csv = csv_files[0]
    process_recipe = get_recipe_name(first_csv)
    
    # 첫 번째 CSV 파일의 첫 번째 데이터 라인
    start_date, start_time = get_first_data_line(first_csv)
    
    if not start_date or not start_time:
        print(f"⚠️ {pjob_id}: 첫 번째 CSV 파일에서 데이터를 읽을 수 없습니다.")
        return None
    
    # 마지막 CSV 파일의 첫 번째 데이터 라인
    last_csv = csv_files[-1]
    end_date, end_time = get_first_data_line(last_csv)
    
    if not end_date or not end_time:
        print(f"⚠️ {pjob_id}: 마지막 CSV 파일에서 데이터를 읽을 수 없습니다.")
        return None
    
    # 날짜/시간 파싱
    process_start_time = parse_datetime(start_date, start_time)
    process_end_time = parse_datetime(end_date, end_time)
    
    if not process_start_time or not process_end_time:
        print(f"⚠️ {pjob_id}: 날짜/시간 파싱 실패")
        return None
    
    return {
        'PJOB_ID': pjob_id,
        'ProcessRecipe': process_recipe,
        'ProcessStartTime': process_start_time,
        'ProcessEndTime': process_end_time,
        'EndStatus': 'NORMAL END'
    }

def insert_equipment_history_data():
    """equipment_history 테이블에 데이터 삽입"""
    base_path = "/home/goo4168/semi_platform/traceData/2025/11"
    
    if not os.path.exists(base_path):
        print(f"❌ 경로가 존재하지 않습니다: {base_path}")
        return
    
    # KE-PJ000000000XX 형식의 폴더 찾기
    pjob_folders = []
    for item in os.listdir(base_path):
        item_path = os.path.join(base_path, item)
        if os.path.isdir(item_path) and item.startswith("KE-PJ"):
            pjob_folders.append(item_path)
    
    pjob_folders.sort()  # 폴더명으로 정렬
    
    print(f"📁 발견된 폴더 수: {len(pjob_folders)}")
    
    # 데이터 수집
    data_list = []
    for folder_path in pjob_folders:
        data = process_pjob_folder(folder_path)
        if data:
            data_list.append(data)
            print(f"✅ {data['PJOB_ID']}: {data['ProcessStartTime']} ~ {data['ProcessEndTime']}")
    
    if not data_list:
        print("❌ 삽입할 데이터가 없습니다.")
        return
    
    # 데이터베이스에 삽입
    conn = get_db_connection()
    cur = conn.cursor()
    
    inserted_count = 0
    skipped_count = 0
    
    for data in data_list:
        try:
            cur.execute("""
                INSERT INTO equipment_history 
                (PJOB_ID, ProcessRecipe, ProcessStartTime, ProcessEndTime, EndStatus)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (ProcessStartTime) DO NOTHING
            """, (
                data['PJOB_ID'],
                data['ProcessRecipe'],
                data['ProcessStartTime'],
                data['ProcessEndTime'],
                data['EndStatus']
            ))
            
            if cur.rowcount > 0:
                inserted_count += 1
            else:
                skipped_count += 1
                print(f"⏭️  {data['PJOB_ID']}: 이미 존재하는 ProcessStartTime으로 스킵됨")
                
        except Exception as e:
            print(f"❌ {data['PJOB_ID']} 삽입 실패: {e}")
            conn.rollback()
    
    conn.commit()
    cur.close()
    conn.close()
    
    print(f"\n📊 결과:")
    print(f"  - 새로 삽입된 레코드: {inserted_count}개")
    print(f"  - 스킵된 레코드: {skipped_count}개")
    print(f"  - 총 처리된 레코드: {len(data_list)}개")

if __name__ == '__main__':
    print("🚀 equipment_history 테이블 데이터 삽입 시작\n")
    
    # 테이블 생성
    create_equipment_history_table()
    
    # 데이터 삽입
    insert_equipment_history_data()
    
    print("\n✅ 완료!")

