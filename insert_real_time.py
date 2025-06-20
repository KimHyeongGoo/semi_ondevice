import time
import os
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver
from watchdog.events import FileSystemEventHandler
import psycopg2
import csv
from datetime import datetime, timedelta
import re
from zoneinfo import ZoneInfo

from insert_old_data import insert_missing_data


# 파일별로 마지막 읽은 위치 저장
file_offsets = [0]
# CSV 파일의 헤더 보관
file_headers = {}

columns = ['Timestamp', 'ObservableTimestamp', 'EquipmentStatus', 'AlarmState', 'O2Density_Monitor_Value', 'O2Density_Set_Value', 'PMstatus', 'PPExecname', 'PPExecStepSeqNo', 'PPExecStepID', 'PPExecStepName', 'ActiveCjobID', 'ActivePjobID', 'PMStoredProcessRecipeName', 'ProcessRecipeEndRemainTime', 'ProcessRecipeStepTime', 'ProcessRecipeStepRemainTime', 'ProcessRecipeStepID', 'ProcessRecipeStepName', 'ProcessRecipeStepSeqNo', 'ProcessRecipeTotalTime', 'Temp_Set_U', 'Temp_HT_Power_U', 'Temp_Monitor_U', 'Temp_TC_Monitor_U', 'Temp_TC_Cascade_U ', 'Temp_Act_U', 'Temp_HT_Power_Cascade_U', 'Temp_Set_CU', 'Temp_HT_Power_CU', 'Temp_Monitor_CU', 'Temp_TC_Monitor_CU', 'Temp_TC_Cascade_CU', 'Temp_Act_CU', 'Temp_HT_Power_Cascade_CU', 'Temp_Set_C', 'Temp_HT_Power_C', 'Temp_Monitor_C', 'Temp_TC_Monitor_C', 'Temp_TC_Cascade_C', 'Temp_Act_C', 'Temp_HT_Power_Cascade_C', 'Temp_Set_CL', 'Temp_HT_Power_CL', 'Temp_Monitor_CL', 'Temp_TC_Monitor_CL', 'Temp_TC_Cascade_CL', 'Temp_Act_CL', 'Temp_HT_Power_Cascade_CL', 'Temp_Set_L', 'Temp_HT_Power_L', 'Temp_Monitor_L', 'Temp_TC_Monitor_L', 'Temp_TC_Cascade_L', 'Temp_Act_L', 'Temp_HT_Power_Cascade_L', 'APC Valve Value (Angle)', 'VG13_LeakPressure_Monitor', 'VG11_LeakPressure_Monitor', 'VG13_LeakQuantity_Monitor', 'VG11_LeakQuantity_Monitor', 'VG13 Press value', 'VG11 Press value', 'PJobProcessingState', 'ValveAct_1:1', 'ValveAct_2:2', 'ValveAct_3:3', 'ValveAct_4:4', 'ValveAct_5:5', 'ValveAct_9:9', 'ValveAct_11:11', 'ValveAct_12:12', 'ValveAct_14:14', 'ValveAct_15:15', 'ValveAct_16:16', 'ValveAct_26:26', 'ValveAct_28:28', 'ValveAct_29:29', 'ValveAct_30:30', 'ValveAct_60:71', 'ValveAct_63:75', 'ValveAct_73:83', 'ValveAct_75:85', 'ValveAct_76:86', 'ValveAct_80:DPO', 'ValveAct_86:HT1', 'ValveAct_87:HT2', 'ValveAct_88:HT3', 'ValveAct_89:RF', 'ValveAct_90:PST', 'ValveAct_95:WAT', 'SubRecipeLoopSettingValue', 'SubRecipeLoopMoniterValue', 'VG12_LeakPressure_Monitor', 'VG12 Press value', 'MFC1_N2-1', 'MFC2_N2-2', 'MFC3_N2-3', 'MFC4_N2-4', 'MFC7_DCS', 'MFC8_NH3', 'MFC9_F2', 'MFC10_N2-R', 'MFC11_NO', 'MFC12_DCSMFM_7', 'MFC16_MFC51 N2', 'MFC26_F.PWR', 'MFC27_L.POS', 'MFC28_R.POS', 'AUX1_MS1', 'AUX2_MS321', 'AUX3_MS2', 'AUX4_MS3', 'AUX5_MS5', 'AUX8_MS8', 'AUX9_MS9', 'AUX16_VG21', 'AUX18_VG12', 'AUX19_VG11', 'AUX20_VG13', 'AUX21_M.WAT', 'AUX22_FS101', 'AUX23_FS102', 'AUX24_FS104', 'AUX26_FS106', 'AUX28_FS111', 'AUX29_FS105', 'AUX33_G.PS1', 'AUX34_G.PS2', 'AUX35_G.PS3', 'AUX36_G.PS4', 'AUX37_G.PS5', 'AUX38_G.PS6', 'AUX45_G.PS13', 'AUX46_G.PS14', 'AUX47_G.PS15', 'AUX48_G.PS16', 'AUX50_Vpp', 'AUX51_Vdc', 'AUX52_R.PWR', 'AUX53_DCS_IN', 'AUX54_IGS_DCS', 'AUX56_DCS1_PIP', 'AUX58_PURGE-1', 'AUX59_DCS_TANK', 'AUX64_REC-1', 'AUX65_REC-2-1', 'AUX66_REC-2-2', 'AUX67_IGS_N2-1', 'AUX69_SEALCAP', 'AUX72_RAXIS', 'AUX73_APC_RING', 'AUX74_APC_OUT', 'AUX89_JH1', 'AUX90_JH2', 'AUX91_JH3', 'AUX92_JH4', 'AUX93_JH5', 'AUX94_JH6', 'AUX95_JH7', 'AUX96_JH8', 'AUX97_JH9', 'AUX98_JH10', 'AUX99_JH11', 'AUX100_JH12', 'AUX101_JH13']
column_types = {'Timestamp': 'TIMESTAMP', 'ObservableTimestamp': 'TEXT', 'EquipmentStatus': 'INTEGER', 'AlarmState': 'BOOLEAN', 'O2Density_Monitor_Value': 'REAL', 'O2Density_Set_Value': 'REAL', 'PMstatus': 'INTEGER', 'PPExecname': 'TEXT', 'PPExecStepSeqNo': 'INTEGER', 'PPExecStepID': 'INTEGER', 'PPExecStepName': 'TEXT', 'ActiveCjobID': 'TEXT', 'ActivePjobID': 'TEXT', 'PMStoredProcessRecipeName': 'TEXT', 'ProcessRecipeEndRemainTime': 'TEXT', 'ProcessRecipeStepTime': 'TEXT', 'ProcessRecipeStepRemainTime': 'TEXT', 'ProcessRecipeStepID': 'INTEGER', 'ProcessRecipeStepName': 'TEXT', 'ProcessRecipeStepSeqNo': 'INTEGER', 'ProcessRecipeTotalTime': 'TEXT', 'Temp_Set_U': 'REAL', 'Temp_HT_Power_U': 'REAL', 'Temp_Monitor_U': 'REAL', 'Temp_TC_Monitor_U': 'REAL', 'Temp_TC_Cascade_U ': 'REAL', 'Temp_Act_U': 'REAL', 'Temp_HT_Power_Cascade_U': 'REAL', 'Temp_Set_CU': 'REAL', 'Temp_HT_Power_CU': 'REAL', 'Temp_Monitor_CU': 'REAL', 'Temp_TC_Monitor_CU': 'REAL', 'Temp_TC_Cascade_CU': 'REAL', 'Temp_Act_CU': 'REAL', 'Temp_HT_Power_Cascade_CU': 'REAL', 'Temp_Set_C': 'REAL', 'Temp_HT_Power_C': 'REAL', 'Temp_Monitor_C': 'REAL', 'Temp_TC_Monitor_C': 'REAL', 'Temp_TC_Cascade_C': 'REAL', 'Temp_Act_C': 'REAL', 'Temp_HT_Power_Cascade_C': 'REAL', 'Temp_Set_CL': 'REAL', 'Temp_HT_Power_CL': 'REAL', 'Temp_Monitor_CL': 'REAL', 'Temp_TC_Monitor_CL': 'REAL', 'Temp_TC_Cascade_CL': 'REAL', 'Temp_Act_CL': 'REAL', 'Temp_HT_Power_Cascade_CL': 'REAL', 'Temp_Set_L': 'REAL', 'Temp_HT_Power_L': 'REAL', 'Temp_Monitor_L': 'REAL', 'Temp_TC_Monitor_L': 'REAL', 'Temp_TC_Cascade_L': 'REAL', 'Temp_Act_L': 'REAL', 'Temp_HT_Power_Cascade_L': 'REAL', 'APC Valve Value (Angle)': 'REAL', 'VG13_LeakPressure_Monitor': 'REAL', 'VG11_LeakPressure_Monitor': 'REAL', 'VG13_LeakQuantity_Monitor': 'REAL', 'VG11_LeakQuantity_Monitor': 'REAL', 'VG13 Press value': 'REAL', 'VG11 Press value': 'REAL', 'PJobProcessingState': 'INTEGER', 'ValveAct_1:1': 'REAL', 'ValveAct_2:2': 'REAL', 'ValveAct_3:3': 'REAL', 'ValveAct_4:4': 'REAL', 'ValveAct_5:5': 'REAL', 'ValveAct_9:9': 'REAL', 'ValveAct_11:11': 'REAL', 'ValveAct_12:12': 'REAL', 'ValveAct_14:14': 'REAL', 'ValveAct_15:15': 'REAL', 'ValveAct_16:16': 'REAL', 'ValveAct_26:26': 'REAL', 'ValveAct_28:28': 'REAL', 'ValveAct_29:29': 'REAL', 'ValveAct_30:30': 'REAL', 'ValveAct_60:71': 'REAL', 'ValveAct_63:75': 'REAL', 'ValveAct_73:83': 'REAL', 'ValveAct_75:85': 'REAL', 'ValveAct_76:86': 'REAL', 'ValveAct_80:DPO': 'REAL', 'ValveAct_86:HT1': 'REAL', 'ValveAct_87:HT2': 'REAL', 'ValveAct_88:HT3': 'REAL', 'ValveAct_89:RF': 'REAL', 'ValveAct_90:PST': 'REAL', 'ValveAct_95:WAT': 'REAL', 'SubRecipeLoopSettingValue': 'INTEGER', 'SubRecipeLoopMoniterValue': 'INTEGER', 'VG12_LeakPressure_Monitor': 'REAL', 'VG12 Press value': 'REAL', 'MFC1_N2-1': 'REAL', 'MFC2_N2-2': 'REAL', 'MFC3_N2-3': 'REAL', 'MFC4_N2-4': 'REAL', 'MFC7_DCS': 'REAL', 'MFC8_NH3': 'REAL', 'MFC9_F2': 'REAL', 'MFC10_N2-R': 'REAL', 'MFC11_NO': 'REAL', 'MFC12_DCSMFM_7': 'REAL', 'MFC16_MFC51 N2': 'REAL', 'MFC26_F.PWR': 'REAL', 'MFC27_L.POS': 'REAL', 'MFC28_R.POS': 'REAL', 'AUX1_MS1': 'REAL', 'AUX2_MS321': 'REAL', 'AUX3_MS2': 'REAL', 'AUX4_MS3': 'REAL', 'AUX5_MS5': 'REAL', 'AUX8_MS8': 'REAL', 'AUX9_MS9': 'REAL', 'AUX16_VG21': 'REAL', 'AUX18_VG12': 'REAL', 'AUX19_VG11': 'REAL', 'AUX20_VG13': 'REAL', 'AUX21_M.WAT': 'REAL', 'AUX22_FS101': 'REAL', 'AUX23_FS102': 'REAL', 'AUX24_FS104': 'REAL', 'AUX26_FS106': 'REAL', 'AUX28_FS111': 'REAL', 'AUX29_FS105': 'REAL', 'AUX33_G.PS1': 'REAL', 'AUX34_G.PS2': 'REAL', 'AUX35_G.PS3': 'REAL', 'AUX36_G.PS4': 'REAL', 'AUX37_G.PS5': 'REAL', 'AUX38_G.PS6': 'REAL', 'AUX45_G.PS13': 'REAL', 'AUX46_G.PS14': 'REAL', 'AUX47_G.PS15': 'REAL', 'AUX48_G.PS16': 'REAL', 'AUX50_Vpp': 'REAL', 'AUX51_Vdc': 'REAL', 'AUX52_R.PWR': 'REAL', 'AUX53_DCS_IN': 'REAL', 'AUX54_IGS_DCS': 'REAL', 'AUX56_DCS1_PIP': 'REAL', 'AUX58_PURGE-1': 'REAL', 'AUX59_DCS_TANK': 'REAL', 'AUX64_REC-1': 'REAL', 'AUX65_REC-2-1': 'REAL', 'AUX66_REC-2-2': 'REAL', 'AUX67_IGS_N2-1': 'REAL', 'AUX69_SEALCAP': 'REAL', 'AUX72_RAXIS': 'REAL', 'AUX73_APC_RING': 'REAL', 'AUX74_APC_OUT': 'REAL', 'AUX89_JH1': 'REAL', 'AUX90_JH2': 'REAL', 'AUX91_JH3': 'REAL', 'AUX92_JH4': 'REAL', 'AUX93_JH5': 'REAL', 'AUX94_JH6': 'REAL', 'AUX95_JH7': 'REAL', 'AUX96_JH8': 'REAL', 'AUX97_JH9': 'REAL', 'AUX98_JH10': 'REAL', 'AUX99_JH11': 'REAL', 'AUX100_JH12': 'REAL', 'AUX101_JH13': 'REAL'}

def find_csv_files(base_dir):
    csv_files = []
    # 정규 표현식: YYYY/MM/DD/HH00.csv
    pattern = re.compile(r'.*/\d{4}/\d{2}/\d{2}/([0-2][0-9]00)\.csv$')
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith('.csv'):
                full_path = os.path.join(root, file)
                match = pattern.match(full_path)
                if match:
                    hour = match.group(1)
                    if hour in {f"{str(h).zfill(2)}00" for h in range(24)}:
                        csv_files.append(full_path)
    csv_files.sort()
    return csv_files


# 날짜 파싱 및 정렬
def extract_date(tname):
    m = re.search(r'rawdata(\d+)', tname)
    return int(m.group(1)) if m else 0

def get_last_tablename():
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432
    )

    cur = conn.cursor()

    # 'rawdata%'로 시작하는 테이블명 가져오기
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name LIKE 'rawdata%';
    """)

    tables = cur.fetchall()

    if len(tables) == 0:
        return "", "", ""
    sorted_tables = sorted(
        [t[0] for t in tables if re.match(r'rawdata\d+$', t[0])],
        key=extract_date,
        reverse=True
    )
    if sorted_tables:
        latest_table = sorted_tables[0] 
        if len(sorted_tables) > 1:
            latest_table2 = sorted_tables[1]
        else:
            latest_table2 = ""
    else:
        latest_table = ""
        latest_table2 = ""
    
    cur.execute(f'SELECT MAX("Timestamp") FROM "{latest_table}"')
    result = cur.fetchone()
    cur.close()
    conn.close()
    latest_timestamp = ""
    if result:
        latest_timestamp = result[0]
            
    return latest_table, latest_table2, latest_timestamp

def transform_value(val):
    if val is None:
        return None
    elif val == "OPEN":
        return '1'
    elif val == "CLOSE":
        return '0'
    elif val == "" or val == "NaN" or val == "nan":
        return None
    return val


def insert_row(row):
    # row는 columns 순서의 값 리스트
    row = [transform_value(v) for v in row]
    if len(row) > 17 and row[17] == "0":
        row[18] = "IDLE"
    if len(row) > 17 and row[17] == "998":
        row[17] = "0"
    #print(row)
    # 2. 날짜 기반 테이블 이름
    
    timestamp_str = row[0]
    #date_part = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f").strftime("%d%H")
    date_part = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f").strftime("%Y%m%d")
    table_name = f"rawdata{date_part}"

    # 3. 테이블 생성 (최초 1회만)    
    columns_sql = ',\n    '.join([
        f'"{col}" {column_types.get(col, "TEXT")}' for col in columns])
    create_sql = f'''
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        {columns_sql},
        UNIQUE ("Timestamp")
    );
    '''
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432
    )
    cur = conn.cursor()
    cur.execute(create_sql)
    try:
        # 4. INSERT
        placeholders = ','.join(['%s'] * len(columns))
        colnames = ','.join([f'"{c}"' for c in columns])
        insert_sql = f'''
            INSERT INTO "{table_name}" ({colnames})
            VALUES ({placeholders})
            ON CONFLICT ("Timestamp") DO NOTHING;
        '''
        cur.execute(insert_sql, row)
        conn.commit()
        cur.close()
        conn.close()

        print(f"[{timestamp_str} row] inserted into {table_name}")
    except Exception as e:
        print(f'[INSERT 중 에러발생] {e}')
        print(f'[문제 row] {row}')


def insert_rows(rows, table_name):
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432
    )
    cur = conn.cursor()

    # 테이블 생성    
    columns_sql = ',\n    '.join([
        f'"{col}" {column_types.get(col, "TEXT")}' + (' PRIMARY KEY' if col == 'Timestamp' else '')
        for col in columns])
    create_sql = f'''
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        {columns_sql},
        UNIQUE ("Timestamp")
    );
    '''
    cur.execute(create_sql)
    try:
        # INSERT
        placeholders = ','.join(['%s'] * len(columns))
        colnames = ','.join([f'"{c}"' for c in columns])
        insert_sql = f'''
            INSERT INTO "{table_name}" ({colnames})
            VALUES ({placeholders})
            ON CONFLICT ("Timestamp") DO NOTHING;
        '''
        cur.executemany(insert_sql, rows)
        conn.commit()
        cur.close()
        conn.close()

        print(f"{len(rows)} rows inserted into {table_name}")
    except Exception as e:
        print(f'[다중 rows INSERT 중 에러발생] {e}')
    
    
def insert_leaked_data(file_path):
    latest_table, latest_table2, latest_timestamp = get_last_tablename()
    if latest_timestamp == "":
        return
    latest_timestamp = str(latest_timestamp)
    if len(latest_timestamp) == 26:
        latest_timestamp = latest_timestamp[:-3]
    print(latest_timestamp)
    csv_files = find_csv_files(file_path)
    pattern = re.compile(r'.*/(\d{4})/(\d{2})/(\d{2})/([0-2][0-9])00\.csv$')
    pattern2 = re.compile(r'.*/(\d{4})/svid 수정전/(\d{2})/(\d{2})/([0-2][0-9])00\.csv$')
    for i in range(len(csv_files)-1, -1, -1):
        if 'svid 수정전' in csv_files[i]:
            match = pattern2.match(csv_files[i])
        else:
            match = pattern.match(csv_files[i])
        if not match:
            continue

        year, month, day, hour = match.groups()
        #print(year, month, day, hour)
        #print(latest_timestamp[:4], latest_timestamp[5:7], latest_timestamp[8:10], latest_timestamp[11:13])
        if month == latest_timestamp[5:7] and day == latest_timestamp[8:10] and hour == latest_timestamp[11:13]:
            csv_files = csv_files[i:]
            break

    
    collected_rows = {}
    for csv_file in csv_files:
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader, [])
                file_headers[csv_file] = header
                for values in reader:
                    if not values:
                        continue
                    row_dict = {header[i]: values[i] for i in range(len(values))}
                    row = [row_dict.get(col) for col in columns]
                    row = [transform_value(v) for v in row]
                    # 현재 시간으로 타임스탬프 덮어쓰기
                    now_ts = row[0]
                    #table_suffix = datetime.strptime(now_ts, "%Y-%m-%d %H:%M:%S.%f").strftime("%d%H")
                    table_suffix = datetime.strptime(now_ts, "%Y-%m-%d %H:%M:%S.%f").strftime("%Y%m%d")
                    table_name = f"rawdata{table_suffix}"
                    collected_rows.setdefault(table_name, []).append(row)
        except Exception as e:
            print(f"Error reading file {csv_file}: {e}")
    for table_name, rows in collected_rows.items():
        insert_rows(rows, table_name)
                
    print(f'누락 데이터 저장 완료')
    print(list(collected_rows.keys()))
    return 


def seek_last_line(file_path, last_offset):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            if last_offset == 0:
                reader = csv.reader(f)
                header = next(reader, [])
                file_headers[file_path] = header
                new_rows = list(reader)
            else:
                f.seek(last_offset)
                header = file_headers.get(file_path, [])
                new_rows = list(csv.reader(f))
            file_offsets[0] = f.tell()

        if new_rows:
            for values in new_rows:
                if not values:
                    continue
                row_dict = {header[i]: values[i] for i in range(len(values))}
                row = [row_dict.get(col) for col in columns]
                insert_row(row)
        
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")


def is_valid_hourly_csv(base):
    if len(base) != 4 or not base.isdigit():
        return False
    if base.endswith("00"):
        hour = int(base[:2])
        return 0 <= hour <= 23
    return False


# watchdog 프로세스 함수
class CSVUpdateHandler(FileSystemEventHandler):
    def on_modified(self, event): # 파일이 수정되었을떄
        #print("[MODIFIED]", event.src_path)
        if event.is_directory or not event.src_path.endswith(".csv") or not is_valid_hourly_csv(os.path.basename(event.src_path).replace(".csv", "")):
            return
        file_path = event.src_path
        last_offset = file_offsets[0]
        seek_last_line(file_path, last_offset)
        
    def on_created(self, event): # 파일이 생성되었을때
        if event.is_directory or not event.src_path.endswith(".csv") or not is_valid_hourly_csv(os.path.basename(event.src_path).replace(".csv", "")):
            return
        print('new csv created')
        file_offsets[0] = 0
        last_offset = file_offsets[0] 
        file_path = event.src_path
        seek_last_line(file_path, last_offset)

    
if __name__ == '__main__':
    realtime_path = "../realtimedata" # 감시할 디렉토리 경로
    print(f"감시대상경로 : {os.path.abspath(realtime_path)}")
    '''
    # ① DB 연결 설정
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432
    )
    cur = conn.cursor()
    
    # 모든 public 테이블 삭제 쿼리 실행
    cur.execute("""
        DO $$
        DECLARE
            r RECORD;
        BEGIN
            FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
                EXECUTE 'DROP TABLE IF EXISTS "' || r.tablename || '" CASCADE';
            END LOOP;
        END
        $$;
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ 모든 public 테이블 제거 완료")
    '''
    
    print("초기 데이터 저장중..\n")
    insert_missing_data(realtime_path)
    
    print("누락 데이터 검사중..\n")
    insert_leaked_data(realtime_path)
        
    # watchdog 실행
    observer = PollingObserver()
    event_handler = CSVUpdateHandler()
    observer.schedule(event_handler, path=realtime_path, recursive=True)
    observer.start()

    print(f"Monitoring '{realtime_path}' for CSV updates...")
    
    
    while True:
        time.sleep(5)

    observer.stop()
    observer.join()

