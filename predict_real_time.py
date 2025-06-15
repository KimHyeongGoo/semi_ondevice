import time
import os
import psycopg2
from datetime import datetime, timedelta
import joblib
import ray
import numpy as np
import pandas as pd
import re
from tensorflow.keras.models import load_model
from tensorflow.keras.losses import MeanAbsoluteError


window_size = 160  # 과거 n초 데이터 사용 (80초 0.5초단위)
predict_steps = 20  # n초 후 예측 (10초후)
#예측할 칼럼 리스트
predict_columns = [          # MFC N2-4 모니터링 값
    'MFC26_F.PWR',          # MFCMon_F.PWR
    'MFC27_L.POS',          # MFCMon_L.POS
    'MFC28_R.POS',          # MFCMon_P.POS (유사값이므로 R.POS로 매핑)
    'MFC7_DCS',             # MFCMon_DCS
    'MFC8_NH3',          ## MFC 암모니아(NH3) 유량 모니터링 값
    'VG11 Press value',     # VG11
    'VG12 Press value',     # VG12
    'VG13 Press value',     # VG13
    'Temp_Act_U',           # TempAct_U
    'Temp_Act_CU',          # TempAct_CU
    'Temp_Act_C',           # TempAct_C
    'Temp_Act_CL',          # TempAct_CL
    'Temp_Act_L'           # TempAct_L
]

# 공정 모니터링 변수
selected_cols = ['PPExecStepID', 'MFC1_N2-1', 'MFC2_N2-2', 'MFC3_N2-3', 'MFC4_N2-4', 'MFC26_F.PWR', 'MFC27_L.POS', 'MFC28_R.POS', 'MFC7_DCS', 'MFC8_NH3', 'MFC9_F2', 'APC Valve Value (Angle)', 'VG11 Press value', 'VG12 Press value', 'VG13 Press value', 'Temp_Act_U', 'Temp_Act_CU', 'Temp_Act_C', 'Temp_Act_CL', 'Temp_Act_L', 'ValveAct_1:1', 'ValveAct_2:2', 'ValveAct_3:3', 'ValveAct_4:4', 'ValveAct_5:5', 'ValveAct_9:9', 'ValveAct_11:11', 'ValveAct_12:12', 'ValveAct_14:14', 'ValveAct_15:15', 'ValveAct_16:16', 'ValveAct_26:26', 'ValveAct_28:28', 'ValveAct_29:29', 'ValveAct_30:30', 'ValveAct_60:71', 'ValveAct_63:75', 'ValveAct_73:83', 'ValveAct_75:85', 'ValveAct_76:86', 'ValveAct_80:DPO', 'ValveAct_86:HT1', 'ValveAct_87:HT2', 'ValveAct_88:HT3', 'ValveAct_89:RF', 'ValveAct_90:PST']

def logg(log_file, content):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f"[{timestamp}] {content}"
    print(log_entry)
    os.makedirs('./log', exist_ok=True)
    with open(os.path.join('./log', log_file), 'a', encoding='utf-8') as f:
        f.write(log_entry + '\n')
        
        
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

def interpolate_rows(data):
    # 새 인덱스 계산: 2배로 확장
    expanded_index = np.repeat(range(len(data) - 1), 2)
    expanded_index = np.append(expanded_index, len(data) - 1)  # 마지막 줄 보존

    # 새 데이터프레임 생성 (행 반복)
    data_expanded = data.iloc[expanded_index].reset_index(drop=True)

    # 보간할 인덱스 (홀수 행)
    interp_rows = list(range(1, len(data_expanded) - 1, 2))

    # 보간 제외 컬럼
    exclude_cols = ['PPExecStepID'] + [col for col in data.columns if col.startswith('ValveAct_')]

    for i in interp_rows:
        prev_row = data_expanded.iloc[i - 1]
        next_row = data_expanded.iloc[i + 1]

        for col in data.columns:
            if col in exclude_cols:
                # 예외 컬럼: 이전 행 값 복사
                data_expanded.at[i, col] = prev_row[col]
            else:
                # 일반 컬럼: 중간값 계산
                val = (prev_row[col] + next_row[col]) / 2
                data_expanded.at[i, col] = val

    return data_expanded


def insert_violation(conn, cur, timestamp, col, step_id, step_name, val, limit_type, threshold):

    symbol = "<=" if limit_type == "min" else ">="
    if symbol == '<=':
        msg = f"[{timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] 하한선 침범\n [파라미터 {col}] {step_name}({step_id})\n 예측값({val:.3f}) {symbol} {limit_type}({threshold})"
    else:
        msg = f"[{timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] 상한선 침범\n [파라미터 {col}] {step_name}({step_id})\n 예측값({val:.3f}) {symbol} {limit_type}({threshold})"
        
    cur.execute("""
        INSERT INTO realtime_violation_log ("Timestamp",  parameter, message)
        VALUES (%s, %s, %s)
        ON CONFLICT ("Timestamp", parameter) DO NOTHING
    """, (timestamp, col, msg))
    conn.commit()
    
    
# 각 칼럼별 예측 수행
@ray.remote
def ray_predict(selected_cols, predict_column, window_size, predict_steps, model_path = './model_10sec', scaler_path = './model_10sec/scaler'):
    proc_pid = os.getpid()
    last_date = ""
    try:
        scaler_X = joblib.load(os.path.join(scaler_path,'scaler_X.pkl'))
        scaler_y = joblib.load(os.path.join(scaler_path,f'scaler_y_{predict_column}.pkl'))
        loaded_model = load_model(os.path.join(model_path,f'{predict_column}_model.keras'), custom_objects={'mae': MeanAbsoluteError()})
    except Exception as e:
        logg(f"[PID|{proc_pid}]{predict_column}.log", str(e))
        return
        
    while True:
        table_name, prev_table_name, new_last_date = get_last_tablename()
        new_last_date = str(new_last_date)
        if len(new_last_date) == 26:
            new_last_date = new_last_date[:-3]
        if table_name == "" or str(new_last_date) == "" or last_date >= new_last_date:
            time.sleep(0.2)
            continue
        last_date = new_last_date
        conn = psycopg2.connect(
            dbname="postgres",
            user="keti",
            password="keti1234!",
            host="localhost",
            port=5432
        )
        
        try:
            interval_sec = window_size // 2 + 1
            colnames = ', '.join([f'"{col}"' for col in selected_cols])
            query = f"""
                SELECT {colnames}, "ProcessRecipeStepRemainTime", "ProcessRecipeStepID", "ProcessRecipeStepName"
                FROM "{table_name}"
                WHERE "Timestamp" BETWEEN
                    (%s::timestamp - INTERVAL '{interval_sec} seconds') AND %s::timestamp
            """
            data = pd.read_sql(query, conn, params=(last_date, last_date))
            step_data = data[["ProcessRecipeStepRemainTime", "ProcessRecipeStepID", "ProcessRecipeStepName"]]
            data = data[selected_cols]
            data = interpolate_rows(data) # 1초 데이터를 0.5초로 변환
            if len(data) < window_size:
                if len(prev_table_name) > 0:
                    query = f"""
                        SELECT {colnames}
                        FROM "{prev_table_name}"
                        WHERE "Timestamp" BETWEEN
                            (%s::timestamp - INTERVAL '{interval_sec} seconds') AND %s::timestamp
                    """

                    data2 = pd.read_sql(query, conn, params=(last_date, last_date))
                    data2 = data2[selected_cols]
                    data2 = interpolate_rows(data2) # 1초 데이터를 0.5초로 변환
                    data = pd.concat([data2, data], ignore_index=True)
                else:
                    time.sleep(0.2)
                    continue
            if len(data) > window_size:
                data = data.iloc[-window_size : ]
        except Exception as e:
            logg(f"[PID|{proc_pid}]{predict_column}.log", " 데이터 쿼리시도중 오류발생")
            logg(f"[PID|{proc_pid}]{predict_column}.log", str(e))
            time.sleep(0.2)
            
        try:
            X_data = scaler_X.transform(data.values)
            pred = loaded_model.predict(np.array([X_data]))
            pred_real = scaler_y.inverse_transform(pred)
            try:
                pred_date = datetime.strptime(last_date, "%Y-%m-%d %H:%M:%S.%f") + timedelta(seconds=predict_steps)
            except:
                pred_date = datetime.strptime(last_date, "%Y-%m-%d %H:%M:%S") + timedelta(seconds=predict_steps)
            pred_data = pred_real[0][0]
            try:
                remain_str = df.iloc[-1]['ProcessRecipeStepRemainTime']  
                if remain_str == "00:00:00":
                    last_remain_sec = 100
                else:
                    h, m, s = map(int, remain_str.split(":"))
                    last_remain_sec = h * 3600 + m * 60 + s
                last_step_id = df.iloc[-1]["ProcessRecipeStepID"]
                last_step_name = df.iloc[-1]["ProcessRecipeStepName"]
                if last_step_id == 0:
                    last_step_name = 'STANDBY'
                if last_remain_sec > 0 and last_step_id >= predict_steps:
                    pass
                else:
                    last_step_id = -1
                    last_step_name = 'UNKNOWN'               
            except:
                    last_step_id = -1
                    last_step_name = 'UNKNOWN'                             
        except Exception as e:
            logg(f"[PID|{proc_pid}]{predict_column}.log", "쿼리후 데이터 전처리 및 예측 시 오류발생")
            logg(f"[PID|{proc_pid}]{predict_column}.log", str(e))
            time.sleep(0.2)
        
        
        try:
            cur = conn.cursor()
            
            predict_column_modified = predict_column.replace('.', '_').replace(' ', '_')
            save_table_name = f"pred_{predict_column_modified}"
            # 테이블 생성 (없을 경우)
            create_query = f"""
            CREATE TABLE IF NOT EXISTS "{save_table_name}" (
                "Timestamp" TIMESTAMP,
                "Parameter" REAL,
                "ProcessRecipeStepID" INTEGER,
                "ProcessRecipeStepName" TEXT,
                UNIQUE ("Timestamp", parameter)
            );
            """
            cur.execute(create_query)

            # 데이터 삽입
            insert_query = f"""
            INSERT INTO "{save_table_name}" ("Timestamp", "Parameter", "ProcessRecipeStepID", "ProcessRecipeStepName")
            VALUES (%s::timestamp, %s::real, %s::integer, %s::text)
            ON CONFLICT ("Timestamp") DO NOTHING
            """
            cur.execute(insert_query, (pred_date, float(pred_data), int(last_step_id), str(last_step_name)))
        except Exception as e:
            logg(f"[PID|{proc_pid}]{predict_column}.log", "예측데이터 DB insert 오류발생")
            logg(f"[PID|{proc_pid}]{predict_column}.log", str(e))
            time.sleep(0.2)
            
        try:   
            if last_step_id != -1 and val is not None:
                # limits.yaml 읽기
                limits = {}
                if os.path.exists("./fastapi/limits.yaml"):
                    with open("./fastapi/limits.yaml", "r", encoding="utf-8") as f:
                        limits = yaml.safe_load(f)
                    step_limits = limits.get(predict_column, {}).get(str(step_id))
                    if step_limits:
                        ts = str(pred_date)
                        if "min" in step_limits and pred_data <= step_limits["min"]:
                            insert_violation(conn, cur, ts, predict_column, last_step_id, last_step_name, pred_data, 'min', step_limits["min"])
                        elif "max" in step_limits and pred_data >= step_limits["max"]:
                            insert_violation(conn, cur, ts, predict_column, last_step_id, last_step_name, pred_data, 'max', step_limits["max"])
        except Exception as e:
            logg(f"[PID|{proc_pid}]{predict_column}.log", "예측데이터 DB insert 오류발생")
            logg(f"[PID|{proc_pid}]{predict_column}.log", str(e))
            time.sleep(0.2)

        # 커밋 및 정리
        conn.commit()
        cur.close()
        conn.close()
        time.sleep(0.1)

    
    
if __name__ == '__main__':
  
    try:
        ray.init()
    except:
        ray.shutdown()
        ray.init()
    
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS realtime_violation_log (
            "Timestamp" TIMESTAMP NOT NULL,
            parameter TEXT NOT NULL,
            message TEXT NOT NULL,
            UNIQUE ("Timestamp", parameter)
        );
    """)
    cur.close()
    conn.close()
    
    obj_id_list = []
    for predict_column in predict_columns:
        obj_id_list.append(ray_predict.remote(selected_cols, predict_column, window_size, predict_steps//2))
    
    while len(obj_id_list):
        done, obj_id_list = ray.wait(obj_id_list)
        ray.get(done[0])
        
    ray.shutdown()
    observer.stop()
    observer.join()

