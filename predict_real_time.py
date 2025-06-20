import time
import os
import psycopg2
from datetime import datetime, timedelta
import joblib
import ray
import numpy as np
import pandas as pd
import re
import yaml
from tensorflow.keras.models import load_model
from tensorflow.keras import layers
import tensorflow as tf

window_size = 192  
predict_steps = [10, 20, 30]  
#예측할 칼럼 리스트
predict_columns = [      
    #'PPExecStepID',
    'MFC7_DCS',           ## MFC Dichlorosilane(DCS) 유량 모니터링 값
    'MFC8_NH3',           ## MFC 암모니아(NH3) 유량 모니터링 값
    #'MFC9_F2',
    'MFC1_N2-1',  # MFC(Mass Flow Controller) N2-1 모니터링 값
    'MFC2_N2-2',          # MFC N2-2 모니터링 값
    'MFC3_N2-3',  # MFC N2-3 모니터링 값
    'MFC4_N2-4',          
    'VG11 Press value',                 ## Baratron Gauge(의 압력 모니터링 값 (프로세스중 작용)
    'VG12 Press value',                 # Baratron Gauge(의 압력 모니터링 값 (프로세스외 작용)
    'VG13 Press value',                 # Baratron Gauge(의 압력 모니터링 값 (프로세스외 작용)
    'Temp_Act_U',            # 상부 위치 실제 온도
    'Temp_Act_CU',           # 중앙 상부 위치 실제 온도
    'Temp_Act_C',            # 중앙 위치 실제 온도
    'Temp_Act_CL',           # 중앙 하부 위치 실제 온도
    'Temp_Act_L',              
    'MFC26_F.PWR',
    'MFC27_L.POS',         # MFC Left Position 위치 모니터링 값
    'MFC28_R.POS'         # MFC P.POS 위치 모니터링 값
]

# 공정 모니터링 변수
#selected_cols = ['PPExecStepID', 'MFC1_N2-1', 'MFC2_N2-2', 'MFC3_N2-3', 'MFC4_N2-4', 'MFC26_F.PWR', 'MFC27_L.POS', 'MFC28_R.POS', 'MFC7_DCS', 'MFC8_NH3', 'MFC9_F2', 'APC Valve Value (Angle)', 'VG11 Press value', 'VG12 Press value', 'VG13 Press value', 'Temp_Act_U', 'Temp_Act_CU', 'Temp_Act_C', 'Temp_Act_CL', 'Temp_Act_L', 'ValveAct_1:1', 'ValveAct_2:2', 'ValveAct_3:3', 'ValveAct_4:4', 'ValveAct_5:5', 'ValveAct_9:9', 'ValveAct_11:11', 'ValveAct_12:12', 'ValveAct_14:14', 'ValveAct_15:15', 'ValveAct_16:16', 'ValveAct_26:26', 'ValveAct_28:28', 'ValveAct_29:29', 'ValveAct_30:30', 'ValveAct_60:71', 'ValveAct_63:75', 'ValveAct_73:83', 'ValveAct_75:85', 'ValveAct_76:86', 'ValveAct_80:DPO', 'ValveAct_86:HT1', 'ValveAct_87:HT2', 'ValveAct_88:HT3', 'ValveAct_89:RF', 'ValveAct_90:PST']
selected_cols = ['PPExecStepID', 'MFC1_N2-1', 'MFC2_N2-2', 'MFC3_N2-3', 'MFC4_N2-4', 'MFC26_F.PWR', 'MFC27_L.POS', 'MFC28_R.POS', 'MFC7_DCS', 'MFC8_NH3', 'MFC9_F2', 'APC Valve Value (Angle)', 'VG11 Press value', 'VG12 Press value', 'VG13 Press value', 'Temp_Act_U', 'Temp_Act_CU', 'Temp_Act_C', 'Temp_Act_CL', 'Temp_Act_L', 'ValveAct_2:2', 'ValveAct_3:3', 'ValveAct_4:4', 'ValveAct_5:5', 'ValveAct_9:9', 'ValveAct_12:12', 'ValveAct_14:14', 'ValveAct_16:16', 'ValveAct_26:26', 'ValveAct_28:28', 'ValveAct_29:29', 'ValveAct_60:71', 'ValveAct_63:75', 'ValveAct_73:83', 'ValveAct_80:DPO', 'ValveAct_89:RF', 'ValveAct_90:PST']
step_reverse_dict = {'END': 2, 'STANDBY': 0, 'START': 1, 'B.UP': 17, 'WAIT': 3, 'S.P-1': 74, 'S.P-2': 75, 'R.UP1': 25, 'STAB1': 22, 'S.P-3': 76, 'M.P-3': 81, 'L.CHK': 72, 'PREPRG1': 44, 'EVAC1': 99, 'EVAC2': 100, 'N-EVA1': 111, 'CLOSE1': 128, 'SI-FL1': 119, 'SI-EVA1': 117, 'CHANGE': 152, 'N-PRE1': 113, 'N-FL1': 115, 'N-FL2': 116, 'pre-NH3P': 110, 'DEPO1': 49, 'post_NH3P': 135, 'N2PRG1': 103, 'SI-EVA4': 149, 'A.VAC2': 85, 'A.PRG2': 90, 'A.VAC1': 84, 'A.PRG1': 89, 'N2PRG2': 104, 'N2PRG3': 105, 'A.VAC3': 86, 'A.PRG3': 91, 'A.VAC4': 87, 'A.PRG4': 92, 'CYCLE1': 130, 'A.PRG5': 93, 'R.DOWN1': 31, 'B.FILL1': 94, 'B.FILL2': 95, 'B.FILL3': 96, 'B.FILL4': 97, 'B.FILL5': 98, 'B.DOWN': 18, 'None': 0, 'nan': 0, 'NaN': 0, 'null': 0, 'NULL': 0, 'IDLE': 0}

def logg(log_file, content):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f"[{timestamp}] {content}"
    print(log_entry)
    os.makedirs('./log', exist_ok=True)
    with open(os.path.join('./log', log_file), 'a', encoding='utf-8') as f:
        f.write(log_entry + '\n')
        
class PatchEmbedding(layers.Layer):
    def __init__(self, patch_len, d_model, **kwargs):
        super().__init__(**kwargs)
        self.patch_len = patch_len
        self.d_model = d_model
        self.proj = None  # 초기화는 build에서 수행

    def build(self, input_shape):
        self.proj = layers.Dense(self.d_model)

    def call(self, x):
        # x: (batch_size, seq_len, num_features)
        batch_size = tf.shape(x)[0]
        seq_len = x.shape[1]
        num_features = x.shape[2]
        num_patches = seq_len // self.patch_len
        x = tf.reshape(x, [batch_size, num_patches, self.patch_len * num_features])
        return self.proj(x)

    def get_config(self):
        config = super().get_config()
        config.update({
            'patch_len': self.patch_len,
            'd_model': self.d_model
        })
        return config

    @classmethod
    def from_config(cls, config):
        return cls(
            patch_len=config.get('patch_len'),
            d_model=config.get('d_model'),
            **{k: v for k, v in config.items() if k not in ['patch_len', 'd_model', 'length']}  # <-- 'length' 제거
        )




class PositionalEncoding(layers.Layer):
    def __init__(self, length, d_model, **kwargs):
        super().__init__(**kwargs)
        self.length = length
        self.d_model = d_model

    def build(self, input_shape):
        self.pos_emb = self.add_weight(
            name="pos_emb",
            shape=[1, self.length, self.d_model],
            initializer='random_normal'
        )

    def call(self, x):
        return x + self.pos_emb

    def get_config(self):
        config = super().get_config()
        config.update({
            'length': self.length,
            'd_model': self.d_model
        })
        return config

    @classmethod
    def from_config(cls, config):
        return cls(
            length=config.get('length'),
            d_model=config.get('d_model'),
            **{k: v for k, v in config.items() if k not in ['length', 'd_model', 'patch_len']}  # <-- 'patch_len' 제거
        )

        
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


def insert_violation(conn, cur, timestamp, col, step_id, step_name, val, limit_type, threshold):
    if len(timestamp) == 26:
        timestamp = timestamp[:-3]
    symbol = "<=" if limit_type == "min" else ">="
    if symbol == '<=':
        msg = f"[{timestamp}] 하한선 침범\n파라미터 : {col}\nStepName : {step_name}({step_id})\n예측값({val:.3f}) {symbol} {limit_type}({threshold})"
    else:
        msg = f"[{timestamp}] 상한선 침범\n[파라미터 {col}] {step_name}({step_id})\n예측값({val:.3f}) {symbol} {limit_type}({threshold})"
        
    cur.execute("""
        INSERT INTO realtime_violation_log ("Timestamp",  parameter, message)
        VALUES (%s, %s, %s)
        ON CONFLICT ("Timestamp", parameter) DO NOTHING
    """, (timestamp, col, msg))
    conn.commit()
    
    
# 각 칼럼별 예측 수행
@ray.remote
def ray_predict(selected_cols, predict_columns, window_size, predict_steps, model_path = './model', scaler_path = './model/scaler'):
    print(predict_columns)
    proc_pid = os.getpid()
    last_date = ""
    scaler_ys = {}
    loaded_models = {}
    try:
        scaler_X = joblib.load(os.path.join(scaler_path,'scaler_X.pkl'))
        for predict_column in predict_columns:
            scaler_ys[predict_column] = joblib.load(os.path.join(scaler_path,f'scaler_y_{predict_column}.pkl'))
            loaded_models[predict_column] = load_model(os.path.join(model_path,f'new_mae_192_patchtst_{predict_column}.keras'), custom_objects={
                'PatchEmbedding': PatchEmbedding,
                'PositionalEncoding': PositionalEncoding
            })
    except Exception as e:
        logg(f"[PID|{proc_pid}].log", str(e))
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
            interval_sec = window_size+5
            colnames = ', '.join([f'"{col}"' for col in selected_cols + ["ProcessRecipeStepRemainTime", "ProcessRecipeStepID", "ProcessRecipeStepName"]])
            query = f"""
                SELECT {colnames}
                FROM "{table_name}"
                WHERE "Timestamp" BETWEEN
                    (%s::timestamp - INTERVAL '{interval_sec} seconds') AND %s::timestamp
            """
            data = pd.read_sql(query, conn, params=(last_date, last_date))
            step_data = data[["ProcessRecipeStepRemainTime", "ProcessRecipeStepID", "ProcessRecipeStepName"]]
            data = data[selected_cols + ['ProcessRecipeStepName']]
            data['PPExecStepID'] = [step_reverse_dict[str(name)] for name in data['ProcessRecipeStepName']]
            data.drop(columns=['ProcessRecipeStepName'], inplace=True)
            data.dropna(inplace=True)
            if len(data) < window_size:
                if len(prev_table_name) > 0:
                    query = f"""
                        SELECT {colnames}
                        FROM "{prev_table_name}"
                        WHERE "Timestamp" BETWEEN
                            (%s::timestamp - INTERVAL '{interval_sec} seconds') AND %s::timestamp
                    """

                    data2 = pd.read_sql(query, conn, params=(last_date, last_date))
                    data2 = data2[selected_cols + ['ProcessRecipeStepName']]
                    data2['PPExecStepID'] = [step_reverse_dict[str(name)] for name in data2['ProcessRecipeStepName']]
                    data2.drop(columns=['ProcessRecipeStepName'], inplace=True)
                    data2.dropna(inplace=True)
                    data = pd.concat([data2, data], ignore_index=True)
                else:
                    conn.close()
                    time.sleep(0.1)
                    continue
            if len(data) > window_size:
                data = data.tail(window_size)
            if len(data) < window_size:
                conn.close()
                time.sleep(0.1)
                continue
        except Exception as e:
            logg(f"[PID|{proc_pid}].log", " 데이터 쿼리시도중 오류발생")
            logg(f"[PID|{proc_pid}].log", str(e))
            conn.close()
            time.sleep(0.2)
            continue
        
        for predict_column in predict_columns:    
            try:
                X_data = scaler_X.transform(data.values)
                pred_scaled = loaded_models[predict_column].predict(np.array([X_data]), verbose=0)
                pred_dates = []
                for predict_step in predict_steps:
                    try:
                        pred_date = datetime.strptime(last_date, "%Y-%m-%d %H:%M:%S.%f") + timedelta(seconds=predict_step)
                    except:
                        pred_date = datetime.strptime(last_date, "%Y-%m-%d %H:%M:%S") + timedelta(seconds=predict_step)
                    pred_dates.append(pred_date)
                pred_datas = np.stack([
                        scaler_ys[predict_column].inverse_transform(pred_scaled[:, [i]])[:, 0]
                        for i in range(3)
                    ], axis=1)
                
                last_step_ids = []
                last_step_names = []
                for predict_step in predict_steps:
                    try:
                        remain_str = df.iloc[-1]['ProcessRecipeStepRemainTime']  
                        if remain_str == "00:00:00":
                            last_remain_sec = 100
                        else:
                            h, m, s = map(int, remain_str.split(":"))
                            last_remain_sec = h * 3600 + m * 60 + s
                        last_step_id = df.iloc[-1]["ProcessRecipeStepID"]
                        last_step_name = df.iloc[-1]["ProcessRecipeStepName"]
                        if last_remain_sec >= predict_step:
                            pass
                        else:
                            last_step_id = -1
                            last_step_name = 'UNKNOWN'               
                    except:
                            last_step_id = -1
                            last_step_name = 'UNKNOWN'
                    last_step_ids.append(last_step_id)
                    last_step_names.append(last_step_name)                
            except Exception as e:
                logg(f"[PID|{proc_pid}].log", "쿼리후 데이터 전처리 및 예측 시 오류발생")
                logg(f"[PID|{proc_pid}].log", str(e))
                time.sleep(0.1)
                continue
            
            for idx, predict_step in enumerate(predict_steps):
                pred_date = pred_dates[idx]
                pred_data = pred_datas[0,idx]
                last_step_id = last_step_ids[idx]
                last_step_name = last_step_names[idx]
                #print(pred_date, pred_data)
                try:
                    cur = conn.cursor()
                    
                    predict_column_modified = predict_column.replace('.', '_').replace(' ', '_').replace('-', '_')
                    save_table_name = f"pred_{predict_step}_{predict_column_modified}"
                    # 테이블 생성 (없을 경우)
                    create_query = f"""
                    CREATE TABLE IF NOT EXISTS "{save_table_name}" (
                        "Timestamp" TIMESTAMP PRIMARY KEY,
                        "Parameter" REAL,
                        "ProcessRecipeStepID" INTEGER,
                        "ProcessRecipeStepName" TEXT,
                        UNIQUE ("Timestamp", "Parameter")
                    );
                    """
                    cur.execute(create_query)

                    # 데이터 삽입
                    insert_query = f"""
                    INSERT INTO "{save_table_name}" ("Timestamp", "Parameter", "ProcessRecipeStepID", "ProcessRecipeStepName")
                    VALUES (%s::timestamp, %s::real, %s::integer, %s::text)
                    ON CONFLICT ("Timestamp", "Parameter") DO NOTHING
                    """
                    cur.execute(insert_query, (pred_date, float(pred_data), int(last_step_id), str(last_step_name)))
                    #print(predict_column, "END")
                except Exception as e:
                    logg(f"[PID|{proc_pid}].log", "예측데이터 DB insert 오류발생")
                    logg(f"[PID|{proc_pid}].log", str(e))
                    continue
                
                try:   
                    if last_step_id != -1 and pred_data is not None:
                        # limits.yaml 읽기
                        limits = {}
                        if os.path.exists("./fastapi/limits.yaml"):
                            with open("./fastapi/limits.yaml", "r", encoding="utf-8") as f:
                                limits = yaml.safe_load(f)
                            step_limits = limits.get(predict_column, {}).get(str(last_step_id))
                            if step_limits:
                                ts = str(pred_date)
                                if "min" in step_limits and pred_data <= step_limits["min"]:
                                    insert_violation(conn, cur, ts, predict_column, last_step_id, last_step_name, pred_data, 'min', step_limits["min"])
                                elif "max" in step_limits and pred_data >= step_limits["max"]:
                                    insert_violation(conn, cur, ts, predict_column, last_step_id, last_step_name, pred_data, 'max', step_limits["max"])
                    elif pred_data is not None:
                        limits = {}
                        if os.path.exists("./fastapi/limits.yaml"):
                            with open("./fastapi/limits.yaml", "r", encoding="utf-8") as f:
                                limits = yaml.safe_load(f)
                            step_limits = limits.get(predict_column, {}).get('all')
                            if step_limits:
                                ts = str(pred_date)
                                if "min" in step_limits and pred_data <= step_limits["min"]:
                                    insert_violation(conn, cur, ts, predict_column, last_step_id, last_step_name, pred_data, 'min', step_limits["min"])
                                elif "max" in step_limits and pred_data >= step_limits["max"]:
                                    insert_violation(conn, cur, ts, predict_column, last_step_id, last_step_name, pred_data, 'max', step_limits["max"])
                except Exception as e:
                    logg(f"[PID|{proc_pid}].log", "예측데이터 DB insert 오류발생")
                    logg(f"[PID|{proc_pid}].log", str(e))
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
            "Timestamp" TIMESTAMP  PRIMARY KEY,
            parameter TEXT NOT NULL,
            message TEXT NOT NULL,
            UNIQUE ("Timestamp", parameter)
        );
    """)
    conn.commit()
    cur.close()
    conn.close()
    
    obj_id_list = []
    cnt = 1
    tasks = []
    for predict_column in predict_columns:
        tasks.append(predict_column)
        if cnt % 3 == 0 or cnt == len(predict_columns):
            obj_id_list.append(ray_predict.remote(selected_cols, tasks, window_size, predict_steps))
            tasks = []
        cnt+=1
    
    while len(obj_id_list):
        done, obj_id_list = ray.wait(obj_id_list)
        ray.get(done[0])
        
    ray.shutdown()

