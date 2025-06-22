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
from dateutil import parser

window_size = 192  
predict_steps = [10, 20, 30]  
#예측할 칼럼 리스트
tasks = [      
    #'ProcessRecipeStepID',
    ['MFC7_DCS',          
    'MFC8_NH3',          
    'MFC26_F.PWR'],
    #'MFC9_F2',
    ['MFC1_N2-1',
    'MFC2_N2-2',         
    'MFC3_N2-3'], 
    ['MFC4_N2-4',          
    'MFC27_L.POS',        
    'MFC28_R.POS'],        
    ['VG11 Press value',              
    'VG12 Press value',                 # Baratron Gauge(의 압력 모니터링 값 (프로세스외 작용)
    'VG13 Press value'],                 # Baratron Gauge(의 압력 모니터링 값 (프로세스외 작용)
    ['Temp_Act_U',            # 상부 위치 실제 온도
    'Temp_Act_CU',           # 중앙 상부 위치 실제 온도
    'Temp_Act_C'],            # 중앙 위치 실제 온도
    ['Temp_Act_CL',           # 중앙 하부 위치 실제 온도
    'Temp_Act_L']    
]

temp_add_columns = [
    'Temp_Set_', #'Temp_Set_'
    'Temp_HT_Power_' #'Temp_HT_Power_'
]    
# 공정 모니터링 변수
#selected_cols = ['ProcessRecipeStepID', 'MFC1_N2-1', 'MFC2_N2-2', 'MFC3_N2-3', 'MFC4_N2-4', 'MFC26_F.PWR', 'MFC27_L.POS', 'MFC28_R.POS', 'MFC7_DCS', 'MFC8_NH3', 'MFC9_F2', 'APC Valve Value (Angle)', 'VG11 Press value', 'VG12 Press value', 'VG13 Press value', 'Temp_Act_U', 'Temp_Act_CU', 'Temp_Act_C', 'Temp_Act_CL', 'Temp_Act_L', 'ValveAct_1:1', 'ValveAct_2:2', 'ValveAct_3:3', 'ValveAct_4:4', 'ValveAct_5:5', 'ValveAct_9:9', 'ValveAct_11:11', 'ValveAct_12:12', 'ValveAct_14:14', 'ValveAct_15:15', 'ValveAct_16:16', 'ValveAct_26:26', 'ValveAct_28:28', 'ValveAct_29:29', 'ValveAct_30:30', 'ValveAct_60:71', 'ValveAct_63:75', 'ValveAct_73:83', 'ValveAct_75:85', 'ValveAct_76:86', 'ValveAct_80:DPO', 'ValveAct_86:HT1', 'ValveAct_87:HT2', 'ValveAct_88:HT3', 'ValveAct_89:RF', 'ValveAct_90:PST']
selected_cols = ['ProcessRecipeStepID', 'MFC1_N2-1', 'MFC2_N2-2', 'MFC3_N2-3', 'MFC4_N2-4', 'MFC26_F.PWR', 'MFC27_L.POS', 'MFC28_R.POS', 'MFC7_DCS', 'MFC8_NH3', 'MFC9_F2', 'APC Valve Value (Angle)', 'VG11 Press value', 'VG12 Press value', 'VG13 Press value', 'Temp_Act_U', 'Temp_Act_CU', 'Temp_Act_C', 'Temp_Act_CL', 'Temp_Act_L', 'ValveAct_2:2', 'ValveAct_3:3', 'ValveAct_4:4', 'ValveAct_5:5', 'ValveAct_9:9', 'ValveAct_12:12', 'ValveAct_14:14', 'ValveAct_16:16', 'ValveAct_26:26', 'ValveAct_28:28', 'ValveAct_29:29', 'ValveAct_60:71', 'ValveAct_63:75', 'ValveAct_73:83', 'ValveAct_80:DPO', 'ValveAct_89:RF', 'ValveAct_90:PST']
step_reverse_dict = {'END': 2, 'STANDBY': 255, 'START': 1, 'B.UP': 17, 'WAIT': 3, 'S.P-1': 74, 'S.P-2': 75, 'R.UP1': 25, 'STAB1': 22, 'S.P-3': 76, 'M.P-3': 81, 'L.CHK': 72, 'PREPRG1': 44, 'EVAC1': 99, 'EVAC2': 100, 'N-EVA1': 111, 'CLOSE1': 128, 'SI-FL1': 119, 'SI-EVA1': 117, 'CHANGE': 152, 'N-PRE1': 113, 'N-FL1': 115, 'N-FL2': 116, 'pre-NH3P': 110, 'DEPO1': 49, 'post_NH3P': 135, 'N2PRG1': 103, 'SI-EVA4': 149, 'A.VAC2': 85, 'A.PRG2': 90, 'A.VAC1': 84, 'A.PRG1': 89, 'N2PRG2': 104, 'N2PRG3': 105, 'A.VAC3': 86, 'A.PRG3': 91, 'A.VAC4': 87, 'A.PRG4': 92, 'CYCLE1': 130, 'A.PRG5': 93, 'R.DOWN1': 31, 'B.FILL1': 94, 'B.FILL2': 95, 'B.FILL3': 96, 'B.FILL4': 97, 'B.FILL5': 98, 'B.DOWN': 18, 'None': 0, 'nan': 0, 'NaN': 0, 'null': 0, 'NULL': 0, 'IDLE': 0}

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


def get_oldest_raw_times():
    conn = psycopg2.connect(dbname='postgres', user='keti', password='keti1234!', host='localhost', port=5432)
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema='public' AND table_name LIKE 'rawdata%';
        """)
        tables = [t[0] for t in cur.fetchall() if re.match(r'rawdata\d+$', t[0])]
        tables.sort(key=extract_date)
        oldest_table = tables[0] if tables else ''
        cur.execute(f'SELECT MIN("Timestamp") FROM "{oldest_table}"')
        result = cur.fetchone()
        if result:
            oldest_ts = result[0]
            oldest_ts = str(oldest_ts)
            if len(oldest_ts) == 26:
                oldest_ts = oldest_ts[:-3]
        else: oldest_ts = ""
    except:
        proc_pid = os.getpid()
        logg(f"[PID|{proc_pid}].log", "get_oldest_raw_times() 오류발생")
        logg(f"[PID|{proc_pid}].log", str(e))
        oldest_table = ""
    cur.close(); conn.close()
    return oldest_ts


def get_last_raw_times():
    conn = psycopg2.connect(dbname='postgres', user='keti', password='keti1234!', host='localhost', port=5432)
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema='public' AND table_name LIKE 'rawdata%';
        """)
        tables = [t[0] for t in cur.fetchall() if re.match(r'rawdata\d+$', t[0])]
        tables.sort(key=extract_date, reverse=True)
        lastest_table = tables[0] if tables else ''
        cur.execute(f'SELECT MAX("Timestamp") FROM "{lastest_table}"')
        result = cur.fetchone()
        if result:
            oldest_ts = result[0]
            oldest_ts = str(oldest_ts)
            if len(oldest_ts) == 26:
                oldest_ts = oldest_ts[:-3]
        else: oldest_ts = ""
    except:
        proc_pid = os.getpid()
        logg(f"[PID|{proc_pid}].log", "get_last_raw_times() 오류발생")
        logg(f"[PID|{proc_pid}].log", str(e))
    cur.close(); conn.close()
    return oldest_ts


def get_last_pred_times(predict_columns):
    conn = psycopg2.connect(dbname='postgres', user='keti', password='keti1234!', host='localhost', port=5432)
    cur = conn.cursor()
    times = {}
    step = predict_steps[0]
    max_ts = None
    for col in predict_columns:
        try:
            tbl = f'pred_{step}_{col.replace(".","_").replace(" ","_").replace("-","_")}'
            cur.execute(f"SELECT MAX(\"Timestamp\") FROM \"{tbl}\"")
            max_ts = cur.fetchone()[0]
            if not max_ts:
                max_ts = ""
            else:
                max_ts = str(max_ts)
                if len(max_ts) == 26:
                    max_ts = max_ts[:-3]
            times[col] = max_ts
        except Exception as e:
            proc_pid = os.getpid()
            logg(f"[PID|{proc_pid}].log", "get_last_pred_times() 오류발생")
            logg(f"[PID|{proc_pid}].log", str(e))
            times[col] = ""
    cur.close(); conn.close()
    return times


def insert_violation(timestamp, col, step_id, step_name, val, limit_type, threshold):
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432,
    )
    cur = conn.cursor()
    try:
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
    except Exception as e:
        proc_pid = os.getpid()
        logg(f"[PID|{proc_pid}].log", "insert_violation() 오류발생")
        logg(f"[PID|{proc_pid}].log", str(e))
    cur.close(); conn.close()
    
    
    
def get_data_by_start_end(selected_cols, start, end, add_columns):
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432,
    )
    cur = conn.cursor()

    from_ts = parser.parse(start)
    to_ts = parser.parse(end)
    date_suffix1 = from_ts.strftime("%Y%m%d")
    date_suffix2 = to_ts.strftime("%Y%m%d")

    raw_tables = [f"rawdata{date_suffix1}"]

    if len(str(from_ts)) >= 26:
        from_ts = str(from_ts)[:23]
        to_ts = str(to_ts)[:23]
    
    if date_suffix1 != date_suffix2:
        raw_tables.append(f"rawdata{date_suffix2}")
    dfs = []
    for raw_table in raw_tables:
        try:
            colnames = ', '.join([f'"{col}"' for col in selected_cols + add_columns + ["ProcessRecipeStepRemainTime", "ProcessRecipeStepName", "Timestamp"]])
            query = f"""
                SELECT {colnames}
                FROM "{raw_table}"
                WHERE "Timestamp" BETWEEN
                    %s::timestamp AND %s::timestamp
            """
            df = pd.read_sql(query, conn, params=(from_ts, to_ts))
            dfs.append(df)
        except Exception as e:
            proc_pid = os.getpid()
            logg(f"[PID|{proc_pid}].log", "get_data_by_start_end() 오류발생")
            logg(f"[PID|{proc_pid}].log", str(e))
    data = pd.concat(dfs, ignore_index = True)
    cur.close(); conn.close()
    return data


def insert_pred_data(predict_column, predict_step, pred_date, pred_data, last_step_id, last_step_name):
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432,
    )
    cur = conn.cursor()
    try:
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
        conn.commit()
        #print(predict_column, "END")
    except Exception as e:
        proc_pid = os.getpid()
        logg(f"[PID|{proc_pid}].log", "insert_pred_data() 오류발생")
        logg(f"[PID|{proc_pid}].log", str(e))
    cur.close(); conn.close()


# 학습 시 loss weighting:
def get_weighted_mae(lval, hval, add_wight):
    def loss(y_true, y_pred):
        weights = tf.where(tf.logical_and(y_true >= lval, y_true <= hval), add_wight, 1.0)  # 중심 정규화 기준
        delta = tf.abs(y_true - y_pred)
        return tf.reduce_mean(weights * delta)
    return loss


# 각 칼럼별 예측 수행
@ray.remote
def ray_predict(selected_cols, predict_columns, window_size, predict_steps, model_path = './model', scaler_path = './model/scaler'):
    #print(predict_columns)
    proc_pid = os.getpid()
    next_pred_start_ts = ""
    scaler_X = None
    scaler_ys = {}
    loaded_models = {}
    now_raw_time = ""
    try:
        scaler_X = joblib.load(os.path.join(scaler_path,'scaler_X.pkl'))
        for predict_column in predict_columns:
            loss_func = 'mae'
            scaler_ys[predict_column] = joblib.load(os.path.join(scaler_path,f'scaler_y_{predict_column}.pkl'))
            if predict_column == "VG11": 
                # 커스텀 weighted loss 함수 생성
                y_low, y_high = scaler_ys[predict_column].transform([[0]]), scaler_ys[predict_column].transform([[9]])
                loss_func = get_weighted_mae(y_low, y_high, 100.0)
            loaded_models[predict_column] = load_model(os.path.join(model_path,f'192_patchtst_{predict_column}.keras'), custom_objects={
                'PatchEmbedding': PatchEmbedding,
                'PositionalEncoding': PositionalEncoding,
                'loss': loss_func
            })
    except Exception as e:
        logg(f"[PID|{proc_pid}].log", f"{predict_columns} 모델 및 scaler 로드중 오류발생")
        logg(f"[PID|{proc_pid}].log", str(e))
        return
        
    while True:
        # 1. 쿼리 시점 탐색
        last_raw_time = get_last_raw_times()
        if last_raw_time == "" or now_raw_time >= last_raw_time:
            time.sleep(0.1)
            continue
        now_raw_time = last_raw_time
        try:
            end = datetime.strptime(now_raw_time, "%Y-%m-%d %H:%M:%S.%f")
            start = datetime.strptime(now_raw_time, "%Y-%m-%d %H:%M:%S.%f") - timedelta(seconds=window_size)
            start = start.strftime("%Y-%m-%d %H:%M:%S.%f")
            end = end.strftime("%Y-%m-%d %H:%M:%S.%f")
        except:
            end = datetime.strptime(now_raw_time, "%Y-%m-%d %H:%M:%S")
            start = datetime.strptime(now_raw_time, "%Y-%m-%d %H:%M:%S") - timedelta(seconds=window_size)
            start = start.strftime("%Y-%m-%d %H:%M:%S")
            end = end.strftime("%Y-%m-%d %H:%M:%S")

        '''
        try:
            start = datetime.strptime(now_raw_time, "%Y-%m-%d %H:%M:%S.%f") - timedelta(seconds=window_size+1)
            end = start + timedelta(seconds=window_size+2)
            start = start.strftime("%Y-%m-%d %H:%M:%S.%f")
            end = end.strftime("%Y-%m-%d %H:%M:%S.%f")
        except:
            start = datetime.strptime(now_raw_time, "%Y-%m-%d %H:%M:%S") - timedelta(seconds=window_size+1)
            end = start + timedelta(seconds=window_size+2)
            start = start.strftime("%Y-%m-%d %H:%M:%S")
            end = end.strftime("%Y-%m-%d %H:%M:%S")
        '''
        if len(start) == 26: start = start[:-3]
        if len(end) == 26: end = end[:-3]
        # 2. 데이터 쿼리
        add_columns = []
        for predict_column in predict_columns:
            if 'Temp_Act_' in predict_column:
                scaler_X = joblib.load(os.path.join(scaler_path,f'scaler_X_{predict_column}.pkl'))
                temp_pos = predict_column.split('_')[-1]
                for add_col in temp_add_columns:
                    add_columns.append(add_col+temp_pos)
        data = get_data_by_start_end(selected_cols, start, end, add_columns)
        data['ProcessRecipeStepID'] = data['ProcessRecipeStepID'].replace(255, 0)
        data.fillna(method='ffill', inplace=True)
        #print(1,len(data))
        # 빈 시점 탐색
        continue_flag = False
        new_rows = []
        for i in range(len(data) - 1):
            row_current = data.iloc[i]
            row_next = data.iloc[i + 1]
            ts_current = row_current['Timestamp']
            ts_next = row_next['Timestamp']
            diff = (ts_next - ts_current).total_seconds()
            # 현재 행 추가
            new_rows.append(row_current)
            # 4초 이상 데이터가 빌경우 skip
            if diff >= 4:
                continue_flag = True
                break
            # 간격이 1.5초 초과 시 보간 대상
            elif diff > 1.5:
                # 1초 간격으로 Timestamp 생성 (ts_current 제외, ts_next 제외)
                n_inserts = int(diff)  # 초 단위로 보간
                for j in range(1, n_inserts):
                    new_ts = ts_current + pd.Timedelta(seconds=j)
                    # ProcessRecipeStepID는 이전 값 사용
                    interpolated = row_current.copy()
                    interpolated['Timestamp'] = new_ts
                    new_rows.append(interpolated)
        if continue_flag:# 중간에 2초이상 빈 데이터가 있을경우
            time.sleep(0.1)
            continue
        # 마지막 행 추가
        new_rows.append(data.iloc[-1])
        # DataFrame으로 변환 후 정렬
        data = pd.DataFrame(new_rows).reset_index(drop=True)
        #print(2,len(data))
        if len(data) < window_size: # 시퀀스 데이터 부족
            time.sleep(0.1)
            continue
        if len(data) > window_size:
            data = data.tail(window_size)
        last_date = str(data.iloc[-1]['Timestamp'])
        data.drop(columns=["Timestamp"], inplace=True)
        
        if len(last_date) == 26: last_date = last_date[:-3]
        #print(predict_column,last_raw_time, start, last_date)
        step_data = data[["ProcessRecipeStepRemainTime", "ProcessRecipeStepID", "ProcessRecipeStepName"]]   
        for predict_column in predict_columns:
            # 3. 데이터 전처리
            try:
                add_columns = []
                if 'Temp_Act_' in predict_column:
                    scaler_X = joblib.load(os.path.join(scaler_path,f'scaler_X_{predict_column}.pkl'))
                    temp_pos = predict_column.split('_')[-1]
                    for add_col in temp_add_columns:
                        add_columns.append(add_col+temp_pos)
                sequence_data = data[selected_cols + add_columns]
                X_data = scaler_X.transform(sequence_data.values)
            except Exception as e:
                logg(f"[PID|{proc_pid}].log", "ray_predict() : 데이터 전처리 오류발생")
                logg(f"[PID|{proc_pid}].log", str(e))
                time.sleep(0.1)
                continue
            
            # 4. 예측 (모델 추론)
            try:
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
            except Exception as e:
                logg(f"[PID|{proc_pid}].log", "ray_predict() : model predict 오류발생")
                logg(f"[PID|{proc_pid}].log", str(e))
                time.sleep(0.1)
                continue
            
            # 5. 상하한선 터치 검사
            last_step_ids = []
            last_step_names = []
            for predict_step in predict_steps:
                try:
                    remain_str = step_data.iloc[-1]['ProcessRecipeStepRemainTime']  
                    if remain_str == "00:00:00":
                        last_remain_sec = 100
                    else:
                        h, m, s = map(int, remain_str.split(":"))
                        last_remain_sec = h * 3600 + m * 60 + s
                    if last_remain_sec >= predict_step:
                        last_step_id = step_data.iloc[-1]["ProcessRecipeStepID"]
                        last_step_name = step_data.iloc[-1]["ProcessRecipeStepName"]
                    else:
                        last_step_id = -1
                        last_step_name = 'UNKNOWN'               
                except:
                        last_step_id = -1
                        last_step_name = 'UNKNOWN'
                last_step_ids.append(last_step_id)
                last_step_names.append(last_step_name)             
                        
            # 6. 에측데이터 저장
            for idx, predict_step in enumerate(predict_steps):
                pred_date = pred_dates[idx]
                pred_data = pred_datas[0,idx]
                last_step_id = last_step_ids[idx]
                last_step_name = last_step_names[idx]
                #print(pred_date, pred_data)
                insert_pred_data(predict_column, predict_step, pred_date, pred_data, last_step_id, last_step_name)
                
                # 7. 상하한선 터치 이벤트 데이터 저장
                try:   
                    if last_step_id != -1 and pred_data is not None:
                        # limits.yaml 읽기
                        limits = {}
                        if os.path.exists("./fastapi/limits.yaml"):
                            with open("./fastapi/limits.yaml", "r", encoding="utf-8") as f:
                                limits = yaml.safe_load(f)
                            step_limits = limits.get(predict_column, {}).get(str(last_step_id))
                            if step_limits:
                                if "min" in step_limits and pred_data <= step_limits["min"]:
                                    insert_violation(str(pred_date), predict_column, last_step_id, last_step_name, pred_data, 'min', step_limits["min"])
                                elif "max" in step_limits and pred_data >= step_limits["max"]:
                                    insert_violation(str(pred_date), predict_column, last_step_id, last_step_name, pred_data, 'max', step_limits["max"])
                    elif pred_data is not None:
                        limits = {}
                        if os.path.exists("./fastapi/limits.yaml"):
                            with open("./fastapi/limits.yaml", "r", encoding="utf-8") as f:
                                limits = yaml.safe_load(f)
                            step_limits = limits.get(predict_column, {}).get('all')
                            if step_limits:
                                if "min" in step_limits and pred_data <= step_limits["min"]:
                                    insert_violation(str(pred_date), predict_column, last_step_id, last_step_name, pred_data, 'min', step_limits["min"])
                                elif "max" in step_limits and pred_data >= step_limits["max"]:
                                    insert_violation(str(pred_date), predict_column, last_step_id, last_step_name, pred_data, 'max', step_limits["max"])
                except Exception as e:
                    logg(f"[PID|{proc_pid}].log", "ray_predict() : 상하한 터치 이벤트 처리시 오류발생")
                    logg(f"[PID|{proc_pid}].log", str(e))
                    time.sleep(0.1)
                    continue
    
    
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

    for task in tasks:
        obj_id_list.append(ray_predict.remote(selected_cols, task, window_size, predict_steps))
    while len(obj_id_list):
        done, obj_id_list = ray.wait(obj_id_list)
        ray.get(done[0])
        
    ray.shutdown()

