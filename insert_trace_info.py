import psycopg2
import re
import time
from datetime import datetime, timedelta
import os
import pandas as pd
import numpy as np
import joblib
import xgboost as xgb
from tensorflow.keras.models import load_model
from tensorflow.keras import layers
import tensorflow as tf

selected_cols = ['ProcessRecipeStepID', 'MFC1_N2-1', 'MFC2_N2-2', 'MFC3_N2-3', 'MFC4_N2-4', 'MFC26_F.PWR', 'MFC27_L.POS', 'MFC28_R.POS', 'MFC7_DCS', 'MFC8_NH3', 'MFC9_F2', 'APC Valve Value (Angle)', 'VG11 Press value', 'VG12 Press value', 'VG13 Press value', 'Temp_Act_U', 'Temp_Act_CU', 'Temp_Act_C', 'Temp_Act_CL', 'Temp_Act_L', 'ValveAct_2:2', 'ValveAct_3:3', 'ValveAct_4:4', 'ValveAct_5:5', 'ValveAct_9:9', 'ValveAct_12:12', 'ValveAct_14:14', 'ValveAct_16:16', 'ValveAct_26:26', 'ValveAct_28:28', 'ValveAct_29:29', 'ValveAct_60:71', 'ValveAct_63:75', 'ValveAct_73:83', 'ValveAct_80:DPO', 'ValveAct_89:RF', 'ValveAct_90:PST']
step_reverse_dict = {'END': 2, 'STANDBY': 0, 'START': 1, 'B.UP': 17, 'WAIT': 3, 'S.P-1': 74, 'S.P-2': 75, 'R.UP1': 25, 'STAB1': 22, 'S.P-3': 76, 'M.P-3': 81, 'L.CHK': 72, 'PREPRG1': 44, 'EVAC1': 99, 'EVAC2': 100, 'N-EVA1': 111, 'CLOSE1': 128, 'SI-FL1': 119, 'SI-EVA1': 117, 'CHANGE': 152, 'N-PRE1': 113, 'N-FL1': 115, 'N-FL2': 116, 'pre-NH3P': 110, 'DEPO1': 49, 'post_NH3P': 135, 'N2PRG1': 103, 'SI-EVA4': 149, 'A.VAC2': 85, 'A.PRG2': 90, 'A.VAC1': 84, 'A.PRG1': 89, 'N2PRG2': 104, 'N2PRG3': 105, 'A.VAC3': 86, 'A.PRG3': 91, 'A.VAC4': 87, 'A.PRG4': 92, 'CYCLE1': 130, 'A.PRG5': 93, 'R.DOWN1': 31, 'B.FILL1': 94, 'B.FILL2': 95, 'B.FILL3': 96, 'B.FILL4': 97, 'B.FILL5': 98, 'B.DOWN': 18, 'None': 0, 'nan': 0, 'NaN': 0, 'null': 0, 'NULL': 0, 'IDLE': 0}
column_types = {'Timestamp': 'TIMESTAMP', 'ObservableTimestamp': 'TEXT', 'EquipmentStatus': 'INTEGER', 'AlarmState': 'BOOLEAN', 'O2Density_Monitor_Value': 'REAL', 'O2Density_Set_Value': 'REAL', 'PMstatus': 'INTEGER', 'PPExecname': 'TEXT', 'PPExecStepSeqNo': 'INTEGER', 'PPExecStepID': 'INTEGER', 'PPExecStepName': 'TEXT', 'ActiveCjobID': 'TEXT', 'ActivePjobID': 'TEXT', 'PMStoredProcessRecipeName': 'TEXT', 'ProcessRecipeEndRemainTime': 'TEXT', 'ProcessRecipeStepTime': 'TEXT', 'ProcessRecipeStepRemainTime': 'TEXT', 'ProcessRecipeStepID': 'INTEGER', 'ProcessRecipeStepName': 'TEXT', 'ProcessRecipeStepSeqNo': 'INTEGER', 'ProcessRecipeTotalTime': 'TEXT', 'Temp_Set_U': 'REAL', 'Temp_HT_Power_U': 'REAL', 'Temp_Monitor_U': 'REAL', 'Temp_TC_Monitor_U': 'REAL', 'Temp_TC_Cascade_U ': 'REAL', 'Temp_Act_U': 'REAL', 'Temp_HT_Power_Cascade_U': 'REAL', 'Temp_Set_CU': 'REAL', 'Temp_HT_Power_CU': 'REAL', 'Temp_Monitor_CU': 'REAL', 'Temp_TC_Monitor_CU': 'REAL', 'Temp_TC_Cascade_CU': 'REAL', 'Temp_Act_CU': 'REAL', 'Temp_HT_Power_Cascade_CU': 'REAL', 'Temp_Set_C': 'REAL', 'Temp_HT_Power_C': 'REAL', 'Temp_Monitor_C': 'REAL', 'Temp_TC_Monitor_C': 'REAL', 'Temp_TC_Cascade_C': 'REAL', 'Temp_Act_C': 'REAL', 'Temp_HT_Power_Cascade_C': 'REAL', 'Temp_Set_CL': 'REAL', 'Temp_HT_Power_CL': 'REAL', 'Temp_Monitor_CL': 'REAL', 'Temp_TC_Monitor_CL': 'REAL', 'Temp_TC_Cascade_CL': 'REAL', 'Temp_Act_CL': 'REAL', 'Temp_HT_Power_Cascade_CL': 'REAL', 'Temp_Set_L': 'REAL', 'Temp_HT_Power_L': 'REAL', 'Temp_Monitor_L': 'REAL', 'Temp_TC_Monitor_L': 'REAL', 'Temp_TC_Cascade_L': 'REAL', 'Temp_Act_L': 'REAL', 'Temp_HT_Power_Cascade_L': 'REAL', 'APC Valve Value (Angle)': 'REAL', 'VG13_LeakPressure_Monitor': 'REAL', 'VG11_LeakPressure_Monitor': 'REAL', 'VG13_LeakQuantity_Monitor': 'REAL', 'VG11_LeakQuantity_Monitor': 'REAL', 'VG13 Press value': 'REAL', 'VG11 Press value': 'REAL', 'PJobProcessingState': 'INTEGER', 'ValveAct_1:1': 'REAL', 'ValveAct_2:2': 'REAL', 'ValveAct_3:3': 'REAL', 'ValveAct_4:4': 'REAL', 'ValveAct_5:5': 'REAL', 'ValveAct_9:9': 'REAL', 'ValveAct_11:11': 'REAL', 'ValveAct_12:12': 'REAL', 'ValveAct_14:14': 'REAL', 'ValveAct_15:15': 'REAL', 'ValveAct_16:16': 'REAL', 'ValveAct_26:26': 'REAL', 'ValveAct_28:28': 'REAL', 'ValveAct_29:29': 'REAL', 'ValveAct_30:30': 'REAL', 'ValveAct_60:71': 'REAL', 'ValveAct_63:75': 'REAL', 'ValveAct_73:83': 'REAL', 'ValveAct_75:85': 'REAL', 'ValveAct_76:86': 'REAL', 'ValveAct_80:DPO': 'REAL', 'ValveAct_86:HT1': 'REAL', 'ValveAct_87:HT2': 'REAL', 'ValveAct_88:HT3': 'REAL', 'ValveAct_89:RF': 'REAL', 'ValveAct_90:PST': 'REAL', 'ValveAct_95:WAT': 'REAL', 'SubRecipeLoopSettingValue': 'INTEGER', 'SubRecipeLoopMoniterValue': 'INTEGER', 'VG12_LeakPressure_Monitor': 'REAL', 'VG12 Press value': 'REAL', 'MFC1_N2-1': 'REAL', 'MFC2_N2-2': 'REAL', 'MFC3_N2-3': 'REAL', 'MFC4_N2-4': 'REAL', 'MFC7_DCS': 'REAL', 'MFC8_NH3': 'REAL', 'MFC9_F2': 'REAL', 'MFC10_N2-R': 'REAL', 'MFC11_NO': 'REAL', 'MFC12_DCSMFM_7': 'REAL', 'MFC16_MFC51 N2': 'REAL', 'MFC26_F.PWR': 'REAL', 'MFC27_L.POS': 'REAL', 'MFC28_R.POS': 'REAL', 'AUX1_MS1': 'REAL', 'AUX2_MS321': 'REAL', 'AUX3_MS2': 'REAL', 'AUX4_MS3': 'REAL', 'AUX5_MS5': 'REAL', 'AUX8_MS8': 'REAL', 'AUX9_MS9': 'REAL', 'AUX16_VG21': 'REAL', 'AUX18_VG12': 'REAL', 'AUX19_VG11': 'REAL', 'AUX20_VG13': 'REAL', 'AUX21_M.WAT': 'REAL', 'AUX22_FS101': 'REAL', 'AUX23_FS102': 'REAL', 'AUX24_FS104': 'REAL', 'AUX26_FS106': 'REAL', 'AUX28_FS111': 'REAL', 'AUX29_FS105': 'REAL', 'AUX33_G.PS1': 'REAL', 'AUX34_G.PS2': 'REAL', 'AUX35_G.PS3': 'REAL', 'AUX36_G.PS4': 'REAL', 'AUX37_G.PS5': 'REAL', 'AUX38_G.PS6': 'REAL', 'AUX45_G.PS13': 'REAL', 'AUX46_G.PS14': 'REAL', 'AUX47_G.PS15': 'REAL', 'AUX48_G.PS16': 'REAL', 'AUX50_Vpp': 'REAL', 'AUX51_Vdc': 'REAL', 'AUX52_R.PWR': 'REAL', 'AUX53_DCS_IN': 'REAL', 'AUX54_IGS_DCS': 'REAL', 'AUX56_DCS1_PIP': 'REAL', 'AUX58_PURGE-1': 'REAL', 'AUX59_DCS_TANK': 'REAL', 'AUX64_REC-1': 'REAL', 'AUX65_REC-2-1': 'REAL', 'AUX66_REC-2-2': 'REAL', 'AUX67_IGS_N2-1': 'REAL', 'AUX69_SEALCAP': 'REAL', 'AUX72_RAXIS': 'REAL', 'AUX73_APC_RING': 'REAL', 'AUX74_APC_OUT': 'REAL', 'AUX89_JH1': 'REAL', 'AUX90_JH2': 'REAL', 'AUX91_JH3': 'REAL', 'AUX92_JH4': 'REAL', 'AUX93_JH5': 'REAL', 'AUX94_JH6': 'REAL', 'AUX95_JH7': 'REAL', 'AUX96_JH8': 'REAL', 'AUX97_JH9': 'REAL', 'AUX98_JH10': 'REAL', 'AUX99_JH11': 'REAL', 'AUX100_JH12': 'REAL', 'AUX101_JH13': 'REAL'}
temp_add_columns = [
    'Temp_Set_',
    'Temp_HT_Power_'
]
window_size = 192  
predict_step = 10
predict_columns = [      
    'MFC7_DCS',          
    'MFC8_NH3',          
    'MFC26_F.PWR',
    'MFC1_N2-1',
    'MFC2_N2-2',         
    'MFC3_N2-3', 
    'MFC4_N2-4',          
    'MFC27_L.POS',        
    'MFC28_R.POS',        
    'VG11 Press value',              
    'VG12 Press value',                 # Baratron Gauge(의 압력 모니터링 값 (프로세스외 작용)
    'VG13 Press value',                 # Baratron Gauge(의 압력 모니터링 값 (프로세스외 작용)
    'Temp_Act_U',            # 상부 위치 실제 온도
    'Temp_Act_CU',           # 중앙 상부 위치 실제 온도
    'Temp_Act_C',            # 중앙 위치 실제 온도
    'Temp_Act_CL',           # 중앙 하부 위치 실제 온도
    'Temp_Act_L'    
]

def fetch_trace_data(start_ts, end_ts, start_table, end_table):
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432
    )

    all_data = []
    add_columns = []
    for predict_column in predict_columns:
        if 'Temp_Act_' in predict_column:
            temp_pos = predict_column.split('_')[-1]
            for add_col in temp_add_columns:
                add_columns.append(add_col+temp_pos)
    colnames = ', '.join([f'"{col}"' for col in ["Timestamp"] + selected_cols + add_columns])

    start_date = datetime.strptime(start_table.replace("rawdata", ""), "%Y%m%d")
    end_date = datetime.strptime(end_table.replace("rawdata", ""), "%Y%m%d")

    if start_table == end_table:
        # ✅ 단일 테이블 처리
        query = f'''
            SELECT {colnames}
            FROM "{start_table}"
            WHERE "Timestamp" BETWEEN %s AND %s
        '''
        try:
            df = pd.read_sql(query, conn, params=(start_ts, end_ts))
            all_data.append(df)
        except Exception as e:
            print(f"❗ {start_table} 조회 실패: {e}")
    else:
        current_date = start_date
        while current_date <= end_date:
            table_name = f'rawdata{current_date.strftime("%Y%m%d")}'
            print(f"📘 테이블 조회: {table_name}")

            # 조건 분기: 시작/중간/종료 테이블
            if current_date == start_date:
                where_clause = 'WHERE "Timestamp" >= %s'
                params = (start_ts,)
            elif current_date == end_date:
                where_clause = 'WHERE "Timestamp" <= %s'
                params = (end_ts,)
            else:
                where_clause = ''
                params = ()

            try:
                query = f'''
                    SELECT {colnames}
                    FROM "{table_name}"
                    {where_clause}
                '''
                df = pd.read_sql(query, conn, params=params)
                all_data.append(df)
            except Exception as e:
                print(f"❗ {table_name} 조회 실패: {e}")

            current_date += timedelta(days=1)

    conn.close()

    # 두 개 이상 테이블을 사용할 경우 concat
    final_df = pd.concat(all_data, ignore_index=True)
    final_df.sort_values(["Timestamp"], inplace=True)
    #print(final_df)
    final_df.reset_index(drop=True, inplace=True)
    return final_df

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


# 학습 시 loss weighting:
def get_weighted_mae(lval, hval, add_wight):
    def loss(y_true, y_pred):
        weights = tf.where(tf.logical_and(y_true >= lval, y_true <= hval), add_wight, 1.0)  # 중심 정규화 기준
        delta = tf.abs(y_true - y_pred)
        return tf.reduce_mean(weights * delta)
    return loss

# 시퀀스 데이터 생성
def create_sequence(X, window, pred_step):
    X_seqs=[]
    for i in range(len(X) - window - pred_step):
        X_seqs.append(X[i:i+window])
    return np.array(X_seqs)


def predict_trace_parameter(start_ts, end_ts, start_table, end_table, model_path = './model', scaler_path = './model/scaler'):
    data = fetch_trace_data(start_ts, end_ts, start_table, end_table)
    data['ProcessRecipeStepID'] = data['ProcessRecipeStepID'].replace(255, 0)
    data.fillna(method='ffill', inplace=True)
    time_data = data['Timestamp'] + timedelta(seconds=window_size -1 + predict_step)
    time_data = data['Timestamp'].iloc[window_size - 1 + predict_step:].reset_index(drop=True)
    pred_datas = {}
    for predict_column in predict_columns:
        scaler_y = joblib.load(os.path.join(scaler_path,f'scaler_y_{predict_column}.pkl'))
        loss_func = 'mae'
        if predict_column == "VG11": 
            # 커스텀 weighted loss 함수 생성
            y_low, y_high = scaler_y.transform([[0]]), scaler_y.transform([[9]])
            loss_func = get_weighted_mae(y_low, y_high, 100.0)
        loaded_model = load_model(os.path.join(model_path,f'192_patchtst_{predict_column}.keras'), custom_objects={
            'PatchEmbedding': PatchEmbedding,
            'PositionalEncoding': PositionalEncoding,
            'loss': loss_func
        })
        try:
            add_columns = []
            if 'Temp_Act_' in predict_column:
                scaler_X = joblib.load(os.path.join(scaler_path,f'scaler_X_{predict_column}.pkl'))
                temp_pos = predict_column.split('_')[-1]
                for add_col in temp_add_columns:
                    add_columns.append(add_col+temp_pos)
            else:
                scaler_X = joblib.load(os.path.join(scaler_path,'scaler_X.pkl'))
            #print(predict_column, add_columns)
            sequence_data = data[selected_cols + add_columns]
            X_scaled = scaler_X.transform(sequence_data.values)
            X_seq = create_sequence(X_scaled, window_size, predict_step)
            y_pred_scaled = loaded_model.predict(X_seq, verbose=0)
            y_pred = np.stack([
                scaler_y.inverse_transform(y_pred_scaled[:, [i]])[:, 0]
                for i in range(3)
            ], axis=1)
            #print(y_pred[:, 0])
            # 예측값 생성 (1-step만 사용하는 경우)
            pred_values = [float(y_pred[i, 0]) for i in range(len(y_pred))]
            pred_datas[predict_column] = pred_values
            print(predict_column, len(pred_values))

            # time_data 길이 조정: pred_values와 동일한 길이로 맞춤
            if len(pred_datas[predict_column]) < len(time_data):
                time_data = time_data.iloc[:len(pred_values)].reset_index(drop=True)

        except Exception as e:
            print("predict parameter : 데이터 전처리 오류발생")
            print(str(e))
            continue
    pred_df = pd.DataFrame(pred_datas)
    trace_pred_df = pd.concat([time_data.reset_index(drop=True), pred_df.reset_index(drop=True)], axis=1)
    return trace_pred_df
    

def predict_thickness(start_ts, end_ts, start_table, end_table):
    data = fetch_trace_data(start_ts, end_ts, start_table, end_table)
    
    X_all = []
    data = data[selected_cols]
    data.fillna(method='ffill', inplace=True)
    tdf = data[(data['ProcessRecipeStepID'] >= 100) & (data['ProcessRecipeStepID'] < 160)]
    if len(tdf) <= 300:
        return []
    end_i = tdf.index[-1]+1
    start_i = tdf.index[0]
    data = data.iloc[start_i : end_i]
    data.reset_index(drop=False, inplace=True)
    start_index_value = data['index'].iloc[0]
    data['seconds'] = data['index'] - start_index_value
    data.drop(columns='index', inplace=True)

    # --- [중요] 입력 피처 생성 ---
    features = []
    stats = data.agg(['mean', 'std', 'min', 'max', 'median'])
    features.extend(stats.values.flatten())

    # --- Append to list ---
    X_all.append(features)

    # --- 최종 DataFrame 변환 ---
    X_all = np.array(X_all)
    #print(f" 전체 데이터셋 크기: {X_all.shape}")
    
    dtest = xgb.DMatrix(X_all)

    # 경로 설정
    model_dir = './xgb_model'
    model_num = len([f for f in os.listdir(model_dir) if f.endswith('.json')])

    # best_iters 로딩
    best_iters = joblib.load(os.path.join(model_dir, "best_iters.pkl"))

    # 모델 로딩
    loaded_models = []
    for i in range(model_num):
        model = xgb.Booster()
        model.load_model(os.path.join(model_dir, f"xgb_model_{i}.json"))
        loaded_models.append(model)
        
    # === 데이터로 예측 ===
    y_preds = []
    for i,model in enumerate(loaded_models):
        y_pred_i = model.predict(dtest, iteration_range=(0, best_iters[i] + 1))
        y_preds.append(y_pred_i)

    # (45, N) → (N, 45)로 transpose
    y_pred = np.array(y_preds).T
    ret = []
    for thicks in list(y_pred[0]):
        ret.append(float(thicks))
    return ret
    
    
def print_existing_trace_info():
    """실행 전에 지금까지 저장된 모든 공정 구간 출력 (오래된 순)"""
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432
    )
    cur = conn.cursor()

    # trace_info 테이블이 없을 수도 있으므로 CREATE 먼저
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

    # 저장된 모든 공정 구간 출력
    cur.execute("""
        SELECT start_time, end_time, start_table, end_table
        FROM trace_info
        ORDER BY start_time ASC;
    """)
    rows = cur.fetchall()

    if not rows:
        print("📂 저장된 공정 이력이 없습니다.")
    else:
        print(f"\n📄 지금까지 저장된 공정 정보 ({len(rows)}건):")
        for idx, (start, end, s_tbl, e_tbl) in enumerate(rows, 1):
            print(f"  {idx:03d}. {start} ~ {end} ({s_tbl} → {e_tbl})")

    cur.close()
    conn.close()
    
def insert_trace_pred(pred_df):
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432,
    )
    cur = conn.cursor()
    table_name = "trace_pred_data"
    # 테이블 생성    
    columns_sql = ',\n    '.join([
        f'"{col}" {column_types.get(col, "TEXT")}' for col in ['Timestamp'] + predict_columns])
    create_sql = f'''
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        {columns_sql},
        PRIMARY KEY ("Timestamp")
    );
    '''
    cur.execute(create_sql)
    conn.commit()
    try:
        # INSERT
        placeholders = ','.join(['%s'] * len( ['Timestamp'] + predict_columns))
        colnames = ','.join([f'"{c}"' for c in  ['Timestamp'] + predict_columns])
        insert_sql = f'''
            INSERT INTO "{table_name}" ({colnames})
            VALUES ({placeholders})
            ON CONFLICT ("Timestamp") DO NOTHING;
        '''
        rows = list(pred_df.itertuples(index=False, name=None))
        cur.executemany(insert_sql, rows)
        conn.commit()
        cur.close()
        conn.close()

        print(f"{len(rows)} rows inserted into {table_name}")
    except Exception as e:
        print(f'[다중 rows INSERT 중 에러발생] {e}')
    

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



def extract_process_ranges_incrementally():
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432
    )
    cur = conn.cursor()

    # 1. trace_info 테이블 생성 (최초 1회)
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

    # 2. 마지막 저장된 공정 end_time 조회
    cur.execute("SELECT MAX(end_time) FROM trace_info;")
    result = cur.fetchone()
    last_end_time = result[0] if result and result[0] else None

    if last_end_time:
        print(f"📌 마지막 공정 종료시각: {last_end_time}")
        last_date = int(last_end_time.strftime("%Y%m%d"))
    else:
        print("📌 이전 공정 기록 없음. 전체 테이블 탐색 시작")
        last_date = 0

    # 3. 테이블 목록 중 이후 날짜만 처리
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name ~ '^rawdata\\d{8}$';
    """)
    tables = [t[0] for t in cur.fetchall()]
    tables_filtered = sorted([
        t for t in tables if int(t.replace("rawdata", "")) >= last_date
    ], key=lambda x: int(x.replace("rawdata", "")))

    # 상태 변수
    current_proc = None
    last_ts = None
    last_table = None

    for table in tables_filtered:
        print(f"📘 처리 중: {table}")
        query = f"""
            SELECT "Timestamp", "ProcessRecipeStepName"
            FROM "{table}"
            WHERE "ProcessRecipeStepName" IS NOT NULL
            ORDER BY "Timestamp" ASC;
        """
        cur.execute(query)
        rows = cur.fetchall()

        for ts, step in rows:
            # 마지막 처리된 이후부터만
            if last_end_time and ts <= last_end_time:
                continue

            step = step.strip().upper() if step else ""

            if current_proc is None:
                if step in ("STANDBY", "START"):
                    current_proc = {
                        "start_time": ts,
                        "start_table": table
                    }
            else:
                if step == "END":
                    duration = ts - current_proc["start_time"]
                    if duration >= timedelta(hours=1):
                        thicknesses = predict_thickness(current_proc["start_time"], ts, current_proc["start_table"], table)
                        if len(thicknesses) == 0:
                            thicknesses = [0 for _ in range(45)]
                        insert_trace_info_with_thickness(current_proc["start_time"], ts, current_proc["start_table"], table, thicknesses)
                        print(current_proc["start_time"], ts, thicknesses, '\n')
                        
                        pred_df = predict_trace_parameter(current_proc["start_time"], ts, current_proc["start_table"], table)
                        insert_trace_pred(pred_df)
                        print(f"예측데이터 저장완료")
                        
                    current_proc = None
                elif step in ("", "NAN", "NULL", "None", "nan"):
                    if last_ts:
                        duration = last_ts - current_proc["start_time"]
                        if duration >= timedelta(hours=1):
                            thicknesses = predict_thickness(current_proc["start_time"], last_ts, current_proc["start_table"], last_table)
                        if len(thicknesses) == 0:
                            thicknesses = [0 for _ in range(45)]
                            insert_trace_info_with_thickness(current_proc["start_time"], last_ts, current_proc["start_table"], last_table, thicknesses)
                            print(current_proc["start_time"], last_ts, thicknesses, '\n')
                            
                            predict_trace_parameter(current_proc["start_time"], last_ts, current_proc["start_table"], last_table)
                            insert_trace_pred(pred_df)
                            print(f"예측데이터 저장완료")
                            
                        current_proc = None
                elif last_ts:
                    gap = ts - last_ts
                    if gap >= timedelta(hours=1):
                        duration = last_ts - current_proc["start_time"]
                        if duration >= timedelta(hours=1):
                            thicknesses = predict_thickness(current_proc["start_time"], last_ts, current_proc["start_table"], last_table)
                        if len(thicknesses) == 0:
                            thicknesses = [0 for _ in range(45)]
                            insert_trace_info_with_thickness(current_proc["start_time"], last_ts, current_proc["start_table"], last_table, thicknesses)
                            print(f"⚠️ 중단 감지 → 저장됨: {current_proc['start_time']} ~ {last_ts}", thicknesses, '\n')
                            
                            pred_df = predict_trace_parameter(current_proc["start_time"], last_ts, current_proc["start_table"], last_table)
                            insert_trace_pred(pred_df)
                            print(f"예측데이터 저장완료")
                            
                        else:
                            print(f"⚠️ 중단 감지 → 무시됨(1시간 미만): {current_proc['start_time']} ~ {last_ts}\n")
                        current_proc = None  # 현재 공정 종료 처리
            last_ts = ts
            last_table = table

    conn.commit()
    cur.close()
    conn.close()

def drop_trace_and_proc_tables():
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432
    )
    cur = conn.cursor()

    # 테이블 삭제
    for table in ["trace_info"]:
        try:
            cur.execute(f'DROP TABLE IF EXISTS {table} CASCADE;')
            print(f"✅ 테이블 삭제됨: {table}")
        except Exception as e:
            print(f"❌ 삭제 실패: {table} → {e}")
    
    conn.commit()
    cur.close()
    conn.close()
    
# 🕒 30분 간격 루프
if __name__ == '__main__':
    #drop_trace_and_proc_tables()
    print_existing_trace_info()  
    try:
        while True:
            extract_process_ranges_incrementally()
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]  10분 후 재실행 대기 중...\n")
            time.sleep(600)
    except KeyboardInterrupt:
        print("\n🛑 수동 종료됨.")