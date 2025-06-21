import os
import time
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
import numpy as np
import joblib
import ray
import yaml
from tensorflow.keras.models import load_model
from tensorflow.keras import layers
import tensorflow as tf
import re

window_size = 192
predict_steps = [10, 20, 30]
predict_columns = [
    'MFC7_DCS', 'MFC8_NH3', 'MFC1_N2-1', 'MFC2_N2-2', 'MFC3_N2-3', 'MFC4_N2-4',
    'VG11 Press value', 'VG12 Press value', 'VG13 Press value',
    'MFC26_F.PWR', 'MFC27_L.POS', 'MFC28_R.POS',
    'Temp_Act_U', 'Temp_Act_CU', 'Temp_Act_C', 'Temp_Act_CL', 'Temp_Act_L'
]

selected_cols = ['PPExecStepID', 'MFC1_N2-1', 'MFC2_N2-2', 'MFC3_N2-3',
                 'MFC4_N2-4', 'MFC26_F.PWR', 'MFC27_L.POS', 'MFC28_R.POS',
                 'MFC7_DCS', 'MFC8_NH3', 'MFC9_F2', 'APC Valve Value (Angle)',
                 'VG11 Press value', 'VG12 Press value', 'VG13 Press value',
                 'Temp_Act_U', 'Temp_Act_CU', 'Temp_Act_C', 'Temp_Act_CL',
                 'Temp_Act_L', 'ValveAct_2:2', 'ValveAct_3:3', 'ValveAct_4:4',
                 'ValveAct_5:5', 'ValveAct_9:9', 'ValveAct_12:12',
                 'ValveAct_14:14', 'ValveAct_16:16', 'ValveAct_26:26',
                 'ValveAct_28:28', 'ValveAct_29:29', 'ValveAct_60:71',
                 'ValveAct_63:75', 'ValveAct_73:83', 'ValveAct_80:DPO',
                 'ValveAct_89:RF', 'ValveAct_90:PST']

step_reverse_dict = {
    'END': 2, 'STANDBY': 0, 'START': 1,
    'None': 0, 'nan': 0, 'NaN': 0, 'null': 0, 'NULL': 0, 'IDLE': 0
}

class PatchEmbedding(layers.Layer):
    def __init__(self, patch_len, d_model, **kwargs):
        super().__init__(**kwargs)
        self.patch_len = patch_len
        self.d_model = d_model

    def build(self, input_shape):
        self.proj = layers.Dense(self.d_model)

    def call(self, x):
        b = tf.shape(x)[0]
        n = x.shape[1] // self.patch_len
        x = tf.reshape(x, [b, n, self.patch_len * x.shape[2]])
        return self.proj(x)

    def get_config(self):
        cfg = super().get_config()
        cfg.update({'patch_len': self.patch_len, 'd_model': self.d_model})
        return cfg

class PositionalEncoding(layers.Layer):
    def __init__(self, length, d_model, **kwargs):
        super().__init__(**kwargs)
        self.length = length
        self.d_model = d_model

    def build(self, input_shape):
        self.pos_emb = self.add_weight(
            name='pos_emb', shape=[1, self.length, self.d_model],
            initializer='random_normal')

    def call(self, x):
        return x + self.pos_emb

    def get_config(self):
        cfg = super().get_config()
        cfg.update({'length': self.length, 'd_model': self.d_model})
        return cfg

def logg(msg):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'[{ts}] {msg}')
    os.makedirs('log', exist_ok=True)
    with open('log/predict.log', 'a', encoding='utf-8') as f:
        f.write(f'[{ts}] {msg}\n')

def extract_date(tname):
    m = re.search(r'rawdata(\d+)', tname)
    return int(m.group(1)) if m else 0

def get_last_table():
    conn = psycopg2.connect(dbname='postgres', user='keti', password='keti1234!', host='localhost', port=5432)
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema='public' AND table_name LIKE 'rawdata%';
    """)
    tables = [t[0] for t in cur.fetchall() if re.match(r'rawdata\d+$', t[0])]
    tables.sort(key=extract_date, reverse=True)
    latest = tables[0] if tables else ''
    prev = tables[1] if len(tables) > 1 else ''
    cur.execute(f'SELECT MAX("Timestamp") FROM "{latest}"')
    last_ts = cur.fetchone()[0]
    cur.close(); conn.close()
    return latest, prev, last_ts

def get_last_pred_times(conn):
    cur = conn.cursor()
    times = {}
    for step in predict_steps:
        max_ts = None
        for col in predict_columns:
            tbl = f'pred_{step}_{col.replace(".","_").replace(" ","_").replace("-","_")}'
            cur.execute(f"SELECT MAX(\"Timestamp\") FROM \"{tbl}\"")
            r = cur.fetchone()[0]
            if r and (not max_ts or r > max_ts):
                max_ts = r
        times[step] = max_ts
    cur.close()
    return times

def fetch_range(conn, start_ts, end_ts, table, prev_table):
    cols = ', '.join([f'"{c}"' for c in ['Timestamp'] + selected_cols +
                      ['ProcessRecipeStepRemainTime', 'ProcessRecipeStepID', 'ProcessRecipeStepName']])
    dfs = []
    for t in [prev_table, table]:
        if not t:
            continue
        query = f'SELECT {cols} FROM "{t}" WHERE "Timestamp" BETWEEN %s AND %s'
        try:
            df = pd.read_sql(query, conn, params=(start_ts, end_ts))
            dfs.append(df)
        except Exception:
            pass
    if not dfs:
        return pd.DataFrame()
    df = pd.concat(dfs)
    df.sort_values('Timestamp', inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df

def split_sequences(df):
    segs = []
    cur = []
    prev_ts = None
    in_proc = False
    for row in df.itertuples(index=False):
        ts = row.Timestamp
        step_id = row.ProcessRecipeStepID
        step_name = str(row.ProcessRecipeStepName)
        if not in_proc:
            if step_id == 1 or step_name in ['STANDBY', 'START']:
                in_proc = True
                cur.append(row)
                prev_ts = ts
            continue
        # in process
        if prev_ts and (ts - prev_ts).total_seconds() != 1:
            if cur:
                segs.append(pd.DataFrame(cur))
            cur = []
            in_proc = step_id == 1 or step_name in ['STANDBY', 'START']
            if in_proc:
                cur.append(row)
            prev_ts = ts
            continue
        cur.append(row)
        prev_ts = ts
        if step_id == 2:
            segs.append(pd.DataFrame(cur))
            cur = []
            in_proc = False
        elif step_id == 0:
            if len(cur) > 1:
                segs.append(pd.DataFrame(cur[:-1]))
            cur = []
            in_proc = False
    if cur:
        segs.append(pd.DataFrame(cur))
    return segs

def prepare_data(df):
    df = df.copy()
    df['PPExecStepID'] = [step_reverse_dict.get(str(x), 0) for x in df['ProcessRecipeStepName']]
    df.dropna(inplace=True)
    return df


@ray.remote
class PredictorActor:
    def __init__(self, columns):
        self.columns = columns
        self.scaler_X = joblib.load('model/scaler/scaler_X.pkl')
        self.scaler_ys = {c: joblib.load(f'model/scaler/scaler_y_{c}.pkl') for c in columns}
        self.models = {
            c: load_model(
                f'model/new_mae_192_patchtst_{c}.keras',
                custom_objects={
                    'PatchEmbedding': PatchEmbedding,
                    'PositionalEncoding': PositionalEncoding,
                },
            )
            for c in columns
        }

    def predict_segments(self, seg_dicts):
        if not seg_dicts:
            return
        conn = psycopg2.connect(dbname='postgres', user='keti', password='keti1234!', host='localhost', port=5432)
        cur = conn.cursor()
        for seg_dict in seg_dicts:
            seg = pd.DataFrame(seg_dict)
            seg = prepare_data(seg)
            if len(seg) < window_size:
                continue
            values = seg[selected_cols].values
            for i in range(window_size, len(seg) + 1):
                window = values[i - window_size : i]
                X = self.scaler_X.transform(window)
                base_ts = seg.iloc[i - 1]['Timestamp']
                step_id = seg.iloc[i - 1]['ProcessRecipeStepID']
                step_name = seg.iloc[i - 1]['ProcessRecipeStepName']
                for col in self.columns:
                    preds = self.models[col].predict(np.array([X]), verbose=0)
                    preds = np.stack([
                        self.scaler_ys[col].inverse_transform(preds[:, [k]])[:, 0]
                        for k in range(len(predict_steps))
                    ], axis=1)
                    for idx, step in enumerate(predict_steps):
                        ts = base_ts + timedelta(seconds=step)
                        tbl = f'pred_{step}_{col.replace(".","_").replace(" ","_").replace("-","_")}'
                        cur.execute(
                            f"CREATE TABLE IF NOT EXISTS \"{tbl}\" (\n"
                            "    \"Timestamp\" TIMESTAMP PRIMARY KEY,\n"
                            "    \"Parameter\" REAL,\n"
                            "    \"ProcessRecipeStepID\" INTEGER,\n"
                            "    \"ProcessRecipeStepName\" TEXT,\n"
                            "    UNIQUE (\"Timestamp\", \"Parameter\")\n"
                            ")"
                        )
                        cur.execute(
                            f"INSERT INTO \"{tbl}\" (\"Timestamp\", \"Parameter\", \"ProcessRecipeStepID\", \"ProcessRecipeStepName\")\n"
                            "VALUES (%s, %s, %s, %s) ON CONFLICT (\"Timestamp\", \"Parameter\") DO NOTHING",
                            (ts, float(preds[0, idx]), int(step_id), str(step_name)),
                        )
        conn.commit()
        cur.close(); conn.close()


def load_segments():
    table, prev_table, raw_last_ts = get_last_table()
    if not table or not raw_last_ts:
        return []
    conn = psycopg2.connect(dbname='postgres', user='keti', password='keti1234!', host='localhost', port=5432)
    last_pred = get_last_pred_times(conn)
    start_candidates = []
    for step, ts in last_pred.items():
        if ts:
            start_candidates.append(ts - timedelta(seconds=step) + timedelta(seconds=1))
    start_ts = min(start_candidates) if start_candidates else raw_last_ts - timedelta(seconds=window_size)
    data = fetch_range(conn, start_ts - timedelta(seconds=window_size), raw_last_ts, table, prev_table)
    conn.close()
    if data.empty:
        return []
    segments = split_sequences(data)
    segs = [seg.to_dict('list') for seg in segments if len(seg) >= window_size]
    return segs

def main():
  
    try:
        ray.init()
    except:
        ray.shutdown()
        ray.init()
    group_size = 3
    groups = [predict_columns[i:i + group_size] for i in range(0, len(predict_columns), group_size)]
    actors = [PredictorActor.remote(g) for g in groups]
    while True:
        try:
            segs = load_segments()
            if segs:
                tasks = [actor.predict_segments.remote(segs) for actor in actors]
                ray.get(tasks)
        except Exception as e:
            logg(str(e))
        time.sleep(1)

if __name__ == '__main__':
    main()