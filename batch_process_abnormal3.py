import csv
import json
import os
import random
import re
from collections import defaultdict
from datetime import datetime, timedelta
from itertools import chain

import joblib
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from tensorflow.keras import layers
from tensorflow.keras.models import load_model

from insert_old_data import column_types as RAW_COLUMN_TYPES
from insert_old_data import columns as RAW_COLUMNS
from insert_old_data import transform_value


WINDOW_SIZE = 192
PREDICT_STEPS = [10, 20, 30]
PREDICT_STEP = 10

# 이상감지 임계값 설정
THRESHOLD_PERCENT = 10.0  # 10% 이상
THRESHOLD_ABS = 0.4      # 절대값 0.4 이상
MIN_DURATION_SEC = 5.0    # 이상 구간 최소 지속시간
CLEAR_GAP_SEC = 2.0       # 2초 이상 정상 구간이면 종료

# 이상감지 대상 파라미터
ALLOWED_PARAMS = {
    "MFC7_DCS",
    "MFC8_NH3",
    "MFC1_N2-1",
    "MFC2_N2-2",
    "MFC3_N2-3",
    "MFC4_N2-4",
}

TASKS = [
    ["MFC7_DCS", "MFC8_NH3", "MFC26_F.PWR"],
    ["MFC1_N2-1", "MFC2_N2-2", "MFC3_N2-3"],
    ["MFC4_N2-4", "MFC27_L.POS", "MFC28_R.POS"],
    ["VG11 Press value", "VG12 Press value", "VG13 Press value"],
    ["Temp_Act_U", "Temp_Act_CU", "Temp_Act_C", "Temp_Act_CL", "Temp_Act_L"],
]

# 예측 테이블 매핑
PARAM_TABLE_MAP = {}
for idx, cols in enumerate(TASKS):
    for col in cols:
        PARAM_TABLE_MAP[col] = f"pred_proc{idx}"

SELECTED_COLS = [
    "PPExecStepID", # PPExecStepID
    "MFC1_N2-1",
    "MFC2_N2-2",
    "MFC3_N2-3",
    "MFC4_N2-4",
    "MFC26_F.PWR",
    "MFC27_L.POS",
    "MFC28_R.POS",
    "MFC7_DCS",
    "MFC8_NH3",
    "MFC9_F2",
    "APC Valve Value (Angle)",
    "VG11 Press value",
    "VG12 Press value",
    "VG13 Press value",
    "Temp_Act_U",
    "Temp_Act_CU",
    "Temp_Act_C",
    "Temp_Act_CL",
    "Temp_Act_L",
    "ValveAct_2:2",
    "ValveAct_3:3",
    "ValveAct_4:4",
    "ValveAct_5:5",
    "ValveAct_9:9",
    "ValveAct_12:12",
    "ValveAct_14:14",
    "ValveAct_16:16",
    "ValveAct_26:26",
    "ValveAct_28:28",
    "ValveAct_29:29",
    "ValveAct_60:71",
    "ValveAct_63:75",
    "ValveAct_73:83",
    "ValveAct_80:DPO",
    "ValveAct_89:RF",
    "ValveAct_90:PST",
]

TEMP_ADD_COLUMNS = ["Temp_Set_", "Temp_HT_Power_"]

MAIN_PROC_IDS = {111, 128, 119, 117, 152, 113, 115, 116}
MAIN_COLS = {
    "MFC1_N2-1",
    "MFC2_N2-2",
    "MFC3_N2-3",
    "MFC4_N2-4",
    "MFC27_L.POS",
    "MFC28_R.POS",
    "VG12 Press value",
    "VG13 Press value",
}

DB_CONF = {
    "dbname": "postgres",
    "user": "keti",
    "password": "keti1234!",
    "host": "localhost",
    "port": 5432,
}


def log(msg: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}")


def find_csv_files(base_dir: str) -> list[str]:
    pattern = re.compile(r".*/\d{4}/\d{2}/\d{2}/([0-2][0-9]00)\.csv$")
    csv_files: list[str] = []
    for root, _, files in os.walk(base_dir):
        for file in files:
            if not file.endswith(".csv"):
                continue
            full_path = os.path.join(root, file)
            match = pattern.match(full_path)
            if match and match.group(1) in {f"{str(h).zfill(2)}00" for h in range(24)}:
                csv_files.append(full_path)
    csv_files.sort()
    return csv_files


def read_csv(path: str) -> pd.DataFrame:
    rows = []
    with open(path, "r", encoding="utf-8-sig", errors="ignore") as f:
        lines = (line.replace("\x00", "") for line in f)
        reader = csv.reader(lines)
        header = next(reader, None)
        if header:
            header = [col.lstrip("\ufeff") for col in header]
        has_header = header == RAW_COLUMNS
        data_iter = reader if has_header else chain([header], reader) if header else reader
        index_map = {c: i for i, c in enumerate(RAW_COLUMNS)}
        for raw_row in data_iter:
            if not raw_row:
                continue
            raw_row = [val.lstrip("\ufeff") for val in raw_row]
            row = [
                transform_value(raw_row[index_map[c]]) if index_map.get(c) is not None and index_map[c] < len(raw_row) else None
                for c in RAW_COLUMNS
            ]
            rows.append(row)
    df = pd.DataFrame(rows, columns=RAW_COLUMNS)
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
    return df.dropna(subset=["Timestamp"]).sort_values("Timestamp").reset_index(drop=True)


def densify(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.sort_values("Timestamp").reset_index(drop=True)
    new_rows = []
    for i in range(len(df) - 1):
        cur = df.iloc[i]
        nxt = df.iloc[i + 1]
        ts_cur = cur["Timestamp"]
        ts_next = nxt["Timestamp"]
        new_rows.append(cur)
        diff = (ts_next - ts_cur).total_seconds()
        if diff > 1.2:
            for j in range(1, int(diff)):
                filler = cur.copy()
                filler["Timestamp"] = ts_cur + timedelta(seconds=j)
                new_rows.append(filler)
    new_rows.append(df.iloc[-1])
    dense = pd.DataFrame(new_rows).reset_index(drop=True)
    dense.ffill(inplace=True)
    return dense


def ensure_raw_table(conn, table_name: str) -> None:
    cols_sql = ",\n    ".join([f'"{col}" {RAW_COLUMN_TYPES.get(col, "TEXT")}' for col in RAW_COLUMNS])
    create_sql = f'''
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        {cols_sql},
        PRIMARY KEY ("Timestamp")
    );
    '''
    with conn.cursor() as cur:
        cur.execute(create_sql)
        conn.commit()


def insert_raw_rows(pool: SimpleConnectionPool, df: pd.DataFrame) -> None:
    grouped = defaultdict(list)
    for _, row in df.iterrows():
        table_name = f"rawdata{row['Timestamp'].strftime('%Y%m%d')}"
        grouped[table_name].append(tuple(row[c] for c in RAW_COLUMNS))
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            for table_name, rows in grouped.items():
                ensure_raw_table(conn, table_name)
                placeholders = ",".join(["%s"] * len(RAW_COLUMNS))
                colnames = ",".join([f'"{c}"' for c in RAW_COLUMNS])
                insert_sql = f'''
                    INSERT INTO "{table_name}" ({colnames})
                    VALUES ({placeholders})
                    ON CONFLICT ("Timestamp") DO NOTHING;
                '''
                cur.executemany(insert_sql, rows)
            conn.commit()
    finally:
        pool.putconn(conn)


class PatchEmbedding(layers.Layer):
    def __init__(self, patch_len, d_model, **kwargs):
        super().__init__(**kwargs)
        self.patch_len = patch_len
        self.d_model = d_model
        self.proj = None

    def build(self, _):
        self.proj = layers.Dense(self.d_model)

    def call(self, x):
        num_patches = x.shape[1] // self.patch_len
        x = layers.Reshape((num_patches, self.patch_len * x.shape[2]))(x)
        return self.proj(x)

    def get_config(self):
        config = super().get_config()
        config.update({"patch_len": self.patch_len, "d_model": self.d_model})
        return config

    @classmethod
    def from_config(cls, config):
        return cls(patch_len=config.get("patch_len"), d_model=config.get("d_model"))


class PositionalEncoding(layers.Layer):
    def __init__(self, length, d_model, **kwargs):
        super().__init__(**kwargs)
        self.length = length
        self.d_model = d_model
        self.pos_emb = None

    def build(self, _):
        self.pos_emb = self.add_weight(
            name="pos_emb",
            shape=[1, self.length, self.d_model],
            initializer="random_normal",
        )

    def call(self, x):
        return x + self.pos_emb

    def get_config(self):
        config = super().get_config()
        config.update({"length": self.length, "d_model": self.d_model})
        return config

    @classmethod
    def from_config(cls, config):
        return cls(length=config.get("length"), d_model=config.get("d_model"))


def check_columns(col: str) -> bool:
    return col in MAIN_COLS


def is_main_proc(step_id) -> bool:
    try:
        return int(step_id) in MAIN_PROC_IDS
    except Exception:
        return False


def get_weighted_mae(lval, hval, add_weight):
    import tensorflow as tf

    def loss(y_true, y_pred):
        weights = tf.where(tf.logical_and(y_true >= lval, y_true <= hval), add_weight, 1.0)
        delta = tf.abs(y_true - y_pred)
        return tf.reduce_mean(weights * delta)

    return loss


class BatchPredictor:
    def __init__(self, pool: SimpleConnectionPool):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        self.model_dir = os.path.join(base_dir, "model")
        self.scaler_dir = os.path.join(self.model_dir, "scaler")
        self.pool = pool
        self.scaler_X = joblib.load(os.path.join(self.scaler_dir, "scaler_X.pkl"))
        self.scaler_X_temp = joblib.load(os.path.join(self.scaler_dir, "scaler_X_Temp.pkl"))
        self.scaler_X_main = joblib.load(os.path.join(self.scaler_dir, "scaler_X_main.pkl"))
        self.scaler_ys = {}
        self.scaler_ys_main = {}
        self.models = {}
        self.models_main = {}
        self.temp_model = None
        self.temp_scaler = None
        self.pred_col_names = {
            idx: [c.replace(".", "_").replace(" ", "_").replace("-", "_") for c in task]
            for idx, task in enumerate(TASKS)
        }
        self._load_models()
        self._ensure_pred_tables()

    def _load_models(self):
        for cols in TASKS:
            for col in cols:
                if "Temp_Act" in col and self.temp_model is None:
                    self.temp_model = load_model(
                        os.path.join(self.model_dir, "192_patchtst_Temp.keras"),
                        custom_objects={
                            "PatchEmbedding": PatchEmbedding,
                            "PositionalEncoding": PositionalEncoding,
                            "loss": "mae",
                        },
                    )
                    self.temp_scaler = joblib.load(os.path.join(self.scaler_dir, "scaler_y_Temp.pkl"))
                    continue
                if col in self.models:
                    continue
                loss_func = "mae"
                scaler_y = joblib.load(os.path.join(self.scaler_dir, f"scaler_y_{col}.pkl"))
                if col == "VG11 Press value":
                    y_low = scaler_y.transform([[0]])
                    y_high = scaler_y.transform([[9]])
                    loss_func = get_weighted_mae(y_low, y_high, 100.0)
                self.models[col] = load_model(
                    os.path.join(self.model_dir, f"192_patchtst_{col}.keras"),
                    custom_objects={
                        "PatchEmbedding": PatchEmbedding,
                        "PositionalEncoding": PositionalEncoding,
                        "loss": loss_func,
                    },
                )
                self.scaler_ys[col] = scaler_y
                if check_columns(col):
                    self.models_main[col] = load_model(
                        os.path.join(self.model_dir, f"192_patchtst_{col}_main.keras"),
                        custom_objects={
                            "PatchEmbedding": PatchEmbedding,
                            "PositionalEncoding": PositionalEncoding,
                            "loss": loss_func,
                        },
                    )
                    self.scaler_ys_main[col] = joblib.load(os.path.join(self.scaler_dir, f"scaler_y_{col}_main.pkl"))

    def _ensure_pred_tables(self):
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                for idx, task in enumerate(TASKS):
                    table_name = f"pred_proc{idx}"
                    cols = self.pred_col_names[idx]
                    col_defs = ", ".join([f'"{c}" REAL' for c in cols])
                    cur.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS "{table_name}" (
                            "Timestamp" TIMESTAMP,
                            "PredictStep" INTEGER,
                            {col_defs},
                            "ProcessRecipeStepID" INTEGER,
                            "ProcessRecipeStepName" TEXT,
                            PRIMARY KEY ("Timestamp")
                        );
                        """
                    )
                conn.commit()
        finally:
            self.pool.putconn(conn)

    def predict(self, df: pd.DataFrame) -> tuple[dict[str, list[tuple]], dict[str, dict]]:
        data = df.copy()
        for col in SELECTED_COLS:
            if col in data:
                data[col] = pd.to_numeric(data[col], errors="coerce")
        all_add_cols = []
        for col in sum(TASKS, []):
            if "Temp_Act_" in col:
                temp_pos = col.split("_")[-1]
                for add_col in TEMP_ADD_COLUMNS:
                    all_add_cols.append(add_col + temp_pos)
        for col in all_add_cols:
            if col in data:
                data[col] = pd.to_numeric(data[col], errors="coerce")
        data.ffill(inplace=True)

        pred_rows: dict[str, list[tuple]] = defaultdict(list)
        pred_buffer: dict[str, dict] = {p: {} for p in PARAM_TABLE_MAP.keys()}

        if len(data) < WINDOW_SIZE:
            return pred_rows, pred_buffer

        # 슬라이딩 윈도우를 한꺼번에 쌓아서 배치 예측
        windows = []
        last_timestamps = []
        ppexec_ids = []
        for idx in range(WINDOW_SIZE - 1, len(data)):
            window = data.iloc[idx - WINDOW_SIZE + 1 : idx + 1]
            windows.append(window)
            last_timestamps.append(window.iloc[-1]["Timestamp"])
            ppexec_ids.append(window.iloc[-1].get("PPExecStepID", -1))

        for proc_idx, predict_columns in enumerate(TASKS):
            add_cols = []
            for col in predict_columns:
                if "Temp_Act_" in col:
                    temp_pos = col.split("_")[-1]
                    for add_col in TEMP_ADD_COLUMNS:
                        add_cols.append(add_col + temp_pos)
            seq_cols = SELECTED_COLS + add_cols
            seq_batch = np.stack([w[seq_cols].astype(float).values for w in windows])
            proc_preds = self._predict_proc_batch(proc_idx, predict_columns, seq_batch, ppexec_ids)
            table_name = f"pred_proc{proc_idx}"
            for w_idx, last_ts in enumerate(last_timestamps):
                pred_dates = [last_ts + timedelta(seconds=step) for step in PREDICT_STEPS]
                # step 정보는 알 수 없으므로 UNKNOWN/-1 유지
                for ps_idx, predict_step in enumerate(PREDICT_STEPS):
                    row_vals = [pred_dates[ps_idx], int(predict_step)]
                    row_vals.extend(proc_preds[w_idx, ps_idx])
                    row_vals.append(-1)
                    row_vals.append("UNKNOWN")
                    pred_rows[table_name].append(tuple(row_vals))
                    if predict_step == PREDICT_STEP:
                        for col_idx, col in enumerate(predict_columns):
                            pred_buffer[col].setdefault(
                                pred_dates[ps_idx].replace(microsecond=0),
                                proc_preds[w_idx, ps_idx, col_idx],
                            )
        return pred_rows, pred_buffer

    def _get_step_info(self, step_df: pd.DataFrame):
        ids = []
        names = []
        for predict_step in PREDICT_STEPS:
            ids.append(-1)
            names.append("UNKNOWN")
        return ids, names

    def _predict_proc_batch(self, proc_idx: int, predict_columns: list[str], seq_batch: np.ndarray, ppexec_ids: list) -> np.ndarray:
        batch_size = seq_batch.shape[0]
        all_preds = np.zeros((batch_size, len(PREDICT_STEPS), len(predict_columns)))
        temp_handled = False
        for col_idx, p_col in enumerate(predict_columns):
            if "Temp_Act" in p_col:
                if temp_handled:
                    continue
                X = self.scaler_X_temp.transform(seq_batch.reshape(-1, seq_batch.shape[2])).reshape(seq_batch.shape[0], WINDOW_SIZE, -1)
                pred_scaled = self.temp_model.predict(X, verbose=0)
                reshaped = pred_scaled.reshape(-1, len(PREDICT_STEPS), len(predict_columns))
                inv = np.stack(
                    [self.temp_scaler.inverse_transform(reshaped[:, i, :]) for i in range(len(PREDICT_STEPS))],
                    axis=1,
                )
                all_preds = inv
                temp_handled = True
            else:
                use_main_mask = np.array([check_columns(p_col) and is_main_proc(pid) for pid in ppexec_ids])
                # main 모델 처리
                if use_main_mask.any() and p_col in self.models_main:
                    X_main = self.scaler_X_main.transform(seq_batch[use_main_mask].reshape(use_main_mask.sum() * WINDOW_SIZE, -1)).reshape(use_main_mask.sum(), WINDOW_SIZE, -1)
                    pred_scaled_main = self.models_main[p_col].predict(X_main, verbose=0)
                    inv_main = np.stack(
                        [self.scaler_ys_main[p_col].inverse_transform(pred_scaled_main[:, [i]])[:, 0] for i in range(len(PREDICT_STEPS))],
                        axis=1,
                    )
                    all_preds[use_main_mask, :, col_idx] = inv_main
                # 기본 모델 처리
                if (~use_main_mask).any():
                    X_base = self.scaler_X.transform(seq_batch[~use_main_mask].reshape((~use_main_mask).sum() * WINDOW_SIZE, -1)).reshape((~use_main_mask).sum(), WINDOW_SIZE, -1)
                    pred_scaled = self.models[p_col].predict(X_base, verbose=0)
                    inv = np.stack(
                        [self.scaler_ys[p_col].inverse_transform(pred_scaled[:, [i]])[:, 0] for i in range(len(PREDICT_STEPS))],
                        axis=1,
                    )
                    all_preds[~use_main_mask, :, col_idx] = inv
        return all_preds


def insert_pred_rows(pool: SimpleConnectionPool, col_names: list[str], rows: list[tuple], table_name: str) -> None:
    if not rows:
        return

    def to_py(val):
        if isinstance(val, np.generic):
            return val.item()
        return val

    safe_rows = [tuple(to_py(v) for v in row) for row in rows]
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            placeholders = ",".join(["%s"] * (2 + len(col_names) + 2))
            col_sql = ", ".join(['"Timestamp"', '"PredictStep"'] + [f'"{c}"' for c in col_names] + ['"ProcessRecipeStepID"', '"ProcessRecipeStepName"'])
            cur.executemany(
                f'''
                INSERT INTO "{table_name}" ({col_sql})
                VALUES ({placeholders})
                ON CONFLICT ("Timestamp") DO NOTHING
                ''',
                safe_rows,
            )
        conn.commit()
    finally:
        pool.putconn(conn)


def load_data_from_table(pool: SimpleConnectionPool, table_name: str, start_time: datetime = None, end_time: datetime = None) -> pd.DataFrame:
    """특정 테이블에서 데이터를 불러옵니다"""
    conn = pool.getconn()
    try:
        where_clause = ''
        params = ()
        
        if start_time and end_time:
            where_clause = 'WHERE "Timestamp" >= %s AND "Timestamp" < %s'
            params = (start_time, end_time)
        elif start_time:
            where_clause = 'WHERE "Timestamp" >= %s'
            params = (start_time,)
        elif end_time:
            where_clause = 'WHERE "Timestamp" < %s'
            params = (end_time,)
        
        colnames = ', '.join([f'"{col}"' for col in RAW_COLUMNS])
        query = f'''
            SELECT {colnames}
            FROM "{table_name}"
            {where_clause}
            ORDER BY "Timestamp" ASC
        '''
        df = pd.read_sql(query, conn, params=params)
        if not df.empty:
            df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
            df = df.dropna(subset=["Timestamp"]).sort_values("Timestamp").reset_index(drop=True)
        return df
    except Exception as e:
        log(f"Warning: Failed to load data from {table_name}: {e}")
        return pd.DataFrame(columns=RAW_COLUMNS)
    finally:
        pool.putconn(conn)


def weighted_violation_type():
    """위반 유형 가중치 랜덤 선택: 1:40%, 2:30%, 3:20%, 4:10%"""
    r = random.random()
    if r < 0.4:
        return 1
    if r < 0.7:
        return 2
    if r < 0.9:
        return 3
    return 4


def ensure_abnormal_log_table(conn):
    """이상감지 로그 테이블 생성"""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS realtime_abnormal_log (
                id SERIAL PRIMARY KEY,
                start_time TIMESTAMP NOT NULL,
                end_time TIMESTAMP NOT NULL,
                parameter TEXT NOT NULL,
                duration_seconds DOUBLE PRECISION,
                avg_diff_percent DOUBLE PRECISION,
                max_diff_percent DOUBLE PRECISION,
                peak_time TIMESTAMP,
                actual_value DOUBLE PRECISION,
                predicted_value DOUBLE PRECISION,
                violation_type INT,
                message TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                UNIQUE (parameter, start_time)
            );
            """
        )
        conn.commit()


class BatchAnomalyDetector:
    """배치 처리용 이상감지 클래스"""
    
    def __init__(self, pool: SimpleConnectionPool):
        self.pool = pool
        self.state = defaultdict(dict)  # param -> event state
        conn = pool.getconn()
        try:
            ensure_abnormal_log_table(conn)
        finally:
            pool.putconn(conn)
    
    def _to_py(self, val):
        """numpy 타입을 Python 기본 타입으로 변환"""
        if isinstance(val, np.generic):
            return val.item()
        return val
    
    def upsert_event(self, param, event):
        """이상감지 이벤트를 데이터베이스에 저장"""
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                message_payload = {
                    "parameter": param,
                    "start": event["start"].isoformat(),
                    "end": event["end"].isoformat(),
                    "duration_seconds": event["duration"],
                    "diff_percent": self._to_py(event["avg_diff"]),
                    "peak_time": event.get("peak_time").isoformat() if event.get("peak_time") else None,
                    "actual_value": self._to_py(event.get("peak_actual")),
                    "predicted_value": self._to_py(event.get("peak_pred")),
                    "step_id": sorted(event.get("step_ids", [])),
                    "step_name": sorted(event.get("step_names", [])),
                    "violation_type": self._to_py(event.get("violation_type")),
                }
                message_text = json.dumps(message_payload, ensure_ascii=False)
                cur.execute(
                    """
                    INSERT INTO realtime_abnormal_log (
                        start_time, end_time, parameter,
                        duration_seconds, avg_diff_percent, max_diff_percent,
                        peak_time, actual_value, predicted_value, violation_type, message,
                        created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (parameter, start_time) DO UPDATE
                    SET end_time = EXCLUDED.end_time,
                        duration_seconds = EXCLUDED.duration_seconds,
                        avg_diff_percent = EXCLUDED.avg_diff_percent,
                        max_diff_percent = EXCLUDED.max_diff_percent,
                        peak_time = EXCLUDED.peak_time,
                        actual_value = EXCLUDED.actual_value,
                        predicted_value = EXCLUDED.predicted_value,
                        violation_type = EXCLUDED.violation_type,
                        message = EXCLUDED.message,
                        updated_at = NOW()
                    """,
                    (
                        event["start"],
                        event["end"],
                        param,
                        self._to_py(event["duration"]),
                        self._to_py(event["avg_diff"]),
                        self._to_py(event["max_diff"]),
                        event.get("peak_time"),
                        self._to_py(event.get("peak_actual")),
                        self._to_py(event.get("peak_pred")),
                        self._to_py(event.get("violation_type")),
                        message_text,
                    ),
                )
            conn.commit()
        finally:
            self.pool.putconn(conn)
    
    def finalize_event(self, param):
        """이상감지 이벤트 종료 처리"""
        state = self.state.get(param)
        if not state or not state.get("active"):
            return
        duration = (state["last_anomaly"] - state["start"]).total_seconds() + 1.0
        if duration < MIN_DURATION_SEC:
            self.state[param] = {}
            return
        avg_diff = state["diff_sum"] / max(1, state["diff_count"])
        event = {
            "start": state["start"],
            "end": state["last_anomaly"],
            "duration": duration,
            "avg_diff": avg_diff,
            "max_diff": state["max_diff"],
            "peak_time": state.get("peak_time"),
            "peak_actual": state.get("peak_actual"),
            "peak_pred": state.get("peak_pred"),
            "step_ids": state.get("step_ids", set()),
            "step_names": state.get("step_names", set()),
            "violation_type": state.get("violation_type"),
        }
        self.upsert_event(param, event)
        self.state[param] = {}
    
    def handle_sample(self, param, ts, actual_val, pred_val, step_id=None, step_name=None):
        """샘플 데이터 처리 및 이상감지"""
        if param not in ALLOWED_PARAMS:
            return
        
        state = self.state.setdefault(param, {})
        if pred_val is None or actual_val is None:
            # missing prediction or actual
            if state.get("active"):
                # check if should close
                if (ts - state["last_anomaly"]).total_seconds() >= CLEAR_GAP_SEC:
                    self.finalize_event(param)
            return

        try:
            diff_pct = abs(actual_val - pred_val) / (abs(actual_val) or 1.0) * 100.0
        except Exception:
            diff_pct = 0.0
        diff_abs = abs(actual_val - pred_val)
        is_anomaly = diff_pct > THRESHOLD_PERCENT and diff_abs > THRESHOLD_ABS

        if is_anomaly:
            if not state.get("active"):
                state["active"] = True
                state["start"] = ts
                state["violation_type"] = weighted_violation_type()
                state["diff_sum"] = diff_pct
                state["diff_count"] = 1
                state["max_diff"] = diff_pct
                state["peak_time"] = ts
                state["peak_actual"] = actual_val
                state["peak_pred"] = pred_val
                state["step_ids"] = set()
                state["step_names"] = set()
            else:
                state["diff_sum"] += diff_pct
                state["diff_count"] += 1
                if diff_pct > state.get("max_diff", 0):
                    state["max_diff"] = diff_pct
                    state["peak_time"] = ts
                    state["peak_actual"] = actual_val
                    state["peak_pred"] = pred_val
            state["last_anomaly"] = ts
            if step_id is not None:
                state.setdefault("step_ids", set()).add(step_id)
            if step_name:
                state.setdefault("step_names", set()).add(step_name)

            duration = (state["last_anomaly"] - state["start"]).total_seconds() + 1.0
            if duration >= MIN_DURATION_SEC:
                avg_diff = state["diff_sum"] / max(1, state["diff_count"])
                self.upsert_event(
                    param,
                    {
                        "start": state["start"],
                        "end": state["last_anomaly"],
                        "duration": duration,
                        "avg_diff": avg_diff,
                        "max_diff": state.get("max_diff"),
                        "peak_time": state.get("peak_time"),
                        "peak_actual": state.get("peak_actual"),
                        "peak_pred": state.get("peak_pred"),
                        "step_ids": state.get("step_ids", set()),
                        "step_names": state.get("step_names", set()),
                        "violation_type": state.get("violation_type"),
                    },
                )
        else:
            if state.get("active"):
                if (ts - state["last_anomaly"]).total_seconds() >= CLEAR_GAP_SEC:
                    self.finalize_event(param)
    
    def run_detection(self, df: pd.DataFrame, pred_buffer: dict[str, dict]) -> None:
        """데이터프레임과 예측 버퍼를 사용하여 이상감지 실행"""
        if df.empty:
            return
        for param in ALLOWED_PARAMS:
            for _, row in df.iterrows():
                ts = row["Timestamp"]
                ts_sec = ts.replace(microsecond=0)
                pred_val = pred_buffer.get(param, {}).get(ts_sec)
                if isinstance(pred_val, np.generic):
                    pred_val = pred_val.item()
                actual_val = row.get(param)
                try:
                    actual_val = pd.to_numeric(actual_val, errors="coerce")
                except Exception:
                    actual_val = None
                self.handle_sample(param, ts_sec, actual_val, pred_val, row.get("ProcessRecipeStepID"), row.get("ProcessRecipeStepName"))
            self.finalize_event(param)




def process_table(pool: SimpleConnectionPool, predictor: BatchPredictor, detector: BatchAnomalyDetector, table_name: str, start_time: datetime = None, end_time: datetime = None) -> None:
    """특정 테이블의 데이터를 불러와 예측을 수행하고 결과를 저장합니다"""
    log(f"Processing table: {table_name} (from {start_time} to {end_time})")
    
    # 1. DB에서 실제값 불러오기
    raw_df = load_data_from_table(pool, table_name, start_time, end_time)
    log(f"Loaded: {len(raw_df)} rows from {table_name}")
    if raw_df.empty:
        log(f"No data found in {table_name}, skipping.")
        return
    
    # 2. 데이터 densify
    dense_df = densify(raw_df)
    log(f"Densified: {len(dense_df)} rows")
    
    # 3. 예측 수행
    log(f"Predicting for {table_name}")
    pred_rows, pred_buffer = predictor.predict(dense_df)
    
    # 4. 예측값 저장
    for idx, _ in enumerate(TASKS):
        pred_table_name = f"pred_proc{idx}"
        rows = pred_rows.get(pred_table_name, [])
        log(f"Saving predictions -> {pred_table_name} ({len(rows)} rows) for {table_name}")
        insert_pred_rows(pool, predictor.pred_col_names[idx], rows, pred_table_name)
    
    # 5. 이상감지 수행
    log(f"Anomaly detection for {table_name}")
    detector.run_detection(dense_df, pred_buffer)
    
    log(f"Completed processing {table_name}")


def main():
    # 기간 설정: 2025-11-02 ~ 2025-12-25
    start_date = datetime(2025, 11, 2)
    end_date = datetime(2025, 12, 25)
    
    pool = SimpleConnectionPool(1, 5, **DB_CONF)
    predictor = BatchPredictor(pool)
    detector = BatchAnomalyDetector(pool)
    
    # 하루씩 반복 처리
    current_date = start_date
    total_days = (end_date - start_date).days + 1
    day_count = 0
    
    while current_date <= end_date:
        day_count += 1
        table_name = f"rawdata{current_date.strftime('%Y%m%d')}"
        
        # 하루의 시작과 끝 시간 설정
        day_start = current_date.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = (current_date + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        
        log(f"[{day_count}/{total_days}] Processing {table_name} ({current_date.strftime('%Y-%m-%d')})")
        
        try:
            # 각 테이블마다: 예측 수행 -> 예측값 저장 -> 이상감지 -> 이상데이터 저장
            process_table(pool, predictor, detector, table_name, day_start, day_end)
            log(f"[{day_count}/{total_days}] Completed {table_name}")
        except Exception as e:
            log(f"[{day_count}/{total_days}] Error processing {table_name}: {e}")
        
        current_date += timedelta(days=1)
    
    pool.closeall()
    log("Batch processing finished.")


if __name__ == "__main__":
    main()
