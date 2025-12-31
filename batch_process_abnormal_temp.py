import csv
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta, time
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
            where_clause = 'WHERE "Timestamp" >= %s AND "Timestamp" <= %s'
            params = (start_time, end_time)
        elif start_time:
            where_clause = 'WHERE "Timestamp" >= %s'
            params = (start_time,)
        elif end_time:
            where_clause = 'WHERE "Timestamp" <= %s'
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




def process_table(pool: SimpleConnectionPool, predictor: BatchPredictor, table_name: str, start_time: datetime = None, end_time: datetime = None) -> None:
    """특정 테이블의 데이터를 불러와 예측을 수행하고 결과를 저장합니다"""
    log(f"Processing table: {table_name}")
    raw_df = load_data_from_table(pool, table_name, start_time, end_time)
    log(f"Loaded: {len(raw_df)} rows from {table_name}")
    if raw_df.empty:
        log(f"No data found in {table_name}, skipping.")
        return
    dense_df = densify(raw_df)
    log(f"Densified: {len(dense_df)} rows")
    log(f"Predicting for {table_name}")
    pred_rows, _ = predictor.predict(dense_df)
    for idx, _ in enumerate(TASKS):
        pred_table_name = f"pred_proc{idx}"
        rows = pred_rows.get(pred_table_name, [])
        log(f"Saving predictions -> {pred_table_name} ({len(rows)} rows) for {table_name}")
        insert_pred_rows(pool, predictor.pred_col_names[idx], rows, pred_table_name)
    log(f"Completed processing {table_name}")


def main():
    """DB에서 데이터를 불러와 예측을 수행합니다"""
    # 처리할 날짜 범위 설정: 2025-11-02 ~ 2025-12-25
    start_date = datetime(2025, 11, 2, 0, 0, 0)
    end_date = datetime(2025, 12, 25, 23, 59, 59)
    
    log(f"Processing data from {start_date.date()} to {end_date.date()}")
    pool = SimpleConnectionPool(1, 5, **DB_CONF)
    predictor = BatchPredictor(pool)
    
    # 각 날짜별 테이블을 개별적으로 처리
    current_date = start_date.date()
    end_date_only = end_date.date()
    processed_count = 0
    skipped_count = 0
    
    while current_date <= end_date_only:
        table_name = f"rawdata{current_date.strftime('%Y%m%d')}"
        
        # 해당 날짜의 시작/종료 시간 설정
        day_start = datetime.combine(current_date, time.min)
        day_end = datetime.combine(current_date, time.max)
        
        # 전체 범위와 겹치는 부분만 처리
        if current_date == start_date.date():
            day_start = start_date
        if current_date == end_date_only:
            day_end = end_date
        
        log(f"[{processed_count + skipped_count + 1}] Processing {table_name} ({current_date})")
        try:
            process_table(pool, predictor, table_name, day_start, day_end)
            processed_count += 1
        except Exception as e:
            log(f"Error processing {table_name}: {e}")
            skipped_count += 1
        
        current_date += timedelta(days=1)
    
    pool.closeall()
    log(f"Batch processing finished. Processed: {processed_count}, Skipped: {skipped_count}")


if __name__ == "__main__":
    main()
