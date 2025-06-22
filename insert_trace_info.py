import psycopg2
import re
import time
from datetime import datetime, timedelta
import os
import pandas as pd
import numpy as np
import joblib
import xgboost as xgb

selected_cols = ['PPExecStepID', 'MFC1_N2-1', 'MFC2_N2-2', 'MFC3_N2-3', 'MFC4_N2-4', 'MFC26_F.PWR', 'MFC27_L.POS', 'MFC28_R.POS', 'MFC7_DCS', 'MFC8_NH3', 'MFC9_F2', 'APC Valve Value (Angle)', 'VG11 Press value', 'VG12 Press value', 'VG13 Press value', 'Temp_Act_U', 'Temp_Act_CU', 'Temp_Act_C', 'Temp_Act_CL', 'Temp_Act_L', 'ValveAct_2:2', 'ValveAct_3:3', 'ValveAct_4:4', 'ValveAct_5:5', 'ValveAct_9:9', 'ValveAct_12:12', 'ValveAct_14:14', 'ValveAct_16:16', 'ValveAct_26:26', 'ValveAct_28:28', 'ValveAct_29:29', 'ValveAct_60:71', 'ValveAct_63:75', 'ValveAct_73:83', 'ValveAct_80:DPO', 'ValveAct_89:RF', 'ValveAct_90:PST']
step_reverse_dict = {'END': 2, 'STANDBY': 0, 'START': 1, 'B.UP': 17, 'WAIT': 3, 'S.P-1': 74, 'S.P-2': 75, 'R.UP1': 25, 'STAB1': 22, 'S.P-3': 76, 'M.P-3': 81, 'L.CHK': 72, 'PREPRG1': 44, 'EVAC1': 99, 'EVAC2': 100, 'N-EVA1': 111, 'CLOSE1': 128, 'SI-FL1': 119, 'SI-EVA1': 117, 'CHANGE': 152, 'N-PRE1': 113, 'N-FL1': 115, 'N-FL2': 116, 'pre-NH3P': 110, 'DEPO1': 49, 'post_NH3P': 135, 'N2PRG1': 103, 'SI-EVA4': 149, 'A.VAC2': 85, 'A.PRG2': 90, 'A.VAC1': 84, 'A.PRG1': 89, 'N2PRG2': 104, 'N2PRG3': 105, 'A.VAC3': 86, 'A.PRG3': 91, 'A.VAC4': 87, 'A.PRG4': 92, 'CYCLE1': 130, 'A.PRG5': 93, 'R.DOWN1': 31, 'B.FILL1': 94, 'B.FILL2': 95, 'B.FILL3': 96, 'B.FILL4': 97, 'B.FILL5': 98, 'B.DOWN': 18, 'None': 0, 'nan': 0, 'NaN': 0, 'null': 0, 'NULL': 0, 'IDLE': 0}

def fetch_trace_data(start_ts, end_ts, start_table, end_table):
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432
    )

    all_data = []

    colnames = ', '.join([f'"{col}"' for col in ["Timestamp"] + selected_cols])

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
    final_df.drop(columns=['Timestamp'], inplace=True)
    final_df.dropna(inplace=True)
    #print(final_df)
    final_df.reset_index(drop=True, inplace=True)
    return final_df


def predict_thickness(start_ts, end_ts, start_table, end_table):
    #print(start_ts, end_ts, start_table, end_table)
    data = fetch_trace_data(start_ts, end_ts, start_table, end_table)
    #print(data)
    
    X_all = []
    data = data[selected_cols]
    tdf = data[(data['PPExecStepID'] >= 100) & (data['PPExecStepID'] < 160)]
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
    
def insert_trace_info_with_thickness(cur, start_time, end_time, start_table, end_table, thicknesses):
    assert len(thicknesses) == 45, "thicknesses must contain exactly 45 values"

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
                        insert_trace_info_with_thickness(cur, current_proc["start_time"], ts, current_proc["start_table"], table, thicknesses)
                        print(current_proc["start_time"], ts, thicknesses, '\n')
                    current_proc = None
                elif step in ("", "NAN", "NULL", "None", "nan"):
                    if last_ts:
                        duration = last_ts - current_proc["start_time"]
                        if duration >= timedelta(hours=1):
                            thicknesses = predict_thickness(current_proc["start_time"], last_ts, current_proc["start_table"], last_table)
                        if len(thicknesses) == 0:
                            thicknesses = [0 for _ in range(45)]
                            insert_trace_info_with_thickness(cur, current_proc["start_time"], last_ts, current_proc["start_table"], last_table, thicknesses)
                            print(current_proc["start_time"], last_ts, thicknesses, '\n')
                        current_proc = None
                elif last_ts:
                    gap = ts - last_ts
                    if gap >= timedelta(hours=1):
                        duration = last_ts - current_proc["start_time"]
                        if duration >= timedelta(hours=1):
                            thicknesses = predict_thickness(current_proc["start_time"], last_ts, current_proc["start_table"], last_table)
                        if len(thicknesses) == 0:
                            thicknesses = [0 for _ in range(45)]
                            insert_trace_info_with_thickness(cur, current_proc["start_time"], last_ts, current_proc["start_table"], last_table, thicknesses)
                            print(f"⚠️ 중단 감지 → 저장됨: {current_proc['start_time']} ~ {last_ts}", thicknesses, '\n')
                        else:
                            print(f"⚠️ 중단 감지 → 무시됨(1시간 미만): {current_proc['start_time']} ~ {last_ts}\n")
                        current_proc = None  # 현재 공정 종료 처리
            last_ts = ts
            last_table = table

    conn.commit()
    cur.close()
    conn.close()
    print("✅ 신규 공정 구간 추출 완료")

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
    for table in ["proc_info", "trace_info"]:
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
