#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import csv
import math
from datetime import datetime, timedelta
from glob import glob

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
    KST = ZoneInfo("Asia/Seoul")
except Exception:
    KST = None  # fallback: system local time

# ===== 사용자 설정 =====
DATA_DIR = '../data_2'
OUTPUT_DIR = '../realtimedata_2'
SLEEP_SEC = 1  # 1초에 1라인씩
SLEEP_SEC_2 = 60  # 1분에 1파일씩

# ===== 출력 컬럼 순서 =====
TARGET_COLUMNS = [
    "Timer",
    "Stage1.Temp1","Stage2.Temp1",
    "Line.Gauge.i","Ion.Gauge.i","Baratron.Gauge.i",
    "Ar.MFC.i","Ar.MFC.o",
    "Power","Current","Volt",
    "PLA5.Match.Load.Posi","PLA5.Match.Tune.Posi",
    "PLA5.Match.Load.Pre","PLA5.Match.Tune.Pre","PLA5.Match.DCBias",
    "SBRF5.Forward","SBRF5.Reflect",
    "PWESC.Volt1","PWESC.Volt2",
    "PWPDS.Data",
]

# ===== 입력 컬럼 별칭 (원본 헤더가 EN4.*, ULVAC.* 여도 인식) =====
SOURCE_ALIASES = {
    "Timer": ["Timer"],
    "Stage1.Temp1": ["ULVAC.Stage1.Temp1", "Stage1.Temp1"],
    "Stage2.Temp1": ["ULVAC.Stage2.Temp1", "Stage2.Temp1"],
    "Line.Gauge.i": ["Line.Gauge.i"],
    "Ion.Gauge.i": ["Ion.Gauge.i"],
    "Baratron.Gauge.i": ["Baratron.Gauge.i"],
    "Ar.MFC.i": ["Ar.MFC.i"],
    "Ar.MFC.o": ["Ar.MFC.o"],
    "Power": ["EN4.Power", "Power"],
    "Current": ["EN4.Current", "Current"],
    "Volt": ["EN4.Volt", "Volt"],
    "PLA5.Match.Load.Posi": ["PLA5.Match.Load.Posi"],
    "PLA5.Match.Tune.Posi": ["PLA5.Match.Tune.Posi"],
    "PLA5.Match.Load.Pre": ["PLA5.Match.Load.Pre"],
    "PLA5.Match.Tune.Pre": ["PLA5.Match.Tune.Pre"],
    "PLA5.Match.DCBias": ["PLA5.Match.DCBias"],
    "SBRF5.Forward": ["SBRF5.Forward"],
    "SBRF5.Reflect": ["SBRF5.Reflect"],
    "PWESC.Volt1": ["PWESC.Volt1"],
    "PWESC.Volt2": ["PWESC.Volt2"],
    "PWPDS.Data": ["PWPDS.Data"],
}

def now_kst():
    if KST is not None:
        return datetime.now(tz=KST).replace(microsecond=0)
    return datetime.now().replace(microsecond=0)

def ensure_dirs():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

def list_input_csvs():
    return sorted(glob(os.path.join(DATA_DIR, '*.csv')))

def output_filename_from_dt(dt: datetime) -> str:
    return f"PVD4_NEW_{dt.strftime('%Y%m%d_%H%M%S')}.csv"

def pick_value(row: dict, alias_list):
    for k in alias_list:
        if k in row and row[k] != "":
            return row[k]
    return math.nan  # 없는 경우 NaN

def reorder_row_without_timer(row: dict) -> dict:
    """Timer는 여기서 채우지 않고, 나머지 컬럼만 재배치/채움(NaN)."""
    out = {}
    for out_col in TARGET_COLUMNS:
        if out_col == "Timer":
            continue
        alias = SOURCE_ALIASES.get(out_col, [out_col])
        out[out_col] = pick_value(row, alias)
    return out

def read_all_rows_in_order(input_csv_path: str):
    with open(input_csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = [row for row in reader]
        return rows

def simulate_append_rows(out_path: str, base_dt: datetime, rows_in_order: list):
    """
    출력 파일에 1초마다 한 줄씩 append.
    Timer는 base_dt부터 1초씩 증가하여 기록.
    """
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=TARGET_COLUMNS)
        writer.writeheader()
        for i, src_row in enumerate(rows_in_order):
            out_row = reorder_row_without_timer(src_row)
            # Timer를 현재 시각 기준으로 재생성: [YYYY.MM.DD HH:MM:SS]
            new_ts = base_dt + timedelta(seconds=i)
            out_row["Timer"] = new_ts.strftime("[%Y.%m.%d %H:%M:%S]")
            writer.writerow(out_row)
            f.flush()
            time.sleep(SLEEP_SEC)

def process_one_csv(input_csv_path: str):
    rows = read_all_rows_in_order(input_csv_path)
    if not rows:
        print(f"[WARN] 빈 파일: {input_csv_path}")
        return

    # 이 CSV의 시작 시각(파일명·첫 줄 Timer 기준)
    base_dt = now_kst()  # 예: 2025-09-24 15:55:55
    out_name = output_filename_from_dt(base_dt)
    out_path = os.path.join(OUTPUT_DIR, out_name)

    if os.path.exists(out_path):
        try:
            os.remove(out_path)
        except Exception as e:
            print(f"[WARN] 기존 파일 삭제 실패: {out_path} ({e})")

    print(f"[INFO] 생성 시작 -> {out_path} (총 {len(rows)} 라인)")
    simulate_append_rows(out_path, base_dt, rows)
    print(f"[INFO] 생성 완료 -> {out_path}")
    time.sleep(SLEEP_SEC_2)

def main():
    ensure_dirs()
    while True:
        input_files = list_input_csvs()
        if not input_files:
            print(f"[WARN] 입력 CSV가 없습니다: {DATA_DIR}")
            time.sleep(3)
            continue

        for csv_path in input_files:
            try:
                process_one_csv(csv_path)
            except KeyboardInterrupt:
                print("\n[INFO] 사용자 중지(Ctrl+C). 종료합니다.")
                return
            except Exception as e:
                print(f"[ERROR] 처리 중 오류: {csv_path} ({e})")
                continue

if __name__ == "__main__":
    main()
