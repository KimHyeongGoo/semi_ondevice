import os
import time
import pandas as pd
from glob import glob
from datetime import datetime

DATA_DIR = '../data'
OUTPUT_DIR = '../realtimedata'
SLEEP_SEC = 1

def load_and_concat_csvs(data_dir):
    csv_files = sorted(glob(os.path.join(data_dir, '*.csv')))
    df_list = [pd.read_csv(file) for file in csv_files]
    return pd.concat(df_list, ignore_index=True)

def get_file_path(data_date):
    year = data_date[0:4]
    month = data_date[5:7]
    day = data_date[8:10]
    hour = data_date[11:13]

    folder_path = os.path.join(OUTPUT_DIR, f'{year}/{month}/{day}')
    os.makedirs(folder_path, exist_ok=True)
    return os.path.join(folder_path, f'{hour}00.csv')

def replace_timestamp(row):
    row['Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # 밀리초까지
    return row


def main():
    df = load_and_concat_csvs(DATA_DIR)
    total_rows = len(df)

    current_idx = 0
    folder_id = 1
    file_id = 0
    line_in_file = 0

    # 새 파일 생성 시 헤더 포함
    row = df.iloc[current_idx].copy()
    row = replace_timestamp(row)
    file_path = get_file_path(row['Timestamp'])
    prev_hour = row['Timestamp'][11:13]
    row.to_frame().T.to_csv(file_path, index=False, mode='w', header=True)
    line_in_file = 1
    current_idx = (current_idx + 1) % total_rows
    time.sleep(SLEEP_SEC)

    while True:
        # 기존 파일에 1줄 추가
        row = df.iloc[current_idx].copy()
        row = replace_timestamp(row)
        now_hour = row['Timestamp'][11:13]
        row.to_frame().T.to_csv(file_path, index=False, mode='a', header=False)
        line_in_file += 1
        current_idx = (current_idx + 1) % total_rows
        time.sleep(SLEEP_SEC)

        if now_hour > prev_hour:
            # 새 파일에 헤더 포함 첫 줄 쓰기
            row = df.iloc[current_idx].copy()
            row = replace_timestamp(row)
            file_path = get_file_path(row['Timestamp'])
            row.to_frame().T.to_csv(file_path, index=False, mode='w', header=True)
            line_in_file = 1
            current_idx = (current_idx + 1) % total_rows
            time.sleep(SLEEP_SEC)
            prev_hour = now_hour

if __name__ == '__main__':
    main()
