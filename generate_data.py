import os
import time
import pandas as pd
from glob import glob
from datetime import datetime

DATA_DIR = './data'
OUTPUT_DIR = './realtimedata'
LINES_PER_FILE = 100
FILES_PER_FOLDER = 24  # 0000 ~ 2300 (100씩 증가)
MAX_FILE_IDX = FILES_PER_FOLDER * LINES_PER_FILE  # 2400줄
SLEEP_SEC = 1

def load_and_concat_csvs(data_dir):
    csv_files = sorted(glob(os.path.join(data_dir, '*.csv')))
    df_list = [pd.read_csv(file) for file in csv_files]
    return pd.concat(df_list, ignore_index=True)

def get_file_path(folder_id, file_id):
    folder_path = os.path.join(OUTPUT_DIR, f'{folder_id:04d}')
    os.makedirs(folder_path, exist_ok=True)
    return os.path.join(folder_path, f'{file_id:04d}.csv')

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

    file_path = get_file_path(folder_id, file_id)
    # 새 파일 생성 시 헤더 포함
    row = df.iloc[current_idx].copy()
    row = replace_timestamp(row)
    row.to_frame().T.to_csv(file_path, index=False, mode='w', header=True)
    line_in_file = 1
    current_idx = (current_idx + 1) % total_rows
    time.sleep(SLEEP_SEC)

    while True:
        # 기존 파일에 1줄 추가
        row = df.iloc[current_idx].copy()
        row = replace_timestamp(row)
        row.to_frame().T.to_csv(file_path, index=False, mode='a', header=False)
        line_in_file += 1
        current_idx = (current_idx + 1) % total_rows
        time.sleep(SLEEP_SEC)

        if line_in_file >= LINES_PER_FILE:
            # 새 파일 번호 계산
            file_id += 100
            if file_id >= MAX_FILE_IDX:
                file_id = 0
                folder_id += 1

            file_path = get_file_path(folder_id, file_id)
            # 새 파일에 헤더 포함 첫 줄 쓰기
            row = df.iloc[current_idx].copy()
            row = replace_timestamp(row)
            row.to_frame().T.to_csv(file_path, index=False, mode='w', header=True)
            line_in_file = 1
            current_idx = (current_idx + 1) % total_rows
            time.sleep(SLEEP_SEC)

if __name__ == '__main__':
    main()
