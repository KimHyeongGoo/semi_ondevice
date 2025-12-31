#!/usr/bin/env python3
"""
abnormal_data/2025/12/dd/hh00.csv 파일들의 Timestamp 컬럼을 업데이트
파일 경로에서 추출한 dd와 hh를 사용하여 Timestamp의 일과 시간을 변경
"""

import os
import csv
import re
from pathlib import Path
from datetime import datetime


def update_timestamp(timestamp_str, new_day, new_hour):
    """
    Timestamp 문자열의 일과 시간을 변경
    예: '2025-12-01 19:00:30.299' -> '2025-12-22 15:00:30.299' (dd=22, hh=15)
    """
    try:
        # Timestamp 파싱: '2025-12-01 19:00:30.299'
        match = re.match(r'(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})(\.\d+)?', timestamp_str)
        if not match:
            return timestamp_str
        
        year = match.group(1)
        month = match.group(2)
        old_day = match.group(3)
        old_hour = match.group(4)
        minute = match.group(5)
        second = match.group(6)
        microsecond = match.group(7) if match.group(7) else ''
        
        # 새 일과 시간으로 변경
        new_timestamp = f"{year}-{month}-{new_day} {new_hour}:{minute}:{second}{microsecond}"
        return new_timestamp
    except Exception as e:
        print(f"⚠️ Timestamp 업데이트 실패: {timestamp_str} → {e}")
        return timestamp_str


def process_csv_file(csv_path, day, hour):
    """
    CSV 파일의 Timestamp 컬럼 업데이트
    """
    try:
        # Path 객체를 문자열로 변환
        csv_path_str = str(csv_path)
        # 임시 파일 경로
        temp_path = csv_path_str + '.tmp'
        
        with open(csv_path_str, 'r', encoding='utf-8') as f_in:
            reader = csv.reader(f_in)
            
            # 헤더 읽기
            header = next(reader)
            
            # Timestamp 컬럼 인덱스 찾기
            try:
                timestamp_idx = header.index('Timestamp')
            except ValueError:
                print(f"❌ {csv_path}: Timestamp 컬럼을 찾을 수 없습니다.")
                return False
            
            # 임시 파일에 쓰기
            with open(temp_path, 'w', encoding='utf-8', newline='') as f_out:
                writer = csv.writer(f_out)
                
                # 헤더 쓰기
                writer.writerow(header)
                
                # 데이터 행 처리
                updated_count = 0
                for row in reader:
                    if len(row) > timestamp_idx:
                        # Timestamp 업데이트
                        old_timestamp = row[timestamp_idx]
                        new_timestamp = update_timestamp(old_timestamp, day, hour)
                        row[timestamp_idx] = new_timestamp
                        if old_timestamp != new_timestamp:
                            updated_count += 1
                    writer.writerow(row)
        
        # 원본 파일을 임시 파일로 교체
        os.replace(temp_path, csv_path_str)
        print(f"✅ {csv_path_str}: {updated_count}개 행 업데이트 완료 (dd={day}, hh={hour})")
        return True
        
    except Exception as e:
        csv_path_str = str(csv_path)
        print(f"❌ {csv_path_str} 처리 실패: {e}")
        # 임시 파일이 있으면 삭제
        temp_path = csv_path_str + '.tmp'
        if os.path.exists(temp_path):
            os.remove(temp_path)
        return False


def main():
    """메인 함수"""
    base_dir = Path("/home/goo4168/semi_platform/abnormal_data/2025/12")
    
    if not base_dir.exists():
        print(f"❌ 경로가 존재하지 않습니다: {base_dir}")
        return
    
    # 모든 CSV 파일 찾기
    csv_files = list(base_dir.glob("*/[0-9][0-9][0-9][0-9].csv"))
    csv_files.sort()
    
    print(f"📁 발견된 CSV 파일 수: {len(csv_files)}\n")
    
    if not csv_files:
        print("❌ 처리할 파일이 없습니다.")
        return
    
    success_count = 0
    fail_count = 0
    
    for csv_path in csv_files:
        # 파일 경로에서 dd와 hh 추출
        # 예: /path/to/abnormal_data/2025/12/22/1500.csv
        #     -> dd = 22, filename = 1500.csv -> hh = 15
        parts = csv_path.parts
        day = parts[-2]  # dd 폴더명
        
        filename = csv_path.stem  # '1500' (확장자 제거)
        if len(filename) == 4 and filename.isdigit():
            hour = filename[:2]  # '15'
        else:
            print(f"⚠️ {csv_path}: 파일명 형식이 올바르지 않습니다 (예상: hh00.csv)")
            fail_count += 1
            continue
        
        # CSV 파일 처리
        if process_csv_file(csv_path, day, hour):
            success_count += 1
        else:
            fail_count += 1
    
    print(f"\n📊 결과:")
    print(f"  - 성공: {success_count}개")
    print(f"  - 실패: {fail_count}개")
    print(f"  - 총: {len(csv_files)}개")


if __name__ == '__main__':
    print("🚀 abnormal_data Timestamp 업데이트 시작\n")
    main()
    print("\n✅ 완료!")

