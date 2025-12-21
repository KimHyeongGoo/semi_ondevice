#!/usr/bin/env python3
"""
semi_platform/traceData/2025/11/KE-PJ000000000XX 경로 내부의 CSV 파일들 중
중간에 있는 파일에서 월,일,시간을 추출하여
semi_platform/abnormal_data/2025/mm/dd/hh00.csv 파일을 생성하는 스크립트
"""

import os
import csv
import glob
from pathlib import Path
from datetime import datetime
import re


def extract_date_time_from_csv(csv_file_path):
    """
    CSV 파일에서 Date와 Time 컬럼을 읽어서 월, 일, 시간을 추출
    """
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            
            # 헤더 찾기
            header = None
            date_idx = None
            time_idx = None
            
            for i, row in enumerate(reader):
                if i == 4:  # 5번째 줄이 헤더 (0-indexed로 4)
                    header = row
                    try:
                        date_idx = header.index('Date')
                        time_idx = header.index('Time')
                    except ValueError:
                        print(f"Date 또는 Time 컬럼을 찾을 수 없습니다: {csv_file_path}")
                        return None, None, None
                    break
            
            # 데이터 행 읽기 (6번째 줄부터)
            for i, row in enumerate(reader):
                if i == 0 and len(row) > max(date_idx, time_idx):  # 첫 번째 데이터 행
                    date_str = row[date_idx].strip()
                    time_str = row[time_idx].strip()
                    
                    # Date 형식: "11/02/2025" -> mm/dd
                    # Time 형식: "17:01:22.0" -> hh
                    try:
                        # Date 파싱
                        date_parts = date_str.split('/')
                        if len(date_parts) == 3:
                            month = date_parts[0].zfill(2)  # mm
                            day = date_parts[1].zfill(2)    # dd
                        else:
                            print(f"날짜 형식 오류: {date_str}")
                            return None, None, None
                        
                        # Time 파싱
                        time_parts = time_str.split(':')
                        if len(time_parts) >= 1:
                            hour = time_parts[0].zfill(2)  # hh
                        else:
                            print(f"시간 형식 오류: {time_str}")
                            return None, None, None
                        
                        return month, day, hour
                    except Exception as e:
                        print(f"날짜/시간 파싱 오류: {e}")
                        return None, None, None
            
            print(f"데이터 행을 찾을 수 없습니다: {csv_file_path}")
            return None, None, None
            
    except Exception as e:
        print(f"CSV 파일 읽기 오류: {csv_file_path}, {e}")
        return None, None, None


def update_timestamp_column(timestamp_str, new_month, new_day, new_hour):
    """
    Timestamp 문자열의 월, 일, 시간을 변경
    형식: "2025-05-15 11:00:30.299" -> "2025-{new_month}-{new_day} {new_hour}:00:30.299"
    """
    try:
        # Timestamp 파싱
        # 형식: "2025-05-15 11:00:30.299"
        match = re.match(r'(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2}\.\d+)', timestamp_str)
        if match:
            year = match.group(1)
            minute = match.group(5)
            second = match.group(6)
            # 새로운 Timestamp 생성
            new_timestamp = f"{year}-{new_month}-{new_day} {new_hour}:{minute}:{second}"
            return new_timestamp
        else:
            # 파싱 실패 시 원본 반환
            return timestamp_str
    except Exception as e:
        print(f"Timestamp 업데이트 오류: {timestamp_str}, {e}")
        return timestamp_str


def create_abnormal_data_file(source_csv, output_path, month, day, hour):
    """
    source_csv의 데이터를 복사하여 output_path에 저장
    Timestamp 컬럼의 월, 일, 시간만 변경
    """
    try:
        # 출력 디렉토리 생성
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        with open(source_csv, 'r', encoding='utf-8-sig') as f_in:  # utf-8-sig는 BOM을 자동으로 제거
            reader = csv.reader(f_in)
            header = next(reader)
            
            # BOM 제거 (혹시 모를 경우를 대비)
            header = [col.strip('\ufeff') if col.startswith('\ufeff') else col for col in header]
            
            # Timestamp 컬럼 인덱스 찾기
            try:
                timestamp_idx = header.index('Timestamp')
            except ValueError:
                print(f"Timestamp 컬럼을 찾을 수 없습니다: {source_csv}")
                print(f"헤더: {header[:5]}...")  # 디버깅용
                return False
            
            with open(output_path, 'w', encoding='utf-8', newline='') as f_out:
                writer = csv.writer(f_out)
                
                # 헤더 쓰기
                writer.writerow(header)
                
                # 데이터 행 처리
                for row in reader:
                    if len(row) > timestamp_idx:
                        # Timestamp 업데이트
                        row[timestamp_idx] = update_timestamp_column(
                            row[timestamp_idx], month, day, hour
                        )
                    writer.writerow(row)
        
        print(f"파일 생성 완료: {output_path}")
        return True
        
    except Exception as e:
        print(f"파일 생성 오류: {output_path}, {e}")
        import traceback
        traceback.print_exc()
        return False


def process_month(base_dir, month_num, source_csv, output_base_dir):
    """
    특정 월의 데이터를 처리하는 함수
    """
    trace_data_dir = base_dir / "traceData" / "2025" / str(month_num).zfill(2)
    
    if not trace_data_dir.exists():
        print(f"경로가 존재하지 않습니다: {trace_data_dir}")
        return
    
    # KE-PJ000000000XX 디렉토리 찾기
    pj_dirs = sorted([d for d in trace_data_dir.iterdir() 
                     if d.is_dir() and d.name.startswith('KE-PJ')])
    
    if not pj_dirs:
        print(f"KE-PJ 디렉토리를 찾을 수 없습니다: {trace_data_dir}")
        return
    
    print(f"\n{'='*60}")
    print(f"{month_num}월 처리 시작 - 총 {len(pj_dirs)}개의 KE-PJ 디렉토리")
    print(f"{'='*60}")
    
    # 각 디렉토리 처리
    for pj_dir in pj_dirs:
        print(f"\n처리 중: {pj_dir.name}")
        
        # CSV 파일 찾기
        csv_files = sorted([f for f in pj_dir.glob("*.csv")])
        
        if not csv_files:
            print(f"  CSV 파일을 찾을 수 없습니다: {pj_dir}")
            continue
        
        # 중간에 있는 파일 선택
        mid_idx = len(csv_files) // 2
        selected_csv = csv_files[mid_idx]
        print(f"  선택된 파일: {selected_csv.name} (전체 {len(csv_files)}개 중 {mid_idx+1}번째)")
        
        # 월, 일, 시간 추출
        month, day, hour = extract_date_time_from_csv(selected_csv)
        
        if month is None or day is None or hour is None:
            print(f"  날짜/시간 추출 실패: {selected_csv}")
            continue
        
        print(f"  추출된 날짜/시간: {month}/{day} {hour}:00")
        
        # 출력 파일 경로
        output_path = output_base_dir / month / day / f"{hour}00.csv"
        
        # 파일 생성
        if create_abnormal_data_file(source_csv, output_path, month, day, hour):
            print(f"  성공: {output_path}")
        else:
            print(f"  실패: {output_path}")


def main():
    # 경로 설정
    base_dir = Path("/home/goo4168/semi_platform")
    source_csv = base_dir / "semi_ondevice" / "data" / "1100.csv"
    output_base_dir = base_dir / "abnormal_data" / "2025"
    
    # 소스 CSV 파일 확인
    if not source_csv.exists():
        print(f"소스 CSV 파일을 찾을 수 없습니다: {source_csv}")
        return
    
    # 처리할 월 목록
    months = [11, 12]
    
    # 각 월 처리
    for month_num in months:
        process_month(base_dir, month_num, source_csv, output_base_dir)
    
    print("\n" + "="*60)
    print("모든 작업 완료!")
    print("="*60)


if __name__ == "__main__":
    main()
