import csv
import os
import glob
import re
from pathlib import Path


def update_date_month_in_csv(csv_path):
    """CSV 파일의 Date 컬럼(mm/dd/yyyy)에서 월을 11로 변경"""
    try:
        # 파일 읽기
        with open(csv_path, 'r', encoding='utf-8', newline='') as f:
            content = f.read()
        
        # mm/dd/yyyy 형식의 날짜를 찾아서 월 부분만 11로 변경
        # 정규식: 숫자/숫자/숫자 형식 (mm/dd/yyyy)
        pattern = r'(\d{1,2})/(\d{1,2})/(\d{4})'
        
        def replace_month(match):
            month, day, year = match.groups()
            # 월을 11로 변경 (단일 숫자면 앞에 0 추가하지 않음)
            return f'11/{day}/{year}'
        
        # Date 컬럼이 있는 헤더 라인 이후의 데이터만 변경
        lines = content.split('\n')
        modified_lines = []
        data_started = False
        date_col_index = None
        
        for line in lines:
            # 헤더 라인 찾기
            if 'Date' in line and ',' in line and not data_started:
                cols = [col.strip() for col in line.split(',')]
                try:
                    date_col_index = cols.index('Date')
                    data_started = True
                    modified_lines.append(line)
                    continue
                except ValueError:
                    modified_lines.append(line)
                    continue
            
            # 헤더 이전 라인은 그대로
            if not data_started:
                modified_lines.append(line)
                continue
            
            # 데이터 라인 처리
            if date_col_index is not None and ',' in line:
                cols = line.split(',')
                if len(cols) > date_col_index:
                    date_value = cols[date_col_index].strip()
                    # mm/dd/yyyy 형식인지 확인하고 월을 11로 변경
                    if date_value and re.match(r'\d{1,2}/\d{1,2}/\d{4}', date_value):
                        parts = date_value.split('/')
                        if len(parts) == 3:
                            # 월을 11로 변경
                            cols[date_col_index] = f'11/{parts[1]}/{parts[2]}'
                            line = ','.join(cols)
            
            modified_lines.append(line)
        
        # 파일 쓰기
        with open(csv_path, 'w', encoding='utf-8', newline='') as f:
            f.write('\n'.join(modified_lines))
        
        return True
        
    except Exception as e:
        print(f"❌ {os.path.basename(csv_path)} 처리 실패: {e}")
        return False


def process_all_csv_files(base_dir):
    """특정 경로의 모든 CSV 파일 처리"""
    csv_pattern = os.path.join(base_dir, "**", "*.csv")
    csv_files = sorted(glob.glob(csv_pattern, recursive=True))
    
    if not csv_files:
        print(f"❌ {base_dir} 경로에 CSV 파일이 없습니다.")
        return
    
    print(f"📂 발견된 CSV 파일: {len(csv_files)}개\n")
    
    success_count = 0
    fail_count = 0
    
    for csv_file in csv_files:
        print(f"📄 처리 중: {os.path.relpath(csv_file, base_dir)}")
        if update_date_month_in_csv(csv_file):
            success_count += 1
            print(f"   ✅ 완료")
        else:
            fail_count += 1
    
    print(f"\n📊 처리 결과:")
    print(f"   ✅ 성공: {success_count}개")
    print(f"   ❌ 실패: {fail_count}개")
    print(f"   📁 전체: {len(csv_files)}개")


if __name__ == "__main__":
    base_directory = '/home/goo4168/semi_platform/traceData/2025/11'
    
    if not os.path.isdir(base_directory):
        print(f"❌ 경로가 존재하지 않습니다: {base_directory}")
    else:
        print(f"🚀 CSV 파일 Date 컬럼 월 변경 시작\n")
        print(f"📂 대상 경로: {base_directory}\n")
        process_all_csv_files(base_directory)
        print("\n✅ 모든 작업 완료")

