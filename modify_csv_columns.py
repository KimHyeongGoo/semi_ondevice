#!/usr/bin/env python3
"""
CSV 파일의 특정 컬럼 데이터를 수정하는 스크립트
- 각 파일당 확률로 0개(30%), 1개(40%), 2개(20%), 3개(10%) 컬럼 선택
- 선택된 컬럼의 연속된 값을 조건에 맞게 수정
"""

import os
import csv
import random
import glob
import shutil
from pathlib import Path
from typing import List, Tuple, Optional
from datetime import datetime


# 수정할 컬럼 목록
TARGET_COLUMNS = ['MFC1_N2-1', 'MFC2_N2-2', 'MFC3_N2-3', 'MFC4_N2-4', 'MFC7_DCS', 'MFC8_NH3']

# 각 컬럼별 수정 규칙
COLUMN_RULES = {
    'MFC1_N2-1': {
        'target_value': 1.5,
        'target_length': (9, 13),  # 9~13라인 연속
        'replace_values': [2.323, -0.3],  # 랜덤 선택
        'tolerance': 0.01  # 값 비교 허용 오차
    },
    'MFC2_N2-2': {
        'target_value': 0,
        'target_length': 8,  # 10라인 연속
        'replace_values': [0.4, -0.323],  # 랜덤 선택
        'tolerance': 0.01
    },
    'MFC3_N2-3': {
        'target_value': 0.996,
        'target_length': 10,  # 10라인 연속
        'replace_values': [1.623, -0.423],  # 랜덤 선택
        'tolerance': 0.01
    },
    'MFC4_N2-4': {
        'target_value': 0.501,
        'target_length': 10,  # 10라인 연속
        'replace_values': [1.323],  # 고정값
        'tolerance': 0.01
    },
    'MFC7_DCS': {
        'target_value': None,  # 1.19xx 패턴
        'target_length': 7,  # 7라인 연속
        'replace_values': None,  # 1.99xx 또는 0.69xx로 변경
        'tolerance': None,
        'pattern': (1.19, 1.20),  # 1.19xx 범위
        'replace_patterns': [(1.99, 2.00), (-0.345, 0.60)]  # 랜덤 선택
    },
    'MFC8_NH3': {
        'target_value': 4.492,
        'target_length': 9,  # 9라인 연속
        'replace_values': [5.592, -0.492],  # 랜덤 선택
        'tolerance': 0.01
    }
}

# 수정 조건
MIN_START_ROW = 200  # 첫 수정 라인은 200라인 이상
MIN_ROW_GAP = 10  # 수정된 라인 간격 최소 10라인


def parse_float(value: str) -> Optional[float]:
    """문자열을 float로 변환, 실패시 None 반환"""
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def find_continuous_sequence(
    values: List[Optional[float]], 
    target_value: Optional[float],
    target_length: int,
    tolerance: Optional[float] = None,
    pattern: Optional[Tuple[float, float]] = None
) -> List[int]:
    """
    연속된 값의 시작 인덱스 리스트 반환
    
    Args:
        values: 값 리스트
        target_value: 찾을 값 (None이면 pattern 사용)
        target_length: 연속 길이
        tolerance: 값 비교 허용 오차
        pattern: 값 범위 패턴 (target_value가 None일 때 사용)
    
    Returns:
        시작 인덱스 리스트
    """
    if target_length is None:
        return []
    
    if isinstance(target_length, tuple):
        min_len, max_len = target_length
    else:
        min_len = max_len = target_length
    
    sequences = []
    i = 0
    
    while i < len(values):
        if values[i] is None:
            i += 1
            continue
        
        # 값 매칭 확인
        matched = False
        if target_value is not None:
            if tolerance is not None:
                matched = abs(values[i] - target_value) <= tolerance
            else:
                matched = abs(values[i] - target_value) < 0.0001  # float 비교
        elif pattern is not None:
            min_val, max_val = pattern
            matched = min_val <= values[i] < max_val
        
        if matched:
            # 연속된 길이 확인
            count = 1
            j = i + 1
            while j < len(values) and count < max_len:
                if values[j] is None:
                    break
                if target_value is not None:
                    if tolerance is not None:
                        if abs(values[j] - target_value) > tolerance:
                            break
                    else:
                        if abs(values[j] - target_value) >= 0.0001:
                            break
                elif pattern is not None:
                    min_val, max_val = pattern
                    if not (min_val <= values[j] < max_val):
                        break
                count += 1
                j += 1
            
            if min_len <= count <= max_len:
                sequences.append(i)
                i = j  # 연속 구간 끝으로 이동
            else:
                i += 1
        else:
            i += 1
    
    return sequences


def select_columns_to_modify() -> List[str]:
    """확률로 0개(30%), 1개(40%), 2개(20%), 3개(10%) 컬럼 선택"""
    num_columns = random.choices([0, 1, 2, 3], weights=[0, 30, 35, 35])[0]
    if num_columns == 0:
        return []
    
    selected = random.sample(TARGET_COLUMNS, num_columns)
    return selected


def find_valid_start_positions(
    sequences: List[int],
    used_positions: List[Tuple[int, int]],
    min_start: int,
    min_gap: int,
    length: int
) -> Optional[int]:
    """
    사용 가능한 시작 위치 찾기
    
    Args:
        sequences: 가능한 시작 인덱스 리스트
        used_positions: 이미 사용된 (start, end) 위치 리스트
        min_start: 최소 시작 라인
        min_gap: 최소 간격
        length: 수정할 연속 길이
    
    Returns:
        사용 가능한 시작 인덱스 또는 None
    """
    for start in sequences:
        if start < min_start:
            continue
        
        end = start + length - 1
        
        # 다른 수정 구간과 겹치는지 확인
        overlap = False
        for used_start, used_end in used_positions:
            # 겹침 확인: (start <= used_end) and (end >= used_start)
            if start <= used_end and end >= used_start:
                overlap = True
                break
            
            # 간격 확인
            gap_before = start - used_end - 1
            gap_after = used_start - end - 1
            if gap_before >= 0 and gap_before < min_gap:
                overlap = True
                break
            if gap_after >= 0 and gap_after < min_gap:
                overlap = True
                break
        
        if not overlap:
            return start
    
    return None


def modify_column_values(
    values: List[str],
    column_name: str,
    used_positions: List[Tuple[int, int]]
) -> Tuple[List[str], Optional[Tuple[int, int]]]:
    """
    컬럼 값 수정
    
    Returns:
        (수정된 값 리스트, 수정된 위치 (start, end) 또는 None)
    """
    rule = COLUMN_RULES[column_name]
    new_values = values.copy()
    
    # float 값으로 변환
    float_values = [parse_float(v) for v in values]
    
    # 연속된 시퀀스 찾기
    if rule.get('pattern') is not None:
        sequences = find_continuous_sequence(
            float_values,
            None,
            rule['target_length'],
            pattern=rule['pattern']
        )
    else:
        sequences = find_continuous_sequence(
            float_values,
            rule['target_value'],
            rule['target_length'],
            rule['tolerance']
        )
    
    if not sequences:
        return new_values, None
    
    # 사용 가능한 위치 찾기
    target_length = rule['target_length']
    if isinstance(target_length, tuple):
        target_length = target_length[1]  # 최대 길이 사용
    
    start_pos = find_valid_start_positions(
        sequences,
        used_positions,
        MIN_START_ROW,
        MIN_ROW_GAP,
        target_length
    )
    
    if start_pos is None:
        return new_values, None
    
    # 값 수정
    if rule.get('pattern') is not None:
        # MFC7_DCS: 1.19xx -> 1.99xx 또는 0.69xx
        replace_pattern = random.choice(rule['replace_patterns'])
        min_replace, max_replace = replace_pattern
        
        for i in range(start_pos, min(start_pos + target_length, len(new_values))):
            if float_values[i] is not None:
                # 원래 값의 소수점 부분 유지하면서 패턴 변경
                original = float_values[i]
                # 1.19xx -> 1.99xx 또는 0.69xx
                # 소수점 4자리 유지
                if min_replace == 1.99:
                    # 1.19xx -> 1.99xx (0.8 증가)
                    new_val = original + 0.8
                else:  # 0.69
                    # 1.19xx -> 0.69xx (0.5 감소)
                    new_val = original - 0.5
                new_values[i] = f"{new_val:.4f}"
    else:
        # 다른 컬럼들
        replace_value = random.choice(rule['replace_values'])
        for i in range(start_pos, min(start_pos + target_length, len(new_values))):
            new_values[i] = f"{replace_value:.3f}"
    
    end_pos = start_pos + target_length - 1
    return new_values, (start_pos, end_pos)


def backup_file(file_path: str, backup_dir: str, source_base: str) -> Optional[str]:
    """파일을 백업 디렉토리에 복사
    
    Args:
        file_path: 원본 파일 경로
        backup_dir: 백업 디렉토리
        source_base: 원본 파일의 기준 디렉토리 (예: /home/goo4168/semi_platform/abnormal_data)
    
    Returns:
        백업 파일 경로 또는 None
    """
    try:
        os.makedirs(backup_dir, exist_ok=True)
        
        # 원본 파일의 source_base 이후 경로 추출
        # 예: /home/goo4168/semi_platform/abnormal_data/2025/12/11/1000.csv
        #     -> 2025/12/11/1000.csv
        if not file_path.startswith(source_base):
            # source_base가 포함되지 않으면 전체 경로 사용
            rel_path = file_path.replace('/', '_').replace('\\', '_')
        else:
            rel_path = os.path.relpath(file_path, source_base)
        
        backup_path = os.path.join(backup_dir, rel_path)
        
        # 백업 디렉토리 생성
        os.makedirs(os.path.dirname(backup_path), exist_ok=True)
        
        # 파일 복사
        shutil.copy2(file_path, backup_path)
        return backup_path
    except Exception as e:
        print(f"  경고: 백업 실패 - {e}")
        return None


def process_csv_file(file_path: str, backup_dir: Optional[str] = None, source_base: Optional[str] = None) -> Tuple[bool, int]:
    """CSV 파일 처리
    
    Args:
        file_path: 처리할 CSV 파일 경로
        backup_dir: 백업 디렉토리 (None이면 백업 안 함)
        source_base: 원본 파일의 기준 디렉토리
    
    Returns:
        (성공 여부, 수정된 파라미터 개수)
    """
    print(f"\n처리 중: {file_path}")
    
    # 백업 생성
    if backup_dir and source_base:
        backup_path = backup_file(file_path, backup_dir, source_base)
        if backup_path:
            print(f"  -> 백업 완료: {backup_path}")
    
    # 수정할 컬럼 선택
    columns_to_modify = select_columns_to_modify()
    
    if not columns_to_modify:
        print(f"  -> 수정할 컬럼 없음 (0개 선택)")
        print(f"  -> 수정된 파라미터: 0개")
        return (False, 0)
    
    print(f"  -> 선택된 컬럼: {', '.join(columns_to_modify)}")
    
    # CSV 파일 읽기
    rows = []
    header = None
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
            
            # 컬럼 인덱스 찾기
            column_indices = {}
            for col in columns_to_modify:
                if col in header:
                    column_indices[col] = header.index(col)
                else:
                    print(f"  경고: 컬럼 '{col}'를 찾을 수 없습니다.")
                    print(f"  -> 수정된 파라미터: 0개")
                    return (False, 0)
            
            rows = list(reader)
    except Exception as e:
        print(f"  오류: 파일 읽기 실패 - {e}")
        print(f"  -> 수정된 파라미터: 0개")
        return (False, 0)
    
    if not rows:
        print(f"  -> 데이터가 없습니다.")
        print(f"  -> 수정된 파라미터: 0개")
        return (False, 0)
    
    # 각 컬럼별로 수정
    used_positions = []  # (start, end) 튜플 리스트
    modifications = {}  # {column_name: (start, end)}
    
    for col_name in columns_to_modify:
        col_idx = column_indices[col_name]
        
        # 해당 컬럼의 모든 값 추출
        column_values = [row[col_idx] if col_idx < len(row) else '' for row in rows]
        
        # 값 수정
        modified_values, mod_pos = modify_column_values(
            column_values,
            col_name,
            used_positions
        )
        
        if mod_pos is not None:
            start, end = mod_pos
            used_positions.append((start, end))
            modifications[col_name] = (start, end)
            
            # 행 데이터 업데이트
            for i, new_val in enumerate(modified_values):
                if i < len(rows) and col_idx < len(rows[i]):
                    rows[i][col_idx] = new_val
            
            print(f"  -> {col_name}: 라인 {start+2}~{end+2} 수정 완료")
        else:
            print(f"  -> {col_name}: 수정 가능한 위치를 찾을 수 없습니다.")
    
    if not modifications:
        print(f"  -> 수정된 내용이 없습니다.")
        print(f"  -> 수정된 파라미터: 0개")
        return (False, 0)
    
    # 수정된 파일 저장
    try:
        with open(file_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(rows)
        
        modified_count = len(modifications)
        print(f"  -> 파일 저장 완료")
        print(f"  -> 수정된 파라미터: {modified_count}개")
        return (True, modified_count)
    except Exception as e:
        print(f"  오류: 파일 저장 실패 - {e}")
        print(f"  -> 수정된 파라미터: 0개")
        return (False, 0)


def main():
    """메인 함수"""
    import sys
    
    if len(sys.argv) >= 2:
        directory = sys.argv[1]
    else:
        directory = '/home/goo4168/semi_platform/abnormal_data/2025'
    
    if not os.path.isdir(directory):
        print(f"오류: 디렉토리를 찾을 수 없습니다: {directory}")
        sys.exit(1)
    
    # 백업 디렉토리 설정
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_base = '/home/goo4168/semi_platform/abnormal_data_backup'
    backup_dir = os.path.join(backup_base, timestamp)
    
    print(f"백업 디렉토리: {backup_dir}")
    
    # 모든 CSV 파일 찾기 (하위 디렉토리 포함)
    # glob 패턴 수정: **/**/*.csv -> **/*.csv (중복 방지)
    csv_files = glob.glob(os.path.join(directory, '**/*.csv'), recursive=True)
    
    # 중복 제거 (절대 경로로 정규화)
    csv_files = list(set(os.path.abspath(f) for f in csv_files))
    csv_files = sorted(csv_files)
    
    if not csv_files:
        print(f"경고: {directory}에 CSV 파일이 없습니다.")
        sys.exit(0)
    
    print(f"총 {len(csv_files)}개의 CSV 파일을 찾았습니다.")
    
    # 원본 파일 기준 디렉토리 설정 (백업 경로 계산용)
    source_base = '/home/goo4168/semi_platform/abnormal_data'
    
    # 각 파일 처리
    success_count = 0
    total_modified_params = 0
    processed_files = set()  # 이미 처리된 파일 추적
    
    for csv_file in csv_files:
        # 이미 처리된 파일인지 확인
        if csv_file in processed_files:
            print(f"\n경고: 파일이 이미 처리되었습니다. 건너뜀: {csv_file}")
            continue
        
        processed_files.add(csv_file)
        success, modified_count = process_csv_file(csv_file, backup_dir, source_base)
        if success:
            success_count += 1
            total_modified_params += modified_count
    
    print(f"\n처리 완료: {success_count}/{len(csv_files)} 파일 수정됨")
    print(f"총 수정된 파라미터: {total_modified_params}개")
    print(f"\n백업 위치: {backup_dir}")
    print(f"원상복구 명령: python restore_csv_backup.py {backup_dir}")


if __name__ == '__main__':
    main()

