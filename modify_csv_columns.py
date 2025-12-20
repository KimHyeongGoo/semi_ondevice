#!/usr/bin/env python3
"""
CSV 파일의 특정 컬럼 데이터를 수정하는 스크립트
- 각 파일당 25% 확률로 0개, 1개, 2개, 3개 컬럼 선택
- 선택된 컬럼의 연속된 값을 조건에 맞게 수정
"""

import os
import csv
import random
import glob
from pathlib import Path
from typing import List, Tuple, Optional


# 수정할 컬럼 목록
TARGET_COLUMNS = ['MFC1_N2-1', 'MFC3_N2-3', 'MFC4_N2-4', 'MFC7_DCS', 'MFC8_NH3']

# 각 컬럼별 수정 규칙
COLUMN_RULES = {
    'MFC1_N2-1': {
        'target_value': 1.5,
        'target_length': (9, 13),  # 9~13라인 연속
        'replace_values': [2.323, 0.853],  # 랜덤 선택
        'tolerance': 0.01  # 값 비교 허용 오차
    },
    'MFC3_N2-3': {
        'target_value': 0.996,
        'target_length': 10,  # 10라인 연속
        'replace_values': [1.923, 0.623],  # 랜덤 선택
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
        'target_length': 5,  # 5라인 연속
        'replace_values': None,  # 1.99xx 또는 0.69xx로 변경
        'tolerance': None,
        'pattern': (1.19, 1.20),  # 1.19xx 범위
        'replace_patterns': [(1.99, 2.00), (0.69, 0.70)]  # 랜덤 선택
    },
    'MFC8_NH3': {
        'target_value': 4.492,
        'target_length': 9,  # 9라인 연속
        'replace_values': [7.492, 2.492],  # 랜덤 선택
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
    """25% 확률로 0개, 1개, 2개, 3개 컬럼 선택"""
    num_columns = random.choices([0, 1, 2, 3], weights=[25, 25, 25, 25])[0]
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


def process_csv_file(file_path: str) -> bool:
    """CSV 파일 처리"""
    print(f"\n처리 중: {file_path}")
    
    # 수정할 컬럼 선택
    columns_to_modify = select_columns_to_modify()
    
    if not columns_to_modify:
        print(f"  -> 수정할 컬럼 없음 (0개 선택)")
        return False
    
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
                    return False
            
            rows = list(reader)
    except Exception as e:
        print(f"  오류: 파일 읽기 실패 - {e}")
        return False
    
    if not rows:
        print(f"  -> 데이터가 없습니다.")
        return False
    
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
        return False
    
    # 수정된 파일 저장
    try:
        with open(file_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(rows)
        
        print(f"  -> 파일 저장 완료")
        return True
    except Exception as e:
        print(f"  오류: 파일 저장 실패 - {e}")
        return False


def main():
    """메인 함수"""
    import sys
    
    if len(sys.argv) < 2:
        print("사용법: python modify_csv_columns.py <디렉토리 경로>")
        print("예시: python modify_csv_columns.py /home/goo4168/semi_platform/data")
        sys.exit(1)
    
    directory = sys.argv[1]
    
    if not os.path.isdir(directory):
        print(f"오류: 디렉토리를 찾을 수 없습니다: {directory}")
        sys.exit(1)
    
    # 모든 CSV 파일 찾기
    csv_files = glob.glob(os.path.join(directory, '*.csv'))
    
    if not csv_files:
        print(f"경고: {directory}에 CSV 파일이 없습니다.")
        sys.exit(0)
    
    print(f"총 {len(csv_files)}개의 CSV 파일을 찾았습니다.")
    
    # 각 파일 처리
    success_count = 0
    for csv_file in sorted(csv_files):
        if process_csv_file(csv_file):
            success_count += 1
    
    print(f"\n처리 완료: {success_count}/{len(csv_files)} 파일 수정됨")


if __name__ == '__main__':
    main()

