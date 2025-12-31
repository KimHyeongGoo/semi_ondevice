# Semi On-Device 프로젝트 코드 기능 분석 노트

## 📋 프로젝트 개요

반도체 공정 장비(ALD/PVD)의 실시간 데이터 수집, 예측, 이상감지 및 시각화를 위한 통합 시스템입니다. 
TensorFlow 기반 시계열 예측 모델과 XGBoost를 활용하여 공정 파라미터를 모니터링하고 이상 상황을 감지합니다.

---

## 🗂️ 코드 구조 및 기능 분류

### 1. 데이터 수집 및 저장 (Data Ingestion)

#### 1.1 실시간 데이터 생성
- **`generate_data.py`**
  - **목적**: 테스트/시뮬레이션용 실시간 CSV 데이터 생성
  - **기능**:
    - `data/` 디렉토리의 CSV 파일들을 읽어서 실시간 형식으로 변환
    - `realtimedata/YYYY/MM/DD/HH00.csv` 형식으로 저장
    - 1초 간격으로 타임스탬프를 갱신하며 데이터 생성
    - `generator_health.json` 파일에 마지막 틱 시간 기록 (헬스체크용)
  - **사용 시점**: 테스트 환경에서만 실행

#### 1.2 실시간 데이터 DB 삽입
- **`insert_real_time.py`**
  - **목적**: 실시간 CSV 파일을 감시하여 PostgreSQL에 자동 삽입
  - **기능**:
    - `realtimedata/` 디렉토리를 감시 (watchdog 사용)
    - 새로 생성되거나 수정된 CSV 행을 실시간으로 DB에 삽입
    - 일별 테이블 자동 생성: `rawdataYYYYMMDD` 형식
    - 누락 데이터 자동 보완 기능 (`insert_old_data.insert_missing_data` 호출)
    - 파일별 오프셋 추적으로 중복 삽입 방지
  - **실행 방식**: 백그라운드 데몬으로 지속 실행

#### 1.3 과거 데이터 일괄 삽입
- **`insert_old_data.py`**
  - **목적**: 과거 CSV 데이터를 일괄적으로 DB에 적재
  - **기능**:
    - 파일 구조 탐색하여 누락된 데이터만 선별 삽입
    - 중복 데이터 방지 (ON CONFLICT 처리)
    - 날짜별 테이블 자동 생성 및 관리

#### 1.4 장비 이력 데이터 삽입
- **`insert_equip_history.py`**
  - **목적**: 공정 장비의 이력 정보 저장
  - **기능**: `equipment_history` 테이블에 공정 레시피, 시작/종료 시간 등 저장

---

### 2. 실시간 예측 (Real-time Prediction)

#### 2.1 실시간 예측 메인 프로세스
- **`predict_real_time.py`**
  - **목적**: DB의 최신 원시 데이터를 주기적으로 읽어 딥러닝 모델로 실시간 예측 수행
  - **핵심 기능**:
    - **예측 대상**: 5개 파라미터 그룹 (TASKS)
      1. MFC7_DCS, MFC8_NH3, MFC26_F.PWR
      2. MFC1_N2-1, MFC2_N2-2, MFC3_N2-3
      3. MFC4_N2-4, MFC27_L.POS, MFC28_R.POS
      4. VG11/12/13 Press value
      5. Temp_Act_U/CU/C/CL/L (온도 5개 위치)
    - **예측 스텝**: 10초, 20초, 30초 후 값 예측
    - **윈도우 크기**: 192초 (약 3분) 시퀀스 데이터 사용
    - **모델 아키텍처**: PatchTST (Patch-based Time Series Transformer)
      - PatchEmbedding 레이어
      - PositionalEncoding 레이어
    - **특수 처리**:
      - 온도 파라미터는 단일 모델로 일괄 예측
      - 특정 공정 단계(Main Process)에서는 별도 보조 모델 사용
      - VG11 Press value는 weighted MAE loss 적용
    - **병렬 처리**: Ray를 사용하여 파라미터 그룹별 병렬 예측
    - **데이터 보간**: 1.2초 이상 간격이 있으면 1초 단위로 보간
    - **결과 저장**: `pred_proc{idx}` 테이블에 예측값 저장
  - **실행 방식**: 백그라운드 데몬으로 지속 실행

---

### 3. 공정 정보 추출 및 두께 예측 (Process Detection & Thickness Prediction)

#### 3.1 공정 구간 추출 및 두께 예측
- **`insert_trace_info.py`**
  - **목적**: 저장된 원시 데이터에서 공정 구간을 추출하고 두께 예측 수행
  - **기능**:
    - 공정 구간 감지 (START ~ END 사이)
    - XGBoost 모델로 박막 두께 예측 (45개 포인트: 9개 포인터 × 5개 웨이퍼 위치)
    - STANDARD 모델을 통한 공정 파라미터 예측
    - 결과 저장:
      - `trace_info`: 공정 정보 및 두께 데이터
      - `trace_parameter_pred`: STANDARD 모델 예측값
    - GPU/CPU 자동 선택
  - **실행 방식**: 주기적 실행 또는 트리거 기반

---

### 4. 이상감지 (Anomaly Detection)

#### 4.1 실시간 이상감지 모니터
- **`abnormal_monitor2.py`**
  - **목적**: 실제값과 예측값의 차이를 기반으로 실시간 이상감지
  - **기능**:
    - **감지 대상**: MFC 파라미터 6개 (MFC7_DCS, MFC8_NH3, MFC1_N2-1, MFC2_N2-2, MFC3_N2-3, MFC4_N2-4)
    - **임계값**:
      - 상대 오차: 10% 이상
      - 절대 오차: 0.4 이상
    - **이상 구간 판정**:
      - 최소 지속시간: 5초
      - 정상 구간 2초 이상이면 이상 구간 종료
    - **장비 시작 그레이스 기간**: 장비 시작 후 60초 동안 이상감지 비활성화
    - **프로세스 재가동 감지**: generate_data.py, insert_real_time.py 재시작 감지
    - **결과 저장**: `realtime_abnormal_log` 테이블
      - 시작/종료 시간, 지속시간, 평균/최대 오차율, 피크 시간/값, violation_type 등
    - **violation_type**: 기존 데이터와의 겹침 확인하여 우선순위 부여
  - **실행 방식**: 백그라운드 데몬으로 지속 실행

#### 4.2 배치 이상감지 처리
- **`batch_process_abnormal_temp.py`**
  - **목적**: DB에 저장된 과거 데이터에 대해 배치로 예측 수행
  - **기능**:
    - 특정 기간(2025-11-02 ~ 2025-12-25)의 데이터를 일별 테이블 단위로 처리
    - 데이터 밀도화 (densify): 1.2초 이상 간격 보간
    - 슬라이딩 윈도우 방식으로 배치 예측
    - 예측 결과를 `pred_proc{idx}` 테이블에 저장
  - **차이점**: 이상감지 기능 없음, 예측만 수행

- **`batch_process_abnormal.py`** (백업 파일: `.back`)
  - **목적**: CSV 파일에서 데이터를 읽어 예측 및 이상감지 수행
  - **기능**:
    - `abnormal_data/` 디렉토리의 CSV 파일 처리
    - 예측 + 이상감지 + 이상 데이터 저장

- **`batch_process_abnormal2.py`**
  - **목적**: DB 테이블에서 데이터를 읽어 예측 및 이상감지 수행
  - **기능**:
    - DB의 `rawdataYYYYMMDD` 테이블에서 데이터 조회
    - 예측 → 이상감지 → 이상 데이터 저장 파이프라인

- **`batch_process_abnormal3.py`**
  - **목적**: `batch_process_abnormal2.py`의 개선 버전
  - **기능**: 동일하지만 코드 구조 개선

---

### 5. 웹 인터페이스 (Web Interface)

#### 5.1 FastAPI 서버
- **`fastapi/main.py`**
  - **목적**: 예측 결과, 로그, 차트 데이터를 제공하는 REST API 서버
  - **주요 엔드포인트**:
    - **데이터 조회**:
      - `/api/data`: 최근 N초간의 실제값/예측값 조회
      - `/api/trace_info`: 공정 정보 및 두께 데이터 조회
      - `/api/current_step`: 현재 공정 단계 조회
      - `/api/process_range`: 특정 시간의 공정 시작/종료 시간 조회
    - **이상감지 로그**:
      - `/api/logs`: 실시간 이상감지 로그 (realtime_abnormal_log2)
      - `/api/anomaly_logs`: 동일 (별칭)
      - `/api/prediction_logs`: 예측 기반 이상감지 로그 (realtime_abnormal_log)
      - `/api/history/logs`: 기간별 로그 조회
      - `/api/alarm_history`: 장비 이력 기반 알람 조회
    - **차트 데이터**:
      - `/api/event_chart`: 특정 파라미터의 시간대별 실제값/예측값
      - `/api/trace_pred_chart`: 공정 구간의 실제값/예측값
    - **설정 관리**:
      - `/api/limits`: 한계값 설정 조회/저장
      - `/api/interlock_limits`: 인터락 한계값 설정
      - `/api/settings`: 시스템 설정
    - **장비 제어**:
      - `/api/equipment/start`: 장비 시작 (generate_data.py, insert_real_time.py 실행)
      - `/api/equipment/stop`: 장비 중지
      - `/api/equipment/status`: 장비 상태 조회
    - **텔레그램 연동**:
      - `/api/telegram/notify`: 텔레그램 알림 전송
      - `/api/telegram/webhook`: 텔레그램 봇 웹훅 (장비 제어 버튼 처리)
    - **장비 이력**:
      - `/api/equipment_history`: 공정 이력 조회
      - `/api/csv_files`: CSV 파일 목록 조회
      - `/api/csv_data`: CSV 파일 데이터 조회
    - **히트맵 생성**: 공정별 두께 분포 히트맵 이미지 생성 (IDW 보간)
  - **템플릿 페이지**:
    - `/`: 메인 대시보드 (index.html)
    - `/index2.html`: 실시간 이상감지 화면
    - `/index3.html`: 추가 모니터링 화면
    - `/index4.html`: 실시간 이상감지 화면 (개선 버전)
    - `/index5.html`: 어시스턴트 화면
    - `/index6.html`: 추가 화면
    - `/pvd`: PVD 공정 모니터링 화면
    - `/logview.html`: 로그 차트 뷰어

- **`fastapi/db.py`**
  - **목적**: 데이터베이스 쿼리 함수 모음
  - **주요 함수**:
    - `get_latest_data()`: 최근 N초간의 실제값/예측값 조회
    - `get_current_step()`: 현재 공정 단계 조회
    - `get_event_chart_data()`: 시간대별 차트 데이터
    - `get_trace_info()`: 공정 정보 및 두께 데이터
    - `get_trace_pred_chart_data()`: 공정 구간 예측 차트 데이터
    - `get_process_range()`: 공정 시작/종료 시간 조회
    - `get_latest_pvd_stream_data()`: PVD 실시간 스트림 데이터
    - `get_recent_pvd_violence_logs()`: PVD 이상 로그

#### 5.2 프론트엔드
- **`fastapi/static/realtime_anomaly.js`**: 실시간 이상감지 차트 및 로그 표시
- **`fastapi/static/process_compare.js`**: 공정 비교 기능
- **`fastapi/static/chart.js`**: 차트 라이브러리
- **`fastapi/static/trace.js`**: 공정 추적 기능
- **`fastapi/static/pvd4.js`**: PVD 모니터링 기능
- **`fastapi/templates/*.html`**: 각종 웹 페이지 템플릿

---

### 6. PVD 공정 모니터링 (PVD Process Monitoring)

#### 6.1 PVD 이상감지
- **`pvd/pvd_detect.py`**
  - **목적**: PVD 공정의 이상감지
  - **기능**:
    - Ion Gauge, Baratron Gauge, AR MFC 파라미터 모니터링
    - 통계적 이상감지 (평균값 기준 편차)
    - `pvd4_abnormals`, `pvd_violence` 테이블에 결과 저장

#### 6.2 PVD 데이터 생성/삽입
- **`pvd/generate_data_2.py`**: PVD용 데이터 생성
- **`pvd/insert_real_time_2.py`**: PVD용 실시간 데이터 삽입

#### 6.3 PVD Setpoint 이상감지
- **`pvd/baco/setpoint_anomaly_pipeline.py`**: Setpoint 기반 이상감지 파이프라인

---

### 7. 유틸리티 및 보조 스크립트

#### 7.1 데이터 변환/마이그레이션
- **`modify_csv_columns.py`**: CSV 컬럼 수정
- **`migrate_abnormal_log.py`**: 이상 로그 마이그레이션
- **`update_timestamp_abnormal.py`**: 타임스탬프 업데이트
- **`process_limit_touches.py`**: 한계값 터치 이벤트 처리

#### 7.2 데이터 생성/테스트
- **`create_abnormal_data.py`**: 이상 데이터 생성 (테스트용)
- **`sample_insert_result_data.py`**: 샘플 데이터 삽입
- **`insert_trace_info.py`**: 공정 정보 삽입 (이미 설명됨)

---

## 🔄 데이터 흐름 (Data Flow)

```
1. 데이터 생성
   generate_data.py → realtimedata/YYYY/MM/DD/HH00.csv

2. 실시간 수집
   insert_real_time.py → rawdataYYYYMMDD 테이블

3. 실시간 예측
   predict_real_time.py → pred_proc{idx} 테이블

4. 이상감지
   abnormal_monitor2.py → realtime_abnormal_log 테이블

5. 공정 정보 추출
   insert_trace_info.py → trace_info, trace_parameter_pred 테이블

6. 웹 시각화
   FastAPI → 사용자 대시보드
```

---

## 🗄️ 데이터베이스 스키마

### 주요 테이블
- **`rawdataYYYYMMDD`**: 일별 원시 데이터 (Timestamp가 Primary Key)
- **`pred_proc{0-4}`**: 파라미터 그룹별 예측값 (5개 테이블)
- **`realtime_abnormal_log`**: 예측 기반 이상감지 로그
- **`realtime_abnormal_log2`**: 한계값 기반 이상감지 로그
- **`realtime_violation_log`**: 한계값 위반 로그 (현재 비활성화)
- **`trace_info`**: 공정 정보 및 두께 데이터
- **`trace_parameter_pred`**: 공정 파라미터 예측값
- **`equipment_history`**: 장비 이력 정보
- **`pvd4_new_*`**: PVD 실시간 데이터 테이블
- **`pvd4_abnormals`**: PVD 이상 데이터
- **`pvd_violence`**: PVD 이상 로그

---

## 🧠 모델 정보

### 예측 모델
- **아키텍처**: PatchTST (Patch-based Time Series Transformer)
- **윈도우 크기**: 192초 (약 3분)
- **예측 스텝**: 10초, 20초, 30초 후
- **모델 위치**: `model/` 디렉토리
  - `192_patchtst_{parameter}.keras`: 개별 파라미터 모델
  - `192_patchtst_{parameter}_main.keras`: 특정 공정 단계용 보조 모델
  - `192_patchtst_Temp.keras`: 온도 파라미터 통합 모델
- **스케일러**: `model/scaler/` 디렉토리
  - `scaler_X.pkl`: 입력 데이터 스케일러
  - `scaler_X_Temp.pkl`: 온도 입력 데이터 스케일러
  - `scaler_X_main.pkl`: 보조 모델용 입력 스케일러
  - `scaler_y_{parameter}.pkl`: 각 파라미터별 출력 스케일러

### 두께 예측 모델
- **모델**: XGBoost
- **위치**: `xgb_model/` 디렉토리
- **출력**: 45개 포인트 (9개 포인터 × 5개 웨이퍼 위치)

---

## ⚙️ 설정 파일

- **`limits.yaml`**: 파라미터별 한계값 설정 (공정 단계별 min/max)
- **`limits2.yaml`**: 인터락 한계값 설정
- **`settings.yaml`**: 시스템 설정
- **`generator_health.json`**: 데이터 생성기 헬스체크 정보
- **`requirements.txt`**: Python 패키지 의존성

---

## 🚀 실행 순서

### 실시간 운영 환경
1. `python3 generate_data.py` - 실시간 CSV 데이터 생성 (테스트용)
2. `python3 insert_real_time.py` - 실시간 데이터 DB 삽입
3. `python3 insert_trace_info.py` - 공정 정보 추출 및 두께 예측
4. `python3 predict_real_time.py` - 실시간 예측
5. `python3 abnormal_monitor2.py` - 실시간 이상감지
6. `cd fastapi && uvicorn main:app --host 0.0.0.0 --port 9300 --reload` - 웹 서버

### 배치 처리
- `python3 batch_process_abnormal_temp.py` - 과거 데이터 예측
- `python3 batch_process_abnormal2.py` - 과거 데이터 예측 + 이상감지

---

## 📝 주요 특징

1. **실시간 처리**: 데이터 수집부터 예측, 이상감지까지 실시간 파이프라인
2. **병렬 처리**: Ray를 활용한 파라미터 그룹별 병렬 예측
3. **다중 모델**: 일반 모델 + 특정 공정 단계용 보조 모델
4. **데이터 보간**: 시계열 데이터의 빈 구간 자동 보간
5. **그레이스 기간**: 장비 시작 후 일정 시간 이상감지 비활성화
6. **웹 기반 모니터링**: FastAPI + JavaScript를 통한 실시간 대시보드
7. **텔레그램 연동**: 이상 상황 알림 및 원격 제어
8. **히트맵 시각화**: 공정별 두께 분포 히트맵 생성

---

## 🔍 코드 버전 관리

- **`.back` 파일**: 백업 파일 (예: `batch_process_abnormal.py.back`)
- **`_temp.py` 파일**: 임시/테스트 버전 (예: `batch_process_abnormal_temp.py`)
- **숫자 접미사**: 버전별 개선 파일 (예: `batch_process_abnormal2.py`, `batch_process_abnormal3.py`)

---

## 📌 참고사항

- 모든 스크립트는 PostgreSQL 데이터베이스 사용 (keti/keti1234!)
- 타임존: Asia/Seoul (KST)
- 로그 파일: `log/` 디렉토리에 저장
- 모델 파일: `model/`, `xgb_model/` 디렉토리
- 정적 파일: `fastapi/static/` 디렉토리
- 템플릿: `fastapi/templates/` 디렉토리

---

*작성일: 2025년*
*프로젝트: Semi On-Device Platform*

