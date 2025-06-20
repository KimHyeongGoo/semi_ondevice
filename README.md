# Semi On-device Project

이 저장소는 공정 장비 데이터를 실시간으로 수집하여 예측 모델에 입력하고, 결과를 시각화하기 위한 스크립트와 웹 서버를 포함합니다.

## 주요 스크립트

- **generate_data.py**: `data/` 디렉터리의 CSV 파일을 읽어 `realtimedata/` 폴더에 실시간 형식으로 변환해 저장합니다. 각 파일은 100줄씩, 폴더는 시간대별로 생성됩니다.
- **insert_real_time.py**: `realtimedata/` 경로를 감시하여 새로 생성되거나 수정된 CSV 행을 PostgreSQL 테이블에 삽입합니다. 하루 단위의 `rawdataYYYYMMDD` 테이블을 자동으로 생성합니다.
- **insert_old_data.py**: 과거 CSV 데이터를 일괄적으로 데이터베이스에 적재할 때 사용합니다. 파일 구조를 탐색하여 누락된 데이터만 선별하여 삽입합니다.
- **predict_real_time.py**: 데이터베이스의 최신 원시 데이터를 주기적으로 읽어 딥러닝 모델을 이용한 실시간 예측을 수행합니다. 예측값은 `pred_*` 테이블에 저장되며 한계값을 벗어나면 `realtime_violation_log` 테이블에 기록합니다.
- **insert_trace_info.py**: 저장된 원시 데이터에서 공정 구간을 추출하고, XGBoost 모델로 두께 예측을 수행한 뒤 `trace_info` 테이블에 저장합니다.

## 디렉터리 설명

- **fastapi/**: 예측 결과와 로그를 조회하기 위한 웹 인터페이스를 제공하는 FastAPI 애플리케이션입니다. 템플릿과 정적 파일이 포함되어 있습니다.
- **model/**: 실시간 예측에 사용되는 TensorFlow 모델과 스케일러 파일이 저장되어 있습니다.
- **xgb_model/**: 두께 예측을 위한 XGBoost 모델과 관련 메타 파일이 들어 있습니다.
- **data/**: `generate_data.py`에서 사용되는 원본 CSV 샘플 데이터입니다.

## 환경 구성

1. `(sudo) pip3 install -r requirements.txt`
2. PostgreSQL 설치 후 사용자 `keti`/`keti1234!` 생성

## 실행 방법
테스트 구동 시
1. `python3 generate_data.py` – 실시간 CSV 데이터 생성 (테스트시에만 실행)

실행법 (계속 실행되고 있을 실시간 관련 코드)
2. `python3 insert_real_time.py` – 실시간 CSV 생성 디렉토리를 감시해 DB에 삽입 + 누락데이터 저장
3. `python3 predict_real_time.py` – 실시간 예측 프로세스 실행
4. `cd ./fastapi`
5. `uvicorn main:app --reload` – FastAPI 서버 구동 (로컬용)
6. `uvicorn main:app --host 0.0.0.0 --port 9300 --reload` – 외부 접속 허용