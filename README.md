링크 댓글확인
https://keti.dooray.com/task/to/3987103909787059817?workflowClass=all

- 테스트전 환경구성
1. (sudo) pip3 install -r requirements.txt
2. postgres 설치 후 (keti/keti1234! 유저생성)

- 테스트 실행법
1. python3 generate_data.py
2. python3 insert_real_time.py # main() 부분의 realtime_path = './real_time_data'로 수정
3. python3 predict_real_time.py
4. cd ./fastapi
5. uvicorn main:app --reload # fast api 구동
6. uvicorn main:app --host 0.0.0.0 --port 9300 --reload # 외부접속 허용 구동
