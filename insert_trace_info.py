import psycopg2
import re
import time
from datetime import datetime, timedelta


def print_existing_trace_info():
    """실행 전에 지금까지 저장된 모든 공정 구간 출력 (오래된 순)"""
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432
    )
    cur = conn.cursor()

    # trace_info 테이블이 없을 수도 있으므로 CREATE 먼저
    cur.execute("""
        CREATE TABLE IF NOT EXISTS trace_info (
            start_time TIMESTAMP PRIMARY KEY,
            end_time TIMESTAMP,
            start_table TEXT,
            end_table TEXT
        );
    """)

    # 저장된 모든 공정 구간 출력
    cur.execute("""
        SELECT start_time, end_time, start_table, end_table
        FROM trace_info
        ORDER BY start_time ASC;
    """)
    rows = cur.fetchall()

    if not rows:
        print("📂 저장된 공정 이력이 없습니다.")
    else:
        print(f"\n📄 지금까지 저장된 공정 정보 ({len(rows)}건):")
        for idx, (start, end, s_tbl, e_tbl) in enumerate(rows, 1):
            print(f"  {idx:03d}. {start} ~ {end} ({s_tbl} → {e_tbl})")

    cur.close()
    conn.close()
    
    
def extract_process_ranges_incrementally():
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432
    )
    cur = conn.cursor()

    # 1. trace_info 테이블 생성 (최초 1회)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS trace_info (
            start_time TIMESTAMP PRIMARY KEY,
            end_time TIMESTAMP,
            start_table TEXT,
            end_table TEXT
        );
    """)

    # 2. 마지막 저장된 공정 end_time 조회
    cur.execute("SELECT MAX(end_time) FROM trace_info;")
    result = cur.fetchone()
    last_end_time = result[0] if result and result[0] else None

    if last_end_time:
        print(f"📌 마지막 공정 종료시각: {last_end_time}")
        last_date = int(last_end_time.strftime("%Y%m%d"))
    else:
        print("📌 이전 공정 기록 없음. 전체 테이블 탐색 시작")
        last_date = 0

    # 3. 테이블 목록 중 이후 날짜만 처리
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name ~ '^rawdata\\d{8}$';
    """)
    tables = [t[0] for t in cur.fetchall()]
    tables_filtered = sorted([
        t for t in tables if int(t.replace("rawdata", "")) >= last_date
    ], key=lambda x: int(x.replace("rawdata", "")))

    # 상태 변수
    current_proc = None
    last_ts = None
    last_table = None

    for table in tables_filtered:
        print(f"📘 처리 중: {table}")
        query = f"""
            SELECT "Timestamp", "ProcessRecipeStepName"
            FROM "{table}"
            WHERE "ProcessRecipeStepName" IS NOT NULL
            ORDER BY "Timestamp" ASC;
        """
        cur.execute(query)
        rows = cur.fetchall()

        for ts, step in rows:
            # 마지막 처리된 이후부터만
            if last_end_time and ts <= last_end_time:
                continue

            step = step.strip().upper() if step else ""

            if current_proc is None:
                if step in ("STANDBY", "START"):
                    current_proc = {
                        "start_time": ts,
                        "start_table": table
                    }
            else:
                if step == "END":
                    duration = ts - current_proc["start_time"]
                    if duration >= timedelta(hours=1):
                        cur.execute("""
                            INSERT INTO trace_info (start_time, end_time, start_table, end_table)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (start_time) DO NOTHING;
                        """, (current_proc["start_time"], ts, current_proc["start_table"], table))
                        print(current_proc["start_time"], ts)
                    current_proc = None
                elif step in ("IDLE", "", "NAN", "NULL"):
                    if last_ts:
                        duration = last_ts - current_proc["start_time"]
                        if duration >= timedelta(hours=1):
                            cur.execute("""
                                INSERT INTO trace_info (start_time, end_time, start_table, end_table)
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT (start_time) DO NOTHING;
                            """, (current_proc["start_time"], last_ts, current_proc["start_table"], last_table))
                            print(current_proc["start_time"], last_ts)
                        current_proc = None
                elif last_ts:
                    gap = ts - last_ts
                    if gap >= timedelta(hours=1):
                        duration = last_ts - current_proc["start_time"]
                        if duration >= timedelta(hours=1):
                            cur.execute("""
                                INSERT INTO trace_info (start_time, end_time, start_table, end_table)
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT (start_time) DO NOTHING;
                            """, (current_proc["start_time"], last_ts, current_proc["start_table"], last_table))
                            print(f"⚠️ 중단 감지 → 저장됨: {current_proc['start_time']} ~ {last_ts}")
                        else:
                            print(f"⚠️ 중단 감지 → 무시됨(1시간 미만): {current_proc['start_time']} ~ {last_ts}")
                        current_proc = None  # 현재 공정 종료 처리
            last_ts = ts
            last_table = table

    conn.commit()
    cur.close()
    conn.close()
    print("✅ 신규 공정 구간 추출 완료")

def drop_trace_and_proc_tables():
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432
    )
    cur = conn.cursor()

    # 테이블 삭제
    for table in ["proc_info", "trace_info"]:
        try:
            cur.execute(f'DROP TABLE IF EXISTS {table} CASCADE;')
            print(f"✅ 테이블 삭제됨: {table}")
        except Exception as e:
            print(f"❌ 삭제 실패: {table} → {e}")

    conn.commit()
    cur.close()
    conn.close()
    
# 🕒 30분 간격 루프
if __name__ == '__main__':
    drop_trace_and_proc_tables()
    print_existing_trace_info()  
    try:
        while True:
            extract_process_ranges_incrementally()
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]  30분 후 재실행 대기 중...\n")
            time.sleep(1800)
    except KeyboardInterrupt:
        print("\n🛑 수동 종료됨.")
