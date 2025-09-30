#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import csv
import time
import signal
import threading
from datetime import datetime
from typing import List, Dict

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileModifiedEvent

import psycopg2
from psycopg2 import sql

# ===== 설정 =====
WATCH_DIR = "../realtimedata_2"
DBCFG = dict(
    dbname="postgres",
    user="keti",
    password="keti1234!",
    host="localhost",
    port=5432,
)

# ===== 유틸: 식별자/타입 =====
def sanitize_identifier(name: str) -> str:
    s = "".join(ch.lower() if ch.isalnum() else "_" for ch in os.path.splitext(name)[0]).strip("_")
    if not s or s[0].isdigit():
        s = "t_" + (s or "table")
    return s

def sanitize_column(name: str) -> str:
    if name == "Timer":
        return "timer"
    s = "".join(ch.lower() if ch.isalnum() else "_" for ch in name).strip("_")
    if not s or s[0].isdigit():
        s = "c_" + (s or "col")
    return s

def parse_timer(s: str):
    try:
        return datetime.strptime(s.strip().strip("[]"), "%Y.%m.%d %H:%M:%S")
    except Exception:
        return None

# ===== DB 헬퍼 =====
class PG:
    def __init__(self, cfg):
        self.conn = psycopg2.connect(**cfg)
        self.conn.autocommit = True
        self.schema_cache: Dict[str, Dict] = {}  # table -> {"cols": [...], "map": raw->san}

    def close(self):
        try: self.conn.close()
        except: pass

    def ensure_table(self, table_raw: str, headers: List[str]) -> str:
        table = sanitize_identifier(table_raw)

        raw_to_san = {}
        cols_order = []
        for h in headers:
            if not h: continue
            c = sanitize_column(h)
            raw_to_san[h] = c
            if c not in cols_order:
                cols_order.append(c)

        with self.conn.cursor() as cur:
            cur.execute(
                """SELECT EXISTS(
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema='public' AND table_name=%s)""",
                (table,)
            )
            exists = cur.fetchone()[0]

            if not exists:
                defs = []
                for h in headers:
                    if not h: continue
                    c = raw_to_san[h]
                    if c == "timer":
                        defs.append(sql.SQL("{} TIMESTAMP PRIMARY KEY").format(sql.Identifier(c)))
                    else:
                        defs.append(sql.SQL("{} DOUBLE PRECISION").format(sql.Identifier(c)))
                cur.execute(
                    sql.SQL("CREATE TABLE {} ({});").format(
                        sql.Identifier(table), sql.SQL(", ").join(defs)
                    )
                )
            else:
                # 누락된 컬럼만 추가
                cur.execute(
                    """SELECT column_name FROM information_schema.columns
                    WHERE table_schema='public' AND table_name=%s""",
                    (table,)
                )
                existing = {r[0] for r in cur.fetchall()}
                for h in headers:
                    if not h: continue
                    c = raw_to_san[h]
                    if c not in existing:
                        if c == "timer":
                            cur.execute(
                                sql.SQL("ALTER TABLE {} ADD COLUMN {} TIMESTAMP;")
                                .format(sql.Identifier(table), sql.Identifier(c))
                            )
                            # PK로 지정
                            cur.execute(
                                sql.SQL("ALTER TABLE {} ADD PRIMARY KEY ({});")
                                .format(sql.Identifier(table), sql.Identifier(c))
                            )
                        else:
                            cur.execute(
                                sql.SQL("ALTER TABLE {} ADD COLUMN {} DOUBLE PRECISION;")
                                .format(sql.Identifier(table), sql.Identifier(c))
                            )

        self.schema_cache[table] = {"cols": cols_order, "map": raw_to_san}
        return table


    def max_timer(self, table: str):
        with self.conn.cursor() as cur:
            cur.execute(sql.SQL("SELECT MAX(timer) FROM {}").format(sql.Identifier(table)))
            return cur.fetchone()[0]  # datetime | None

    def insert_rows(self, table: str, raw_to_san: Dict[str, str], rows: List[dict]):
        if not rows: return
        # 컬럼 순서 고정: Timer 먼저(있다면), 나머지
        cols = []
        if "Timer" in raw_to_san: cols.append(raw_to_san["Timer"])
        for k, v in raw_to_san.items():
            if k == "Timer": continue
            cols.append(v)

        placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in cols)
        q = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT DO NOTHING").format(
            sql.Identifier(table),
            sql.SQL(", ").join(sql.Identifier(c) for c in cols),
            placeholders,
        )

        with self.conn.cursor() as cur:
            for r in rows:
                vals = []
                # Timer
                if "Timer" in raw_to_san:
                    ts = parse_timer(r.get("Timer", ""))
                    vals.append(ts)
                # floats
                for k, v in raw_to_san.items():
                    if k == "Timer": continue
                    x = r.get(k, None)
                    if x is None or str(x).strip() == "" or str(x).lower() == "nan":
                        vals.append(None)
                    else:
                        try: vals.append(float(x))
                        except: vals.append(None)
                cur.execute(q, vals)

# ===== 파일 감시 핸들러 =====
class CsvHandler(FileSystemEventHandler):
    def __init__(self, db: PG):
        super().__init__()
        self.db = db
        self.lock = threading.Lock()
        self.headers: Dict[str, List[str]] = {}  # path -> header list

    def _is_csv(self, path: str) -> bool:
        return path.lower().endswith(".csv")

    def _load_header(self, path: str) -> List[str]:
        with open(path, "r", encoding="utf-8") as f:
            r = csv.reader(f)
            return next(r)

    def _read_new_rows_after(self, path: str, last_dt) -> List[dict]:
        # 모든 행을 읽되 last_dt 이후만 반환 (안전)
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            buf = []
            for row in reader:
                ts = parse_timer(row.get("Timer", ""))
                # Timer가 없거나 파싱 실패 → 건너뜀(원한다면 포함도 가능)
                if ts is None:
                    continue
                if (last_dt is None) or (ts > last_dt):
                    buf.append(row)
        return buf

    def _process(self, path: str):
        if not os.path.exists(path) or not self._is_csv(path):
            return

        with self.lock:
            # 헤더 확보
            if path not in self.headers:
                try:
                    self.headers[path] = self._load_header(path)
                except Exception:
                    return

            headers = self.headers[path]
            table = self.db.ensure_table(os.path.basename(path), headers)
            schema = self.db.schema_cache[table]
            raw_to_san = schema["map"]

            # DB 최신 타임스탬프 확인
            last_dt = self.db.max_timer(table)

            # 파일에서 last_dt 이후 행만 읽기
            new_rows = self._read_new_rows_after(path, last_dt)

            if not new_rows:
                return

            # INSERT (중복은 UNIQUE(timer) + ON CONFLICT로 방어)
            self.db.insert_rows(table, raw_to_san, new_rows)
            print(f"[INFO] {os.path.basename(path)}: +{len(new_rows)} rows inserted (after {last_dt})")

    # watchdog 콜백
    def on_created(self, event):
        if isinstance(event, FileCreatedEvent) and not event.is_directory and self._is_csv(event.src_path):
            # 약간의 지연 후 처리(파일 쓰기 중인 경우 대비)
            time.sleep(0.1)
            self._process(event.src_path)

    def on_modified(self, event):
        if isinstance(event, FileModifiedEvent) and not event.is_directory and self._is_csv(event.src_path):
            time.sleep(0.05)
            self._process(event.src_path)

# ===== 메인 =====
def main():
    if not os.path.isdir(WATCH_DIR):
        raise RuntimeError(f"WATCH_DIR not found: {WATCH_DIR}")

    db = PG(DBCFG)
    observer = Observer()
    handler = CsvHandler(db)
    observer.schedule(handler, WATCH_DIR, recursive=True)
    observer.start()
    print(f"[INFO] Watching {os.path.abspath(WATCH_DIR)}")

    def shutdown(*_):
        print("\n[INFO] Shutting down...")
        observer.stop()
        observer.join(timeout=2)
        db.close()
        print("[INFO] Bye.")
        os._exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        shutdown()

if __name__ == "__main__":
    main()
