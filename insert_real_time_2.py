# -*- coding: utf-8 -*-
from datetime import datetime, timedelta, timezone
from dateutil import parser
import numpy as np
import pandas as pd
import psycopg2

# =========================
# 네가 준 코드 (원형 유지)
# =========================
def _get_latest_pvd_table(cur):
    cur.execute(
        """
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public' AND tablename LIKE 'pvd4_new_%'
        ORDER BY tablename DESC
        LIMIT 1
        """
    )
    row = cur.fetchone()
    return row[0] if row else None

def get_latest_pvd_stream_data(last_table=None, since=None):
    conn = psycopg2.connect(
        dbname="postgres",
        user="keti",
        password="keti1234!",
        host="localhost",
        port=5432,
    )
    cur = conn.cursor()

    latest_table = _get_latest_pvd_table(cur)
    if not latest_table:
        cur.close(); conn.close()
        return {"table": None, "is_new_table": False, "rows": []}

    is_new_table = last_table != latest_table

    query = (
        f'SELECT "timer", "ion_gauge_i", "baratron_gauge_i", "ar_mfc_i" '
        f'FROM "{latest_table}"'
    )

    params = []
    if not is_new_table and since:
        try:
            since_dt = parser.parse(since)
        except (ValueError, TypeError):
            since_dt = None
        if since_dt is not None:
            query += ' WHERE "timer" > %s'
            params.append(since_dt)

    query += ' ORDER BY "timer" ASC'

    cur.execute(query, tuple(params) if params else None)

    rows = []
    for timer, ion, baratron, ar_mfc in cur.fetchall():
        rows.append(
            {
                "timer": timer.isoformat() if timer else None,
                "ion_gauge_i": ion,
                "baratron_gauge_i": baratron,
                "ar_mfc_i": ar_mfc,
            }
        )

    cur.close(); conn.close()
    return {"table": latest_table, "is_new_table": is_new_table, "rows": rows}

# =========================
# 간단 ON/OFF 판정 유틸
# =========================
def _median_smooth(x: np.ndarray, w: int = 3) -> np.ndarray:
    if w <= 1: return x
    return pd.Series(x, dtype="float64").rolling(w, center=True, min_periods=1).median().to_numpy()

def _robust_01(x: np.ndarray) -> np.ndarray:
    """[5,95]% 분위수로 0..1 스케일, 엣지케이스 안전 처리"""
    x = np.asarray(x, dtype=float).ravel()
    if x.size == 0: return np.zeros_like(x)
    finite = np.isfinite(x)
    if finite.sum() == 0: return np.zeros_like(x)
    xx = x[finite]
    q = np.nanpercentile(xx, [5, 95])
    if np.ndim(q) == 0 or (q[1] - q[0]) < 1e-12:
        return np.zeros_like(x)
    y = (x - q[0]) / (q[1] - q[0])
    y = np.clip(y, 0, 1)
    y[~finite] = 0.0
    return y

def _auto_hysteresis(ref_n: np.ndarray, margin: float = 0.08):
    """
    상·하위 20% 값의 중앙값을 잡아 중간점을 임계로 사용 → 히스테리시스(+/- margin)
    (sklearn 없이 간단하게)
    """
    v = np.sort(ref_n[~np.isnan(ref_n)])
    if v.size < 10:
        return 0.6, 0.4
    k = max(1, int(v.size * 0.2))
    base = float(np.median(v[:k]))
    high = float(np.median(v[-k:]))
    thr = (base + high) / 2.0
    up = min(1.0, thr + margin)
    down = max(0.0, thr - margin)
    if up <= down:
        up, down = min(1.0, thr + 0.05), max(0.0, thr - 0.05)
    return up, down

def _hysteresis_mask(x_n: np.ndarray, up: float, down: float) -> np.ndarray:
    on = False
    out = np.zeros_like(x_n, dtype=int)
    for i, v in enumerate(x_n):
        if not on and v >= up:
            on = True
        elif on and v <= down:
            on = False
        out[i] = 1 if on else 0
    return out

# =========================
# 최신 샘플 ON 판정 (핵심 API)
# =========================
def get_latest_on_state(window_sec: int = 60,
                        ref_field: str = "ar_mfc_i",
                        smooth_win: int = 3,
                        margin: float = 0.08):
    """
    최근 window_sec초 데이터만 가져와 자동 임계(히스테리시스)로 ON/OFF 판정.
    반환 dict에 최신샘플 state/임계/마지막값 포함.
    """
    since = (datetime.now(timezone.utc) - timedelta(seconds=window_sec)).isoformat()
    res = get_latest_pvd_stream_data(since=since)

    rows = res["rows"]
    if not rows:
        return {"table": res["table"], "state": "OFF", "is_on": False, "reason": "no_rows"}

    # 1Hz 가정: 최근 window_sec개의 값만 사용(혹시 더 많이 왔으면 꼬리만)
    vals = [r.get(ref_field) for r in rows if r.get(ref_field) is not None]
    if len(vals) < 3:
        return {"table": res["table"], "state": "OFF", "is_on": False, "reason": "too_few_samples"}

    ref = np.array(vals[-window_sec:], dtype=float)
    ref_s = _median_smooth(ref, smooth_win)
    ref_n = _robust_01(ref_s)
    up, down = _auto_hysteresis(ref_n, margin=margin)
    mask = _hysteresis_mask(ref_n, up, down)
    is_on = bool(mask[-1] == 1)

    # 아주 간단한 램프 감지(최근 1초 변동량으로)
    state = "ON" if is_on else "OFF"
    if len(ref_n) >= 2:
        dy = ref_n[-1] - ref_n[-2]
        if dy >= 0.1: state = "RAMP_UP"
        elif dy <= -0.1: state = "RAMP_DOWN"

    latest = rows[-1]
    return {
        "table": res["table"],
        "is_new_table": res["is_new_table"],
        "is_on": is_on,
        "state": state,
        "up": float(up), "down": float(down),
        "ref_field": ref_field,
        "ref_last": float(latest.get(ref_field)) if latest.get(ref_field) is not None else None,
        "timer": latest.get("timer"),
    }

# =========================
# 예시 실행
# =========================
if __name__ == "__main__":
    out = get_latest_on_state(window_sec=60, ref_field="ar_mfc_i")
    print(out)
