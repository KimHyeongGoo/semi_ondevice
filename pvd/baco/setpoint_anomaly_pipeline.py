# setpoint_anomaly_pipeline.py
# -*- coding: utf-8 -*-
"""
단계 1) 정상 ON값 CSV로 '셋팅값 라이브러리' 생성(클러스터링 → 각 세트의 중심 μ 저장)
단계 2) 라이브러리 로드
단계 3) 실시간 1초 샘플(Ion/Bar/Ar) 판정: Ar±1%, Ion/Bar±10% 규칙 + K-of-M 안정화

CLI 예시
1) 라이브러리 생성:
   python setpoint_anomaly_pipeline.py build --csv /path/normal_set.csv --out setpoints.yaml --kmax 6

2) 실시간 데모(스트림 CSV를 1줄씩 읽어 판정):
   python setpoint_anomaly_pipeline.py demo --model setpoints.yaml --stream /path/stream.csv --k 2 --m 3
"""
from __future__ import annotations
import argparse, json, math, yaml
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

# --- (옵션) sklearn 사용: 없으면 자동 폴백 ---
try:
    from sklearn.cluster import KMeans
    from sklearn.metrics import silhouette_score
    HAVE_SK = True
except Exception:
    HAVE_SK = False

# ---------- 공통 설정 ----------
COLS = ["Ion.Gauge.i", "Baratron.Gauge.i", "Ar.MFC.i"]  # CSV 정상값 컬럼명

@dataclass
class Thresholds:
    ar_ratio: float = 0.01   # Ar ±1%
    ion_ratio: float = 0.10  # Ion ±10%
    bar_ratio: float = 0.10  # Bar ±10%
    ion_min_abs: float = 0.0 # (옵션) Ion 절대 tol (작은 값 과민 방지)
    bar_min_abs: float = 0.0 # (옵션) Bar 절대 tol

@dataclass
class KMConfig:
    kmax: int = 6           # 1D(Ar) 군집 최대 시도 수 (>=2 권장)
    method: str = "auto"    # 'auto'|'fixed'
    kfixed: Optional[int] = None

# ==========================================================
# 단계 1) 정상 ON값 CSV → 셋팅값 라이브러리(클러스터링)
# ==========================================================
def _auto_kmeans_1d_ar(ar_vals: np.ndarray, cfg: KMConfig) -> np.ndarray:
    """
    Ar 1D 값으로 군집 라벨 반환. sklearn 없으면 사분위 기반 대체.
    """
    x = ar_vals.reshape(-1,1)
    n = len(ar_vals)

    if cfg.method == "fixed" and cfg.kfixed and cfg.kfixed >= 1:
        if HAVE_SK:
            km = KMeans(n_clusters=cfg.kfixed, n_init=10, random_state=42).fit(x)
            return km.labels_
        else:
            # 폴백: 균등분위 binning
            bins = np.quantile(ar_vals, np.linspace(0,1,cfg.kfixed+1))
            labels = np.digitize(ar_vals, bins[1:-1], right=True)
            return labels

    # auto 선택
    if HAVE_SK and n >= 10:
        best_k, best_score, best_labels = 1, -np.inf, np.zeros(n, dtype=int)
        for k in range(2, min(cfg.kmax, n) + 1):
            km = KMeans(n_clusters=k, n_init=10, random_state=42).fit(x)
            labels = km.labels_
            try:
                score = silhouette_score(x, labels)
            except Exception:
                score = -np.inf
            if score > best_score:
                best_k, best_score, best_labels = k, score, labels
        return best_labels
    else:
        # 폴백: 사분위 4-bin 정도 (데이터 분포따라 3~5 가변)
        q = np.unique(np.quantile(ar_vals, [0.2, 0.4, 0.6, 0.8]))
        if len(q) == 0:
            return np.zeros(n, dtype=int)
        labels = np.digitize(ar_vals, q, right=True)
        return labels

def build_setpoint_library(csv_path: str, kcfg: KMConfig) -> Dict:
    """
    정상값 CSV(각 행 = ON 구간의 정상 샘플)를 레시피 세트로 분할하고
    각 세트의 중심 μ를 계산해 라이브러리(dict)로 반환.
    """
    df = pd.read_csv(csv_path)
    for c in COLS:
        if c not in df.columns:
            raise ValueError(f"CSV에 '{c}' 컬럼이 없습니다.")
    # Ar로 1차 분할
    ar = df["Ar.MFC.i"].to_numpy(dtype=float)
    labels = _auto_kmeans_1d_ar(ar, kcfg)

    lib = {"columns": COLS, "sets": []}
    for gid, g in df.groupby(labels):
        mu = g[COLS].median().to_dict()  # 중앙값 중심(이상치 견고)
        # 보조 정보
        cnt = int(len(g))
        spread = (g[COLS] - g[COLS].median()).abs().median().to_dict()
        lib["sets"].append({
            "id": int(gid),
            "count": cnt,
            "mu": {k: float(v) for k, v in mu.items()},
            "spread_absdev": {k: float(spread[k]) for k in COLS}
        })

    # Ar 중심 순으로 정렬(해석 편의)
    lib["sets"].sort(key=lambda s: s["mu"]["Ar.MFC.i"])
    # id 재부여(0..N-1)
    for i, s in enumerate(lib["sets"]):
        s["id"] = i
    return lib

# ==========================================================
# 단계 2) 라이브러리 저장/로드
# ==========================================================
def save_library(lib: Dict, path: str):
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(lib, f, allow_unicode=True, sort_keys=False)

def load_library(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

# ==========================================================
# 단계 3) 실시간 1초 샘플 판정
# ==========================================================
def _pick_set_by_ar(lib: Dict, ar_value: float) -> Dict:
    """
    실시간 샘플의 Ar 값이 가장 가까운 세트를 선택.
    """
    sets = lib["sets"]
    best, best_d = sets[0], float("inf")
    for s in sets:
        d = abs(ar_value - s["mu"]["Ar.MFC.i"])
        if d < best_d:
            best, best_d = s, d
    return best  # dict with id, mu,...

def _percent_deviation(val: float, ref: float) -> float:
    if ref == 0:
        return float("inf") if val != 0 else 0.0
    return abs(val - ref) / abs(ref)

def evaluate_sample(sample: Dict[str, float],
                    lib: Dict,
                    thr: Thresholds = Thresholds(),
                    assign_mode: str = "by_ar",
                    unknown_ar_gate: float = 0.05) -> Dict:
    """
    입력: sample = {Ion.Gauge.i, Baratron.Gauge.i, Ar.MFC.i}
    출력: 판정 결과(dict)
    - assign_mode: 'by_ar'만 제공(단순·견고). 필요시 Mahalanobis로 확장 가능.
    - unknown_ar_gate: 어떤 세트의 Ar μ와도 5% 이상 차이 나면 'unknown' 처리(선택).
    """
    # 컬럼 매핑(스트림에서 소문자/언더스코어가 올 수도 있어서 보정)
    alias = {
        "Ion.Gauge.i": ["Ion.Gauge.i", "ion_gauge_i", "ion", "Ion"],
        "Baratron.Gauge.i": ["Baratron.Gauge.i", "baratron_gauge_i", "baratron", "Baratron"],
        "Ar.MFC.i": ["Ar.MFC.i", "ar_mfc_i", "ar", "Ar"]
    }
    x = {}
    for k, keys in alias.items():
        for kk in keys:
            if kk in sample and sample[kk] is not None:
                x[k] = float(sample[kk]); break
        if k not in x:
            raise ValueError(f"샘플에 '{k}' 값이 없습니다. 받은 키: {list(sample.keys())}")

    # 세트 선택
    s = _pick_set_by_ar(lib, x["Ar.MFC.i"]) if assign_mode == "by_ar" else lib["sets"][0]
    mu = s["mu"]

    # unknown 레시피 감지(Ar 기준)
    ar_dev = _percent_deviation(x["Ar.MFC.i"], mu["Ar.MFC.i"])
    is_unknown = (ar_dev > unknown_ar_gate) if unknown_ar_gate is not None else False

    # 규칙 판정: Ar ±1%, Ion/Bar ±10% (+ 절대 tol)
    ion_dev = _percent_deviation(x["Ion.Gauge.i"], mu["Ion.Gauge.i"])
    bar_dev = _percent_deviation(x["Baratron.Gauge.i"], mu["Baratron.Gauge.i"])

    ion_violate = (abs(x["Ion.Gauge.i"] - mu["Ion.Gauge.i"]) > max(thr.ion_ratio*abs(mu["Ion.Gauge.i"]), thr.ion_min_abs))
    bar_violate = (abs(x["Baratron.Gauge.i"] - mu["Baratron.Gauge.i"]) > max(thr.bar_ratio*abs(mu["Baratron.Gauge.i"]), thr.bar_min_abs))
    ar_violate  = (abs(x["Ar.MFC.i"] - mu["Ar.MFC.i"]) > thr.ar_ratio*abs(mu["Ar.MFC.i"]))

    violated = bool(ion_violate or bar_violate or ar_violate)

    # 심각도(직관용): 기준 대비 최대 배수(>=1이면 위반)
    severity = max(
        ion_dev / thr.ion_ratio if thr.ion_ratio>0 else 0.0,
        bar_dev / thr.bar_ratio if thr.bar_ratio>0 else 0.0,
        ar_dev  / thr.ar_ratio  if thr.ar_ratio>0  else 0.0
    )

    return {
        "set_id": s["id"],
        "mu": mu,
        "sample": x,
        "deviation": {"Ion": ion_dev, "Bar": bar_dev, "Ar": ar_dev},
        "violated": violated,
        "violated_items": {
            "Ion": bool(ion_violate),
            "Bar": bool(bar_violate),
            "Ar":  bool(ar_violate)
        },
        "severity": float(severity),
        "unknown_recipe": bool(is_unknown)
    }

# --------- K-of-M 안정화 래퍼 ----------
@dataclass
class KofM:
    k: int = 2
    m: int = 3
    buf: List[int] = None

    def __post_init__(self):
        self.buf = []

    def update(self, violated: bool) -> Tuple[bool, str]:
        self.buf.append(1 if violated else 0)
        if len(self.buf) > self.m:
            self.buf.pop(0)
        s = sum(self.buf)
        return (s >= self.k), f"{s}/{len(self.buf)}"

# ==========================================================
# CLI
# ==========================================================
def cli_build(args):
    kcfg = KMConfig(kmax=args.kmax, method=("fixed" if args.kfixed else "auto"), kfixed=args.kfixed)
    lib = build_setpoint_library(args.csv, kcfg)
    save_library(lib, args.out)
    print(f"[OK] saved library -> {args.out}")
    for s in lib["sets"]:
        mu = s["mu"]
        print(f"  - set {s['id']:>2} (n={s['count']:>4})  Ar≈{mu['Ar.MFC.i']:.6g}  "
              f"Ion≈{mu['Ion.Gauge.i']:.6g}  Bar≈{mu['Baratron.Gauge.i']:.6g}")

def cli_demo(args):
    lib = load_library(args.model)
    thr = Thresholds(ar_ratio=0.01, ion_ratio=0.10, bar_ratio=0.10)
    kofm = KofM(k=args.k, m=args.m)

    df = pd.read_csv(args.stream)
    need = set(COLS)
    if not need.issubset(df.columns):
        # 컬럼명이 다른 경우(언더스코어)도 지원
        pass

    for i, r in df.iterrows():
        sample = {k: r.get(k) for k in df.columns}  # 전체 dict 넘겨 alias에서 고름
        out = evaluate_sample(sample, lib, thr, assign_mode="by_ar", unknown_ar_gate=0.05)
        stable, stamp = kofm.update(out["violated"])
        print({
            "row": int(i),
            "set": out["set_id"],
            "unknown": out["unknown_recipe"],
            "violated": out["violated"],
            "kofm": stamp,
            "violated_items": out["violated_items"],
            "severity": round(out["severity"], 3),
            "Ar(dev%)": round(out["deviation"]["Ar"]*100, 3),
            "Ion(dev%)": round(out["deviation"]["Ion"]*100, 3),
            "Bar(dev%)": round(out["deviation"]["Bar"]*100, 3),
        })

def main():
    ap = argparse.ArgumentParser(description="셋팅값 라이브러리 기반 실시간 이상 판정 (Ar±1%, Ion/Bar±10%)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    ap_b = sub.add_parser("build", help="정상 ON CSV로 라이브러리 생성")
    ap_b.add_argument("--csv", required=True, help="정상값 CSV 경로 (Ion.Gauge.i,Baratron.Gauge.i,Ar.MFC.i 포함)")
    ap_b.add_argument("--out", default="setpoints.yaml", help="출력 라이브러리 경로")
    ap_b.add_argument("--kmax", type=int, default=6, help="Ar 1D 군집 최대 수")
    ap_b.add_argument("--kfixed", type=int, default=None, help="Ar 1D 군집 수 고정(자동 대신)")

    ap_d = sub.add_parser("demo", help="스트림 CSV로 간단 판정 데모")
    ap_d.add_argument("--model", required=True, help="setpoints.yaml 경로")
    ap_d.add_argument("--stream", required=True, help="1초 샘플 CSV(헤더에 세 컬럼 포함)")
    ap_d.add_argument("--k", type=int, default=2, help="K-of-M: k")
    ap_d.add_argument("--m", type=int, default=3, help="K-of-M: m")

    args = ap.parse_args()
    if args.cmd == "build": cli_build(args)
    elif args.cmd == "demo": cli_demo(args)

if __name__ == "__main__":
    main()
