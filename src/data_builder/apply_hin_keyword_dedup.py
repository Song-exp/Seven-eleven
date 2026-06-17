"""HIN 최종 산출물 키워드 dedup 보장 (04 모든 단계 이후 후처리).

문제: Phase 3.6에서 IP명/프로모션/수동 키워드를 NOISE_REMOVE로 제거해도,
      후속 단계(패치·키워드 합산)가 원본 소스에서 제품 단위로 키워드를 재병합하며
      IP명(K리그 등)·캐릭터(헬로키티)가 되살아남.
해결: 모든 단계가 끝난 뒤 최종 parquet에서 dedup 집합을 일괄 제거(멱등).

dedup 집합:
  - IP명      : product_ip_mapping.xlsx 'IP별_키워드' 시트 ip_name (product_ip_edges로 이미 표현됨)
  - 프로모션  : product_promo_keywords.csv 프로모션_키워드 (product_nodes promo_* 원핫으로 이미 표현됨)
  - 수동      : MANUAL (IP엣지 커버리지 체크 완료 — 손실 없는 것만. 산리오는 유일 IP신호라 제외)

실행: python -m src.data_builder.apply_hin_keyword_dedup
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

# 수동 제거 — IP엣지 커버리지 체크 완료(손실 없음). 산리오는 진짜 산리오 4제품의 유일 IP신호라 유지.
MANUAL = {"김혜자", "디진다", "마리오", "헬로키티"}


def build_dedup_set(hin_dir: Path) -> set:
    ipn = pd.read_excel(hin_dir / "product_ip_mapping.xlsx", sheet_name="IP별_키워드")["ip_name"]
    ip_names = set(ipn.dropna().astype(str))
    promo = pd.read_csv(hin_dir / "product_promo_keywords.csv", encoding="utf-8-sig")["프로모션_키워드"]
    promos = {p.strip() for v in promo.dropna() for p in str(v).split(",") if p.strip()}
    return ip_names | promos | MANUAL


def apply_dedup(hin_dir: str = "data/processed/hin", verbose: bool = True) -> dict:
    """최종 HIN parquet에서 dedup 집합 제거 (멱등). 변경 통계 반환."""
    hin = Path(hin_dir)
    dedup = build_dedup_set(hin)
    stats = {"dedup_size": len(dedup)}

    def strip_list(L):
        return [k for k in L if str(k) not in dedup] if isinstance(L, (list, np.ndarray)) else []

    # 노드 키워드 리스트
    for f in ["product_nodes.parquet", "ip_nodes.parquet"]:
        d = pd.read_parquet(hin / f)
        if "키워드_final" in d.columns:
            d["키워드_final"] = d["키워드_final"].apply(strip_list)
            if "키워드_개수" in d.columns:
                d["키워드_개수"] = d["키워드_final"].apply(len)
            d.to_parquet(hin / f, index=False)

    # keyword_nodes
    kn = pd.read_parquet(hin / "keyword_nodes.parquet")
    b = len(kn)
    kn = kn[~kn["keyword"].astype(str).isin(dedup)].reset_index(drop=True)
    kn.to_parquet(hin / "keyword_nodes.parquet", index=False)
    stats["keyword_nodes"] = (b, len(kn))

    # 엣지
    for f, cols in [("product_keyword_edges.parquet", ["keyword"]),
                    ("ip_keyword_edges.parquet", ["keyword"]),
                    ("trend_keyword_edges.parquet", ["src_keyword", "tgt_keyword"])]:
        p = hin / f
        if not p.exists():
            continue
        e = pd.read_parquet(p)
        b = len(e)
        for c in cols:
            if c in e.columns:
                e = e[~e[c].astype(str).isin(dedup)]
        e = e.reset_index(drop=True)
        e.to_parquet(p, index=False)
        stats[f] = (b, len(e))

    if verbose:
        print(f"[dedup] 집합 {len(dedup)}개 (IP+프로모션+수동) 제거")
        for k, v in stats.items():
            if isinstance(v, tuple):
                print(f"  {k}: {v[0]} → {v[1]}")
    return stats


if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding="utf-8")
    apply_dedup()
