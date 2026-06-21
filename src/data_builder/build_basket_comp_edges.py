"""Model v2 / 접근 3 — 장바구니 → 키워드-키워드 보완 엣지.

설계: docs/final_model_leakfree_switch_plan.md §9-2.
동반구매 제품쌍(A,B)의 키워드를 교차 연결 → `keyword__basket_comp__keyword`.
**키워드 단위 집계(지지도≥N개 제품쌍)** 라 특정 제품 성공이 직접 인코딩 안 됨(누수 희석).
누수 안전을 위해 train 제품쌍만 사용 옵션(restrict_ids) 제공.

출력: data/processed/hin/keyword_basket_comp_edges.parquet  [src_keyword, tgt_keyword, weight, support]
실행: python -m src.data_builder.build_basket_comp_edges
"""
from __future__ import annotations

import os
from collections import defaultdict
from itertools import product as iproduct
from typing import Optional, Set

import numpy as np
import pandas as pd

from .build_hetero_data import norm_id

HIN = "data/processed/hin"
PROC = "data/processed"


def _load_product_keywords() -> dict:
    """ITEM_CD(norm) → set(keyword).  product_keyword_edges_final 기반."""
    pk = pd.read_parquet(os.path.join(HIN, "product_keyword_edges_final.parquet"))
    col_id = "ITEM_CD" if "ITEM_CD" in pk.columns else pk.columns[0]
    col_kw = "keyword" if "keyword" in pk.columns else pk.columns[1]
    m = defaultdict(set)
    for cid, kw in zip(pk[col_id].map(norm_id), pk[col_kw].astype(str)):
        m[cid].add(kw)
    return m


def build_basket_comp_edges(min_support: int = 2, restrict_ids: Optional[Set[str]] = None,
                            save: bool = True) -> pd.DataFrame:
    """동반구매 제품쌍 → 키워드 교차쌍 집계 → 보완 엣지.

    restrict_ids: 주어지면 그 제품(예: train)만 사용 (누수 안전).
    min_support: 보완 엣지가 성립하려면 ≥N개의 제품쌍이 그 키워드쌍을 동반구매해야 함.
    """
    p2kw = _load_product_keywords()
    w_sum = defaultdict(float)   # (ka,kb) -> Lift 합
    supp = defaultdict(int)      # (ka,kb) -> 제품쌍 수
    n_pairs = 0
    for nm in ["offline_commerce_edge_lift_pair_out.csv", "quick_commerce_edge_lift_pair_out.csv"]:
        fp = os.path.join(PROC, nm)
        if not os.path.exists(fp):
            continue
        df = pd.read_csv(fp, encoding="utf-8-sig")
        for a, b, lift in zip(df["상품코드_A"].map(norm_id), df["상품코드_B"].map(norm_id),
                              df["향상도(Lift)"].astype(float)):
            if restrict_ids is not None and (a not in restrict_ids or b not in restrict_ids):
                continue
            ka, kb = p2kw.get(a, set()), p2kw.get(b, set())
            if not ka or not kb:
                continue
            n_pairs += 1
            seen = set()
            for x, y in iproduct(ka, kb):
                if x == y:
                    continue
                key = (x, y) if x < y else (y, x)
                if key in seen:
                    continue
                seen.add(key)
                w_sum[key] += float(lift)
                supp[key] += 1
    rows = [dict(src_keyword=k[0], tgt_keyword=k[1], weight=round(w_sum[k], 4), support=supp[k])
            for k in supp if supp[k] >= min_support]
    out = pd.DataFrame(rows).sort_values("weight", ascending=False).reset_index(drop=True)
    if save and len(out):
        out.to_parquet(os.path.join(HIN, "keyword_basket_comp_edges.parquet"), index=False)
    return out


def main():
    full = build_basket_comp_edges(min_support=2, save=True)
    print(f"기여 제품쌍 처리 / 보완 엣지(support≥2): {len(full):,}개 "
          f"(고유 키워드 {len(set(full['src_keyword']) | set(full['tgt_keyword'])):,})")
    for s in (2, 3, 5):
        print(f"  support≥{s}: {int((full['support'] >= s).sum()):,}개")
    print("상위 보완 키워드쌍:")
    print(full.head(12).to_string(index=False))


if __name__ == "__main__":
    main()
