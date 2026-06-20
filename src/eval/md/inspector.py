"""키워드 확정용 증거 인스펙터 — "이 키워드가 진짜 흥행을 유발하나" 다각 검증.

파라미터 조정(EngineConfig) → 장부 재생성 → 각 키워드를 아래 4축 증거로 확인 → 확정.
  ① 통계: purity·support·score (장부 선정 근거)
  ② 인과: Δprob = 이 키워드를 빈 제품에 더하면 모델 성공확률이 오르나 (개입 머신)
  ③ 실증: 이 키워드를 가진 실제 제품들의 성공률·실매출 (pos_product_features)
  ④ 유의성: 순열 귀무 대비 (선택, §9 Tier1)
"""
from __future__ import annotations

import os
from typing import List, Optional

import numpy as np
import pandas as pd

from .engine import MDEngine, PK_MAIN, _rk


def _pos_sales(eng: MDEngine) -> dict:
    """ITEM_CD(norm) → (성공여부 라벨, sales_30d_amt). 세븐 POS 제품만 매출 존재."""
    fp = "data/processed/pos_product_features.parquet"
    if not os.path.exists(fp):
        return {}
    from src.data_builder.build_hetero_data import norm_id
    df = pd.read_parquet(fp, columns=["ITEM_CD", "sales_30d_amt"])
    return {norm_id(c): float(a) for c, a in zip(df["ITEM_CD"], df["sales_30d_amt"])}


def keyword_evidence(eng: MDEngine, keyword: str, universe: str = "full",
                     n_products: int = 8, n_causal: int = 25, sales_map: Optional[dict] = None) -> dict:
    """단일 키워드의 4축 증거. 흥행 유발 여부 판단용."""
    k = eng.seed_to_idx(keyword)
    if k is None:
        return {"keyword": keyword, "error": "그래프에 없음"}
    lg = eng.ledger.get(universe) or eng.build_ledger(universe)
    c = eng.cache
    base = c["base_rate"]

    # ── 이 키워드를 가진 제품 (has_kw) ──
    ei = c["eidx"][PK_MAIN].numpy()
    carriers = ei[0][ei[1] == k]
    y = c["y"]; prob = c["prob"]
    succ_rate = float(y[carriers].mean()) if len(carriers) else float("nan")

    # ① 통계
    stat = dict(tag=lg.tag(k), purity=round(float(lg.purity[k]), 3),
                balance=round(abs(float(lg.purity[k]) - base), 3),
                support_succ=int(lg.support_succ[k]), support_fail=int(lg.support_fail[k]),
                hub_score=round(float(lg.hub_score[k]), 3))

    # ② 인과 — 빈(미보유) 헤드룸 제품에 키워드 추가 시 Δprob
    has_k = set(carriers.tolist())
    headroom = [p for p in np.where(prob < 0.8)[0] if p not in has_k]
    rng = np.random.default_rng(0)
    sample = rng.choice(headroom, min(n_causal, len(headroom)), replace=False) if headroom else []
    deltas = [eng.delta_prob(eng.product_keywords(int(p)), add=[k]) for p in sample]
    causal = dict(delta_prob_mean=round(float(np.mean(deltas)), 4) if deltas else None,
                  delta_prob_pos_rate=round(float(np.mean([d > 0 for d in deltas])), 2) if deltas else None,
                  n=len(deltas))

    # ③ 실증 — 보유 제품 실매출·성공
    if sales_map is None:
        sales_map = _pos_sales(eng)
    rows = []
    order = carriers[np.argsort(-prob[carriers])]
    for p in order[:n_products]:
        cid = eng.cache["product_ids"][int(p)]
        rows.append(dict(product=c["product_names"][int(p)], 성공=("성공" if y[int(p)] else "실패"),
                         prob=round(float(prob[int(p)]), 3), 매출30d=sales_map.get(cid)))
    sold = [r["매출30d"] for r in rows if r["매출30d"]]

    return dict(keyword=keyword, n_carriers=len(carriers), 성공률=round(succ_rate, 3),
                base_rate=round(base, 3), **stat, **causal,
                매출중앙값=round(float(np.median(sold)), 0) if sold else None,
                예시제품=pd.DataFrame(rows))


def evidence_table(eng: MDEngine, keywords: List[str], universe: str = "full", n_causal: int = 15) -> pd.DataFrame:
    """여러 키워드를 한 표로 스캔 (확정 작업용). 예시제품 컬럼 제외."""
    sm = _pos_sales(eng)
    out = []
    for kw in keywords:
        e = keyword_evidence(eng, kw, universe, n_causal=n_causal, sales_map=sm)
        if "error" in e:
            continue
        e.pop("예시제품", None)
        out.append(e)
    cols = ["keyword", "tag", "성공률", "base_rate", "purity", "balance", "support_succ",
            "support_fail", "delta_prob_mean", "delta_prob_pos_rate", "매출중앙값", "hub_score"]
    df = pd.DataFrame(out)
    return df[[c for c in cols if c in df.columns]]


def ledger_keywords(eng: MDEngine, tag: str, universe: str = "full") -> List[str]:
    """장부 태그별 키워드 목록 (확정 검토 대상)."""
    lg = eng.ledger.get(universe) or eng.build_ledger(universe)
    s = {"killer": lg.killer, "mine": lg.mine, "hub": lg.hub}[tag]
    return sorted(eng.kw_name(k) for k in s)


def export_keyword_final(eng: MDEngine, out_path: str = "data/processed/hin/keyword_final.csv",
                         universe: str = "full", n_causal: int = 15) -> pd.DataFrame:
    """확정용 마스터 CSV — 전 키워드(태그·include) + 태그 키워드엔 Δprob 증거·추천액션.

    serve.py 가 이 파일을 읽어 대시보드에 반영(include 필터 + tag 색).
    당신은 이 CSV에서 include/tag 만 손보면 됨 (Δprob·매출 보고 판단).
    """
    lg = eng.ledger.get(universe) or eng.build_ledger(universe)
    c = eng.cache; base = c["base_rate"]; K = c["K"]
    # 전 키워드 성공률(has_kw carriers) 벡터화
    ei = c["eidx"][PK_MAIN].numpy(); y = c["y"]
    succ_sum = np.zeros(K); cnt = np.zeros(K)
    np.add.at(succ_sum, ei[1], y[ei[0]].astype(float)); np.add.at(cnt, ei[1], 1.0)
    succ_rate = np.divide(succ_sum, cnt, out=np.full(K, np.nan), where=cnt > 0)
    sm = _pos_sales(eng)

    rows = []
    for k in range(K):
        tag = lg.tag(k); kw = eng.kw_name(k)
        supp = int(lg.support_succ[k] + lg.support_fail[k])
        row = dict(keyword=kw, tag=tag, include="Y",
                   성공률=round(float(succ_rate[k]), 3) if cnt[k] else None,
                   purity=round(float(lg.purity[k]), 3) if np.isfinite(lg.purity[k]) else None,
                   balance=round(abs(float(lg.purity[k]) - base), 3) if np.isfinite(lg.purity[k]) else None,
                   support=supp, delta_prob_mean=None, delta_prob_pos_rate=None,
                   매출중앙값=None, suggested="")
        if tag != "neutral":   # 태그 키워드만 인과(Δprob) 증거 — 비용 절약
            e = keyword_evidence(eng, kw, universe, n_causal=n_causal, sales_map=sm)
            d = e.get("delta_prob_mean")
            row.update(delta_prob_mean=d, delta_prob_pos_rate=e.get("delta_prob_pos_rate"),
                       매출중앙값=e.get("매출중앙값"))
            if tag == "killer":
                row["suggested"] = "유지(흥행유발 확인)" if (d or 0) > 0 else "강등검토(상관만 Δ≤0)"
            elif tag == "mine":
                row["suggested"] = "유지(악재 확인)" if (d or 0) < 0 else "재검토(Δ≥0)"
            else:
                row["suggested"] = "유지(일반어)" if (e.get("balance") or 1) < 0.15 else "재검토(편향)"
        rows.append(row)
    df = pd.DataFrame(rows)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    return df
