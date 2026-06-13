"""순회 기반 키워드 조합 추천 (학습된 가중치만 사용).

RECOMMEND 결정: 하이브리드 α×성공.
메타패스 keyword_s → product → keyword_t 를 따라
  score(kt | ks) = Σ_{j : ks,kt ∈ kw(j)}  att(j, ks) · p_success(j) · att(j, kt)
로 조합 점수를 매겨 시드 키워드와 잘 맞는 키워드를 랭킹.

att(j, ·) = 학습된 (product, has_kw, keyword) 엣지 어텐션(마지막 층, head 평균).
p_success(j) = product j 의 예측 성공 확률.
"""
from collections import defaultdict
from typing import Dict, List, Tuple

import torch

from .success_predictor import predict_proba

PK_KEY = "product__has_kw__keyword"


def _build_artifacts(model, edge_index_dict, has_promo, edge_attr_dict=None):
    """forward 1회 → (성공확률 p, product별 [(keyword_idx, att)]) 캐시."""
    model.eval()
    with torch.no_grad():
        p = predict_proba(model, edge_index_dict, has_promo, edge_attr_dict).cpu()  # (P,)
    att = model.last_edge_attention().get(PK_KEY)                       # (E_pk,)
    ei = edge_index_dict[("product", "has_kw", "keyword")].cpu()        # (2, E_pk)
    if att is None:
        att = torch.ones(ei.size(1))
    att = att.cpu()

    prod2kw: Dict[int, List[Tuple[int, float]]] = defaultdict(list)     # j -> [(kw, att)]
    kw2prod: Dict[int, List[Tuple[int, float]]] = defaultdict(list)     # kw -> [(j, att)]
    for e in range(ei.size(1)):
        j = int(ei[0, e]); kw = int(ei[1, e]); a = float(att[e])
        prod2kw[j].append((kw, a))
        kw2prod[kw].append((j, a))
    return p, prod2kw, kw2prod


def recommend_combinations(
    model,
    edge_index_dict: Dict,
    maps: Dict,
    seed_keywords: List[str],
    has_promo: torch.Tensor = None,
    edge_attr_dict: Dict = None,
    top_k: int = 20,
) -> List[Tuple[str, float]]:
    """시드 키워드 집합과 잘 맞는 조합 키워드 top_k 를 (키워드, 점수)로 반환."""
    p, prod2kw, kw2prod = _build_artifacts(model, edge_index_dict, has_promo, edge_attr_dict)
    k2i = maps["k2i"]; kw_ids = maps["keyword_ids"]
    seed_idx = [k2i[s] for s in seed_keywords if s in k2i]
    if not seed_idx:
        raise KeyError(f"시드 키워드가 그래프에 없음: {seed_keywords}")

    score = defaultdict(float)
    for ks in seed_idx:
        for j, att_in in kw2prod.get(ks, []):       # ks 를 가진 product j
            pj = float(p[j])
            for kt, att_out in prod2kw.get(j, []):  # j 의 다른 키워드 kt
                if kt in seed_idx:
                    continue
                score[kt] += att_in * pj * att_out  # 하이브리드 α×성공
    ranked = sorted(score.items(), key=lambda x: x[1], reverse=True)[:top_k]
    return [(kw_ids[kt], round(s, 6)) for kt, s in ranked]


@torch.no_grad()
def export_relation_importance(model) -> List[Dict[str, float]]:
    """XAI: 층별 DiffMG 관계 중요도 α_r (MD 대시보드용)."""
    return model.relation_importance()


def export_weighted_kw_edges(
    model, edge_index_dict, maps, has_promo=None, edge_attr_dict=None, out_csv: str = None
):
    """학습된 (product↔keyword) 가중 엣지를 DataFrame 으로 추출(오프라인 순회용)."""
    import pandas as pd
    p, prod2kw, _ = _build_artifacts(model, edge_index_dict, has_promo, edge_attr_dict)
    rows = []
    pn = maps["product_names"]; kw_ids = maps["keyword_ids"]
    for j, lst in prod2kw.items():
        for kw, a in lst:
            rows.append({"product": pn[j], "keyword": kw_ids[kw],
                         "attention": a, "product_success_prob": float(p[j])})
    df = pd.DataFrame(rows)
    if out_csv:
        df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    return df
