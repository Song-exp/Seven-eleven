"""EDA Stage — 혼동행렬 진단(Cell 4a/4b/4c) + 거시/미시 차분 토폴로지(G-1/G-2).

설계: docs/eda_channel_prescription_plan.md §2~§4.
입력: MDEngine (단일 추론 캐시 + 장부 + Mass).  모든 통계는 캐시 슬라이싱(원칙 0).
행렬: 키워드 투영 A = Wᵀ W (W = 제품×키워드 has_kw 어텐션 가중치) → 우주별 평균 차분.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import scipy.sparse as sp

from .engine import MDEngine, PK_MAIN, PK_PATHS, _rk

# 성공유형 → 채널/메커니즘 매핑 (§4)
POS_SRC = {"POS", "POS+인스타"}
INSTA_SRC = {"인스타", "CU_인스타", "GS25_인스타"}


# ============================================================ 키워드 투영 행렬
def _W_matrix(eng: MDEngine) -> sp.csr_matrix:
    """제품×키워드 has_kw 어텐션 가중 행렬 W (P×K).  A = Wᵀ W."""
    ei = eng.cache["eidx"][PK_MAIN].numpy()
    a = eng.cache["att"][_rk(PK_MAIN)]
    P, K = eng.cache["P"], eng.cache["K"]
    return sp.csr_matrix((a, (ei[0], ei[1])), shape=(P, K))


def projection_matrix(eng: MDEngine, mask: np.ndarray, W: Optional[sp.csr_matrix] = None) -> np.ndarray:
    """A = (W[mask]ᵀ W[mask]) / |mask|  — 우주(mask) 키워드 투영 평균 행렬 (K×K)."""
    if W is None:
        W = _W_matrix(eng)
    n = int(mask.sum())
    if n == 0:
        return np.zeros((eng.cache["K"], eng.cache["K"]))
    Wm = W[mask]
    return np.asarray((Wm.T @ Wm).todense()) / n


@dataclass
class DiffTopology:
    A_succ: np.ndarray
    A_fail: np.ndarray
    A_diff_plus: np.ndarray    # max(0, succ - fail)  성공 특이망
    A_diff_minus: np.ndarray   # max(0, fail - succ)  실패 특이망


def stage_g1_macro(eng: MDEngine) -> DiffTopology:
    """G-1 거시: 성공망 vs 실패망 차분 (§3)."""
    W = _W_matrix(eng)
    y = eng.cache["y"]
    A_s = projection_matrix(eng, y == 1, W)
    A_f = projection_matrix(eng, y == 0, W)
    return DiffTopology(A_s, A_f, np.maximum(0, A_s - A_f), np.maximum(0, A_f - A_s))


@dataclass
class ChannelDiff:
    A_pos: np.ndarray
    A_insta: np.ndarray
    A_diff_pos: np.ndarray     # POS 매출 독점망 (내실화 속성)
    A_diff_insta: np.ndarray   # 인스타 반응 독점망 (카피 속성)
    track: str
    pos_mask: Optional[np.ndarray] = None      # A_diff_pos 우주 (경유 제품 복원용)
    insta_mask: Optional[np.ndarray] = None    # A_diff_insta 우주


def stage_g2_channel(eng: MDEngine, track: str = "track1") -> ChannelDiff:
    """G-2 미시: 성공 내부 POS-주도 vs 인스타-주도 차분 (§4, Route A).

    track1 = 전 채널(인스타 265, 채널차 포함) / track2 = 세븐 한정(POS전용 vs POS+인스타).
    """
    W = _W_matrix(eng)
    y = eng.cache["y"]; src = eng.cache["succ_src"]; ch = eng.cache["channel"]
    succ = y == 1
    if track == "track2":
        seven = ch == "세븐일레븐"
        pos_mask = succ & seven & (src == "POS")
        insta_mask = succ & seven & (src == "POS+인스타")
    else:
        pos_mask = succ & np.isin(src, list(POS_SRC))
        insta_mask = succ & np.isin(src, list(INSTA_SRC))
    A_p = projection_matrix(eng, pos_mask, W)
    A_i = projection_matrix(eng, insta_mask, W)
    return ChannelDiff(A_p, A_i, np.maximum(0, A_p - A_i), np.maximum(0, A_i - A_p), track,
                       pos_mask=pos_mask, insta_mask=insta_mask)


# ============================================================ Cell 4a 정량 개요
def cell_4a(eng: MDEngine) -> Dict:
    """분면×채널 교차표 + 오분류 확신도 분포 (§2 Cell 4a)."""
    q = eng.confusion_quadrant()
    ch = eng.cache["channel"]; prob = eng.cache["prob"]
    crosstab = pd.crosstab(pd.Series(q, name="분면"), pd.Series(ch, name="채널"), margins=True)
    err = np.isin(q, ["FP", "FN"])
    return dict(
        crosstab=crosstab,
        prob_err=prob[err], quad_err=q[err],
        thr=eng.cfg.thr,
        # 경계선(±0.1) vs 극단 오류 비율
        borderline_frac=float((np.abs(prob[err] - eng.cfg.thr) < 0.1).mean()) if err.any() else 0.0,
    )


# ============================================================ Cell 4b 위상 대조
def _hub_score_vec(eng: MDEngine, universe: str = "full") -> np.ndarray:
    return eng.ledger[universe].hub_score if universe in eng.ledger else eng.build_ledger(universe).hub_score


def product_hubness(eng: MDEngine, hub_score: np.ndarray) -> np.ndarray:
    """일반성(Hub도)[Pm] = Σ att(Pm,k)·Hub_Score(k) / Σ att(Pm,k)  (§2 Cell 4b)."""
    ei = eng.cache["eidx"][PK_MAIN].numpy()
    a = eng.cache["att"][_rk(PK_MAIN)]
    P = eng.cache["P"]
    num = np.zeros(P); den = np.zeros(P)
    np.add.at(num, ei[0], a * hub_score[ei[1]])
    np.add.at(den, ei[0], a)
    return np.divide(num, den, out=np.zeros(P), where=den > 0)


def cell_4b(eng: MDEngine, universe: str = "full", within_channel: bool = True) -> pd.DataFrame:
    """분면별 1-hop 피처 대조표 (within-channel).  §2 Cell 4b."""
    lg = eng.ledger.get(universe) or eng.build_ledger(universe)
    q = eng.confusion_quadrant()
    ch = eng.cache["channel"]
    P = eng.cache["P"]
    hubness = product_hubness(eng, lg.hub_score)

    # 제품 degree (4대 경로 + has_ip incident)
    deg = np.zeros(P); ip_deg = np.zeros(P)
    for et in PK_PATHS:
        if et in eng.cache["eidx"]:
            deg += np.bincount(eng.cache["eidx"][et].numpy()[0], minlength=P)
    if ("product", "has_ip", "ip") in eng.cache["eidx"]:
        ip_deg = np.bincount(eng.cache["eidx"][("product", "has_ip", "ip")].numpy()[0], minlength=P)

    # killer/mine 비율 (제품 has_kw 이웃 중 장부 태그 비율)
    ei = eng.cache["eidx"][PK_MAIN].numpy()
    kill_cnt = np.zeros(P); mine_cnt = np.zeros(P); kw_cnt = np.zeros(P)
    killer_mask = np.zeros(eng.cache["K"], bool); killer_mask[list(lg.killer)] = True
    mine_mask = np.zeros(eng.cache["K"], bool); mine_mask[list(lg.mine)] = True
    np.add.at(kw_cnt, ei[0], 1)
    np.add.at(kill_cnt, ei[0], killer_mask[ei[1]].astype(float))
    np.add.at(mine_cnt, ei[0], mine_mask[ei[1]].astype(float))
    kill_ratio = np.divide(kill_cnt, kw_cnt, out=np.zeros(P), where=kw_cnt > 0)
    mine_ratio = np.divide(mine_cnt, kw_cnt, out=np.zeros(P), where=kw_cnt > 0)

    df = pd.DataFrame(dict(quad=q, channel=ch, degree=deg, hubness=hubness,
                           ip_count=ip_deg, killer_ratio=kill_ratio, mine_ratio=mine_ratio))
    feats = ["degree", "hubness", "ip_count", "killer_ratio", "mine_ratio"]
    if within_channel:
        return df.groupby(["channel", "quad"])[feats].mean().round(3)
    return df.groupby("quad")[feats].mean().round(3)


# ============================================================ Cell 4c 극단 오분류
def cell_4c(eng: MDEngine, universe: str = "full", top_k: int = 10) -> Dict[str, pd.DataFrame]:
    """극단 FN/FP Top-K + att 내림차순 이웃 키워드 + 장부 태그 (§2 Cell 4c)."""
    lg = eng.ledger.get(universe) or eng.build_ledger(universe)
    q = eng.confusion_quadrant()
    prob = eng.cache["prob"]; names = eng.cache["product_names"]; ch = eng.cache["channel"]
    ei = eng.cache["eidx"][PK_MAIN].numpy(); a = eng.cache["att"][_rk(PK_MAIN)]

    def neighbors(p):
        m = ei[0] == p
        ks, aa = ei[1][m], a[m]
        order = np.argsort(-aa)[:8]
        return ", ".join(f"{eng.kw_name(int(ks[o]))}[{lg.tag(int(ks[o]))[0]}]" for o in order)

    out = {}
    for quad, asc in [("FN", True), ("FP", False)]:  # FN=가장 억울(저prob) / FP=가장 황당(고prob)
        idx = np.where(q == quad)[0]
        idx = idx[np.argsort(prob[idx])]
        idx = idx[:top_k] if asc else idx[::-1][:top_k]
        out[quad] = pd.DataFrame([dict(product=names[p], channel=ch[p], prob=round(float(prob[p]), 3),
                                       neighbors=neighbors(p)) for p in idx])
    return out


# ============================================================ 차분 행렬 → 키워드 랭킹
def top_diff_partners(eng: MDEngine, A_diff: np.ndarray, seed_idx: int, top_k: int = 10) -> List[tuple]:
    """차분 행렬에서 seed와 가장 강한 키워드 파트너 top_k (kw_name, weight)."""
    row = A_diff[seed_idx].copy()
    row[seed_idx] = 0
    order = np.argsort(-row)[:top_k]
    return [(eng.kw_name(int(j)), float(row[j])) for j in order if row[j] > 0]


def bridge_product(eng: MDEngine, k1: int, k2: int,
                   mask: Optional[np.ndarray] = None) -> Optional[tuple]:
    """K-P-K 경유 제품 Top-1 복원.

    A_diff[k1,k2] = Σ_p W[p,k1]·W[p,k2] (W=has_kw 어텐션 가중) 는 두 키워드가
    '같은 제품을 공유'해 묶인 것 — 행렬곱이 그 경유 제품 p를 summation으로 합쳐 없앤 상태.
    기여도 W[p,k1]·W[p,k2] 최대 제품을 되살림. mask(bool, P) 주면 그 우주(예: 성공/POS)로 한정.
    반환 (p_idx, contrib, product_name) 또는 None (공유 제품 없음).
    """
    ei = eng.cache["eidx"][PK_MAIN].numpy()
    a = eng.cache["att"][_rk(PK_MAIN)]
    P = eng.cache["P"]
    w1 = np.zeros(P); w2 = np.zeros(P)
    m1, m2 = ei[1] == k1, ei[1] == k2
    np.add.at(w1, ei[0][m1], a[m1])
    np.add.at(w2, ei[0][m2], a[m2])
    contrib = w1 * w2
    if mask is not None:
        contrib = contrib * mask.astype(float)
    p = int(np.argmax(contrib))
    return (p, float(contrib[p]), eng.product_name(p)) if contrib[p] > 0 else None
