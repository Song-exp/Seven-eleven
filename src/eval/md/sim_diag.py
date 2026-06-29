"""sim 엣지 영향력 EDA 헬퍼 — `docs/sim_edge_influence_eda_plan.md` S1~S4.

로직은 여기(.py), 구동·표·그림은 `eda/sim_edge_influence/sim_edge_influence.ipynb`.
MDEngine 단일 추론 캐시(eidx/y/masks/prob) 위에서 동작. 재학습(S5)은 노트북 밖(src.train).

- S1 구조진단: sim degree 분포 · 공유키워드(퇴화) · min_shared sweep
- S2 변별력:   sim_kw vs sim_ip 이웃성공률의 test 단독 PR-AUC (라벨 train+val → 누수 0)
- S3 전이ablation: train↔test sim 제거 후 재forward → test PR-AUC 하락폭
- S4 처방토글: 키워드 Δprob를 include_sim False/True 로 비교
"""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Dict, Sequence, Tuple

import numpy as np
import pandas as pd
import torch

from .engine import MDEngine, PK_MAIN

SIM_KW = ("product", "sim_kw", "product")
SIM_IP = ("product", "sim_ip", "product")


def _product_keywords_map(eng: MDEngine) -> Dict[int, set]:
    pk = eng.cache["eidx"][PK_MAIN].numpy()
    d: Dict[int, set] = defaultdict(set)
    for p, k in zip(pk[0].tolist(), pk[1].tolist()):
        d[p].add(k)
    return d


# ════════════════════ S1: 구조 진단 ════════════════════
def sim_degree_stats(eng: MDEngine) -> pd.DataFrame:
    """sim_kw/sim_ip 제품별 degree 분포 요약 (엣지수·평균·분위·max·고립수)."""
    P = eng.cache["P"]
    rows = []
    for name, et in [("sim_kw", SIM_KW), ("sim_ip", SIM_IP)]:
        if et not in eng.cache["eidx"]:
            continue
        ei = eng.cache["eidx"][et].numpy()
        deg = np.bincount(ei[0], minlength=P)
        rows.append(dict(relation=name, edges=int(ei.shape[1]), deg_mean=round(float(deg.mean()), 1),
                         deg_p50=int(np.percentile(deg, 50)), deg_p90=int(np.percentile(deg, 90)),
                         deg_p99=int(np.percentile(deg, 99)), deg_max=int(deg.max()),
                         isolated=int((deg == 0).sum())))
    return pd.DataFrame(rows)


def sim_degree_hist(eng: MDEngine, et=SIM_KW) -> np.ndarray:
    """제품별 sim degree 배열 (히스토그램용)."""
    ei = eng.cache["eidx"][et].numpy()
    return np.bincount(ei[0], minlength=eng.cache["P"])


def sim_kw_shared_keywords(eng: MDEngine, top: int = 30, n_generic: int = 30,
                           max_edges: int = 30000, rng_seed: int = 0) -> Tuple[pd.DataFrame, float]:
    """sim_kw 엣지를 만든 '공유 키워드' 빈도 top + 퇴화 지표.

    퇴화 = 공유 키워드가 일반 허브어(등장상품수 상위 n_generic)뿐인 엣지 비율.
    반환: (top 키워드 표, all_generic_ratio).
    """
    kwset = _product_keywords_map(eng)
    ei = eng.cache["eidx"][SIM_KW].numpy().T
    rng = np.random.default_rng(rng_seed)
    if len(ei) > max_edges:
        ei = ei[rng.choice(len(ei), max_edges, replace=False)]
    kdeg = np.bincount(eng.cache["eidx"][PK_MAIN].numpy()[1], minlength=eng.cache["K"])
    generic = set(np.argsort(-kdeg)[:n_generic].tolist())
    cnt: Counter = Counter()
    all_generic = 0
    for p, q in ei.tolist():
        shared = kwset[p] & kwset[q]
        if shared and shared <= generic:
            all_generic += 1
        cnt.update(shared)
    rows = [dict(keyword=eng.kw_name(k), 공유엣지수=c, 키워드_등장상품수=int(kdeg[k]),
                 generic허브=(k in generic)) for k, c in cnt.most_common(top)]
    return pd.DataFrame(rows), round(all_generic / max(len(ei), 1), 3)


def _kw_idf(eng: MDEngine) -> np.ndarray:
    """키워드 IDF = log(P / df_k), df_k=등장 제품수 (has_kw dst 빈도). build_hetero_data와 동일."""
    pk = eng.cache["eidx"][PK_MAIN].numpy()
    df_k = np.bincount(pk[1], minlength=eng.cache["K"]).clip(min=1)
    return np.log(float(eng.cache["P"]) / df_k).astype(np.float32)


def idf_sim_sweep(eng: MDEngine, taus: Sequence[float] = (1.0, 2.0, 4.0, 6.0, 8.0, 12.0, 16.0),
                  n_generic: int = 30, sample: int = 8000, rng_seed: int = 0) -> pd.DataFrame:
    """★ 재학습 전 미리보기 — IDF 가중 sim_kw를 현재 그래프에서 재계산해 τ별 구조 변화.

    `build_hetero_data._hop2(hop2_kw_idf=True)`와 **동일 공식**(S = A·diag(idf)·Aᵀ ≥ τ).
    ★ 핵심 지표 `generic_only`: 살아남은 엣지 중 공유 키워드가 일반 허브어뿐인 비율(샘플).
       이게 현행 0.66 → 낮게 떨어지는 τ가 노이즈를 거른 지점. deg도 같이 정상화되는 τ 선택.
    """
    import scipy.sparse as sp
    pk = eng.cache["eidx"][PK_MAIN].numpy()
    P, K = eng.cache["P"], eng.cache["K"]
    A = sp.csr_matrix((np.ones(pk.shape[1], dtype=np.float32), (pk[0], pk[1])), shape=(P, K))
    idf = _kw_idf(eng)
    S = (A.multiply(idf[None, :]).tocsr() @ A.T).tocoo()
    off = S.row != S.col
    srow, scol, sdata = S.row[off], S.col[off], S.data[off]
    kdeg = np.bincount(pk[1], minlength=K)
    generic = set(np.argsort(-kdeg)[:n_generic].tolist())
    kwset = _product_keywords_map(eng)
    rng = np.random.default_rng(rng_seed)
    rows = []
    for t in taus:
        m = sdata >= t
        ne = int(m.sum())
        deg = np.bincount(srow[m], minlength=P) if ne else np.zeros(P)
        # generic-only 비율 (샘플)
        idxs = np.where(m)[0]
        gonly = np.nan
        if len(idxs):
            sel = idxs if len(idxs) <= sample else rng.choice(idxs, sample, replace=False)
            g = 0
            for e in sel:
                sh = kwset[int(srow[e])] & kwset[int(scol[e])]
                if sh and sh <= generic:
                    g += 1
            gonly = round(g / len(sel), 3)
        rows.append(dict(tau=t, edges=ne, deg_mean=round(float(deg.mean()), 1),
                         deg_max=int(deg.max()) if ne else 0,
                         isolated=int((deg == 0).sum()), generic_only=gonly))
    return pd.DataFrame(rows)
    """공유키워드 임계별 sim_kw 엣지수·평균degree (A@A.T 재계산). 현재 임계는 sim_kw_min_shared."""
    import scipy.sparse as sp
    pk = eng.cache["eidx"][PK_MAIN].numpy()
    P, K = eng.cache["P"], eng.cache["K"]
    A = sp.csr_matrix((np.ones(pk.shape[1], dtype=np.float32), (pk[0], pk[1])), shape=(P, K))
    S = (A @ A.T).tocoo()
    cur = eng.cache.get("sim_kw_min_shared")
    rows = []
    for t in thresholds:
        m = (S.data >= t) & (S.row != S.col)
        ne = int(m.sum())
        deg = np.bincount(S.row[m], minlength=P) if ne else np.zeros(P)
        rows.append(dict(min_shared=t, edges=ne, deg_mean=round(float(deg.mean()), 1),
                         deg_max=int(deg.max()) if ne else 0, isolated=int((deg == 0).sum()),
                         현재=("◀" if t == cur else "")))
    return pd.DataFrame(rows)


# ════════════════════ S2: 변별력 분해 ════════════════════
def _neighbor_success_from_ei(eng: MDEngine, a: np.ndarray, b: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """엣지(a→b)에서 제품별 이웃 성공률 (라벨=train+val만 → test 누수 0)."""
    y = eng.cache["y"]
    P = eng.cache["P"]
    mask = eng.cache["train_mask"] | eng.cache["val_mask"]
    keep = mask[b]
    succ = np.zeros(P)
    cnt = np.zeros(P)
    np.add.at(succ, a[keep], y[b[keep]])
    np.add.at(cnt, a[keep], 1)
    return np.divide(succ, cnt, out=np.full(P, np.nan), where=cnt > 0), cnt


def neighbor_success_rate(eng: MDEngine, et, label_universe: str = "trainval") -> Tuple[np.ndarray, np.ndarray]:
    """제품별 sim 이웃 성공률 (라벨=train+val만 → test 누수 0). 반환 (rate(P,), labeled_cnt(P,))."""
    ei = eng.cache["eidx"][et].numpy()
    return _neighbor_success_from_ei(eng, ei[0], ei[1])


def idf_sim_discrimination(eng: MDEngine, taus: Sequence[float] = (8.0, 12.0, 16.0)) -> pd.DataFrame:
    """★ 재학습 전 핵심 미리보기 — IDF 정제 sim_kw의 이웃성공률이 test 변별력을 회복하나.

    현행 sim_kw(균등 count≥3) test PR-AUC 0.332(평탄) → IDF τ별로 재계산. sim_ip 0.617이 상한 참고.
    이게 오르면 "정제하면 sim_kw가 신호가 된다" → 재학습 정당화 (재학습 없이 사전 증거).
    """
    from sklearn.metrics import average_precision_score
    import scipy.sparse as sp
    pk = eng.cache["eidx"][PK_MAIN].numpy()
    P, K = eng.cache["P"], eng.cache["K"]
    y = eng.cache["y"]
    te = eng.cache["test_mask"]
    A = sp.csr_matrix((np.ones(pk.shape[1], dtype=np.float32), (pk[0], pk[1])), shape=(P, K))
    idf = _kw_idf(eng)
    S = (A.multiply(idf[None, :]).tocsr() @ A.T).tocoo()
    off = S.row != S.col
    srow, scol, sdata = S.row[off], S.col[off], S.data[off]
    rows = []
    for t in taus:
        m = sdata >= t
        rate, _ = _neighbor_success_from_ei(eng, srow[m], scol[m])
        sel = te & np.isfinite(rate)
        prauc = float(average_precision_score(y[sel], rate[sel])) if sel.sum() else np.nan
        deg = np.bincount(srow[m], minlength=P)
        rows.append(dict(tau=t, edges=int(m.sum()), deg_mean=round(float(deg.mean()), 1),
                         보유율_test=round(float(sel.sum() / te.sum()), 2),
                         test_PRAUC=round(prauc, 3)))
    df = pd.DataFrame(rows)
    df.attrs["현행_sim_kw_PRAUC"] = 0.332
    df.attrs["sim_ip_상한_PRAUC"] = 0.617
    return df


def discrimination_table(eng: MDEngine) -> pd.DataFrame:
    """sim_kw vs sim_ip 이웃성공률의 test 단독 변별력 (PR-AUC·ROC-AUC) + 4분면 평균.

    '어텐션은 큰데 변별은 작다' 모순(sim_kw) vs '진짜 변별자'(sim_ip) 를 한 표로.
    """
    from sklearn.metrics import average_precision_score, roc_auc_score
    y = eng.cache["y"]
    te = eng.cache["test_mask"]
    quad = eng.confusion_quadrant()
    base = float(y[te].mean())
    rows = []
    for name, et in [("sim_kw", SIM_KW), ("sim_ip", SIM_IP)]:
        if et not in eng.cache["eidx"]:
            continue
        rate, _ = neighbor_success_rate(eng, et)
        m = te & np.isfinite(rate)
        prauc = float(average_precision_score(y[m], rate[m])) if m.sum() else np.nan
        rocauc = float(roc_auc_score(y[m], rate[m])) if m.sum() and len(np.unique(y[m])) > 1 else np.nan
        q = {qd: round(float(np.nanmean(rate[quad == qd])), 3) for qd in ["TP", "FP", "FN", "TN"]}
        rows.append(dict(relation=name, test_PRAUC=round(prauc, 3), test_ROCAUC=round(rocauc, 3),
                         보유율_test=round(float(m.sum() / te.sum()), 2), **q))
    df = pd.DataFrame(rows)
    df.attrs["base_rate_test"] = round(base, 3)
    return df


# ════════════════════ S3: 전이적 낙관 ablation ════════════════════
def _forward_with(eng: MDEngine, eidx: dict) -> np.ndarray:
    from src.eval.success_predictor import predict_proba
    eidx_dev = {et: ei.to(eng.device) for et, ei in eidx.items()}
    hp = torch.tensor(eng.cache["has_promo"], dtype=torch.float, device=eng.device)
    im = torch.tensor(eng.cache["insta_m30"], dtype=torch.float, device=eng.device)
    eattr = ({et: ea.to(eng.device) for et, ea in eng.cache["eattr"].items()} if eng.cache["eattr"] else None)
    with torch.no_grad():
        return predict_proba(eng.model, eidx_dev, hp, eattr, insta_m30=im).cpu().numpy()


def ablate_cross_split_sim(eng: MDEngine, rels=(SIM_KW, SIM_IP)) -> dict:
    """train↔test 를 잇는 sim 엣지를 제거하고 재forward → test PR-AUC 비교 (전이적 낙관 크기).

    하락폭 큼 → metric이 'train 이웃 조회'에 의존 → cold-start 별도평가 필요.
    재학습 없음(inference-time edge drop). 반환에 prob_ablated 포함(추가 분석용).
    """
    from sklearn.metrics import average_precision_score
    tr = eng.cache["train_mask"]
    te = eng.cache["test_mask"]
    y = eng.cache["y"]
    eidx = {et: ei.clone() for et, ei in eng.cache["eidx"].items()}
    removed = {}
    for et in rels:
        if et not in eidx:
            continue
        ei = eidx[et].numpy()
        a, b = ei[0], ei[1]
        cross = (te[a] & tr[b]) | (tr[a] & te[b])
        removed["__".join(et)] = int(cross.sum())
        eidx[et] = eidx[et][:, torch.tensor(~cross)]
    prob = _forward_with(eng, eidx)
    full = float(average_precision_score(y[te], eng.cache["prob"][te]))
    abl = float(average_precision_score(y[te], prob[te]))
    return dict(full_test_prauc=round(full, 4), ablated_test_prauc=round(abl, 4),
                drop=round(full - abl, 4), removed=removed, prob_ablated=prob)


def train_neighbor_counts(eng: MDEngine, et=SIM_IP) -> pd.DataFrame:
    """test 제품별 train-이웃 / train-성공이웃 수 (전이적 의존 노출도)."""
    tr = eng.cache["train_mask"]
    te = eng.cache["test_mask"]
    y = eng.cache["y"]
    P = eng.cache["P"]
    ei = eng.cache["eidx"][et].numpy()
    a, b = ei[0], ei[1]
    keep = te[a] & tr[b]
    tn = np.zeros(P)
    ts = np.zeros(P)
    np.add.at(tn, a[keep], 1)
    np.add.at(ts, a[keep], y[b[keep]])
    idx = np.where(te)[0]
    return pd.DataFrame(dict(train_neighbors=tn[idx].astype(int),
                             train_succ_neighbors=ts[idx].astype(int)))


# ════════════════════ S4: include_sim 처방 토글 ════════════════════
def compare_include_sim(eng: MDEngine, keywords: Sequence[str], n_headroom: int = 20,
                        thr: float = 0.8, rng_seed: int = 0) -> pd.DataFrame:
    """키워드별 평균 Δprob(headroom)을 direct vs +sim 두 모드로 → 처방 신호 변화·순위 변동.

    margin = mean_p[ score(kw(p)+k) − score(kw(p)) ]. include_sim 켜면 '닮아지는 성공제품'까지 반영.
    """
    prob = eng.cache["prob"]
    pool = np.where(prob < thr)[0]
    rng = np.random.default_rng(rng_seed)
    sample = rng.choice(pool, min(n_headroom, len(pool)), replace=False).tolist()
    pk = {p: set(eng.product_keywords(p)) for p in sample}
    base = [sorted(pk[p]) for p in sample]
    bd = eng.score_concept_batch(base, include_sim=False)
    bs = eng.score_concept_batch(base, include_sim=True)
    rows = []
    for kw in keywords:
        ki = eng.seed_to_idx(kw)
        if ki is None:
            continue
        addk = [sorted(pk[p] | {ki}) for p in sample]
        ad = eng.score_concept_batch(addk, include_sim=False)
        as_ = eng.score_concept_batch(addk, include_sim=True)
        rows.append(dict(keyword=kw, margin_direct=round(float((ad - bd).mean()), 4),
                         margin_sim=round(float((as_ - bs).mean()), 4)))
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["Δ(sim-direct)"] = (df.margin_sim - df.margin_direct).round(4)
    df["rank_direct"] = df.margin_direct.rank(ascending=False).astype(int)
    df["rank_sim"] = df.margin_sim.rank(ascending=False).astype(int)
    df["rank_flip"] = (df.rank_direct - df.rank_sim).abs()
    return df.sort_values("margin_sim", ascending=False).reset_index(drop=True)
