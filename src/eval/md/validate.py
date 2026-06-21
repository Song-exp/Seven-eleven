"""검증 하니스 — §9 Tier 1/2/4/5 + Precision@K 백테스트(§5-5).

설계: docs/eda_channel_prescription_plan.md §9.
순환성 차단: ① 순열 귀무 ② train→test hold-out ③ 모델 개입 ④ 백테스트.
Tier 1·2·4·5 통과 = "확정", 미통과 = "후보/참고".
"""
from __future__ import annotations

from typing import Dict, List

import numpy as np
from sklearn.metrics import average_precision_score

from .engine import MDEngine, PK_PATHS, _rk


# ---------------------------------------------------- 공통: 라벨 임의화 score
def _pk_score_with_labels(eng: MDEngine, y: np.ndarray):
    """주어진 라벨 y로 Score_succ/fail 재집계 (순열 검정용)."""
    K = eng.cache["K"]
    ssucc = np.zeros(K); sfail = np.zeros(K)
    for et in PK_PATHS:
        key = _rk(et)
        if et not in eng.cache["eidx"] or key not in eng.cache["att"]:
            continue
        ei = eng.cache["eidx"][et].numpy(); a = eng.cache["att"][key]
        ys = y[ei[0]] == 1
        np.add.at(ssucc, ei[1][ys], a[ys])
        np.add.at(sfail, ei[1][~ys], a[~ys])
    tot = ssucc + sfail
    return np.divide(ssucc, tot, out=np.full(K, np.nan), where=tot > 0)


# ---------------------------------------------------- Tier 1: 순열 귀무
def tier1_permutation(eng: MDEngine, universe: str = "full", n_perm: int = 500, seed: int = 42) -> Dict:
    """라벨 셔플 귀무분포 대비 관측 Purity가 95pct 초과하는 killer 비율."""
    lg = eng.ledger.get(universe) or eng.build_ledger(universe)
    y = eng.cache["y"].copy()
    obs = lg.purity
    rng = np.random.default_rng(seed)
    null = np.zeros((n_perm, eng.cache["K"]))
    for i in range(n_perm):
        yp = rng.permutation(y)
        null[i] = _pk_score_with_labels(eng, yp)
    p95 = np.nanpercentile(null, 95, axis=0)
    killers = np.array(sorted(lg.killer))
    if len(killers) == 0:
        return dict(n_killer=0, pass_rate=float("nan"))
    passed = obs[killers] > p95[killers]
    return dict(n_killer=len(killers), pass_rate=float(np.nanmean(passed)),
                passed_idx=killers[passed].tolist())


# ---------------------------------------------------- Tier 2: hold-out 예측
def tier2_holdout(eng: MDEngine) -> Dict:
    """train-only 장부의 signed-purity 키워드 점수가 test에서 base 초과 PR-AUC인가."""
    lg = eng.build_ledger("train")
    K = eng.cache["K"]
    signed = np.zeros(K)
    for k in lg.killer:
        signed[k] = +1.0
    for k in lg.mine:
        signed[k] = -1.0
    ei = eng.cache["eidx"][("product", "has_kw", "keyword")].numpy()
    P = eng.cache["P"]
    score = np.zeros(P)
    np.add.at(score, ei[0], signed[ei[1]])
    test = eng.cache["test_mask"]; y = eng.cache["y"]
    base = float(y[test].mean())
    prauc = float(average_precision_score(y[test], score[test])) if len(np.unique(y[test])) > 1 else float("nan")
    # bootstrap CI
    rng = np.random.default_rng(0)
    idx = np.where(test)[0]
    boot = []
    for _ in range(300):
        s = rng.choice(idx, len(idx), replace=True)
        if len(np.unique(y[s])) > 1:
            boot.append(average_precision_score(y[s], score[s]))
    lo = float(np.percentile(boot, 2.5)) if boot else float("nan")
    return dict(test_prauc=round(prauc, 4), base=round(base, 4),
                ci_lo=round(lo, 4), passed=bool(lo > base))


# ---------------------------------------------------- Tier 4: 모델 개입
def tier4_intervention(eng: MDEngine, universe: str = "full", n_sample: int = 40, seed: int = 0) -> Dict:
    """killer add → Δprob>0, mine add → Δprob<0 부호 일치율.

    표본은 **헤드룸 있는 제품**(prob<0.8)에서 추출 — 이미 prob≈1인 성공품은 천장효과로
    killer를 더해도 못 오르므로 검정이 왜곡됨. + 랜덤 키워드 대비도 측정(우월성).
    """
    lg = eng.ledger.get(universe) or eng.build_ledger(universe)
    rng = np.random.default_rng(seed)
    prob = eng.cache["prob"]; K = eng.cache["K"]
    headroom = np.where(prob < 0.8)[0]           # 올릴 여지
    droproom = np.where(prob > 0.2)[0]           # 내릴 여지
    killers = list(lg.killer); mines = list(lg.mine)
    pos_hit = []; rand_hit = []; neg_hit = []
    for _ in range(n_sample):
        if killers and len(headroom):
            p = int(rng.choice(headroom)); base = eng.product_keywords(p)
            pos_hit.append(eng.delta_prob(base, add=[int(rng.choice(killers))]) > 0)
            rand_hit.append(eng.delta_prob(base, add=[int(rng.integers(K))]) > 0)
        if mines and len(droproom):
            p = int(rng.choice(droproom)); base = eng.product_keywords(p)
            neg_hit.append(eng.delta_prob(base, add=[int(rng.choice(mines))]) <= 0)
    kp = float(np.mean(pos_hit)) if pos_hit else float("nan")
    rp = float(np.mean(rand_hit)) if rand_hit else float("nan")
    return dict(killer_pos_rate=round(kp, 3), random_pos_rate=round(rp, 3),
                mine_nonpos_rate=round(float(np.mean(neg_hit)), 3) if neg_hit else None,
                passed=bool(pos_hit and kp >= 0.7 and kp > rp))


# ---------------------------------------------------- Tier 5: 조합 시너지
def tier5_synergy(presc, n_pairs: int = 20, seed: int = 0) -> Dict:
    """Δprob(A+B) > Δprob(A)+Δprob(B) 인 조합 비율 (초가법 시너지)."""
    eng = presc.eng
    rng = np.random.default_rng(seed)
    killers = list(presc.lg.killer)
    if len(killers) < 2:
        return dict(n=0, superadd_rate=float("nan"))
    super_cnt = 0; n = 0
    base = []
    for _ in range(n_pairs):
        a, b = rng.choice(killers, 2, replace=False)
        da = eng.delta_prob(base, add=[int(a)]); db = eng.delta_prob(base, add=[int(b)])
        dab = eng.delta_prob(base, add=[int(a), int(b)])
        super_cnt += int(dab > da + db); n += 1
    return dict(n=n, superadd_rate=round(super_cnt / n, 3))


# ---------------------------------------------------- 백테스트 Precision@K (§5-5)
def backtest_precision_at_k(presc, k: int = 10, n_sample: int = 100, seed: int = 0) -> Dict:
    """hold-out 성공 제품의 실제 키워드를, 그 제품 시드로 추천한 top-K가 적중하나."""
    eng = presc.eng
    rng = np.random.default_rng(seed)
    y = eng.cache["y"]; test = eng.cache["test_mask"]
    succ_test = np.where((y == 1) & test)[0]
    if len(succ_test) == 0:
        return dict(n=0, precision_at_k=float("nan"))
    hits = []; covered = []
    for p in rng.choice(succ_test, min(n_sample, len(succ_test)), replace=False):
        kws = eng.product_keywords(int(p))
        if len(kws) < 2:
            continue
        seed_k = int(rng.choice(kws))
        truth = set(kws) - {seed_k}
        rec = [kt for kt, _ in presc.partner_beam(seed_k, top_k=k)]
        if not rec:
            continue
        hits.append(len(truth & set(rec)) / len(rec))
        covered.append(len(truth & set(rec)) > 0)
    return dict(n=len(hits), precision_at_k=round(float(np.mean(hits)), 4) if hits else 0.0,
                hit_rate=round(float(np.mean(covered)), 4) if covered else 0.0)


def run_all(eng: MDEngine, presc, light: bool = True) -> Dict:
    """전체 게이트 실행 → 통과 요약. light=True면 순열 축소."""
    r = {}
    r["tier1_permutation"] = tier1_permutation(eng, n_perm=100 if light else 500)
    r["tier2_holdout"] = tier2_holdout(eng)
    r["tier4_intervention"] = tier4_intervention(eng, n_sample=20 if light else 40)
    r["tier5_synergy"] = tier5_synergy(presc, n_pairs=10 if light else 20)
    r["backtest_precision_at_k"] = backtest_precision_at_k(presc, n_sample=40 if light else 100)
    return r
