"""키워드 라이브 분류 (A안) — 데이터/모델 로드본에서 killer/mine/매개/hub/neutral 태그를 재계산.

정적 `keyword_final.csv`를 대체한다. 데이터를 갈아끼우면 엔진을 새로 로드할 때 자동 재계산되어
대시보드 마커가 따라온다(요청당이 아니라 로드당 = 엔진에 1회 캐시).

기준 코드화 = docs/keyword_classification_criteria.md 그대로:
  1) 관찰 게이트  : build_ledger(purity·Score·support) + WoE 교차검증
  2) 인과 검증    : Δprob tier (killer 확실/조건부만 생존, 가짜 강등) / mine Δ<0
  3) 매개         : review.review_mediator (보편 리프트 delta_pos·delta_mean)
  4) hub          : 내부 게이트 (att_lift<1.5 ∧ |WoE|<0.5)
단일 노이즈 플로어 Δ=±0.01 (review.NOISE) — pair synergy·분류 공통 기준선.
"""
from __future__ import annotations

from typing import Dict
import numpy as np
import pandas as pd

from .engine import MDEngine
from . import review as R

NOISE = R.NOISE   # 0.01 — 단일 노이즈 플로어 (2·SE 스케일)


def _woe_attlift(eng: MDEngine, lg):
    """키워드별 WoE·att_lift 벡터 + 성공/실패 제품 수. 단일추론(카운트·스코어 기반)."""
    y = eng.cache["y"]
    n_succ = max(int((y == 1).sum()), 1)
    n_fail = max(int((y == 0).sum()), 1)
    ss = lg.support_succ.astype(float)
    sf = lg.support_fail.astype(float)
    with np.errstate(divide="ignore", invalid="ignore"):
        woe = np.log((ss / n_succ + 1e-9) / (sf / n_fail + 1e-9))
        att_lift = (lg.score_succ / n_succ) / (lg.score_fail / n_fail + 1e-12)
    return woe, att_lift, n_succ, n_fail


def _batch_delta(eng: MDEngine, cand_idx, n_headroom: int = 12, chunk: int = 48):
    """후보 키워드들의 평균 Δprob을 **공유 헤드룸 표본 + 1배치**로 계산 (review_mediator와 동일 방식).

    per-keyword 루프(수천 forward)를 배치 2회로 축약 → warmup 대폭 단축.
    반환: {k_idx: delta_mean}.
    """
    from .subnet import _headroom_products
    if not cand_idx:
        return {}
    sample = _headroom_products(eng, n_headroom)
    pk = {c: list(eng.product_keywords(c)) for c in sample}
    M = len(sample)
    base = eng.score_concept_batch([pk[c] for c in sample])
    concepts = [pk[c] + [int(k)] for k in cand_idx for c in sample]
    scores = eng.score_concept_batch(concepts, chunk_size=chunk)
    out = {}
    for i, k in enumerate(cand_idx):
        d = np.array([scores[i * M + j] - base[j] for j in range(M)])
        out[int(k)] = float(d.mean())
    return out


def _scoreboard(eng: MDEngine, lg, woe, att_lift) -> pd.DataFrame:
    """review_mediator가 요구하는 최소 스코어보드 (전 키워드: tag·supp·purity·woe)."""
    K = eng.cache["K"]
    ss, sf = lg.support_succ, lg.support_fail
    rows = []
    for k in range(K):
        rows.append(dict(keyword=eng.kw_name(k), tag=lg.tag(k),
                         purity=float(lg.purity[k]) if np.isfinite(lg.purity[k]) else np.nan,
                         supp_s=int(ss[k]), supp_f=int(sf[k]), supp_all=int(ss[k] + sf[k]),
                         woe=float(woe[k]), att_lift=float(att_lift[k])))
    return pd.DataFrame(rows)


def classify_keywords_live(eng: MDEngine, universe: str = "full", verify: bool = True,
                           include_mediator: bool = True) -> Dict[str, str]:
    """전 키워드 → tag (killer/mine/매개/hub). 미수록 키워드 = neutral.

    verify=True  : 인과(Δprob) tier로 가짜 killer 강등 + 매개 산출 (torch 재추론, 로드 시 1회).
    verify=False : 관찰만 (빠름, 가짜 강등·매개 없음).
    결과는 eng._live_tags 에 캐시 — 같은 (universe,verify,mediator)면 즉시 반환.
    """
    key = (universe, verify, include_mediator)
    cached = getattr(eng, "_live_tags", None)
    if cached is not None and cached[0] == key:
        return cached[1]

    lg = eng.build_ledger(universe)
    woe, att_lift, _, _ = _woe_attlift(eng, lg)
    tags: Dict[str, str] = {}

    # ── 관찰 게이트 통과 후보 (WoE 교차검증) ──
    kc = [k for k in lg.killer if woe[k] > 0 and lg.support_fail[k] > 0]
    mc = [k for k in lg.mine if woe[k] < 0 and lg.support_succ[k] > 0]

    if not verify:
        for k in kc:
            tags[eng.kw_name(k)] = "killer"
        for k in mc:
            if eng.kw_name(k) not in tags:
                tags[eng.kw_name(k)] = "mine"
    else:
        # 인과 Δprob 1배치 (killer+mine 공통 헤드룸 표본)
        deltas = _batch_delta(eng, kc + mc)
        # killer: Δ tier (확실/조건부만 생존, 가짜 강등)
        for k in kc:
            d = deltas.get(int(k), 0.0)
            cond = np.nan
            if d < -NOISE:                       # 의심분만 IP 조건부 (비용 절약)
                cond, _ = R.conditional_delta(eng, k)
            if R._killer_tier(d, cond) in ("killer_확실", "조건부killer(IP)"):
                tags[eng.kw_name(k)] = "killer"
        # mine: Δ<노이즈 유지(악재·노이즈=주의 prior); 명확 +Δ면 의심 제외
        for k in mc:
            nm = eng.kw_name(k)
            if nm not in tags and deltas.get(int(k), 0.0) < NOISE:
                tags[nm] = "mine"

    # ── hub: 내부 게이트 (att_lift<1.5 ∧ |WoE|<0.5), killer/mine 우선 ──
    for k in lg.hub:
        nm = eng.kw_name(k)
        if nm not in tags and att_lift[k] < 1.5 and abs(woe[k]) < 0.5:
            tags[nm] = "hub"

    # ── 매개: 보편 리프트 (review_mediator 배치 Δ) ──
    if include_mediator and verify:
        try:
            med = R.review_mediator(eng, _scoreboard(eng, lg, woe, att_lift))
            if not med.empty:
                for kw in med["keyword"].tolist():
                    if kw not in tags:
                        tags[kw] = "매개"
        except Exception:
            pass

    eng._live_tags = (key, tags)
    return tags
