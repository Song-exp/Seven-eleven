"""성공 조합 마이닝 — 모델-가이드 다단계 빔 (combo_mining_plan.md Step 1).

naive 어텐션 워크(rare 편향)도, 순수 Δprob 빔(역시 rare·과적합 편향)도 폐기.
빈도를 제약(floor)으로 넣어 "널리 쓰이면서 흥행을 견인하는 정예 조합"만 살린다.

핵심 프리미티브: engine.score_concept(S) = 키워드 집합 S만 가진 가상 제품의 예측 성공확률
  (cold-start 시뮬 그 자체 — 신제품 기획이 바로 이 상황. has_kw(+has_ip)만 연결 → leak-free)

목적함수:  f(S) = logit(score_concept(S)) + λ·log1p(min_support(S))
  - logit 사용: prob(0~1) ceiling 회피 → 조합 간 차이가 의미있게 벌어짐
  - support: 곱이 아니라 ① 하드 floor(rare·과적합 배제) ② 약한 가산 tiebreak(λ작게, 흔한 쪽 선호)

초가산성(synergy)은 ★logit 공간★에서: 모델이 로지스틱이라 효과의 가산 기준선 = logit 합.
  synergy(S) = logit(p_S) − [logit(p_∅) + Σ_k (logit(p_k) − logit(p_∅))]
  prob 공간 합은 ceiling 때문에 항상 음수로 보이는 함정 → logit이 정답.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np

from .engine import MDEngine, PK_MAIN

_EPS = 1e-6


def _logit(p: float) -> float:
    p = min(max(float(p), _EPS), 1 - _EPS)
    return float(np.log(p / (1 - p)))


# ────────────────────────────────────────────────────────── 지지도
def keyword_support(eng: MDEngine) -> np.ndarray:
    """키워드별 has_kw 보유 제품 수 (K,)."""
    ei = eng.cache["eidx"][PK_MAIN].numpy()
    return np.bincount(ei[1], minlength=eng.cache["K"]).astype(int)


def _cooccur(eng: MDEngine, seed_idx: int) -> Counter:
    """seed와 같은 제품을 공유하는 키워드별 공기(共起) 횟수 — forward 0 prefilter용."""
    ei = eng.cache["eidx"][PK_MAIN].numpy()
    carriers = set(ei[0][ei[1] == seed_idx].tolist())
    cnt: Counter = Counter()
    mask = np.isin(ei[0], list(carriers))
    for k in ei[1][mask]:
        if k != seed_idx:
            cnt[int(k)] += 1
    return cnt


class _ConceptCache:
    """score_concept(frozenset) 메모이즈 — 빔이 같은 집합을 여러 번 평가하므로 필수."""

    def __init__(self, eng: MDEngine):
        self.eng = eng
        self._c: Dict[frozenset, float] = {}

    def __call__(self, S) -> float:
        fs = frozenset(S)
        v = self._c.get(fs)
        if v is None:
            v = self.eng.score_concept(sorted(fs))
            self._c[fs] = v
        return v

    def warm(self, sets, chunk_size: int = 64) -> int:
        """미캐시 집합들을 배치 1-forward로 일괄 선계산 → 캐시 적재.

        이후 __call__(S)는 전부 캐시 히트가 되어 seed_partners/combo_grow가 단일 forward로 가속.
        반환: 새로 계산한 집합 수.
        """
        uniq = list(dict.fromkeys(frozenset(s) for s in sets))
        miss = [fs for fs in uniq if fs not in self._c]
        if not miss:
            return 0
        probs = self.eng.score_concept_batch([sorted(fs) for fs in miss], chunk_size=chunk_size)
        for fs, p in zip(miss, probs):
            self._c[fs] = float(p)
        return len(miss)


@dataclass
class ComboPath:
    keywords: List[str]            # 조합 키워드명 (seed 포함, 추가 순서)
    idxs: List[int]
    score: float                   # score_concept(S) — 조합 성공확률
    obj: float                     # 목적함수 f(S)
    min_support: int
    synergy: float                 # ★logit 공간 초가산성 (>0 = 1+1>2)
    steps: List[Tuple[str, float]] = field(default_factory=list)  # (추가키워드, marginal Δscore)

    def __repr__(self):
        arrow = " ➔ ".join(self.keywords)
        return (f"[{arrow}]  score={self.score:.3f} synergy(logit)={self.synergy:+.3f} "
                f"min_sup={self.min_support}")


# ────────────────────────────────────────────────────────── 빔
def combo_beam(eng: MDEngine, seed: str, depth: int = 3, beam_width: int = 8,
               support_floor: int = 3, cand_pool: int = 50, support_lambda: float = 0.25,
               require_synergy: bool = False, sc: Optional[_ConceptCache] = None
               ) -> List[ComboPath]:
    """시드에서 출발하는 모델-가이드 조합 빔 (보조 — 포화 주의).

    ⚠ 실측: 가상 올스타 노드 score_concept(S)는 ~2키워드에서 포화(3번째 marginal≈0.0001).
       따라서 depth≥3 조합은 의미가 약하고, synergy를 selector로 쓰면 강한 키워드끼리
       대체재(substitute)라 음수만 남음. **파트너 판별은 `seed_partners`(headroom Δ)를 쓸 것.**
       이 함수는 "예측 성공확률 최대 코히런트 조합" 예시 생성용으로만 (depth≤2 권장).

    1) prefilter: seed와 공기(共起)하며 support≥floor 인 키워드 상위 cand_pool (forward 0).
    2) 빔 확장: f(S)=logit(score)+λ·log1p(min_support) 최대화, 폭 beam_width, 깊이 depth.
    반환: 목적함수 내림차순 ComboPath 리스트.
    """
    si = eng.seed_to_idx(seed)
    if si is None:
        return []
    sup = keyword_support(eng)
    sc = sc or _ConceptCache(eng)

    p_empty = sc(frozenset())
    L_empty = _logit(p_empty)

    def solo(k: int) -> float:                          # 키워드 k 단독 logit 기여
        return _logit(sc(frozenset([k]))) - L_empty

    def obj(S) -> Tuple[float, float, int]:
        score = sc(S)
        msup = min(int(sup[k]) for k in S)
        return _logit(score) + support_lambda * np.log1p(msup), score, msup

    def synergy(S) -> float:                            # logit 공간 초가산성
        return _logit(sc(S)) - (L_empty + sum(solo(k) for k in S))

    # 1) prefilter — 공기 상위 cand_pool (support floor 적용)
    co = _cooccur(eng, si)
    cands = [k for k, _ in co.most_common() if sup[k] >= support_floor][:cand_pool]
    if not cands:
        return []

    # 2) 빔 확장
    beams: List[frozenset] = [frozenset([si])]
    for _ in range(depth):
        scored: Dict[frozenset, float] = {}
        for S in beams:
            for X in cands:
                if X in S:
                    continue
                S2 = S | {X}
                if S2 in scored:
                    continue
                if require_synergy and synergy(S2) <= 0:
                    continue
                o, _, _ = obj(S2)
                scored[S2] = o
        if not scored:
            break
        beams = [s for s, _ in sorted(scored.items(), key=lambda t: -t[1])[:beam_width]]

    # 3) ComboPath 조립 (그리디 경로 순서 복원)
    out = []
    for S in beams:
        o, score, msup = obj(S)
        path_idx = _greedy_order(S, si, sc)
        steps, cur = [], frozenset([si])
        for k in path_idx[1:]:
            prev = sc(cur)
            cur = cur | {k}
            steps.append((eng.kw_name(k), round(sc(cur) - prev, 4)))
        out.append(ComboPath(
            keywords=[eng.kw_name(k) for k in path_idx], idxs=path_idx,
            score=round(score, 4), obj=round(o, 4), min_support=msup,
            synergy=round(synergy(S), 4), steps=steps))
    out.sort(key=lambda c: -c.obj)
    return out


def _greedy_order(S: frozenset, seed: int, sc: _ConceptCache) -> List[int]:
    """집합 S를 seed부터 score 상승 순서로 정렬(설명용 경로)."""
    order, rest, cur = [seed], set(S) - {seed}, frozenset([seed])
    while rest:
        best = max(rest, key=lambda k: sc(cur | {k}))
        order.append(best)
        cur = cur | {best}
        rest.discard(best)
    return order


# ────────────────────────────────────────────────────────── 인과 파트너표 (★핵심)
def seed_partners(eng: MDEngine, seed: str, base_extra: Sequence[str] = (),
                  support_floor: int = 3, cand_pool: int = 60, n_headroom: int = 20,
                  headroom_thr: float = 0.8, top: int = 25, sc: Optional[_ConceptCache] = None,
                  rng_seed: int = 0, batch_chunk: int = 64) -> "object":
    """시드(+기존 조합)에 각 후보 키워드를 더했을 때 headroom 제품 성공확률이 오르는가 = 인과 파트너표.

    ★ 가상 올스타 노드(score_concept(S))는 ~2키워드에서 포화 → 깊은 빔이 무의미.
       대신 '보통 제품(prob<thr)에 [seed+X] 추가 시 Δprob'을 평균 → 포화 없이 파트너/anti 판별.
       Δ>0 = 보강 파트너 / Δ<0 = anti-파트너(피해야 할 조합).

    반환: DataFrame [keyword, delta_mean, pos_rate, support] (delta_mean 내림차순).
    """
    import pandas as pd
    si = eng.seed_to_idx(seed)
    if si is None:
        return pd.DataFrame([{"error": f"'{seed}' 그래프에 없음"}])
    sc = sc or _ConceptCache(eng)
    sup = keyword_support(eng)
    base = [si] + [k for k in (eng.seed_to_idx(b) for b in base_extra) if k is not None]
    base_set = set(base)

    # 후보 — seed 공기 상위 cand_pool (support floor)
    co = _cooccur(eng, si)
    cands = [k for k, _ in co.most_common() if sup[k] >= support_floor and k not in base_set][:cand_pool]

    # headroom 제품 표본 (prob<thr, seed 미보유)
    prob = eng.cache["prob"]
    ei = eng.cache["eidx"][PK_MAIN].numpy()
    has_seed = set(ei[0][ei[1] == si].tolist())
    pool = [int(p) for p in np.where(prob < headroom_thr)[0] if p not in has_seed]
    rng = np.random.default_rng(rng_seed)
    sample = rng.choice(pool, min(n_headroom, len(pool)), replace=False).tolist() if pool else []

    # 제품 키워드 1회 캐시 + 평가할 모든 집합 배치 선계산(1-forward) → 이후 sc()는 전부 히트
    pk = {p: set(eng.product_keywords(p)) for p in sample}
    all_sets = [pk[p] | base_set for p in sample]
    all_sets += [pk[p] | base_set | {X} for X in cands for p in sample]
    sc.warm(all_sets, chunk_size=batch_chunk)

    base_score = {p: sc(frozenset(pk[p] | base_set)) for p in sample}

    rows = []
    for X in cands:
        deltas = []
        for p in sample:
            deltas.append(sc(frozenset(pk[p] | base_set | {X})) - base_score[p])
        rows.append(dict(keyword=eng.kw_name(X),
                         delta_mean=round(float(np.mean(deltas)), 4),
                         pos_rate=round(float(np.mean([d > 0 for d in deltas])), 2),
                         support=int(sup[X])))
    if not rows:
        return pd.DataFrame(columns=["keyword", "delta_mean", "pos_rate", "support"])
    df = pd.DataFrame(rows).sort_values("delta_mean", ascending=False).reset_index(drop=True)
    return df.head(top) if top else df


# ────────────────────────────────────────────────────────── 개방형 인과 전진 (★메인)
def combo_grow(eng: MDEngine, seed: str, eps: float = 0.01, max_hops: int = 6,
               cand_pool: int = 40, n_headroom: int = 15, anti_log: int = 3,
               sc: Optional[_ConceptCache] = None, batch_chunk: int = 64) -> dict:
    """개방형 인과 전진 — 데이터가 깊이를 결정(고정 depth 없음). + 우회 장부.

    매 hop: 현재 레일 S 기준 headroom Δ(margin) 최대 후보 X.
      margin ≥ eps → 합격 (S∪{X}, logit synergy로 찐보완재/강한보조 라벨), 다음 hop.
      top margin < eps → Early Stop (전부 대체재/포화) → 경로 폐쇄.
      margin<0 후보는 매 hop Bypass Registry에 [대체재 잠식]으로 적재 (MD 뇌절 금지 장부).

    반환: dict(seed, rail[키워드], accepted[hop별 라벨], bypass[기각장부], stop[종료사유]).
    """
    si = eng.seed_to_idx(seed)
    if si is None:
        return dict(seed=seed, error="그래프에 없음")
    sc = sc or _ConceptCache(eng)
    rail, rail_names, accepted, bypass = [si], [seed], [], []
    solo_margin: Dict[str, float] = {}    # base={seed} 일 때 각 X의 margin (synergy 기준선)

    for hop in range(1, max_hops + 1):
        base_extra = rail_names[1:]
        df = seed_partners(eng, seed, base_extra=base_extra, sc=sc, top=0,
                           cand_pool=cand_pool, n_headroom=n_headroom, batch_chunk=batch_chunk)
        if df.empty or "error" in df.columns:
            return dict(seed=seed, rail=rail_names, accepted=accepted, bypass=bypass,
                        stop=f"Hop{hop}: 후보 없음 → 종료")
        if hop == 1:                      # 시드 단독 기준 margin 저장 (synergy 비교 기준선)
            solo_margin = dict(zip(df["keyword"], df["delta_mean"]))
        # anti(잠식) 적재
        for r in df[df.delta_mean < 0].head(anti_log).itertuples():
            bypass.append(dict(hop=hop, keyword=r.keyword, margin=float(r.delta_mean),
                               support=int(r.support), reason="대체재 잠식(anti, Δ<0)"))
        top = df.iloc[0]
        if float(top.delta_mean) < eps:                       # Early Stop
            for r in df[(df.delta_mean >= 0) & (df.delta_mean < eps)].head(anti_log).itertuples():
                bypass.append(dict(hop=hop, keyword=r.keyword, margin=float(r.delta_mean),
                                   support=int(r.support), reason="조기 성능 포화(0≤Δ<ε)"))
            return dict(seed=seed, rail=rail_names, accepted=accepted, bypass=bypass,
                        stop=f"Hop{hop}: 큐 전부 margin<{eps} (대체재/포화) → 경로 폐쇄")
        # 합격 + headroom synergy 라벨 (X가 레일 맥락에서 시드단독 때보다 더 기여하나 = 효과수정)
        #   synergy = margin(X | 현재레일) − margin(X | 시드단독).  >0 = 레일이 X를 증폭(찐보완재).
        X = eng.seed_to_idx(top.keyword)
        syn = float(top.delta_mean) - float(solo_margin.get(top.keyword, top.delta_mean))
        rail.append(X)
        rail_names.append(top.keyword)
        accepted.append(dict(hop=f"{hop}->{hop+1}", keyword=top.keyword,
                             margin=round(float(top.delta_mean), 4),
                             synergy=round(syn, 4), support=int(top.support),
                             label=("★찐보완재(emergent)" if syn > 0 else "강한보조(중복가능)")))
    return dict(seed=seed, rail=rail_names, accepted=accepted, bypass=bypass,
                stop=f"max_hops={max_hops} 도달")


def print_grow_log(res: dict) -> None:
    """combo_grow 결과를 MD 감사 장부 텍스트로 출력."""
    if res.get("error"):
        print(f"[{res['seed']}] {res['error']}"); return
    print(f"🧾 [{res['seed']}] 동적 확장 감사 장부")
    print(f"  최종 레일: {' ➔ '.join(res['rail'])}")
    print("  ── 합격(전진) ──")
    for a in res["accepted"]:
        print(f"   [Hop {a['hop']}] +{a['keyword']}  margin={a['margin']:+.4f} "
              f"synergy={a['synergy']:+.4f} sup={a['support']}  → {a['label']}")
    print("  ── Bypass Registry (기각·블랙리스트) ──")
    for b in res["bypass"]:
        print(f"   [Hop {b['hop']}] ✗{b['keyword']}  margin={b['margin']:+.4f} "
              f"sup={b['support']}  사유: {b['reason']}")
    print(f"  🚨 종료: {res['stop']}")


# ────────────────────────────────────────────────────────── 조합쌍(pair) 마이닝
def mine_pairs(eng: MDEngine, candidates: Sequence[str], top: int = 40,
               support_floor: int = 3, sc: Optional[_ConceptCache] = None) -> "object":
    """후보 키워드(보통 killer 장부) 간 모든 쌍의 초가산성(logit synergy) → 상위 조합쌍.

    synergy(a,b) = logit(p_ab) − [logit(p_∅) + solo(a) + solo(b)].  >0 이면 1+1>2.
    반환: DataFrame [a, b, synergy, score_pair, min_support, support_a, support_b].
    """
    import pandas as pd
    sup = keyword_support(eng)
    sc = sc or _ConceptCache(eng)
    L0 = _logit(sc(frozenset()))
    idxs = [eng.seed_to_idx(k) for k in candidates]
    idxs = [k for k in idxs if k is not None and sup[k] >= support_floor]

    def solo(k):
        return _logit(sc(frozenset([k]))) - L0

    rows = []
    for i in range(len(idxs)):
        for j in range(i + 1, len(idxs)):
            a, b = idxs[i], idxs[j]
            sp = sc(frozenset([a, b]))
            rows.append(dict(a=eng.kw_name(a), b=eng.kw_name(b),
                             synergy=round(_logit(sp) - (L0 + solo(a) + solo(b)), 4),
                             score_pair=round(sp, 3),
                             min_support=int(min(sup[a], sup[b])),
                             support_a=int(sup[a]), support_b=int(sup[b])))
    df = pd.DataFrame(rows).sort_values("synergy", ascending=False).reset_index(drop=True)
    return df.head(top)


# ────────────────────────────────────────────────────────── 확정 export
def export_combo_final(eng: MDEngine, seeds: Sequence[str], n_partner: int = 4, n_anti: int = 2,
                       out_path: str = "data/processed/hin/combo_final.csv",
                       sc: Optional[_ConceptCache] = None, **partner_kw) -> "object":
    """시드별 인과 파트너/anti를 모아 확정 CSV — MD 처방·대시보드가 읽을 조합 장부.

    포화·대체 아티팩트를 피하려 `seed_partners`(headroom Δprob) 기반.
    스키마: seed | partners(Δ>0 보강) | anti(Δ<0 피할것) | top_delta | include(Y/N).
    """
    import os
    import pandas as pd
    sc = sc or _ConceptCache(eng)
    rows = []
    for s in seeds:
        df = seed_partners(eng, s, sc=sc, top=0, **partner_kw)
        if "error" in df.columns or df.empty:
            continue
        pos = df[df.delta_mean > 0].head(n_partner)
        neg = df[df.delta_mean < 0].tail(n_anti)
        rows.append(dict(
            seed=s,
            partners=", ".join(f"{r.keyword}(+{r.delta_mean:.3f})" for r in pos.itertuples()),
            anti=", ".join(f"{r.keyword}({r.delta_mean:.3f})" for r in neg.itertuples()),
            top_delta=round(float(pos.delta_mean.max()) if len(pos) else 0.0, 4),
            include="Y"))
    out = pd.DataFrame(rows)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    out.to_csv(out_path, index=False, encoding="utf-8-sig")
    return out
