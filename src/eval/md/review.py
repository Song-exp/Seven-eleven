"""키워드 분류 질적 리뷰 — 카테고리 카운트 + 키워드별 증거표 (판정이 실제 맞는지 눈검사).

분류(WoE)는 관찰, 검증은 개입(Δprob). 이 모듈은 둘을 한 표로 묶어 *질적 검토*를 돕는다:
  · Δprob (무조건): 임의 캐리어에 키워드 추가 시 성공확률 변화 = 인과 유발
  · Δprob_cond (IP 조건부): 그 키워드의 대표 IP가 있는 가상제품에 추가 → "IP와 함께면 유발하나"
      → 공룡류(무조건 Δ≤0이나 IP 후광에 얹힌) 를 '가짜' vs '조건부 killer'로 가른다
  · 예시제품: 그 키워드 보유 실제 제품(성공/실패) — 도메인 눈검사용

판정 tier (killer 기준, 노이즈 밴드 ±0.01 ≈ 2·SE 스케일):
  Δ>+0.01            → killer_확실
  -0.01≤Δ≤+0.01      → killer_불명확(노이즈)
  Δ<-0.01 ∧ cond>0   → 조건부killer(IP)
  Δ<-0.01 ∧ cond≤0   → 가짜(강등)
"""
from __future__ import annotations

from typing import List, Optional, Tuple
import numpy as np
import pandas as pd

from .engine import MDEngine, PK_MAIN
from .inspector import evidence_table

IP_HASKW = ("ip", "has_kw", "keyword")
NOISE = 0.01   # Δprob 노이즈 밴드 (±) — 우리 finding의 2·SE 스케일


def _examples(eng: MDEngine, k_idx: int, n: int = 3) -> str:
    """키워드 보유 실제 제품 top-n (성공확률 순) + 성공/실패 라벨."""
    ei = eng.cache["eidx"][PK_MAIN].numpy()
    prob = eng.cache["prob"]; y = eng.cache["y"]
    ps = sorted(set(ei[0][ei[1] == k_idx].tolist()), key=lambda p: -prob[p])[:n]
    return " / ".join(f"{eng.product_name(p)}({'성공' if y[p] == 1 else '실패'})" for p in ps)


def solo_success(eng: MDEngine, k_idx: int) -> float:
    """단독성공률 = 이 키워드 보유 실제 제품의 raw 성공 비율 (model-free, 단독강도 = headroom 반비례).

    synergy ⟂ 단독강도 (corr −0.61): 이게 높으면(강함) 더할 여지(headroom)가 적어 killer끼리 묶어도
    시너지가 안 난다(포화·대체). killer 기준에 병기해 '강한 base인가 vs 살리는 modifier인가' 구분."""
    ei = eng.cache["eidx"][PK_MAIN].numpy()
    y = eng.cache["y"]
    ps = ei[0][ei[1] == k_idx]
    return round(float(y[ps].mean()), 3) if len(ps) else np.nan


def raw_freq(eng: MDEngine, k_idx: int) -> int:
    """직접 has_kw 제품 수 (우회 경로 supp 말고 = 실제 증거량). 단일추론."""
    ei = eng.cache["eidx"][PK_MAIN].numpy()
    return int((ei[1] == k_idx).sum())


def solo_wilson(eng: MDEngine, k_idx: int, z: float = 1.96) -> float:
    """단독성공률 Wilson 신뢰구간 하한 — 소표본 자동 페널티 (n=1 의 1.0을 base로 수축). 단일추론."""
    ei = eng.cache["eidx"][PK_MAIN].numpy(); y = eng.cache["y"]
    ps = ei[0][ei[1] == k_idx]
    n = len(ps)
    if n == 0:
        return np.nan
    p = float(y[ps].sum()) / n
    lo = (p + z * z / (2 * n) - z * np.sqrt(p * (1 - p) / n + z * z / (4 * n * n))) / (1 + z * z / n)
    return round(float(lo), 3)


def _strength(s) -> str:
    """단독강도 라벨."""
    if s is None or (isinstance(s, float) and np.isnan(s)):
        return "?"
    return "강" if s > 0.5 else ("중" if s > 0.3 else "약")


def why_killer(r) -> str:
    d = r["delta"]
    return (f"purity {r['purity']}(성공특이)·WoE {r['woe']:+.2f}·Δ {d:+.3f}(유발{'✓' if d > 0 else '✗'})"
            f"·단독 {r['단독성공률']}({_strength(r['단독성공률'])}base)")


def why_mediator(r) -> str:
    return (f"단독 {r['단독성공률']}(혼자 {_strength(r['단독성공률'])})·delta_pos {r['delta_pos']}(거의 전캐리어 +)"
            f"·Δ {r['delta_mean']:+.3f}(보편리프트)·purity {r['purity']}(성공특이 아님)")


def why_mine(r) -> str:
    d = r["delta"]
    return (f"purity {r['purity']}(실패특이)·WoE {r['woe']:+.2f}·Δ {d:+.3f}(악재{'✓' if d < 0 else '✗'})")


def _top_ip(eng: MDEngine, k_idx: int) -> Optional[int]:
    """키워드와 가장 강하게 연결된 IP (ip_has_kw 어텐션 최대). 없으면 None."""
    if IP_HASKW not in eng.cache["eidx"]:
        return None
    ei = eng.cache["eidx"][IP_HASKW].numpy()
    att = eng.cache["att"].get("ip__has_kw__keyword")
    mask = ei[1] == k_idx
    ips = ei[0][mask]
    if len(ips) == 0:
        return None
    if att is not None:
        return int(ips[np.argmax(att[mask])])
    return int(ips[0])


def conditional_delta(eng: MDEngine, k_idx: int) -> Tuple[float, Optional[str]]:
    """IP 조건부 Δprob = score([k]|IP) − score([]|IP). 대표 IP 고정 후 키워드만 토글.

    공룡류: 무조건 Δ≤0(임의제품엔 안 먹힘)이나, IP가 있으면 +면 '조건부 유발'. 반환 (Δcond, IP명)."""
    ip = _top_ip(eng, k_idx)
    if ip is None:
        return (np.nan, None)
    base = eng.score_concept([], ip_idx=[ip])
    withk = eng.score_concept([int(k_idx)], ip_idx=[ip])
    ip_name = eng.cache["maps"]["ip_ids"][ip]
    return (float(withk - base), ip_name)


def _killer_tier(delta: float, cond: float) -> str:
    if delta > NOISE:
        return "killer_확실"
    if delta >= -NOISE:
        return "killer_불명확(노이즈)"
    # delta < -NOISE = 무조건엔 안 먹힘 → IP 조건부로 구제 판정
    if not np.isnan(cond) and cond > NOISE:
        return "조건부killer(IP)"
    return "가짜(강등)"


def review_killers(eng: MDEngine, df_kw: pd.DataFrame, n_examples: int = 3) -> pd.DataFrame:
    """killer 후보 질적 리뷰표 — Δ·Δcond·tier·예시제품. (Δprob 수십초 + 의심키워드 조건부 추가)"""
    killers = df_kw.loc[df_kw.tag == "killer", "keyword"].tolist()
    ev = evidence_table(eng, killers).set_index("keyword")
    meta = df_kw.set_index("keyword")
    rows = []
    for kw in killers:
        k_idx = eng.seed_to_idx(kw)
        delta = float(ev.loc[kw, "delta_prob_mean"])
        cond, ip_name = (np.nan, None)
        if delta < -NOISE:                       # 의심 키워드만 조건부 계산 (비용 절약)
            cond, ip_name = conditional_delta(eng, k_idx)
        rows.append(dict(
            keyword=kw, purity=round(float(meta.loc[kw, "purity"]), 3),
            빈도=raw_freq(eng, k_idx),                       # 직접 has_kw 제품 수 (소표본 거품 식별)
            단독성공률=solo_success(eng, k_idx),            # 단독강도 = headroom 반비례 (시너지 ⟂ 이것)
            단독_wilson=solo_wilson(eng, k_idx),            # 빈도 보정 (소표본 페널티 → base 수축)
            woe=round(float(meta.loc[kw, "woe"]), 3), att_lift=round(float(meta.loc[kw, "att_lift"]), 2),
            supp_s=int(meta.loc[kw, "supp_s"]), supp_f=int(meta.loc[kw, "supp_f"]),
            delta=round(delta, 4), delta_pos=ev.loc[kw, "delta_prob_pos_rate"],
            delta_cond=round(cond, 4) if not np.isnan(cond) else None, cond_IP=ip_name,
            판정=_killer_tier(delta, cond), 예시제품=_examples(eng, k_idx, n_examples)))
    df = pd.DataFrame(rows).sort_values("delta", ascending=False).reset_index(drop=True)
    df["근거"] = df.apply(why_killer, axis=1)
    return df


def review_mine_hub(eng: MDEngine, df_kw: pd.DataFrame, tag: str, n_examples: int = 3) -> pd.DataFrame:
    """mine/hub 질적 리뷰. mine: Δ<0 이어야 진짜 악재 / hub: balance 작아야 진짜 일반어."""
    kws = df_kw.loc[df_kw.tag == tag, "keyword"].tolist()
    ev = evidence_table(eng, kws).set_index("keyword")
    meta = df_kw.set_index("keyword")
    rows = []
    for kw in kws:
        k_idx = eng.seed_to_idx(kw)
        delta = float(ev.loc[kw, "delta_prob_mean"])
        bal = float(ev.loc[kw, "balance"])
        if tag == "mine":
            verdict = "진짜 악재(Δ<0)" if delta < -NOISE else ("통계만(의심)" if delta > NOISE else "불명확")
        else:
            verdict = "진짜 일반어" if bal < 0.1 else "경계(편향)"
        rows.append(dict(keyword=kw, 빈도=raw_freq(eng, k_idx),
                         purity=round(float(meta.loc[kw, "purity"]), 3),
                         단독성공률=solo_success(eng, k_idx),
                         woe=round(float(meta.loc[kw, "woe"]), 3), balance=round(bal, 3),
                         delta=round(delta, 4), 판정=verdict, 예시제품=_examples(eng, k_idx, n_examples)))
    sort_col = "delta" if tag == "mine" else "balance"
    df = pd.DataFrame(rows).sort_values(sort_col).reset_index(drop=True)
    if tag == "mine":
        df["근거"] = df.apply(why_mine, axis=1)
    return df


def review_summary(killer_df: pd.DataFrame, mine_df: pd.DataFrame, hub_df: pd.DataFrame,
                   n_neutral: int) -> pd.DataFrame:
    """카테고리/판정별 개수 요약."""
    rows = []
    for name, df, col in [("killer", killer_df, "판정"), ("mine", mine_df, "판정"), ("hub", hub_df, "판정")]:
        for verdict, c in df[col].value_counts().items():
            rows.append(dict(카테고리=name, 판정=verdict, 개수=int(c)))
    rows.append(dict(카테고리="neutral", 판정="-", 개수=int(n_neutral)))
    return pd.DataFrame(rows)


def review_mediator(eng: MDEngine, df_kw: pd.DataFrame, n_headroom: int = 12,
                    t1_pos: float = 1.0, t1_delta: float = 0.04, t2_pos: float = 0.7, t2_delta: float = 0.005,
                    solo_max: float = 0.45, supp_min: int = 10, supp_max: int = 400, freq_floor: int = 5,
                    n_examples: int = 3, chunk: int = 48) -> pd.DataFrame:
    """매개/증폭 키워드 — 혼자 약한데(단독↓) 어디 붙여도 올림(delta_pos↑). killer/mine과 **직교 축**.

    배치 Δ(가상제품 동시평가)로 후보 풀의 delta_mean·delta_pos 1회 계산 후 필터.
    killer(성공특이) 아닌, 보편 강화제만. 메가허브(supp≥supp_max)는 제외(=사실상 hub).
    반환: keyword·단독성공률·연결수·delta_mean·delta_pos·purity·근거·예시제품.
    """
    from .subnet import _headroom_products
    meta = df_kw.set_index("keyword")
    cand_rows = df_kw[(~df_kw.tag.isin(["killer", "mine"])) &
                      (df_kw.supp_all >= supp_min) & (df_kw.supp_all < supp_max)]
    cands = []
    for kw in cand_rows.keyword:
        k = eng.seed_to_idx(kw)
        s = solo_success(eng, k)
        if not np.isnan(s) and s < solo_max and raw_freq(eng, k) >= freq_floor:   # 빈도 floor
            cands.append((kw, k, s))
    if not cands:
        return pd.DataFrame()
    sample = _headroom_products(eng, n_headroom)
    pk = {c: list(eng.product_keywords(c)) for c in sample}
    M = len(sample)
    base = eng.score_concept_batch([pk[c] for c in sample])
    concepts = [pk[c] + [k] for _, k, _ in cands for c in sample]
    scores = eng.score_concept_batch(concepts, chunk_size=chunk)
    rows = []
    for i, (kw, k, s) in enumerate(cands):
        d = np.array([scores[i * M + j] - base[j] for j in range(M)])
        dm, dp = float(d.mean()), float((d > 0).mean())
        if dp >= t2_pos and dm > t2_delta:                       # T2(완화) 통과
            grade = "T1" if (dp >= t1_pos and dm > t1_delta) else "T2"   # T1=무조건+ / T2=확장
            rows.append(dict(keyword=kw, 빈도=raw_freq(eng, k), 단독성공률=s,
                             연결수=int(meta.loc[kw, "supp_all"]),
                             delta_mean=round(dm, 4), delta_pos=round(dp, 2), grade=grade,
                             purity=round(float(meta.loc[kw, "purity"]), 3),
                             예시제품=_examples(eng, k, n_examples)))
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.sort_values(["delta_mean", "delta_pos"], ascending=False).reset_index(drop=True)
    df["근거"] = df.apply(why_mediator, axis=1)
    return df
