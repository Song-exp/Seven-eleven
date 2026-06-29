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

from .engine import MDEngine, PK_MAIN
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


# ────────────────────────────────────────────────────────────────────
# 채널 적합도 (인스타 vs POS) — docs/channel_fit_methodology_and_results.md
# gtag(killer/mine)와 동일 패턴: 로드 시 1회 전역 산출 → 캐시 → combo가 노드에 부착.
# ────────────────────────────────────────────────────────────────────
INSTA_SRC = ("인스타", "CU_인스타", "GS25_인스타")


def classify_channel_live(eng: MDEngine, pos_floor: int = 3, insta_floor: int = 1) -> Dict[str, dict]:
    """전 키워드 → 채널 태그 {keyword: {"ctag": POS형/인스타형/범용, "low": 저신뢰}}.

    배타 성공군(POS 단독 vs 인스타 단독)에서 ablation Δ를 채널별로 재서, 부호 게이트(Δ>+0.01)+margin으로 분류.
    드래그/효과미미/미정 키워드는 태그를 부여하지 않는다(=채널 안내 없음). 결과는 eng._channel_tags 캐시.
    """
    key = (pos_floor, insta_floor)
    cached = getattr(eng, "_channel_tags", None)
    if cached is not None and cached[0] == key:
        return cached[1]

    y = eng.cache["y"]
    src = eng.cache["succ_src"]
    pos_idx = np.where((y == 1) & (src == "POS"))[0]                       # POS 단독성공
    insta_idx = np.where((y == 1) & np.isin(src, list(INSTA_SRC)))[0]     # 인스타 단독성공

    ei = eng.cache["eidx"][PK_MAIN].numpy()
    K = eng.cache["K"]

    def _holder_count(idx):
        m = np.isin(ei[0], idx)
        return np.bincount(ei[1][m], minlength=K)

    total_cnt = np.bincount(ei[1], minlength=K)
    pos_n = _holder_count(pos_idx)
    insta_n = _holder_count(insta_idx)
    cand = {int(k) for k in range(K)
            if total_cnt[k] >= 3 and (pos_n[k] >= pos_floor or insta_n[k] >= insta_floor)}
    if not cand:
        eng._channel_tags = (key, {})
        return {}

    HASIP = ("product", "has_ip", "ip")
    have_ip = HASIP in eng.cache["eidx"]
    union = sorted(set(pos_idx.tolist()) | set(insta_idx.tolist()))
    pk = {p: eng.product_keywords(p) for p in union}
    ip = {p: (eng.product_keywords(p, HASIP) if have_ip else []) for p in union}

    def _channel_delta(idx):
        """채널 성공작에서 보유 후보 키워드를 빼본 contrib 평균. {k_idx: Δ}."""
        from collections import defaultdict
        acc = defaultdict(list)
        for p in idx:
            p = int(p)
            kws, ips = pk[p], ip[p]
            targets = [k for k in kws if k in cand]
            if not targets:
                continue
            concepts = [(kws, ips)] + [([x for x in kws if x != k], ips) for k in targets]
            s = eng.score_concept_batch(concepts, chunk_size=len(concepts))  # full/ablate 동일 청크 → 상쇄
            base = s[0]
            for i, k in enumerate(targets):
                acc[k].append(float(base - s[i + 1]))
        return {k: float(np.mean(v)) for k, v in acc.items()}

    d_pos = _channel_delta(pos_idx)
    d_insta = _channel_delta(insta_idx)

    tags: Dict[str, dict] = {}
    for k in cand:
        dp = d_pos.get(k, np.nan)
        di = d_insta.get(k, np.nan)
        pos_drive = (pos_n[k] >= pos_floor) and (not np.isnan(dp)) and (dp > NOISE)
        ins_drive = (insta_n[k] >= insta_floor) and (not np.isnan(di)) and (di > NOISE)
        if pos_drive and ins_drive:
            m = di - dp
            ctag = "인스타형" if m > NOISE else ("POS형" if m < -NOISE else "범용")
        elif ins_drive:
            ctag = "인스타형"
        elif pos_drive:
            ctag = "POS형"
        else:
            continue   # 드래그(Δ<0)/효과미미/미정 → 채널 태그 없음
        tags[eng.kw_name(k)] = {"ctag": ctag, "low": bool(ctag == "인스타형" and insta_n[k] < 3)}

    eng._channel_tags = (key, tags)
    return tags


# ────────────────────────────────────────────────────────────────────
# IP 분류 — 키워드와 동일 체계(역할 killer/매개/일반 + 채널 인스타/POS/범용)를 IP로 이식.
# 데이터: product→IP(has_ip) 멤버십이 키워드의 has_kw 역할. 개입: score_concept가 ip_idx 네이티브 지원.
# IP는 제품당 희소 → support_floor=1 (키워드 3 → IP 1로 완화).
# ────────────────────────────────────────────────────────────────────
HASIP = ("product", "has_ip", "ip")
# 대시보드 NODE_DROP_SUBSTR과 동일 — HIN(ip_nodes) 미재빌드라 분류 단계에서 런타임 제외.
# (정식 제거는 patch_ip_keywords_all.DELETE_IPS 추가 + HIN 재빌드 후 재export)
IP_DROP_SUBSTR = ("경동나비엔", "APEC정상회의")


def classify_ips_live(eng: MDEngine, support_floor: int = 1, n_headroom: int = 12,
                      chunk: int = 48) -> Dict[str, dict]:
    """전 IP → {ip_name: {"gtag": killer/mine/매개/hub, "ctag": 인스타형/POS형/범용/None, "low": bool}}.

    역할: 관찰(has_ip 성공/실패 보유율 → 성공특이도) + 인과(헤드룸 캐리어에 IP 주입 시 Δprob).
    채널: 배타 성공군(POS단독·인스타단독)에서 IP ablation Δ → 부호게이트+margin (키워드 채널핏과 동일).
    결과는 eng._ip_tags 캐시. has_ip 엣지 없으면 빈 dict.
    """
    key = (support_floor, n_headroom)
    cached = getattr(eng, "_ip_tags", None)
    if cached is not None and cached[0] == key:
        return cached[1]
    if HASIP not in eng.cache["eidx"]:
        eng._ip_tags = (key, {})
        return {}

    y = eng.cache["y"]
    srcv = eng.cache["succ_src"]
    I = eng.cache["I"]
    ip_names = eng.cache["maps"]["ip_ids"]
    ei = eng.cache["eidx"][HASIP].numpy()                  # [0]=product, [1]=ip

    def _hold(idx):
        m = np.isin(ei[0], idx)
        return np.bincount(ei[1][m], minlength=I)

    succ_idx = np.where(y == 1)[0]
    fail_idx = np.where(y == 0)[0]
    pos_idx = np.where((y == 1) & (srcv == "POS"))[0]
    insta_idx = np.where((y == 1) & np.isin(srcv, list(INSTA_SRC)))[0]
    supp_succ, supp_fail = _hold(succ_idx), _hold(fail_idx)
    pos_n, insta_n = _hold(pos_idx), _hold(insta_idx)
    total = supp_succ + supp_fail
    n_succ = max(int((y == 1).sum()), 1)
    n_fail = max(int((y == 0).sum()), 1)

    cand = [i for i in range(I) if total[i] >= support_floor
            and not any(s in str(ip_names[i]) for s in IP_DROP_SUBSTR)]
    if not cand:
        eng._ip_tags = (key, {})
        return {}
    cand_set = set(cand)

    # ── 역할 인과: 헤드룸 캐리어에 IP 주입 시 평균 Δprob (키워드 _batch_delta의 IP판) ──
    from .subnet import _headroom_products
    sample = _headroom_products(eng, n_headroom)
    pk = {c: list(eng.product_keywords(c)) for c in sample}
    cip = {c: list(eng.product_keywords(c, HASIP)) for c in sample}
    M = len(sample)
    base = eng.score_concept_batch([(pk[c], cip[c]) for c in sample])
    add = eng.score_concept_batch([(pk[c], cip[c] + [i]) for i in cand for c in sample], chunk_size=chunk)
    head_delta = {i: float(np.mean([add[j * M + t] - base[t] for t in range(M)]))
                  for j, i in enumerate(cand)}

    # ── 채널 인과: 배타 성공군에서 IP ablation Δ (키워드 _channel_delta의 IP판) ──
    def _ip_channel_delta(idx):
        from collections import defaultdict
        acc = defaultdict(list)
        for p in idx:
            p = int(p)
            kws = eng.product_keywords(p)
            ips = list(eng.product_keywords(p, HASIP))
            targets = [i for i in ips if i in cand_set]
            if not targets:
                continue
            concepts = [(kws, ips)] + [(kws, [x for x in ips if x != i]) for i in targets]
            s = eng.score_concept_batch(concepts, chunk_size=len(concepts))
            b = s[0]
            for t, i in enumerate(targets):
                acc[i].append(float(b - s[t + 1]))
        return {i: float(np.mean(v)) for i, v in acc.items()}

    d_pos = _ip_channel_delta(pos_idx)
    d_insta = _ip_channel_delta(insta_idx)

    tags: Dict[str, dict] = {}
    for i in cand:
        name = ip_names[i]
        succ_rate = supp_succ[i] / total[i] if total[i] else 0.0
        dprob = head_delta.get(i, 0.0)
        # 역할(gtag): killer=성공특이∧유발 / 매개=비특이인데 보편유발 / mine=실패특이∧무유발 / hub=일반
        if succ_rate >= 0.50 and dprob > NOISE:
            gtag = "killer"
        elif dprob > NOISE:
            gtag = "매개"
        elif succ_rate <= 0.15 and dprob < NOISE and supp_fail[i] >= 2:
            gtag = "mine"          # 싱글톤 실패는 제외 — 반복 실패(≥2) 증거 있을 때만 주의
        else:
            gtag = "hub"
        # 채널(ctag): 부호게이트(Δ>+noise)+margin
        dp, di = d_pos.get(i, np.nan), d_insta.get(i, np.nan)
        pos_drive = (pos_n[i] >= support_floor) and (not np.isnan(dp)) and (dp > NOISE)
        ins_drive = (insta_n[i] >= support_floor) and (not np.isnan(di)) and (di > NOISE)
        if pos_drive and ins_drive:
            m = di - dp
            ctag = "인스타형" if m > NOISE else ("POS형" if m < -NOISE else "범용")
        elif ins_drive:
            ctag = "인스타형"
        elif pos_drive:
            ctag = "POS형"
        else:
            ctag = None
        tags[name] = {"gtag": gtag, "ctag": ctag,
                      "low": bool(ctag == "인스타형" and insta_n[i] < 3),
                      # 근거 수치 (보고서·디버그용; combo_serve는 gtag/ctag/low만 사용)
                      "supp_succ": int(supp_succ[i]), "supp_fail": int(supp_fail[i]),
                      "succ_rate": round(succ_rate, 3), "dprob": round(dprob, 4),
                      "d_pos": (None if np.isnan(dp) else round(dp, 4)),
                      "d_insta": (None if np.isnan(di) else round(di, 4)),
                      "pos_n": int(pos_n[i]), "insta_n": int(insta_n[i])}

    eng._ip_tags = (key, tags)
    return tags
