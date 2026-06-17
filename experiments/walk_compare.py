"""두 네트워크 버전 정성 비교 (methodB_network_walk_eda Phase F).

엣지 값 높은 경로(K-P-K)를 타고 조합을 생성했을 때 '좋은 네트워크'인지 평가.
서빙 백엔드(serve.py)는 건드리지 않고, 버전별 export 산출물을 직접 로드해 대조한다.

구성:
  [1] 전처리: 글로벌 허브 마스킹(deg 상위 pct%) + 킬러/지뢰 장부(success-freq score)
  [2] walk: bundle(일관 묶음) / path(고가중 단일 경로) — 허브·지뢰 제외
  [3] 지표: C1 일관성 / C2 구체성 / C3 성공지향(장부) / C4 트렌드 / C6 노이즈
            + Rule A(카테고리 앵커) / Rule B(base:trend 밸런스)
  [4] F1 샘플 대조 / F2 정량표 / F3 IP 경유 분석
"""
from __future__ import annotations

import os
from collections import defaultdict
from itertools import combinations

import numpy as np
import pandas as pd

RESULTS_ROOT = "experiments/results"
HIN_DIR = "data/processed/hin"
TREND_PATH = "data/processed/trend_keywords.parquet"

# 기본 무색무취 stopword (허브 마스킹과 별개로 항상 제외)
BASE_STOP = {"간식", "야식", "식사", "디저트", "간편", "음식", "음료", "과자", "밥", "먹거리"}

# 조건 A — 카테고리 앵커(1차 명사형 식품 유형/제형). 필요 시 편집.
ANCHOR_NOUNS = {
    "라면", "스낵", "젤리", "빵", "음료", "도시락", "김밥", "우유", "커피", "아이스크림",
    "초콜릿", "사탕", "쿠키", "케이크", "샌드위치", "버거", "피자", "만두", "떡", "시리얼",
    "요거트", "치즈", "소시지", "햄", "어묵", "죽", "면", "국수", "파스타", "차", "젤",
    "바", "칩", "팝콘", "푸딩", "무스", "타르트", "마카롱", "도넛", "와플", "호빵", "핫바",
    "삼각김밥", "샐러드", "디저트류", "주류", "맥주", "소주", "와인", "위스키", "탄산",
}


# ─────────────────────────────────────────────────────────────────────
# [전처리] 버전 로드 / 허브 마스킹 / 킬러·지뢰 장부
# ─────────────────────────────────────────────────────────────────────
def load_trend_kw() -> set:
    kn = pd.read_parquet(os.path.join(HIN_DIR, "keyword_nodes.parquet"))
    return set(kn.loc[kn["is_trend_keyword"], "keyword"])


def load_version(name: str) -> dict:
    rdir = os.path.join(RESULTS_ROOT, name)
    scores = pd.read_parquet(os.path.join(rdir, "learned_product_scores.parquet"))
    wedges = pd.read_parquet(os.path.join(rdir, "weighted_product_keyword_edges.parquet"))
    prod2kw, kw2prod, success = defaultdict(list), defaultdict(list), {}
    for prod, kw, att, sp in zip(wedges["product"], wedges["keyword"],
                                 wedges["attention"], wedges["product_success_prob"]):
        prod2kw[prod].append((kw, float(att)))
        kw2prod[kw].append((prod, float(att)))
        success[prod] = float(sp)
    deg = {k: len(v) for k, v in kw2prod.items()}
    kw_prodset = {k: set(p for p, _ in v) for k, v in kw2prod.items()}
    ytrue = dict(zip(scores["ITEM_NM"], scores["y_true"]))
    return dict(name=name, prod2kw=prod2kw, kw2prod=kw2prod, success=success,
                deg=deg, kw_prodset=kw_prodset, graph_kw=set(kw2prod), ytrue=ytrue)


def build_hub_mask(D: dict, pct: float = 5.0) -> set:
    """deg(연결 차수) 상위 pct% 토큰 = 무색무취 허브 → walk/매칭 선제외."""
    if not D["deg"]:
        return set()
    cut = np.percentile(list(D["deg"].values()), 100 - pct)
    return {k for k, dv in D["deg"].items() if dv >= cut}


def build_ledger(D: dict, top_pct: float = 10.0, min_support: int = 3):
    """킬러/지뢰 장부. score(k) = 성공제품 보유수 / 전체 보유수 (min_support 이상만)."""
    score = {}
    for k, prods in D["kw2prod"].items():
        tot = len(prods)
        if tot < min_support:
            continue
        succ = sum(D["ytrue"].get(p, 0) == 1 for p, _ in prods)
        score[k] = succ / tot
    ranked = sorted(score.items(), key=lambda x: -x[1])
    n = len(ranked)
    cut = max(1, int(n * top_pct / 100))
    killer = {k for k, _ in ranked[:cut]}
    landmine = {k for k, _ in ranked[-cut:]}
    return score, killer, landmine


# ─────────────────────────────────────────────────────────────────────
# [walk] 고가중 K-P-K — 허브/지뢰 제외
# ─────────────────────────────────────────────────────────────────────
def _kpk_cand(D, k, exclude, stop, min_support=3):
    """키워드 k에서 K-P-K 1스텝 후보 (att·success·att × deg = 성공가중 동시출현)."""
    deg = D["deg"]
    sc = defaultdict(float)
    for prod, att_in in D["kw2prod"].get(k, []):
        pj = D["success"].get(prod, 0.0)
        for kt, att_out in D["prod2kw"].get(prod, []):
            if kt in exclude or kt in stop:
                continue
            sc[kt] += att_in * pj * att_out
    return [(kt, s * deg[kt]) for kt, s in sc.items() if deg.get(kt, 0) >= min_support]


def walk_bundle(D, seeds, stop, max_size=6, stop_ratio=0.3, min_support=3):
    """① 일관 묶음 — 현재 묶음 전체와 가장 강하게 연결된 키워드를 하나씩 추가."""
    cur = list(dict.fromkeys(s for s in seeds if s in D["graph_kw"]))[:2]
    if not cur:
        return []
    first = None
    while len(cur) < max_size:
        curset = set(cur)
        cand = defaultdict(float)
        for ks in cur:
            for kt, v in _kpk_cand(D, ks, curset, stop, min_support):
                cand[kt] += v
        if not cand:
            break
        best, bs = max(cand.items(), key=lambda x: x[1])
        if first is None:
            first = bs
        elif bs < stop_ratio * first:
            break
        cur.append(best)
    return cur


def walk_path(D, seeds, stop, max_steps=3, min_support=3):
    """② 고가중 단일 경로 — 매 스텝 최강 엣지로 진행. (경로 키워드, 경유 제품) 반환."""
    seeds = [s for s in seeds if s in D["graph_kw"]]
    if not seeds:
        return [], []
    path = [seeds[0]]
    visited = set(seeds)
    prods_seen = []
    for _ in range(max_steps):
        cand = _kpk_cand(D, path[-1], visited, stop, min_support)
        if not cand:
            break
        nxt, _ = max(cand, key=lambda x: x[1])
        # 경유 제품(최강 K-P-K) 기록
        best_p, best_w = None, -1
        for prod, att_in in D["kw2prod"].get(path[-1], []):
            for kt, att_out in D["prod2kw"].get(prod, []):
                if kt == nxt and att_in * att_out > best_w:
                    best_w, best_p = att_in * att_out, prod
        if best_p:
            prods_seen.append(best_p)
        path.append(nxt)
        visited.add(nxt)
    return path, prods_seen


# ─────────────────────────────────────────────────────────────────────
# [지표] C1~C6 + Rule A/B
# ─────────────────────────────────────────────────────────────────────
def c1_coherence(combo, D):
    """조합 내 키워드쌍 평균 동시출현(공유 제품 수)."""
    ps = D["kw_prodset"]
    pairs = [len(ps.get(a, set()) & ps.get(b, set())) for a, b in combinations(combo, 2)]
    return float(np.mean(pairs)) if pairs else 0.0


def c2_specificity(combo, D, mask):
    """평균 제품빈도(deg, 낮을수록 구체적) + 무색무취(마스크) 비율."""
    if not combo:
        return 0.0, 0.0
    avg_deg = float(np.mean([D["deg"].get(k, 0) for k in combo]))
    hub_ratio = float(np.mean([k in mask for k in combo]))
    return avg_deg, hub_ratio


def c3_success(combo, prods_seen, killer, landmine, D):
    """성공지향: 킬러 비율↑ / 지뢰 비율↓ / 경유 제품 평균 success."""
    if not combo:
        return 0.0, 0.0, 0.0
    kill = float(np.mean([k in killer for k in combo]))
    land = float(np.mean([k in landmine for k in combo]))
    psucc = float(np.mean([D["success"].get(p, 0.0) for p in prods_seen])) if prods_seen else 0.0
    return kill, land, psucc


def c4_trend(combo, TREND):
    return float(np.mean([k in TREND for k in combo])) if combo else 0.0


def c6_noise(combo, landmine, D, rare_deg=2):
    """노이즈 프록시: 지뢰 ∪ 극희귀(deg≤rare_deg) 비율. (브랜드/IP 파편 근사)"""
    if not combo:
        return 0.0
    return float(np.mean([(k in landmine) or (D["deg"].get(k, 0) <= rare_deg) for k in combo]))


def rule_a_anchor(combo):
    """조건 A: 카테고리 앵커(1차 명사형) 최소 1개 포함?"""
    return any(k in ANCHOR_NOUNS for k in combo)


def rule_b_balance(combo, TREND):
    """조건 B: 트렌드 비율 30~40%(base:trend 7:3~6:4) 충족?"""
    if not combo:
        return False, 0.0
    tr = np.mean([k in TREND for k in combo])
    return (0.30 <= tr <= 0.40), float(tr)


# ─────────────────────────────────────────────────────────────────────
# [F1] 샘플 대조 / [F2] 정량표 / [F3] IP 분석
# ─────────────────────────────────────────────────────────────────────
def seed_to_attrs(D, seed, trend_attrs):
    if seed in D["graph_kw"]:
        return [seed]
    return [a for a in trend_attrs.get(seed, []) if a in D["graph_kw"]][:4]


def f1_side_by_side(versions, seeds, trend_attrs, max_steps=3):
    """F1 — 시드별 두 버전 bundle/path 좌우 대조 DataFrame."""
    rows = []
    for seed in seeds:
        row = {"시드": seed}
        for tag, (D, mask) in versions.items():
            attrs = seed_to_attrs(D, seed, trend_attrs)
            if not attrs:
                row[f"[{tag}] 묶음"] = "(시드 부재)"
                row[f"[{tag}] 경로"] = ""
                continue
            bundle = walk_bundle(D, attrs, mask)
            path, _ = walk_path(D, attrs, mask, max_steps=max_steps)
            row[f"[{tag}] 묶음"] = " · ".join(bundle)
            row[f"[{tag}] 경로"] = " → ".join(path)
        rows.append(row)
    return pd.DataFrame(rows)


def f2_quant_table(versions, seeds, trend_attrs, TREND, ledgers, max_steps=3):
    """F2 — C1~C6 + Rule A/B 시드 평균, 버전별 컬럼."""
    out = {}
    for tag, (D, mask) in versions.items():
        _, killer, landmine = ledgers[tag]
        agg = defaultdict(list)
        for seed in seeds:
            attrs = seed_to_attrs(D, seed, trend_attrs)
            if not attrs:
                continue
            combo = walk_bundle(D, attrs, mask)
            _, prods = walk_path(D, attrs, mask, max_steps=max_steps)
            if not combo:
                continue
            agg["C1_일관성"].append(c1_coherence(combo, D))
            ad, hr = c2_specificity(combo, D, mask)
            agg["C2_평균deg"].append(ad)
            agg["C2_허브비율"].append(hr)
            kill, land, ps = c3_success(combo, prods, killer, landmine, D)
            agg["C3_킬러비율"].append(kill)
            agg["C3_지뢰비율"].append(land)
            agg["C3_경유성공도"].append(ps)
            agg["C4_트렌드비율"].append(c4_trend(combo, TREND))
            agg["C6_노이즈비율"].append(c6_noise(combo, landmine, D))
            agg["RuleA_앵커충족"].append(rule_a_anchor(combo))
            agg["RuleB_밸런스충족"].append(rule_b_balance(combo, TREND)[0])
        out[tag] = {k: round(float(np.mean(v)), 3) if v else None for k, v in agg.items()}
    return pd.DataFrame(out)


def f3_ip_analysis(versions, ip_seed_products, trend_attrs, TREND, ledgers, max_steps=3, n_sample=300):
    """F3 — IP 보유 제품을 시드로 walk → 조합 다양성·품질 대조.

    ip_seed_products: IP 보유 제품명 리스트. 각 제품의 키워드를 시드로 사용.
    다양성 = 생성 조합의 고유 키워드 수 / 총 키워드 수 (반복 출력일수록 낮음).
    """
    rng = np.random.default_rng(42)
    sample = ip_seed_products if len(ip_seed_products) <= n_sample else \
        list(rng.choice(ip_seed_products, n_sample, replace=False))
    rows = {}
    for tag, (D, mask) in versions.items():
        _, killer, landmine = ledgers[tag]
        all_kw, combos, kill_r, land_r, coh = [], 0, [], [], []
        for prod in sample:
            seedkws = [k for k, _ in sorted(D["prod2kw"].get(prod, []), key=lambda x: -x[1])
                       if k not in mask][:2]
            if not seedkws:
                continue
            combo = walk_bundle(D, seedkws, mask)
            if len(combo) < 2:
                continue
            combos += 1
            all_kw += combo
            kill_r.append(np.mean([k in killer for k in combo]))
            land_r.append(np.mean([k in landmine for k in combo]))
            coh.append(c1_coherence(combo, D))
        diversity = len(set(all_kw)) / max(len(all_kw), 1)
        rows[tag] = {
            "생성 조합수": combos,
            "고유키워드 다양성": round(diversity, 3),
            "평균 킬러비율": round(float(np.mean(kill_r)), 3) if kill_r else None,
            "평균 지뢰비율": round(float(np.mean(land_r)), 3) if land_r else None,
            "평균 일관성(C1)": round(float(np.mean(coh)), 1) if coh else None,
        }
    return pd.DataFrame(rows)
