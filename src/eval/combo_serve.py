"""조합 서브네트워크 라이브 서빙 — MDEngine(torch) 싱글톤 + 배치 가속.

serve.py(오프라인 parquet, torch-free)와 분리한다: combo는 인과 margin(`score_concept_batch`)을
계산해야 해서 모델 로드가 필수다. 그래서 MDEngine을 지연 로드(첫 요청 시 1회) 싱글톤으로 보유.

★ 단일 진실 소스: 배치 캐시(`scripts.export_combo_dashboard`)와 이 모듈이 **같은 `build_seed`**를
   호출 → 오프라인 `combo_data.js`와 라이브 `/combo` 응답이 byte 단위로 일치
   (serve.py 함수를 api.py·export_dashboard가 공유하는 것과 동일한 패턴).

배치 프리미티브(`engine.score_concept_batch`, drift=0·x40~77) 덕에 시드당 ~1-2s → 라이브 가능.
`combo_network`는 lru_cache로 같은 시드 재요청은 즉시(= 동적 + 자동 캐시).
"""
from __future__ import annotations

import itertools
from functools import lru_cache
from typing import List, Optional

# ── 서브네트 파라미터 (배치 캐시 export와 동일 값 — 캐시/라이브 일치 보장) ──
SERVING_MODEL = "v2_sweepA"
MAX_HOPS = 4
EPS = 0.02
CAND_POOL = 28
N_HEADROOM = 10
CORE_K = 8           # 시너지 행렬 대상 노드 수 (rail 우선 + 상위 문맥)
SYN_HEADROOM = 8

_ENG = None          # MDEngine 싱글톤
_SC = None           # _ConceptCache (세션 누적 → 시드 거듭할수록 빨라짐)


def _engine():
    """MDEngine + ConceptCache 지연 로드(첫 호출 1회). 이후 재사용."""
    global _ENG, _SC
    if _ENG is None:
        from src.eval.md.engine import MDEngine, EngineConfig
        from src.eval.md.combo import _ConceptCache
        cfg = EngineConfig.v2_sweepA() if SERVING_MODEL == "v2_sweepA" else EngineConfig.exp47()
        _ENG = MDEngine(cfg).run_single_inference()
        _ENG.build_mass()
        _SC = _ConceptCache(_ENG)
    return _ENG, _SC


def _core_nodes(net, k=CORE_K):
    rail = [n["label"] for n in net["nodes"] if n["type"] == "rail"]
    others = [n["label"] for n in net["nodes"] if n["type"] in ("trend", "basket", "ip2")]
    return list(dict.fromkeys(rail + others))[:k]


def build_seed(eng, seed, sc, max_hops=MAX_HOPS, eps=EPS, cand_pool=CAND_POOL,
               n_headroom=N_HEADROOM) -> Optional[dict]:
    """시드 1개 → 대시보드 페이로드(rail·accepted·bypass·nodes·edges·recommend·coreNodes·synergy).

    export_combo_dashboard.build_seed와 동일 — 여기가 단일 진실 소스(거기서 import).
    """
    from src.eval.md import subnet as SN
    net = SN.build_subnetwork(eng, seed, eps=eps, max_hops=max_hops,
                              cand_pool=cand_pool, n_headroom=n_headroom, sc=sc)
    if net.get("error"):
        return None
    # 전역 분류(1층) 라이브 태그를 키워드 노드에 부착 — killer/mine/매개/hub (A안, 엔진 1회 캐시)
    try:
        from src.eval.md.classify import classify_keywords_live
        gtags = classify_keywords_live(eng)
        for n in net["nodes"]:
            if n["type"] in ("rail", "trend", "basket", "ip2"):
                t = gtags.get(n["label"])
                if t:
                    n["gtag"] = t
    except Exception:
        pass
    # IP 추천: 어텐션 대신 인과 margin(pair_synergy_ip, seed 기준) — 클릭 시 궁합 패널과 동일 수치 = 일관
    for n in net["nodes"]:
        if n["type"] == "ip":
            try:
                r = SN.pair_synergy_ip(eng, seed, n["label"], n_headroom=SYN_HEADROOM)
                if "margin_b_given_a" in r:
                    n["ip_margin"] = round(float(r["margin_b_given_a"]), 4)
            except Exception:
                pass
    pool = [n["label"] for n in net["nodes"] if n["type"] in ("rail", "trend", "basket", "ip2")]
    rec = SN.recommend_within(eng, seed, pool, n_headroom=n_headroom, top=10, sc=sc)
    core = _core_nodes(net)
    syn = {}
    for a, b in itertools.combinations(core, 2):
        r = SN.pair_synergy(eng, a, b, n_headroom=SYN_HEADROOM, sc=sc)
        if "error" in r:
            continue
        syn[f"{a}|{b}"] = {"synergy": r["synergy"], "label": r["label"], "margin": r["margin_b_given_a"]}
    return dict(rail=net["rail"], accepted=net["grow"]["accepted"], bypass=net["grow"]["bypass"],
                nodes=net["nodes"], edges=net["edges"],
                recommend=rec.to_dict("records"), coreNodes=core, synergy=syn)


# ── 라이브 엔드포인트용 래퍼 (api.py가 호출) ─────────────────────────
@lru_cache(maxsize=256)
def combo_network(seed: str, max_hops: int = MAX_HOPS) -> dict:
    """시드 서브네트워크 페이로드(오프라인 combo_data.js와 동일 구조). 같은 시드 재요청은 캐시 즉시."""
    eng, sc = _engine()
    d = build_seed(eng, seed, sc, max_hops=max_hops)
    return d if d else {"error": f"'{seed}' 그래프에 없음"}


def combo_pair(a: str, b: str) -> dict:
    """두 노드 보완/대체 인과 판정. 키워드-키워드 / 키워드-IP 자동 분기."""
    eng, sc = _engine()
    from src.eval.md import subnet as SN
    aki, bki = eng.seed_to_idx(a), eng.seed_to_idx(b)
    ipset = set(eng.cache["maps"]["ip_ids"])
    if aki is not None and bki is not None:                 # 키워드 × 키워드
        return SN.pair_synergy(eng, a, b, n_headroom=SYN_HEADROOM, sc=sc)
    if aki is not None and b in ipset:                      # 키워드 × IP
        return SN.pair_synergy_ip(eng, a, b, n_headroom=SYN_HEADROOM)
    if bki is not None and a in ipset:                      # IP × 키워드
        return SN.pair_synergy_ip(eng, b, a, n_headroom=SYN_HEADROOM)
    return dict(a=a, b=b, error="키워드-키워드 또는 키워드-IP 조합만 지원")


def combo_recommend(seed: str, pool: List[str], top: int = 10) -> List[dict]:
    """서브네트 내 seed에 붙일 best 노드 (headroom Δ)."""
    eng, sc = _engine()
    from src.eval.md import subnet as SN
    df = SN.recommend_within(eng, seed, pool, n_headroom=N_HEADROOM, top=top, sc=sc)
    return df.to_dict("records")


def has_keyword(seed: str) -> bool:
    """시드가 그래프 어휘에 있는지 (프론트가 combo 가능 여부 판단)."""
    eng, _ = _engine()
    return eng.seed_to_idx(seed) is not None
