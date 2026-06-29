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
    # 전역 분류(1층) 태그 부착 — killer/mine/매개/hub.
    # ★ 논블로킹: 분류 Δprob 배치는 무거워서 요청 경로에서 계산하지 않는다(첫 /combo 타임아웃 방지).
    #   api.py warmup이 백그라운드에서 미리 캐시(eng._live_tags)해 두고, 여기선 준비됐을 때만 붙인다.
    cached = getattr(eng, "_live_tags", None)
    gtags = cached[1] if cached else {}
    # 채널 태그(인스타/POS/범용) — classify_channel_live 로드 1회 캐시. gtag와 동일하게 논블로킹 부착.
    cch = getattr(eng, "_channel_tags", None)
    ctags = cch[1] if cch else {}
    if gtags or ctags:
        for n in net["nodes"]:
            if n["type"] in ("rail", "trend", "basket", "ip2"):
                t = gtags.get(n["label"])
                if t:
                    n["gtag"] = t
                c = ctags.get(n["label"])
                if c:
                    n["ctag"] = c["ctag"]
                    n["ctag_low"] = c["low"]
    # IP 노드 태그(역할 killer/매개/일반 + 채널 인스타/POS/범용) — classify_ips_live 로드 1회 캐시.
    iptags = getattr(eng, "_ip_tags", None)
    iptags = iptags[1] if iptags else {}
    if iptags:
        for n in net["nodes"]:
            if n["type"] == "ip":
                c = iptags.get(n["label"])
                if c:
                    if c.get("gtag"):
                        n["gtag"] = c["gtag"]
                    if c.get("ctag"):
                        n["ctag"] = c["ctag"]
                        n["ctag_low"] = c["low"]
    # ── IP 관련성 필터 (구조 기준) ──
    #   rail 키워드가 '직접 보유'(ip 엣지: kw→ip)한 IP만 유지. '대체 IP'(attr_ip)는 제네릭 속성
    #   (예: '맛') 브릿지로 무관 IP(디저트망에 디진다돈까스)를 끌어와 노이즈 → 제거.
    #   ※ IP 주입 Δprob은 거의 항상 양수(예: 두바이+디진다돈까스 margin +0.0066)라 margin>0로는
    #     무관 IP가 안 걸러짐 → 인과 대신 '직접 연결' 구조로 판별.
    from src.eval.md.classify import IP_DROP_SUBSTR   # 경동나비엔·APEC 등 제거 대상(렌더·분류와 동일)
    direct_ip = {e["tgt"] for e in net["edges"] if e.get("type") == "ip"}
    drop_ip = {n["id"] for n in net["nodes"] if n["type"] == "ip"
               and (n["id"] not in direct_ip                                    # 대체IP(attr_ip) 노이즈
                    or any(s in str(n.get("label", "")) for s in IP_DROP_SUBSTR))}  # 명시 제거 IP
    if drop_ip:
        net["nodes"] = [n for n in net["nodes"] if n["id"] not in drop_ip]
        net["edges"] = [e for e in net["edges"] if e["src"] not in drop_ip and e["tgt"] not in drop_ip]
        connected = {e["src"] for e in net["edges"]} | {e["tgt"] for e in net["edges"]}
        net["nodes"] = [n for n in net["nodes"]    # 드롭된 IP에만 매달렸던 시그니처(ip2) 고아 제거
                        if n["type"] != "ip2" or n["id"] in connected]
    # 남은(직접연결) IP의 인과 margin — 클릭 궁합·"잘 어울리는 IP" 랭킹용.
    ip_w = {}
    for e in net["edges"]:
        if e.get("type") == "ip":
            ip_w[e["tgt"]] = max(ip_w.get(e["tgt"], 0.0), e.get("weight", 0.0))
    ip_nodes = sorted((n for n in net["nodes"] if n["type"] == "ip"),
                      key=lambda n: -ip_w.get(n["id"], 0.0))
    for n in ip_nodes[:8]:
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
