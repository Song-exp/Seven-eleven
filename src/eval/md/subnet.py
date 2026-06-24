"""서브네트워크 — combo_grow 레일 + 1-hop 타입별 문맥 + 2노드 패스/시너지/추천.

combo_mining_plan.md Step 2~3 구현. 원칙:
  · 깊이는 레일(combo_grow)이 책임 → 문맥은 얕게(1-hop, IP만 2-hop).
  · 패스는 구조(서브네트 엣지)로 찾고 score_concept로 재랭킹 / 시너지는 인과(headroom Δ)로 판정 — 분리.
  · 이종 엣지 가중치 융합 금지(타입별 분리·색).

메인:
  build_subnetwork(eng, seed)        레일 + IP/트렌드/바스켓 1-hop + IP 2-hop (+ anti)
  pair_synergy(eng, a, b)            두 노드 보완/대체 인과 판정 (headroom)
  recommend_within(eng, start, pool) 서브네트 내 start에 붙일 best 노드
  best_path(eng, a, b, subnet)       두 노드 구조 최단경로 → score_concept 재랭킹 + 끝점 시너지
  draw_subnetwork(subnet, ...)       matplotlib 인라인 시각화 (타입색)
"""
from __future__ import annotations

from collections import deque
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np

from .combo import _ConceptCache, combo_grow, keyword_support
from .engine import MDEngine, PK_MAIN
from . import tasks as T

IP_HASKW = ("ip", "has_kw", "keyword")
TREND = ("keyword", "trend_to", "keyword")
BASKET = ("keyword", "basket_comp", "keyword")
IPIP = ("ip", "has_ip", "ip")

# 대시보드 3색 계약 (dashboard.html COL3와 일치): 속성=초록 · 제품=빨강 · IP=주황.
# 서브넷 노드는 키워드 계열(rail/trend/basket/ip2/anti)=초록, ip=주황. 제품 노드는 서브넷에 없음.
# 트렌드는 색 대신 🔥 배지, 레일은 색 대신 굵은 테두리+굵은 엣지로 구분 (대시보드와 동일).
DASH_GREEN = "#16A34A"   # 키워드 계열(속성)
DASH_ORANGE = "#F59E0B"  # IP
DASH_RED = "#EF4444"     # 제품(서브넷 미사용, 계약 보존용)
DASH_GREY = "#9CA3AF"    # 모든 엣지 공통
KW_FAMILY = {"rail", "trend", "basket", "ip2", "anti"}


def _node_fill(ntype: str) -> str:
    return DASH_ORANGE if ntype == "ip" else (DASH_RED if ntype == "product" else DASH_GREEN)


def _pshort(name: str, n: int = 12) -> str:
    """제품명 짧게 — 브랜드 접두(`롯데)…`) 제거 + 길이 컷."""
    name = str(name)
    if ")" in name:
        tail = name.split(")")[-1].strip()
        if tail:
            name = tail
    return name[:n] + "…" if len(name) > n else name


def _att(eng: MDEngine, rel) -> Optional[np.ndarray]:
    return eng.cache["att"].get("__".join(rel))


def _top_kw_neighbors(eng: MDEngine, k: int, rel, topn: int) -> List[Tuple[int, float]]:
    """키워드 k의 keyword↔keyword 이웃(양방향) 상위 topn (어텐션 가중). 벡터화."""
    ei = eng.cache["eidx"][rel].numpy()
    att = _att(eng, rel)
    w = att if att is not None else np.ones(ei.shape[1])
    m1, m2 = ei[0] == k, ei[1] == k          # k가 src / k가 tgt
    nbr = np.concatenate([ei[1][m1], ei[0][m2]])
    ww = np.concatenate([w[m1], w[m2]])
    best: Dict[int, float] = {}
    for n_, w_ in zip(nbr.tolist(), ww.tolist()):
        if n_ == k:
            continue
        best[n_] = max(best.get(n_, 0.0), float(w_))
    return sorted(best.items(), key=lambda x: -x[1])[:topn]


def _top_ips(eng: MDEngine, k: int, topn: int) -> List[Tuple[int, float]]:
    """키워드 k를 가진 IP 상위 topn (ip__has_kw__keyword 어텐션)."""
    ei = eng.cache["eidx"][IP_HASKW].numpy()
    att = _att(eng, IP_HASKW)
    mask = ei[1] == k
    ips = ei[0][mask]
    w = att[mask] if att is not None else np.ones(mask.sum())
    order = np.argsort(-w)
    return [(int(ips[i]), float(w[i])) for i in order[:topn]]


def _ip_top_kw(eng: MDEngine, ip: int, topn: int) -> List[int]:
    """IP의 시그니처 키워드 상위 topn (IP 2-hop용)."""
    ei = eng.cache["eidx"][IP_HASKW].numpy()
    att = _att(eng, IP_HASKW)
    mask = ei[0] == ip
    kws = ei[1][mask]
    w = att[mask] if att is not None else np.ones(mask.sum())
    order = np.argsort(-w)
    return [int(kws[i]) for i in order[:topn]]


def _ip_kw_sets(eng: MDEngine) -> Dict[int, set]:
    """IP idx → 키워드 idx 집합 (공통 속성 IP 계산용). eng에 1회 캐시."""
    cache = getattr(eng, "_ip_kw_sets_cache", None)
    if cache is None:
        ei = eng.cache["eidx"][IP_HASKW].numpy()
        from collections import defaultdict
        cache = defaultdict(set)
        for i in range(ei.shape[1]):
            cache[int(ei[0, i])].add(int(ei[1, i]))
        eng._ip_kw_sets_cache = cache
    return cache


def _causal_kw_set(eng: MDEngine) -> set:
    """인과적으로 향상(killer/매개) 키워드 idx 집합. keyword_final.csv 기준, eng에 1회 캐시.
    generic 일반어(neutral)·악재(mine)는 제외 → '대체 IP' 브릿지로 의미있는 속성만."""
    cache = getattr(eng, "_causal_kw_cache", None)
    if cache is None:
        import os
        import pandas as pd
        cache = set()
        fp = "data/processed/hin/keyword_final.csv"
        if os.path.exists(fp):
            df = pd.read_csv(fp)
            for kw in df[df["tag"].isin(["killer", "매개"])]["keyword"]:
                i = eng.seed_to_idx(str(kw))
                if i is not None:
                    cache.add(i)
        eng._causal_kw_cache = cache
    return cache


def _ip_kw_att(eng: MDEngine, ip: int) -> Dict[int, float]:
    """IP의 키워드 idx → 어텐션 (브릿지 키워드 선택용)."""
    ei = eng.cache["eidx"][IP_HASKW].numpy()
    att = _att(eng, IP_HASKW)
    mask = ei[0] == ip
    kws = ei[1][mask]
    w = att[mask] if att is not None else np.ones(int(mask.sum()))
    return {int(kws[i]): float(w[i]) for i in range(len(kws))}


def _ip_df(eng: MDEngine) -> Dict[int, int]:
    """키워드 idx → 그 키워드를 가진 IP 수 (IDF 가중용). 모든 IP가 가진 범용어 식별."""
    cache = getattr(eng, "_ip_df_cache", None)
    if cache is None:
        from collections import Counter
        c = Counter()
        for kws in _ip_kw_sets(eng).values():
            for k in kws:
                c[k] += 1
        cache = dict(c)
        eng._ip_df_cache = cache
    return cache


def similar_ips(eng: MDEngine, ip: int, topn: int = 2, min_shared: int = 1) -> List[Tuple[int, int, list]]:
    """ip 대신 붙일만한 대체 IP 추천. 기준 = 공유 인과(killer/매개) 속성을 어텐션×IDF로 가중.

    · 인과 필터: killer/매개만(neutral 일반어·mine 제외).
    · 가중 w(k) = 어텐션(IP→k) ÷ log(IP_df+1)  → salient(어텐션↑) & distinctive(범용어↓).
    · 대체 IP는 Σw(공유속성) 내림차순, 브릿지 = w 최대 공유 속성(가장 변별력 있는 공통 성공축).
    반환 [(대체ip_idx, 브릿지키워드_idx, [공유 인과키워드 idx, w 내림차순])]."""
    causal = _causal_kw_set(eng)
    sets = _ip_kw_sets(eng)
    tgt = sets.get(ip, set()) & causal
    if not tgt:
        return []
    att = _ip_kw_att(eng, ip)
    df = _ip_df(eng)

    def w(k):
        return att.get(k, 0.0) / np.log(df.get(k, 1) + 1.0)

    scored = []
    for j, kws in sets.items():
        if j == ip:
            continue
        shared = tgt & kws
        if len(shared) >= min_shared:
            score = sum(w(k) for k in shared)
            bridge = max(shared, key=w)
            scored.append((score, j, bridge, sorted(shared, key=lambda k: -w(k))))
    scored.sort(key=lambda x: -x[0])
    return [(j, bridge, sh) for _, j, bridge, sh in scored[:topn]]


def build_subnetwork(eng: MDEngine, seed: str, eps: float = 0.02, max_hops: int = 5,
                     cand_pool: int = 28, n_headroom: int = 10,
                     top_ip: int = 2, top_trend: int = 2, top_basket: int = 3, ip_2hop: int = 2,
                     top_sim_ip: int = 2, sim_min_shared: int = 1,
                     show_anti: int = 3, sc: Optional[_ConceptCache] = None) -> dict:
    """레일(combo_grow) + 레일 노드별 1-hop 문맥(IP/트렌드/바스켓, 타입분리) + IP 2-hop + anti.

    반환: dict(seed, rail[키워드], grow, nodes[{id,type,label,...}], edges[{src,tgt,type,weight,label}]).
      노드 id 규약: 'kw:{idx}' / 'ip:{idx}'.
    cand_pool·n_headroom 은 combo_grow로 전달 — §3.7과 같은 값이면 SC 캐시 재사용(빠름).
    """
    sc = sc or _ConceptCache(eng)
    grow = combo_grow(eng, seed, eps=eps, max_hops=max_hops,
                      cand_pool=cand_pool, n_headroom=n_headroom, sc=sc)
    if grow.get("error"):
        return dict(seed=seed, error=grow["error"])
    rail_idx = [eng.seed_to_idx(n) for n in grow["rail"]]

    nodes: Dict[str, dict] = {}
    edges: List[dict] = []

    def add_node(nid, ntype, label, **kw):
        if nid not in nodes:
            nodes[nid] = dict(id=nid, type=ntype, label=label, **kw)

    # ── 척추(레일) ──
    for i, k in enumerate(rail_idx):
        add_node(f"kw:{k}", "rail" if i > 0 else "rail", eng.kw_name(k), hop=i, seed=(i == 0))
    succ_mask = eng.cache["y"] == 1
    for a in grow["accepted"]:
        ki = eng.seed_to_idx(a["keyword"])
        # 직전 레일 노드와 연결
        prev = rail_idx[rail_idx.index(ki) - 1]
        edges.append(dict(src=f"kw:{prev}", tgt=f"kw:{ki}", type="rail",
                          weight=a["margin"], label=f"{a['label']} Δ{a['margin']:+.3f}"))
        # K-P-K 경유 제품 (Top-1) — prev·ki를 실제 공유한 성공 제품을 중간 노드로 복원
        br = T.bridge_product(eng, prev, ki, mask=succ_mask)
        if br:
            p, _, pname = br
            add_node(f"p:{p}", "product", _pshort(pname))
            edges.append(dict(src=f"kw:{prev}", tgt=f"p:{p}", type="bridge", weight=1.0))
            edges.append(dict(src=f"p:{p}", tgt=f"kw:{ki}", type="bridge", weight=1.0))

    # ── 1-hop 문맥 (타입별) ──
    for k in rail_idx:
        for ip, w in _top_ips(eng, k, top_ip):
            ipname = eng.cache["maps"]["ip_ids"][ip]
            add_node(f"ip:{ip}", "ip", ipname)
            edges.append(dict(src=f"kw:{k}", tgt=f"ip:{ip}", type="ip", weight=round(w, 4)))
            for k2 in _ip_top_kw(eng, ip, ip_2hop):       # IP 2-hop (시그니처 키워드)
                if f"kw:{k2}" not in nodes:
                    add_node(f"kw:{k2}", "ip2", eng.kw_name(k2))
                edges.append(dict(src=f"ip:{ip}", tgt=f"kw:{k2}", type="ip2", weight=1.0))
        for kt, w in _top_kw_neighbors(eng, k, TREND, top_trend):
            add_node(f"kw:{kt}", "trend", eng.kw_name(kt))
            edges.append(dict(src=f"kw:{k}", tgt=f"kw:{kt}", type="trend", weight=round(w, 4)))
        for kb, w in _top_kw_neighbors(eng, k, BASKET, top_basket):
            add_node(f"kw:{kb}", "basket", eng.kw_name(kb))
            edges.append(dict(src=f"kw:{k}", tgt=f"kw:{kb}", type="basket", weight=round(w, 4)))

    # ── 대체 IP 추천: IP → 공통 인과속성(브릿지) → 대체 IP (속성을 노드로 노출) ──
    if top_sim_ip > 0:
        ip_in_net = sorted({int(nid.split(":")[1]) for nid in list(nodes)
                            if nodes[nid]["type"] == "ip"})
        seen_e = set()
        for ip in ip_in_net:
            for j, bridge, shared in similar_ips(eng, ip, topn=top_sim_ip, min_shared=sim_min_shared):
                bn, jn = f"kw:{bridge}", f"ip:{j}"
                if bn not in nodes:
                    add_node(bn, "ip2", eng.kw_name(bridge))      # 브릿지 속성 = 키워드 노드
                if jn not in nodes:
                    add_node(jn, "ip", eng.cache["maps"]["ip_ids"][j])
                if (f"ip:{ip}", bn) not in seen_e:
                    seen_e.add((f"ip:{ip}", bn))
                    edges.append(dict(src=f"ip:{ip}", tgt=bn, type="ip_attr", weight=1.0, label="핵심 공통 속성"))
                if (bn, jn) not in seen_e:
                    seen_e.add((bn, jn))
                    extra = "·".join(eng.kw_name(s) for s in shared[:3])
                    edges.append(dict(src=bn, tgt=jn, type="attr_ip", weight=float(len(shared)),
                                      label=f"이 속성 공유 → 대체 IP 후보 (공통: {extra})"))

    # ── anti (Bypass 잠식, 시드에 회색 점선) ──
    seen_anti = set()
    for b in grow["bypass"][:show_anti * max_hops]:
        if b["keyword"] in seen_anti:
            continue
        seen_anti.add(b["keyword"])
        ki = eng.seed_to_idx(b["keyword"])
        if ki is None or f"kw:{ki}" in nodes:
            continue
        add_node(f"kw:{ki}", "anti", b["keyword"], margin=b["margin"])
        edges.append(dict(src=f"kw:{rail_idx[0]}", tgt=f"kw:{ki}", type="anti",
                          weight=b["margin"], label=f"잠식 Δ{b['margin']:+.3f}"))
        if len(seen_anti) >= show_anti:
            break

    # ── kw-kw 엣지에 margin(절대 기여)·synergy(겹침) 부착 — margin=표시(단일목록과 동일 지표), syn=경로선택 ──
    KW_TYPES = {"rail", "trend", "basket", "anti", "ip2"}
    _pcache: Dict[tuple, Optional[dict]] = {}
    for e in edges:
        sn, tn = nodes.get(e["src"]), nodes.get(e["tgt"])
        if not sn or not tn or sn["type"] not in KW_TYPES or tn["type"] not in KW_TYPES:
            continue
        if sn["label"] == tn["label"]:
            continue
        dk = (sn["label"], tn["label"])             # 방향 보존: margin(tgt|src)
        if dk not in _pcache:
            r = pair_synergy(eng, sn["label"], tn["label"], sc=sc)
            _pcache[dk] = None if "error" in r else r
        r = _pcache[dk]
        if r is not None:
            e["syn"] = round(float(r["synergy"]), 4)               # 경로 선택용(겹침=음수)
            e["margin"] = round(float(r["margin_b_given_a"]), 4)   # 표시용(src에 tgt 더하면 Δ)

    return dict(seed=seed, rail=grow["rail"], grow=grow,
                nodes=list(nodes.values()), edges=edges)


# ──────────────────────────────────────────── 인과 프리미티브 (headroom)
def _headroom_products(eng: MDEngine, n: int, thr: float = 0.8, rng_seed: int = 0) -> List[int]:
    prob = eng.cache["prob"]
    pool = [int(p) for p in np.where(prob < thr)[0]]
    rng = np.random.default_rng(rng_seed)
    return rng.choice(pool, min(n, len(pool)), replace=False).tolist() if pool else []


def pair_synergy(eng: MDEngine, a: str, b: str, base_extra: Sequence[str] = (),
                 n_headroom: int = 15, sc: Optional[_ConceptCache] = None) -> dict:
    """두 노드 보완/대체 인과 판정. synergy = margin(b|base+a) − margin(b|base). >0 보완 / <0 잠식."""
    ai, bi = eng.seed_to_idx(a), eng.seed_to_idx(b)
    if ai is None or bi is None:
        return dict(a=a, b=b, error="노드 없음")
    sc = sc or _ConceptCache(eng)
    base = [eng.seed_to_idx(x) for x in base_extra if eng.seed_to_idx(x) is not None]
    sample = _headroom_products(eng, n_headroom)
    pk = {p: set(eng.product_keywords(p)) | set(base) for p in sample}
    sc.warm([pk[p] | extra for p in sample for extra in (set(), {bi}, {ai}, {ai, bi})])
    alone, with_a = [], []
    for p in sample:
        kp = pk[p]
        s0 = sc(frozenset(kp))
        alone.append(sc(frozenset(kp | {bi})) - s0)
        sa = sc(frozenset(kp | {ai}))
        with_a.append(sc(frozenset(kp | {ai, bi})) - sa)
    m_alone, m_with = float(np.mean(alone)), float(np.mean(with_a))
    syn = m_with - m_alone
    label = "✅보완(synergy>0)" if syn > 1e-4 else ("⚠대체/잠식" if syn < -1e-4 else "중립")
    return dict(a=a, b=b, synergy=round(syn, 4), margin_b_given_a=round(m_with, 4),
                margin_b_alone=round(m_alone, 4), label=label)


def pair_synergy_ip(eng: MDEngine, kw: str, ip_name: str, base_extra: Sequence[str] = (),
                    n_headroom: int = 15) -> dict:
    """키워드 × IP 인과 시너지. margin = 'IP 제품에 키워드 더하면 Δ', synergy = IP 증폭분(= margin|IP − margin|無IP).

    sc 캐시는 ip_idx를 키에 안 넣으므로 미사용(직접 score_concept). 헤드룸 제품 기준."""
    ki = eng.seed_to_idx(kw)
    ipids = eng.cache["maps"]["ip_ids"]
    ipx = {n: i for i, n in enumerate(ipids)}.get(ip_name)
    if ki is None or ipx is None:
        return dict(a=kw, b=ip_name, error="노드 없음")
    base = [eng.seed_to_idx(x) for x in base_extra if eng.seed_to_idx(x) is not None]
    sample = _headroom_products(eng, n_headroom)
    no_ip, with_ip = [], []
    for p in sample:
        kp = list(set(eng.product_keywords(p)) | set(base))
        no_ip.append(eng.score_concept(kp + [ki]) - eng.score_concept(kp))
        with_ip.append(eng.score_concept(kp + [ki], ip_idx=[ipx]) - eng.score_concept(kp, ip_idx=[ipx]))
    m_no, m_with = float(np.mean(no_ip)), float(np.mean(with_ip))
    syn = m_with - m_no
    label = "✅IP 증폭(synergy>0)" if syn > 1e-4 else ("⚠IP 잠식" if syn < -1e-4 else "IP 무영향")
    return dict(a=kw, b=ip_name, synergy=round(syn, 4), margin_b_given_a=round(m_with, 4),
                margin_b_alone=round(m_no, 4), label=label, kind="kw-ip")


def recommend_within(eng: MDEngine, start: str, pool: Sequence[str], n_headroom: int = 12,
                     top: int = 8, sc: Optional[_ConceptCache] = None) -> "object":
    """서브네트 노드 pool 중 start에 붙일 best — margin(C|{start}) headroom Δ 내림차순."""
    import pandas as pd
    si = eng.seed_to_idx(start)
    sc = sc or _ConceptCache(eng)
    sample = _headroom_products(eng, n_headroom)
    pk = {p: set(eng.product_keywords(p)) for p in sample}
    cand_idx = [(name, eng.seed_to_idx(name)) for name in dict.fromkeys(pool)]
    cand_idx = [(n, X) for n, X in cand_idx if X is not None and X != si]
    sc.warm([pk[p] | {si} for p in sample]
            + [pk[p] | {si, X} for _, X in cand_idx for p in sample])
    base_p = {p: sc(frozenset(pk[p] | {si})) for p in sample}
    rows = []
    for name, X in cand_idx:
        d = [sc(frozenset(pk[p] | {si, X})) - base_p[p] for p in sample]
        m = float(np.mean(d))
        rows.append(dict(keyword=name, margin=round(m, 4),
                         label=("✅보강" if m > 1e-4 else "⚠잠식" if m < -1e-4 else "중립")))
    if not rows:
        return pd.DataFrame(columns=["keyword", "margin", "label"])
    df = pd.DataFrame(rows).sort_values("margin", ascending=False).reset_index(drop=True)
    return df.head(top) if top else df


def synergy_within(eng: MDEngine, start: str, pool: Sequence[str], n_headroom: int = 12,
                   top: int = 8, sc: Optional[_ConceptCache] = None) -> "object":
    """pool 각 X의 start와의 **시너지** = margin(X|start) − margin(X|∅), headroom 평균.

    recommend_within(margin-given-start)과 달리 X 단독 리프트를 빼 seed-independent 증폭기를 제거 →
    시드별로 차별화된 보강어. pair_synergy(2노드)를 풀 전체로 배치화한 판(1 warm)."""
    import pandas as pd
    si = eng.seed_to_idx(start)
    sc = sc or _ConceptCache(eng)
    sample = _headroom_products(eng, n_headroom)
    pk = {p: set(eng.product_keywords(p)) for p in sample}
    cand_idx = [(name, eng.seed_to_idx(name)) for name in dict.fromkeys(pool)]
    cand_idx = [(n, X) for n, X in cand_idx if X is not None and X != si]
    sc.warm([pk[p] for p in sample]
            + [pk[p] | {si} for p in sample]
            + [pk[p] | {X} for _, X in cand_idx for p in sample]
            + [pk[p] | {si, X} for _, X in cand_idx for p in sample])
    base0 = {p: sc(frozenset(pk[p])) for p in sample}
    base_s = {p: sc(frozenset(pk[p] | {si})) for p in sample}
    rows = []
    for name, X in cand_idx:
        m_alone = float(np.mean([sc(frozenset(pk[p] | {X})) - base0[p] for p in sample]))
        m_with = float(np.mean([sc(frozenset(pk[p] | {si, X})) - base_s[p] for p in sample]))
        syn = m_with - m_alone
        rows.append(dict(keyword=name, synergy=round(syn, 4),
                         label=("✅시너지" if syn > 1e-4 else "⚠잠식" if syn < -1e-4 else "중립")))
    if not rows:
        return pd.DataFrame(columns=["keyword", "synergy", "label"])
    df = pd.DataFrame(rows).sort_values("synergy", ascending=False).reset_index(drop=True)
    return df.head(top) if top else df


# ──────────────────────────────────────────── 패스파인딩 (구조 + 재랭킹)
def _adjacency(subnet: dict) -> Dict[str, set]:
    adj: Dict[str, set] = {}
    for e in subnet["edges"]:
        adj.setdefault(e["src"], set()).add(e["tgt"])
        adj.setdefault(e["tgt"], set()).add(e["src"])
    return adj


def _bfs_paths(adj, src, dst, max_paths=6, max_len=6) -> List[List[str]]:
    """src→dst 단순경로 여러 개 (짧은 것 우선)."""
    out, q = [], deque([[src]])
    while q and len(out) < max_paths:
        path = q.popleft()
        if len(path) > max_len:
            continue
        last = path[-1]
        if last == dst:
            out.append(path)
            continue
        for nb in adj.get(last, ()):
            if nb not in path:
                q.append(path + [nb])
    return sorted(out, key=len)


def best_path(eng: MDEngine, a: str, b: str, subnet: dict, sc: Optional[_ConceptCache] = None) -> dict:
    """두 노드 구조 경로들을 찾아 score_concept으로 재랭킹 + 끝점 시너지. 없으면 글루 노드 제안."""
    sc = sc or _ConceptCache(eng)
    id_a, id_b = f"kw:{eng.seed_to_idx(a)}", f"kw:{eng.seed_to_idx(b)}"
    label_by_id = {n["id"]: n["label"] for n in subnet["nodes"]}
    type_by_id = {n["id"]: n["type"] for n in subnet["nodes"]}
    adj = _adjacency(subnet)
    if id_a not in adj or id_b not in adj:
        return dict(a=a, b=b, error="끝점이 서브네트에 없음")

    paths = _bfs_paths(adj, id_a, id_b)
    syn = pair_synergy(eng, a, b, sc=sc)
    if not paths:
        # 글루 노드 제안: argmax_C min(syn(a,C), syn(C,b))
        cands = [n["label"] for n in subnet["nodes"]
                 if n["type"] in ("rail", "trend", "basket") and n["label"] not in (a, b)]
        best_c, best_s = None, -9
        for c in cands[:15]:
            sa = pair_synergy(eng, a, c, sc=sc)["synergy"]
            sb = pair_synergy(eng, c, b, sc=sc)["synergy"]
            if min(sa, sb) > best_s:
                best_s, best_c = min(sa, sb), c
        return dict(a=a, b=b, path=None, synergy=syn,
                    glue=dict(node=best_c, min_synergy=round(best_s, 4)),
                    note="직접 경로 없음 → 글루 노드 끼워 연결 제안")

    # 각 경로의 키워드/IP를 score_concept으로 채점
    def score(path):
        kw = [int(i.split(":")[1]) for i in path if i.startswith("kw:")]
        ip = [int(i.split(":")[1]) for i in path if i.startswith("ip:")]
        return eng.score_concept(kw, ip)

    ranked = sorted(paths, key=lambda p: -score(p))
    top = ranked[0]
    return dict(a=a, b=b,
                path=[label_by_id.get(i, i) for i in top],
                path_types=[type_by_id.get(i) for i in top],
                path_score=round(score(top), 4),
                synergy=syn, n_paths=len(paths))


# ──────────────────────────────────────────── 시각화
def draw_subnetwork(subnet: dict, highlight_path: Optional[List[str]] = None,
                    figsize=(11, 8), seed_layout: int = 7, rail_w: float = 4.0):
    """matplotlib 인라인 — 대시보드 3색 계약 그대로 (EDA 화면 = MD 화면).

    · 노드색: 키워드 계열=초록 / IP=주황 (트렌드는 색 대신 🔥 배지).
    · 레일: 색이 아니라 굵은 테두리 + 굵은 회색 엣지(rail_w)로 표시 (대시보드와 동일).
    · 그 외 엣지: 전부 회색, 굵기는 가중치 비례. anti(잠식)는 회색 점선.
    · highlight_path(라벨 리스트) 주면 그 경로를 검게 강조.
    """
    import matplotlib.pyplot as plt
    import networkx as nx
    if subnet.get("error"):
        print(subnet["error"]); return
    nodes, edges = subnet["nodes"], subnet["edges"]
    G = nx.Graph()
    for n in nodes:
        G.add_node(n["id"], **n)
    for e in edges:
        G.add_edge(e["src"], e["tgt"], **e)
    pos = nx.spring_layout(G, seed=seed_layout, k=0.9)
    fig, ax = plt.subplots(figsize=figsize)

    # ── 엣지: 전부 회색. 레일=굵게(rail_w), anti=점선, bridge(경유제품)=점, 나머지=가중치 비례 ──
    wmax = max([abs(e.get("weight", 0.0)) for e in edges
                if e["type"] not in ("rail", "anti", "bridge")] or [1.0])
    for grp, style in (("rail", "solid"), ("anti", "dashed"), ("bridge", "dotted"), ("other", "solid")):
        el, ew = [], []
        for e in edges:
            cat = e["type"] if e["type"] in ("rail", "anti", "bridge") else "other"
            if cat != grp:
                continue
            el.append((e["src"], e["tgt"]))
            ew.append(rail_w if grp == "rail" else
                      1.2 if grp in ("anti", "bridge") else 1.0 + 2.5 * abs(e.get("weight", 0.0)) / wmax)
        if el:
            nx.draw_networkx_edges(G, pos, edgelist=el, edge_color=DASH_GREY, width=ew,
                                   style=style, ax=ax, alpha=0.55 if grp == "anti" else 0.85)

    # ── 노드: 초록(키워드)/주황(IP). seed=굵은 테두리, rail=중간, 그외 얇게 ──
    fills = [_node_fill(n["type"]) for n in nodes]
    sizes = [820 if n.get("seed") else (520 if n["type"] == "rail" else 320) for n in nodes]
    lws = [3.0 if n.get("seed") else (2.0 if n["type"] == "rail" else 1.0) for n in nodes]
    borders = ["#111" if (n.get("seed") or n["type"] == "rail") else "white" for n in nodes]
    nx.draw_networkx_nodes(G, pos, nodelist=[n["id"] for n in nodes], node_color=fills,
                           node_size=sizes, ax=ax, edgecolors=borders, linewidths=lws,
                           alpha=[0.55 if n["type"] == "anti" else 1.0 for n in nodes])

    if highlight_path:
        id_by_label = {n["label"]: n["id"] for n in nodes}
        hp = [id_by_label.get(x, x) for x in highlight_path]
        nx.draw_networkx_edges(G, pos, edgelist=list(zip(hp, hp[1:])), edge_color="#111",
                               width=3.5, ax=ax)

    # 라벨: 트렌드 노드엔 🔥 배지 (색 대신)
    labels = {n["id"]: (f"🔥{n['label']}" if n["type"] == "trend" else n["label"]) for n in nodes}
    nx.draw_networkx_labels(G, pos, labels=labels, font_size=9, font_family="Malgun Gothic", ax=ax)
    ax.set_title(f"[{subnet['seed']}] 서브네트워크  (초록=속성 · 주황=IP · 빨강=경유제품(K-P-K) · "
                 f"🔥=트렌드 · 굵은테두리/엣지=레일 · 점선=잠식 · 점=경유)")
    ax.axis("off"); plt.tight_layout(); plt.show()


def subnet_node_is_seed(subnet: dict, nid: str) -> bool:
    for n in subnet["nodes"]:
        if n["id"] == nid:
            return bool(n.get("seed"))
    return False
