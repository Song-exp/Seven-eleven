"""키워드 조합 설명 드라이버 — Layer1(인과) × Layer2(4유형) 교차.

목표:
  1. 키워드 K의 메타패스+1hop 이웃을 자동 탐색하고 (build_subnetwork)
  2. 각 이웃을 prob 기반 시너지/카니발리제이션으로 판정하고 (pair_synergy = 인과)
  3. 사전 정의된 4유형(killer/mine/hub/neutral)과 교차해 *왜* 그런지 설명한다.

핵심 — 유형(구조) × 인과(sign) 교차표가 설명의 본체.
  유형과 인과가 **불일치하는 칸이 가장 정보량 큼** (예: killer인데 시너지≤0 = 상관만/후광).

유형 소스는 핫스왑(keyword_types.KeywordTypeSource): 지금은 초안 keyword_final.csv,
확정되면 같은 경로 재생성만으로 갈아끼워짐.
"""
from __future__ import annotations

from typing import List, Optional, Sequence

import numpy as np
import pandas as pd

from .engine import MDEngine
from .combo import _ConceptCache
from .subnet import build_subnetwork, pair_synergy
from .keyword_types import KeywordTypeSource, TYPE_KO

_EPS = 1e-4

# (유형, 인과부호) → (역할, 설명). 불일치 칸이 인사이트.
_CROSS = {
    ("killer", "pos"): "진짜 유발 — 성공 특이 속성이 인과적으로도 성공확률↑",
    ("killer", "neg"): "상관만/후광 — 성공제품에 붙어있을 뿐, 이 조합선 오히려 끌어내림",
    ("killer", "zero"): "성공 특이지만 이 시드엔 인과 효과 미미",
    ("mine", "pos"): "의외의 구원 — 실패 특이(지뢰)인데 이 조합에선 살림",
    ("mine", "neg"): "예상된 잠식 — 실패 특이 속성이 성공확률을 끌어내림",
    ("mine", "zero"): "지뢰지만 이 시드엔 영향 적음",
    ("hub", "pos"): "백본 보강 — 일반 속성이 약하게 받쳐줌",
    ("hub", "neg"): "희석 — 무색무취가 메시지를 흐림",
    ("hub", "zero"): "일반 백본 — 중립적",
    ("neutral", "pos"): "보조 — 약한 + 효과",
    ("neutral", "neg"): "보조 — 약한 − 효과",
    ("neutral", "zero"): "보조 — 인과 효과 거의 없음",
}


def _sign(syn: float) -> str:
    return "pos" if syn > _EPS else ("neg" if syn < -_EPS else "zero")


def _role(syn: float) -> str:
    return "보완(시너지)" if syn > _EPS else ("잠식(카니발)" if syn < -_EPS else "중립")


def _neighbors_from_subnet(eng: MDEngine, sn: dict) -> List[str]:
    """서브넷에서 키워드 이웃(시드·제품·IP 제외) 이름 목록 — 중복 제거."""
    out, seen = [], set()
    for n in sn.get("nodes", []):
        if not n["id"].startswith("kw:") or n.get("seed"):
            continue
        name = n["label"]
        if name in seen:
            continue
        seen.add(name)
        out.append(name)
    return out


def explain_keyword(eng: MDEngine, seed: str, types: Optional[KeywordTypeSource] = None,
                    n_headroom: int = 12, max_neighbors: int = 12,
                    sc: Optional[_ConceptCache] = None) -> dict:
    """키워드 seed의 조합 이웃을 인과×유형으로 설명.

    반환 dict(seed, type_source, table[DataFrame], text).
      table 컬럼: 이웃 | 유형 | 역할 | synergy | margin_alone | margin_given | 설명
    """
    types = types or KeywordTypeSource.default(eng)
    si = eng.seed_to_idx(seed)
    if si is None:
        return dict(seed=seed, error=f"'{seed}'는 그래프에 없음")

    sc = sc or _ConceptCache(eng)
    sn = build_subnetwork(eng, seed, sc=sc)
    if sn.get("error"):
        return dict(seed=seed, error=sn["error"])

    seed_tag = types.tag_name(seed)
    neighbors = _neighbors_from_subnet(eng, sn)[:max_neighbors]

    rows = []
    for nb in neighbors:
        ps = pair_synergy(eng, seed, nb, n_headroom=n_headroom, sc=sc)
        if ps.get("error"):
            continue
        syn = ps["synergy"]
        tag = types.tag_name(nb)
        role, expl = _role(syn), _CROSS[(tag, _sign(syn))]
        rows.append(dict(이웃=nb, 유형=TYPE_KO[tag], _tag=tag, 역할=role, synergy=syn,
                         margin_alone=ps["margin_b_alone"], margin_given=ps["margin_b_given_a"],
                         설명=expl))

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("synergy", ascending=False).reset_index(drop=True)

    text = _render(seed, seed_tag, types.source, df)
    return dict(seed=seed, seed_tag=seed_tag, type_source=types.source, table=df, text=text)


def _render(seed: str, seed_tag: str, source: str, df: pd.DataFrame) -> str:
    lines = [f"[{seed}] (유형: {TYPE_KO[seed_tag]})  —  유형소스={source}"]
    if df.empty:
        lines.append("  · 분석할 이웃 없음")
        return "\n".join(lines)
    syn = df[df.synergy > _EPS]
    can = df[df.synergy < -_EPS]
    if not syn.empty:
        lines.append("· 보완(시너지) 이웃:")
        for _, r in syn.head(6).iterrows():
            lines.append(f"   + {r['이웃']}({r['유형']}) Δ{r['synergy']:+.3f} → {r['설명']}")
    if not can.empty:
        lines.append("· 잠식(카니발) 이웃:")
        for _, r in can.head(6).iterrows():
            lines.append(f"   − {r['이웃']}({r['유형']}) Δ{r['synergy']:+.3f} → {r['설명']}")
    # 불일치 칸 하이라이트 (killer인데 잠식 / mine인데 보완)
    flags = df[((df._tag == "killer") & (df.synergy < -_EPS)) |
               ((df._tag == "mine") & (df.synergy > _EPS))]
    if not flags.empty:
        lines.append("· ⚠ 유형↔인과 불일치(주목):")
        for _, r in flags.iterrows():
            lines.append(f"   ! {r['이웃']}({r['유형']}) {r['역할']} → {r['설명']}")
    return "\n".join(lines)
