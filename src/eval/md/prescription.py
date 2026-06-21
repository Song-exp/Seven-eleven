"""MD 처방 엔진 — §5-1~5-7 (장바구니·A 빔·B anti·C 포화신흥·D·E·F).

설계: docs/eda_channel_prescription_plan.md §5.
바인딩: MDEngine(캐시·장부·개입 머신) + tasks(차분 행렬) + 장바구니 raw CSV.
"""
from __future__ import annotations

import os
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import torch

from .engine import MDEngine, PK_MAIN, _rk, norm_id
from . import tasks as T


@dataclass
class Prescription:
    seed: str
    resolved: str
    verdict: str                 # 승인 / 반려 / 중립
    saturation: str              # 블루오션 / 레드오션 / 중립
    pos_partners: List[tuple]    # 내실화 (제품 속성)
    insta_partners: List[tuple]  # 카피 (마케팅)
    anti_partners: List[tuple]   # 피해야 할 조합
    basket: List[tuple]          # 장바구니 보완재 제품
    revive: List[tuple]          # 실패 컨셉 소생 키워드
    text: str
    confidence: str


class MDPrescriptionEngine:
    """처방 파이프라인. 노트북에서 engine·diff 행렬 바인딩 후 get(seed) 호출."""

    def __init__(self, eng: MDEngine, g2: "T.ChannelDiff", universe: str = "full",
                 data_dir: str = "data/processed", hin_dir: str = "data/processed/hin"):
        self.eng = eng
        self.lg = eng.ledger.get(universe) or eng.build_ledger(universe)
        self.A_diff_pos = g2.A_diff_pos
        self.A_diff_insta = g2.A_diff_insta
        self._build_metapath_index()
        self._build_saturation(hin_dir)
        self._load_basket(data_dir)
        self._kw_emb = eng.model.keyword_emb.weight.detach().cpu().numpy()

    # ------------------------------------------------ (A) 메타패스 빔 인덱스
    def _build_metapath_index(self):
        ei = self.eng.cache["eidx"][PK_MAIN].numpy()
        a = self.eng.cache["att"][_rk(PK_MAIN)]
        self.prod2kw = defaultdict(list); self.kw2prod = defaultdict(list)
        for e in range(ei.shape[1]):
            j, k, w = int(ei[0, e]), int(ei[1, e]), float(a[e])
            self.prod2kw[j].append((k, w)); self.kw2prod[k].append((j, w))

    def partner_beam(self, seed_idx: int, top_k: int = 20) -> List[Tuple[int, float]]:
        """A: score(kt|ks)=Σ_j att(j,ks)·p_success(j)·att(j,kt)  (recommend_combinations 통일)."""
        p = self.eng.cache["prob"]
        score = defaultdict(float)
        for j, att_in in self.kw2prod.get(seed_idx, []):
            pj = float(p[j])
            for kt, att_out in self.prod2kw.get(j, []):
                if kt == seed_idx:
                    continue
                score[kt] += att_in * pj * att_out
        return sorted(score.items(), key=lambda x: -x[1])[:top_k]

    # ------------------------------------------------ (C) 포화 vs 신흥
    def _build_saturation(self, hin_dir):
        pn = pd.read_parquet(os.path.join(hin_dir, "product_nodes_final.parquet"))
        first = pd.to_datetime(pn["첫_등장일"], errors="coerce")
        ei = self.eng.cache["eidx"][PK_MAIN].numpy()
        K = self.eng.cache["K"]
        # 키워드 최초 등장일 = min(보유 제품 첫_등장일), 누적 제품수 = 보유 제품수
        firstday = np.full(K, np.datetime64("NaT"), dtype="datetime64[ns]")
        cumcnt = np.bincount(ei[1], minlength=K)
        fd = first.to_numpy()
        for k in range(K):
            ps = ei[0][ei[1] == k]
            if len(ps):
                vals = fd[ps]; vals = vals[~np.isnat(vals)]
                if len(vals):
                    firstday[k] = vals.min()
        self.kw_firstday = firstday
        self.kw_cumcnt = cumcnt
        self._cnt_hi = np.quantile(cumcnt[cumcnt > 0], 0.75) if (cumcnt > 0).any() else 0

    def saturation_flag(self, k_idx: int) -> str:
        cnt = self.kw_cumcnt[k_idx]
        fd = self.kw_firstday[k_idx]
        recent = (not np.isnat(fd)) and fd >= np.datetime64("2025-09-01")
        if cnt >= self._cnt_hi and not recent:
            return "레드오션(포화·자기잠식 위험)"
        if recent and cnt < self._cnt_hi:
            return "블루오션(선점 기회)"
        return "중립"

    # ------------------------------------------------ (장바구니) 보완재
    def _load_basket(self, data_dir):
        self.basket = {}
        p2name = {norm_id(c): n for c, n in zip(self.eng.cache["product_ids"], self.eng.cache["product_names"])}
        for nm in ["offline_commerce_edge_lift_pair_out.csv", "quick_commerce_edge_lift_pair_out.csv"]:
            fp = os.path.join(data_dir, nm)
            if not os.path.exists(fp):
                continue
            df = pd.read_csv(fp, encoding="utf-8-sig")
            ch = "offline" if "offline" in nm else "quick"
            self.basket[ch] = df

    def basket_partners(self, seed_idx: int, top_k: int = 6) -> List[tuple]:
        """§5-1: seed 키워드 보유 제품 → 동반구매 파트너 제품(Lift 상위) → 보완 속성."""
        seed_prods = {norm_id(self.eng.cache["product_ids"][j]) for j, _ in self.kw2prod.get(seed_idx, [])}
        out = []
        for ch, df in self.basket.items():
            a = df["상품코드_A"].map(norm_id); b = df["상품코드_B"].map(norm_id)
            hit = df[a.isin(seed_prods) | b.isin(seed_prods)].sort_values("향상도(Lift)", ascending=False)
            for _, r in hit.head(top_k).iterrows():
                partner = r["상품명_B"] if norm_id(r["상품코드_A"]) in seed_prods else r["상품명_A"]
                out.append((ch, str(partner), round(float(r["향상도(Lift)"]), 2)))
        return out[:top_k]

    # ------------------------------------------------ (E) 시드 견고성
    def resolve_seed(self, seed: str) -> Tuple[Optional[int], str]:
        idx = self.eng.seed_to_idx(seed)
        if idx is not None:
            return idx, seed
        # 임베딩 최근접 대안 (그래프에 없을 때)
        # 부분 문자열 우선
        for kw, i in self.eng.cache["k2i"].items():
            if seed in kw or kw in seed:
                return i, kw
        return None, seed

    # ------------------------------------------------ (B) anti-partner
    def anti_partners(self, seed_idx: int, candidates: List[int], top_k: int = 5) -> List[tuple]:
        """개입 머신: seed에 후보 주입 시 Δprob<0 (점수 깎는) 조합."""
        base = [seed_idx]
        scored = []
        for k in candidates:
            d = self.eng.delta_prob(base, add=[k])
            if d < 0:
                scored.append((self.eng.kw_name(k), round(d, 4)))
        # mine 장부 동반도 포함
        return sorted(scored, key=lambda x: x[1])[:top_k]

    # ------------------------------------------------ (F) 실패 컨셉 소생
    def revive(self, concept_kw: List[int], pool: Optional[List[int]] = None, top_k: int = 5) -> List[tuple]:
        """개입 머신 전수조사: Δprob 극대화하는 단일 보완 키워드."""
        if pool is None:
            pool = list(self.lg.killer)
        base = list(concept_kw)
        scored = [(self.eng.kw_name(k), round(self.eng.delta_prob(base, add=[k]), 4))
                  for k in pool if k not in base]
        return sorted(scored, key=lambda x: -x[1])[:top_k]

    # ------------------------------------------------ 통합 처방
    def get(self, seed: str, top_k: int = 8) -> Prescription:
        idx, resolved = self.resolve_seed(seed)
        if idx is None:
            return Prescription(seed, seed, "근거부족", "중립", [], [], [], [], [],
                                f"'{seed}'는 그래프·유사어에 없어 분석 불가(근거 부족).", "없음")

        tag = self.lg.tag(idx)
        verdict = {"killer": "기획 승인", "mine": "기획 반려", "hub": "중립(주의)"}.get(tag, "중립")
        sat = self.saturation_flag(idx)

        # 파트너: 빔으로 1차 후보 → A_diff로 POS/insta 분류
        beam = [k for k, _ in self.partner_beam(idx, top_k=30)]
        pos = T.top_diff_partners(self.eng, self.A_diff_pos, idx, top_k)
        insta = T.top_diff_partners(self.eng, self.A_diff_insta, idx, top_k)
        anti = self.anti_partners(idx, beam[:15], top_k=5)
        basket = self.basket_partners(idx, top_k=6)
        revive = self.revive([idx], top_k=5) if verdict != "기획 승인" else []

        # 신뢰도: 지지도 기반
        supp = int(self.lg.support_succ[idx] + self.lg.support_fail[idx])
        conf = "높음" if supp >= 10 else ("중간" if supp >= 3 else "낮음")

        text = self._render(resolved, verdict, sat, pos, insta, anti, basket, revive, conf, supp)
        return Prescription(seed, resolved, verdict, sat, pos, insta, anti, basket, revive, text, conf)

    @staticmethod
    def _render(seed, verdict, sat, pos, insta, anti, basket, revive, conf="?", supp=0):
        pj = lambda lst: ", ".join(k for k, _ in lst[:4]) if lst else "(없음)"
        bj = ", ".join(f"{n}({l})" for _, n, l in basket[:3]) if basket else "(없음)"
        lines = [
            f"MD님께서 활용하실 '{seed}' 키워드는 성공 특이망 분석 결과 [{verdict}] 대상입니다. "
            f"(포화도: {sat} · 근거 신뢰도: {conf}/지지도 {supp})",
            f"· 함께 조합할 [{pj(pos)}] 속성은 POS 매출 독점망 기반 → 제품 속성으로 내실화하십시오.",
            f"· [{pj(insta)}] 속성은 인스타 반응 독점망 기반 → 초기 마케팅 카피로 활용하십시오.",
        ]
        if anti:
            lines.append(f"· ⚠ 피해야 할 조합(점수 하락): [{pj(anti)}].")
        if basket:
            lines.append(f"· 🛒 장바구니 보완재(번들 제안): {bj}.")
        if revive:
            lines.append(f"· 💉 점수 보강이 필요하면 [{pj(revive)}] 추가를 검토하십시오.")
        return "\n".join(lines)


def get_md_prescription(presc_engine: MDPrescriptionEngine, seed_keyword: str) -> str:
    """스펙 시그니처 — 처방 문구 반환."""
    return presc_engine.get(seed_keyword).text
