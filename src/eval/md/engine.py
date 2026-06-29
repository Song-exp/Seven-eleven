"""MD 처방 엔진 — 단일 추론 캐시 + 마스터 장부 + 개입 머신.

설계: docs/eda_channel_prescription_plan.md §0~§1, §5(개입 머신).
원칙 0: exp47 전체 그래프 forward 단 1회 → 캐시. 이후 분석은 슬라이싱/조건부 집계.
        예외: 개입 머신(Δprob)은 가상 제품 노드 추가 후 재forward (개입 실험, 채널 재추론과 구분).

핵심 객체 (노트북 전역 메모리에 바인딩):
  MDEngine.cache   = 단일 추론 산출 (prob/y/att 24관계/eidx/maps/channel/성공유형/masks)
  MDEngine.mass    = 제품 체급 Mass[p]  (z(sim_kw)+z(sim_ip), leak-free)
  MDEngine.ledger  = Killer/Mine/Hub 장부 (full / train-only 2벌)
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import torch

from src.data_builder.build_hetero_data import (
    build_graph,
    forward_edge_index_dict,
    forward_edge_attr_dict,
    norm_id,
)
from src.eval.export_results import _rebuild
from src.eval.success_predictor import predict_proba

# 4대 순방향 product→keyword 경로 (#1/#10/#11/#12)
PK_PATHS = [
    ("product", "has_kw", "keyword"),
    ("product", "has_kw_via_ip", "keyword"),
    ("product", "has_kw_ipip", "keyword"),
    ("product", "has_kw_trend", "keyword"),
]
PK_MAIN = ("product", "has_kw", "keyword")  # 메타패스 빔·개입에 쓰는 주 경로(#1)


def _rk(et: Tuple[str, str, str]) -> str:
    return "__".join(et)


@dataclass
class EngineConfig:
    exp_dir: str = "experiments/results/exp47_no_copurchase"
    data_dir: str = "data/processed/hin"
    thr: float = 0.7755            # exp47 생존율 동기화 (Plan 1)
    basket_comp_support: int = 3   # v2: basket_comp 엣지 지지도 하한 (학습과 일치)
    min_support: int = 3           # MIN_SUPPORT (우주별 지지도)
    # 커버리지 확대 프리셋 (2026-06-21) — 더 많은 키워드에 의미 태그 부여. 신뢰도는 support로 등급화.
    killer_purity: float = 0.50    # Killer Purity 하한 (base 0.238 위)
    killer_top_q: float = 0.75     # Score_succ 상위 25%
    mine_purity: float = 0.15      # Mine: Purity 상한 (실패 편중)
    mine_top_q: float = 0.75       # Score_fail 상위 25%
    hub_top_q: float = 0.80        # Hub_Score 상위 20%
    hub_balance_eps: float = 0.15  # |Purity - base| < eps (균형 게이트, 완화)
    device: str = "auto"

    # --- 모델 프리셋 (두 모델 명시적 선택) ---
    @classmethod
    def exp47(cls, **kw):
        """현 서빙 — leak-free, copurchase 제거 (HINGNN). THR 0.7755."""
        return cls(exp_dir="experiments/results/exp47_no_copurchase", thr=0.7755, **kw)

    @classmethod
    def v2_sweepA(cls, **kw):
        """v2 — 멀티태스크 + basket_comp (HINGNNv2). THR 0.7757(생존율 동기화)."""
        return cls(exp_dir="experiments/results/v2_sweepA", thr=0.7757, **kw)


@dataclass
class Ledger:
    """키워드 장부 한 벌 (universe = full 또는 train)."""
    universe: str
    score_succ: np.ndarray          # (K,)
    score_fail: np.ndarray
    purity: np.ndarray              # (K,) nan = 미등장
    support_succ: np.ndarray        # (K,) 성공 우주 지지도(제품수)
    support_fail: np.ndarray
    hub_score: np.ndarray
    killer: set = field(default_factory=set)   # keyword idx
    mine: set = field(default_factory=set)
    hub: set = field(default_factory=set)
    base_rate: float = 0.0

    def tag(self, k_idx: int) -> str:
        if k_idx in self.killer:
            return "killer"
        if k_idx in self.mine:
            return "mine"
        if k_idx in self.hub:
            return "hub"
        return "neutral"


class MDEngine:
    """단일 추론 캐시 + 장부 + 개입 머신. 노트북에서 한 번 생성해 전역 바인딩."""

    def __init__(self, cfg: Optional[EngineConfig] = None):
        self.cfg = cfg or EngineConfig()
        dev = ("cuda" if torch.cuda.is_available() else "cpu") if self.cfg.device == "auto" else self.cfg.device
        self.device = torch.device(dev)
        self.cache: Dict = {}
        self.mass: Optional[np.ndarray] = None
        self.ledger: Dict[str, Ledger] = {}
        self._loaded = False

    # ---------------------------------------------------------------- 단일 추론
    def run_single_inference(self) -> "MDEngine":
        """exp47 전체 그래프 forward 1회 → 캐시 채움 (원칙 0)."""
        ck_path = os.path.join(self.cfg.exp_dir, "hin_gnn_best.pt")
        ckpt = torch.load(ck_path, weights_only=False, map_location="cpu")
        cfg, maps = ckpt["config"], ckpt["maps"]
        g = cfg["graph"]

        data, _ = build_graph(
            data_dir=self.cfg.data_dir,
            seed=cfg["split"]["seed"], ratios=tuple(cfg["split"]["ratios"]),
            include_offline_copurchase=g.get("include_offline_copurchase", False),
            include_quick_copurchase=g.get("include_quick_copurchase", False),
            lift_norm=g.get("lift_normalization", "log1p"),
            use_lift_weights=g.get("use_lift_weights", False),
            use_idf_keyword_weights=g.get("use_idf_keyword_weights", False),
            add_2hop_edges=g.get("add_2hop_edges", False),
            hop2_kw_min_shared=g.get("hop2_kw_min_shared", 3),
            hop2_ip_min_shared=g.get("hop2_ip_min_shared", 1),
            hop2_kw_idf=g.get("hop2_kw_idf", False),
            hop2_kw_idf_tau=g.get("hop2_kw_idf_tau", 1.0),
            add_via_ip_edges=g.get("add_via_ip_edges", False),
            add_ipip_kw_edges=g.get("add_ipip_kw_edges", False),
            add_trend_kw_edges=g.get("add_trend_kw_edges", False),
        )
        eidx = {et: ei.to(self.device) for et, ei in forward_edge_index_dict(data).items()}
        raw_attr = forward_edge_attr_dict(data)
        eattr = {et: ea.to(self.device) for et, ea in raw_attr.items()} if raw_attr else None
        hp = data["product"].has_promo.to(self.device)
        im30 = data["product"].insta_mention_30d.to(self.device)
        y = data["product"].y.cpu().numpy().astype(int)

        # --- 모델 분기: v2(HINGNNv2 + basket_comp) vs exp47(HINGNN) ---
        self.is_v2 = cfg["model"].get("model_class") == "HINGNNv2" or g.get("add_basket_comp_edges", False)
        if self.is_v2:
            eidx = self._inject_basket_comp(eidx, maps, self.cfg.basket_comp_support)
            model = self._rebuild_v2(cfg, data, self.device)
        else:
            model = _rebuild(cfg, data, self.device)
        model.load_state_dict(ckpt["model"])
        model.eval()
        with torch.no_grad():
            prob = predict_proba(model, eidx, hp, eattr, insta_m30=im30).cpu().numpy()
        att = {k: v.cpu().numpy() for k, v in model.last_edge_attention().items()}

        # 채널/성공유형 (product_nodes_final 순서 == 노드 순서)
        pn = pd.read_parquet(os.path.join(self.cfg.data_dir, "product_nodes_final.parquet"))
        channel = pn["편의점명"].astype(str).to_numpy()
        succ_src = pn["성공_소스"].astype(str).to_numpy() if "성공_소스" in pn.columns else np.array(["?"] * len(pn))

        self.model = model
        self.data = data
        self.cache = dict(
            ckpt_config=cfg, maps=maps,
            prob=prob, y=y, att=att,
            eidx={et: ei.cpu() for et, ei in eidx.items()},
            eattr={et: ea.cpu() for et, ea in raw_attr.items()} if raw_attr else None,
            has_promo=hp.cpu().numpy(), insta_m30=im30.cpu().numpy(),
            channel=channel, succ_src=succ_src,
            train_mask=data["product"].train_mask.cpu().numpy(),
            val_mask=data["product"].val_mask.cpu().numpy(),
            test_mask=data["product"].test_mask.cpu().numpy(),
            P=data["product"].num_nodes, K=data["keyword"].num_nodes, I=data["ip"].num_nodes,
            base_rate=float(y.mean()),
            # sim 엣지 빌드 임계 (_hop2와 동일) — include_sim 가상노드 sim 재계산용
            sim_kw_min_shared=g.get("hop2_kw_min_shared", 3),
            sim_ip_min_shared=g.get("hop2_ip_min_shared", 1),
            k2i=maps["k2i"], kw_ids=maps["keyword_ids"],
            product_ids=maps["product_ids"], product_names=maps["product_names"],
        )
        self._loaded = True
        return self

    # ---------------------------------------------------------------- v2 어댑터
    def _inject_basket_comp(self, eidx, maps, support):
        """v2: keyword_basket_comp_edges.parquet → ('keyword','basket_comp','keyword') 주입 (학습과 동일)."""
        BC = ("keyword", "basket_comp", "keyword")
        fp = os.path.join(self.cfg.data_dir, "keyword_basket_comp_edges.parquet")
        if not os.path.exists(fp):
            raise FileNotFoundError(f"v2 basket_comp 엣지 없음: {fp} (build_basket_comp_edges 먼저 실행)")
        df = pd.read_parquet(fp)
        df = df[df["support"] >= support]
        s = df["src_keyword"].astype(str).map(maps["k2i"])
        t = df["tgt_keyword"].astype(str).map(maps["k2i"])
        ok = s.notna() & t.notna()
        ei = torch.tensor([s[ok].astype(int).tolist(), t[ok].astype(int).tolist()], dtype=torch.long)
        eidx = dict(eidx)
        eidx[BC] = ei.to(self.device)
        return eidx

    def _rebuild_v2(self, cfg, data, dev):
        """HINGNNv2 재구성 — edge_types는 cfg.graph.edge_types(basket_comp 포함)."""
        from src.models.hin_gnn_v2 import HINGNNv2
        m = cfg["model"]
        return HINGNNv2(
            cfg["graph"]["node_types"], [tuple(e) for e in cfg["graph"]["edge_types"]],
            {"product": data["product"].num_nodes, "keyword": data["keyword"].num_nodes,
             "ip": data["ip"].num_nodes},
            m["hidden_dim"], m["num_layers"], m["num_heads"], m["dropout"],
            m["use_diffmg_gate"], m["diffmg_temperature"],
            cfg["node_feat"]["product_aggr"], cfg["node_feat"]["use_has_promo_feature"],
            m.get("readout_hop_mode", "final"),
        ).to(dev)

    # ---------------------------------------------------------------- Mass
    def build_mass(self) -> np.ndarray:
        """Mass[p] = z(deg sim_kw)+z(deg sim_ip).  동반구매 제거 → leak-free (§1-1)."""
        P = self.cache["P"]
        deg = {}
        for nm, et in [("sim_kw", ("product", "sim_kw", "product")),
                       ("sim_ip", ("product", "sim_ip", "product"))]:
            d = np.zeros(P)
            if et in self.cache["eidx"]:
                ei = self.cache["eidx"][et].numpy()
                d += np.bincount(ei[0], minlength=P) + np.bincount(ei[1], minlength=P)
            deg[nm] = d

        def z(x):
            s = x.std()
            return (x - x.mean()) / (s + 1e-9)

        self.mass = z(deg["sim_kw"]) + z(deg["sim_ip"])
        self.cache["mass_raw"] = deg
        return self.mass

    # ---------------------------------------------------------------- 장부
    def _pk_aggregate(self, universe_mask: np.ndarray):
        """4대 경로 어텐션을 성공/실패 우주로 집계 → score·support."""
        K = self.cache["K"]
        y = self.cache["y"]
        ssucc = np.zeros(K); sfail = np.zeros(K)
        ps_s = [set() for _ in range(K)]; ps_f = [set() for _ in range(K)]
        for et in PK_PATHS:
            key = _rk(et)
            if et not in self.cache["eidx"] or key not in self.cache["att"]:
                continue
            ei = self.cache["eidx"][et].numpy()
            a = self.cache["att"][key]
            ps, ks = ei[0], ei[1]
            in_uni = universe_mask[ps]
            ys = (y[ps] == 1) & in_uni
            yf = (y[ps] == 0) & in_uni
            np.add.at(ssucc, ks[ys], a[ys])
            np.add.at(sfail, ks[yf], a[yf])
            for p, k in zip(ps[ys], ks[ys]):
                ps_s[k].add(int(p))
            for p, k in zip(ps[yf], ks[yf]):
                ps_f[k].add(int(p))
        supp_s = np.array([len(s) for s in ps_s])
        supp_f = np.array([len(s) for s in ps_f])
        return ssucc, sfail, supp_s, supp_f

    def build_ledger(self, universe: str = "full") -> Ledger:
        """Killer/Mine/Hub 장부 (universe='full' | 'train').  §1-2/1-3."""
        if self.mass is None:
            self.build_mass()
        c = self.cfg
        P = self.cache["P"]
        if universe == "train":
            umask = self.cache["train_mask"]
        else:
            umask = np.ones(P, dtype=bool)

        ssucc, sfail, supp_s, supp_f = self._pk_aggregate(umask)
        tot = ssucc + sfail
        purity = np.divide(ssucc, tot, out=np.full(self.cache["K"], np.nan), where=tot > 0)
        base = self.cache["base_rate"]

        # Hub_Score = Σ_m Σ_{p∈P(k)} att_m(p,k)·Mass[p]  (전 우주)
        hub_score = np.zeros(self.cache["K"])
        for et in PK_PATHS:
            key = _rk(et)
            if et not in self.cache["eidx"] or key not in self.cache["att"]:
                continue
            ei = self.cache["eidx"][et].numpy(); a = self.cache["att"][key]
            ps, ks = ei[0], ei[1]
            in_uni = umask[ps]
            np.add.at(hub_score, ks[in_uni], (a * self.mass[ps])[in_uni])

        lg = Ledger(universe=universe, score_succ=ssucc, score_fail=sfail, purity=purity,
                    support_succ=supp_s, support_fail=supp_f, hub_score=hub_score, base_rate=base)

        # Killer: 성공지지도≥3 & Purity≥하한 & Score_succ 상위(top_q)
        valid_s = supp_s >= c.min_support
        if valid_s.any():
            thr_succ = np.quantile(ssucc[valid_s], c.killer_top_q)
            lg.killer = set(np.where(valid_s & (purity >= c.killer_purity) & (ssucc >= thr_succ))[0].tolist())
        # Mine: 실패지지도≥3 & Purity≤상한 & Score_fail 상위
        valid_f = supp_f >= c.min_support
        if valid_f.any():
            thr_fail = np.quantile(sfail[valid_f], c.mine_top_q)
            lg.mine = set(np.where(valid_f & (purity <= c.mine_purity) & (sfail >= thr_fail))[0].tolist())
        # Hub: Hub_Score 상위10% & |Purity-base|<eps & 전역지지도≥3
        supp_all = supp_s + supp_f
        valid_h = (supp_all >= c.min_support) & np.isfinite(purity)
        if valid_h.any():
            thr_hub = np.quantile(hub_score[valid_h], c.hub_top_q)
            bal = np.abs(purity - base) < c.hub_balance_eps
            lg.hub = set(np.where(valid_h & bal & (hub_score >= thr_hub))[0].tolist())
        # 상호배타: killer/mine 우선, hub에서 제거
        lg.hub -= (lg.killer | lg.mine)
        lg.mine -= lg.killer

        self.ledger[universe] = lg
        return lg

    # ---------------------------------------------------------------- 개입 머신
    def _virtual_sim_neighbors(self, keyword_idx, ip_idx) -> Dict[tuple, np.ndarray]:
        """가상 노드(키워드/IP 집합)가 sim_kw/sim_ip로 연결될 기존 제품 이웃 인덱스.

        배포 시점의 유사도 재계산을 모사 — `_hop2`와 동일 임계(공유 키워드/IP 수 ≥ min_shared).
        include_sim=True 일 때만 호출. 모델에 sim 엣지 타입이 없으면(2hop 미사용) 빈 dict.
        """
        out: Dict[tuple, np.ndarray] = {}
        P = self.cache["P"]
        for et, hkey, thr_key in (
            (("product", "sim_kw", "product"), PK_MAIN, "sim_kw_min_shared"),
            (("product", "sim_ip", "product"), ("product", "has_ip", "ip"), "sim_ip_min_shared"),
        ):
            members = list(keyword_idx) if hkey == PK_MAIN else list(ip_idx)
            if not members or et not in self.cache["eidx"]:
                continue
            ei = self.cache["eidx"][hkey].numpy()
            mask = np.isin(ei[1], np.asarray(members))
            shared = np.bincount(ei[0][mask], minlength=P)
            out[et] = np.where(shared >= self.cache[thr_key])[0]
        return out

    @torch.no_grad()
    def score_concept(self, keyword_idx: Sequence[int], ip_idx: Sequence[int] = (),
                      has_promo: float = 0.0, insta_m30: float = 0.0,
                      include_sim: bool = False) -> float:
        """가상 제품 노드(키워드/IP 집합)를 그래프에 1개 추가 후 재forward → 그 노드 성공확률.

        cold-start 시뮬레이션의 핵심 프리미티브. B(anti-partner)·C·F·처방이 공유.
        기본: has_kw(+has_ip)로만 연결(직접 채널 — via_ip/ipip/trend 미부여, Δ에서 대부분 상쇄).
        include_sim=True: 배포 현실 모사 — 공유 키워드/IP ≥ 임계인 기존 제품과 sim_kw/sim_ip 양방향
          연결(모델 주력 채널 α_r~0.87). 직접채널 vs 배포현실 비교용. rev_sim_*는 모델이 자동 생성.
        """
        P = self.cache["P"]
        newp = P  # 새 제품 인덱스
        eidx = {et: ei.clone() for et, ei in self.cache["eidx"].items()}

        def _append(et, dst_list):
            if len(dst_list) == 0:
                return
            extra = torch.tensor([[newp] * len(dst_list), list(dst_list)], dtype=torch.long)
            eidx[et] = torch.cat([eidx[et], extra], dim=1) if et in eidx else extra

        _append(PK_MAIN, list(keyword_idx))
        if ip_idx:
            _append(("product", "has_ip", "ip"), list(ip_idx))
        if include_sim:
            for et, nbrs in self._virtual_sim_neighbors(keyword_idx, ip_idx).items():
                if len(nbrs):                         # 양방향 (newp↔nbr) — _hop2 대칭 materialize와 동일
                    src = np.concatenate([np.full(len(nbrs), newp), nbrs])
                    dst = np.concatenate([nbrs, np.full(len(nbrs), newp)])
                    extra = torch.tensor([src.tolist(), dst.tolist()], dtype=torch.long)
                    eidx[et] = torch.cat([eidx[et], extra], dim=1) if et in eidx else extra

        eidx_dev = {et: ei.to(self.device) for et, ei in eidx.items()}
        hp = np.append(self.cache["has_promo"], float(has_promo))
        im = np.append(self.cache["insta_m30"], float(insta_m30))
        hp_t = torch.tensor(hp, dtype=torch.float, device=self.device)
        im_t = torch.tensor(im, dtype=torch.float, device=self.device)
        eattr = None
        if self.cache["eattr"]:
            eattr = {et: ea.to(self.device) for et, ea in self.cache["eattr"].items()}
        prob = predict_proba(self.model, eidx_dev, hp_t, eattr, insta_m30=im_t).cpu().numpy()
        return float(prob[newp])

    @torch.no_grad()
    def score_concept_batch(self, concepts: Sequence, has_promo: float = 0.0,
                            insta_m30: float = 0.0, chunk_size: int = 64,
                            include_sim: bool = False) -> np.ndarray:
        """가상 제품 M개를 한 그래프에 동시 추가 → 1 forward로 M개 성공확률을 한 번에 반환.

        `score_concept`의 배치판 — 콜드스타트 시뮬을 N번 돌릴 때 forward N회 대신 ceil(N/chunk)회.
        combo_grow·seed_partners·서브넷 빌드의 병목(시드당 ~85s → ~1-2s)을 푸는 핵심 프리미티브.

        concepts: 각 원소가 keyword_idx 시퀀스, 또는 (keyword_idx_seq, ip_idx_seq) 튜플.
        ⚠ 정확성: 같은 청크의 가상 제품들이 키워드를 공유하면 reverse 엣지로 keyword 임베딩이
           상호 오염됨(특히 전 노드가 공유하는 seed/rail 키워드). 차분(margin=base+X − base)
           기준 quantity는 같은 청크 내에서 1차 상쇄되나, 절대 prob은 단일 forward와 미세히 다를
           수 있음. → chunk_size로 청크당 노드 수를 제한하고, 노트북 drift 셀로 검증 후 사용.
           chunk_size=1 이면 단일 forward 반복(완전 동일, 느림) — 안전 다이얼.
        반환: np.ndarray (len(concepts),).
        """
        def _split(c):
            if len(c) == 2 and all(isinstance(s, (list, tuple, np.ndarray)) for s in c):
                return list(c[0]), list(c[1])
            return list(c), []

        P = self.cache["P"]
        ip_et = ("product", "has_ip", "ip")
        eattr = ({et: ea.to(self.device) for et, ea in self.cache["eattr"].items()}
                 if self.cache["eattr"] else None)
        out = np.empty(len(concepts), dtype=float)
        for start in range(0, len(concepts), max(1, chunk_size)):
            chunk = concepts[start:start + chunk_size]
            M = len(chunk)
            eidx = {et: ei.clone() for et, ei in self.cache["eidx"].items()}
            pk_s, pk_d, ip_s, ip_d = [], [], [], []
            sim_app: Dict[tuple, List] = {}        # et → [src_list, dst_list]
            for i, c in enumerate(chunk):
                kw_seq, ip_seq = _split(c)
                newp = P + i
                pk_s += [newp] * len(kw_seq); pk_d += list(kw_seq)
                if ip_seq:
                    ip_s += [newp] * len(ip_seq); ip_d += list(ip_seq)
                if include_sim:
                    for et, nbrs in self._virtual_sim_neighbors(kw_seq, ip_seq).items():
                        if len(nbrs):
                            s, d = sim_app.setdefault(et, ([], []))
                            s += [newp] * len(nbrs) + nbrs.tolist()   # 양방향
                            d += nbrs.tolist() + [newp] * len(nbrs)
            if pk_d:
                extra = torch.tensor([pk_s, pk_d], dtype=torch.long)
                eidx[PK_MAIN] = torch.cat([eidx[PK_MAIN], extra], dim=1) if PK_MAIN in eidx else extra
            if ip_d:
                extra = torch.tensor([ip_s, ip_d], dtype=torch.long)
                eidx[ip_et] = torch.cat([eidx[ip_et], extra], dim=1) if ip_et in eidx else extra
            for et, (s, d) in sim_app.items():
                extra = torch.tensor([s, d], dtype=torch.long)
                eidx[et] = torch.cat([eidx[et], extra], dim=1) if et in eidx else extra
            eidx_dev = {et: ei.to(self.device) for et, ei in eidx.items()}
            hp = np.append(self.cache["has_promo"], np.full(M, float(has_promo)))
            im = np.append(self.cache["insta_m30"], np.full(M, float(insta_m30)))
            hp_t = torch.tensor(hp, dtype=torch.float, device=self.device)
            im_t = torch.tensor(im, dtype=torch.float, device=self.device)
            prob = predict_proba(self.model, eidx_dev, hp_t, eattr, insta_m30=im_t).cpu().numpy()
            out[start:start + M] = prob[P:P + M]
        return out

    def delta_prob(self, base_keywords: Sequence[int], add: Sequence[int] = (),
                   remove: Sequence[int] = (), **kw) -> float:
        """Δprob = score(base ∪ add − remove) − score(base).  B/F/검증 공용."""
        base = list(base_keywords)
        new = [k for k in base if k not in set(remove)] + [k for k in add if k not in base]
        return self.score_concept(new, **kw) - self.score_concept(base, **kw)

    # ---------------------------------------------------------------- accessors
    def product_keywords(self, p_idx: int, path=PK_MAIN) -> List[int]:
        ei = self.cache["eidx"][path].numpy()
        return ei[1][ei[0] == p_idx].tolist()

    def seed_to_idx(self, seed: str) -> Optional[int]:
        return self.cache["k2i"].get(seed)

    def kw_name(self, k_idx: int) -> str:
        return self.cache["kw_ids"][k_idx]

    def product_name(self, p_idx: int) -> str:
        return self.cache["product_names"][p_idx]

    def confusion_quadrant(self) -> np.ndarray:
        """THR 기준 TP/TN/FP/FN 라벨 배열 (P,)."""
        pred = (self.cache["prob"] >= self.cfg.thr).astype(int)
        y = self.cache["y"]
        q = np.where((pred == 1) & (y == 1), "TP",
            np.where((pred == 0) & (y == 0), "TN",
            np.where((pred == 1) & (y == 0), "FP", "FN")))
        return q

    # ---------------------------------------------------------------- 영속화
    def save_cache(self, out_dir: str) -> str:
        """단일 추론 캐시 + 장부를 npz/parquet로 저장 (반복 분석 시 재추론 불필요)."""
        os.makedirs(out_dir, exist_ok=True)
        np.savez_compressed(
            os.path.join(out_dir, "inference_cache.npz"),
            prob=self.cache["prob"], y=self.cache["y"],
            channel=self.cache["channel"], succ_src=self.cache["succ_src"],
            train_mask=self.cache["train_mask"], val_mask=self.cache["val_mask"],
            test_mask=self.cache["test_mask"],
            mass=self.mass if self.mass is not None else np.array([]),
            base_rate=self.cache["base_rate"], thr=self.cfg.thr,
        )
        rows = []
        for uni, lg in self.ledger.items():
            for k in range(self.cache["K"]):
                tg = lg.tag(k)
                if tg == "neutral":
                    continue
                rows.append(dict(universe=uni, keyword=self.kw_name(k), tag=tg,
                                 score_succ=lg.score_succ[k], score_fail=lg.score_fail[k],
                                 purity=lg.purity[k], support_succ=int(lg.support_succ[k]),
                                 support_fail=int(lg.support_fail[k]), hub_score=lg.hub_score[k]))
        if rows:
            pd.DataFrame(rows).to_parquet(os.path.join(out_dir, "ledgers.parquet"), index=False)
        meta = dict(exp_dir=self.cfg.exp_dir, thr=self.cfg.thr, base_rate=self.cache["base_rate"],
                    P=self.cache["P"], K=self.cache["K"], I=self.cache["I"],
                    ledger_sizes={u: dict(killer=len(l.killer), mine=len(l.mine), hub=len(l.hub))
                                  for u, l in self.ledger.items()})
        with open(os.path.join(out_dir, "engine_meta.json"), "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
        return out_dir
