"""HIN parquet 7종 -> PyG HeteroData + id 맵 + 계층화 split.

Data Leakage 차단(src/data_builder/.claude-rules.md):
- 성공여부(KPI)는 오직 product 노드의 타겟 레이블(y)로만 사용.
- keyword/ip 노드 초기 피처에 KPI 미주입(임베딩은 모델 내부 학습).
- product 초기 피처는 모델 내부 content aggregation (여기선 has_promo 만 부가 피처로 전달).

노드 타입: product / keyword / ip (트렌드는 keyword 의 is_trend_keyword 플래그로 포함).
※ data_builder 규칙의 4노드(상품/속성/트렌드/영수증) 중 영수증은 미구현,
   트렌드는 keyword 에 흡수됨 — 현행 HIN 파일 구조(product/keyword/ip)를 그대로 로드.
"""
from __future__ import annotations

import os
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import torch
from torch_geometric.data import HeteroData

EdgeType = Tuple[str, str, str]


def norm_id(x) -> str:
    """ITEM_CD leading-zero 통일. CU/GS25 합성 ID(CU_xxx)는 그대로 통과."""
    try:
        return str(int(float(x)))
    except (ValueError, TypeError):
        return str(x)


def _stratified_masks(
    strata: np.ndarray, ratios=(0.70, 0.15, 0.15), seed: int = 42
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """stratum 라벨 배열 -> (train, val, test) boolean 마스크."""
    rng = np.random.default_rng(seed)
    n = len(strata)
    train = np.zeros(n, dtype=bool)
    val = np.zeros(n, dtype=bool)
    test = np.zeros(n, dtype=bool)
    for s in np.unique(strata):
        idx = np.where(strata == s)[0]
        rng.shuffle(idx)
        n_tr = int(round(len(idx) * ratios[0]))
        n_va = int(round(len(idx) * ratios[1]))
        train[idx[:n_tr]] = True
        val[idx[n_tr:n_tr + n_va]] = True
        test[idx[n_tr + n_va:]] = True
    return train, val, test


def build_graph(
    data_dir: str = "data/processed/hin",
    ratios=(0.70, 0.15, 0.15),
    seed: int = 42,
    positive_value: str = "성공",
    include_offline_copurchase: bool = False,
    offline_copurchase_path: str = "data/processed/offline_commerce_edge_lift_pair_out.csv",
    include_quick_copurchase: bool = False,
    quick_copurchase_path: str = "data/processed/quick_commerce_edge_lift_pair_out.csv",
    lift_norm: str = "log1p",
    use_lift_weights: bool = True,
) -> Tuple[HeteroData, Dict[str, object]]:
    """HIN parquet 로드 -> (HeteroData, maps).

    maps: product_ids/keyword_ids/ip_ids (idx->원본키), 역방향 dict 포함.
    """
    pj = lambda f: os.path.join(data_dir, f)
    pnodes = pd.read_parquet(pj("product_nodes.parquet"))
    knodes = pd.read_parquet(pj("keyword_nodes.parquet"))
    inodes = pd.read_parquet(pj("ip_nodes.parquet"))
    pk = pd.read_parquet(pj("product_keyword_edges.parquet"))
    ik = pd.read_parquet(pj("ip_keyword_edges.parquet"))
    tk = pd.read_parquet(pj("trend_keyword_edges.parquet"))
    pi = pd.read_parquet(pj("product_ip_edges.parquet"))

    # --- 노드 id 맵 ---
    pnodes["_id"] = pnodes["ITEM_CD"].map(norm_id)
    prod_ids: List[str] = pnodes["_id"].tolist()
    kw_ids: List[str] = knodes["keyword"].astype(str).tolist()
    ip_ids: List[str] = inodes["ip_name"].astype(str).tolist()
    p2i = {v: i for i, v in enumerate(prod_ids)}
    k2i = {v: i for i, v in enumerate(kw_ids)}
    i2i = {v: i for i, v in enumerate(ip_ids)}

    def _edge(df, src_col, dst_col, src_map, dst_map, src_norm=False):
        s = df[src_col].map(norm_id) if src_norm else df[src_col].astype(str)
        d = df[dst_col].astype(str)
        si = s.map(src_map)
        di = d.map(dst_map)
        ok = si.notna() & di.notna()
        dropped = int((~ok).sum())
        ei = torch.tensor([si[ok].astype(int).tolist(), di[ok].astype(int).tolist()],
                          dtype=torch.long)
        return ei, dropped

    def _edge_with_lift(df, src_col, dst_col, lift_col, src_map, dst_map, lift_norm="log1p"):
        """엣지 인덱스 + 정규화 Lift edge_attr 반환."""
        s = df[src_col].apply(norm_id)
        d = df[dst_col].astype(str)
        si = s.map(src_map)
        di = d.map(dst_map)
        ok = si.notna() & di.notna()
        dropped = int((~ok).sum())
        ei = torch.tensor([si[ok].astype(int).tolist(), di[ok].astype(int).tolist()],
                          dtype=torch.long)
        raw = df.loc[ok, lift_col].values.astype(float)
        if lift_norm == "log1p":
            normed = np.log1p(raw) / (np.log1p(raw.max()) + 1e-8)
        elif lift_norm == "minmax":
            normed = (raw - raw.min()) / (raw.max() - raw.min() + 1e-8)
        else:
            normed = raw
        return ei, torch.tensor(normed, dtype=torch.float), dropped

    data = HeteroData()
    data["product"].num_nodes = len(prod_ids)
    data["keyword"].num_nodes = len(kw_ids)
    data["ip"].num_nodes = len(ip_ids)

    drops = {}
    data["product", "has_kw", "keyword"].edge_index, drops["pk"] = _edge(pk, "ITEM_CD", "keyword", p2i, k2i, src_norm=True)
    data["ip", "has_kw", "keyword"].edge_index, drops["ik"] = _edge(ik, "ip_name", "keyword", i2i, k2i)
    data["keyword", "trend_to", "keyword"].edge_index, drops["tk"] = _edge(tk, "src_keyword", "tgt_keyword", k2i, k2i)
    data["product", "has_ip", "ip"].edge_index, drops["pi"] = _edge(pi, "ITEM_CD", "ip_name", p2i, i2i, src_norm=True)

    if include_offline_copurchase and os.path.exists(offline_copurchase_path):
        df_off = pd.read_csv(offline_copurchase_path, encoding="utf-8-sig")
        ei, attr, dropped = _edge_with_lift(
            df_off, "상품코드_A", "상품코드_B", "향상도(Lift)", p2i, p2i, lift_norm
        )
        data["product", "co_offline", "product"].edge_index = ei
        if use_lift_weights:
            data["product", "co_offline", "product"].edge_attr = attr
        drops["co_offline"] = dropped

    if include_quick_copurchase and os.path.exists(quick_copurchase_path):
        df_qk = pd.read_csv(quick_copurchase_path, encoding="utf-8-sig")
        ei, attr, dropped = _edge_with_lift(
            df_qk, "상품코드_A", "상품코드_B", "향상도(Lift)", p2i, p2i, lift_norm
        )
        data["product", "co_quick", "product"].edge_index = ei
        if use_lift_weights:
            data["product", "co_quick", "product"].edge_attr = attr
        drops["co_quick"] = dropped

    # --- product 노드 부가 피처 / 타겟 ---
    # Phase 7.5 이후 product_nodes는 has_promo_30d 대신 promo_* 18종 one-hot 컬럼 사용
    promo_cols = [c for c in pnodes.columns if c.startswith("promo_")]
    if promo_cols:
        has_promo = (pnodes[promo_cols].sum(axis=1) > 0).astype(float).to_numpy()
    elif "has_promo_30d" in pnodes.columns:
        has_promo = pnodes["has_promo_30d"].fillna(False).astype(float).to_numpy()
    else:
        has_promo = np.zeros(len(pnodes), dtype=float)
    data["product"].has_promo = torch.tensor(has_promo, dtype=torch.float)

    insta_m30 = pnodes["insta_mention_30d"].fillna(0).astype(float).to_numpy()
    data["product"].insta_mention_30d = torch.tensor(insta_m30, dtype=torch.float)

    ts_cols = [c for c in pnodes.columns if c.startswith("ts_") and c.endswith("_ratio")]
    if ts_cols:
        ts_feat = pnodes[ts_cols].fillna(0).astype(float).to_numpy()
        data["product"].timeslot_ratio = torch.tensor(ts_feat, dtype=torch.float)

    y = (pnodes["성공여부"].astype(str) == positive_value).astype(int).to_numpy()
    data["product"].y = torch.tensor(y, dtype=torch.float)

    # --- 계층화 split (편의점명 × 성공여부) ---
    strata = (pnodes["편의점명"].astype(str) + "|" + pnodes["성공여부"].astype(str)).to_numpy()
    tr, va, te = _stratified_masks(strata, ratios, seed)
    data["product"].train_mask = torch.tensor(tr)
    data["product"].val_mask = torch.tensor(va)
    data["product"].test_mask = torch.tensor(te)

    maps = {
        "product_ids": prod_ids, "keyword_ids": kw_ids, "ip_ids": ip_ids,
        "p2i": p2i, "k2i": k2i, "i2i": i2i,
        "product_names": pnodes["ITEM_NM"].astype(str).tolist(),
        "edges_dropped": drops,
        "label_pos": int(y.sum()), "label_neg": int((y == 0).sum()),
    }
    return data, maps


def forward_edge_index_dict(data: HeteroData) -> Dict[EdgeType, torch.Tensor]:
    """모델 입력용 forward triple edge_index_dict 추출 (역방향 제외, 동적)."""
    return {
        et: data[et].edge_index
        for et in data.edge_types
        if not et[1].startswith("rev_")
    }


def forward_edge_attr_dict(data: HeteroData) -> Dict[EdgeType, torch.Tensor]:
    """edge_attr 가 있는 forward triple 만 추출. 없는 엣지 타입은 포함하지 않음."""
    return {
        et: data[et].edge_attr
        for et in data.edge_types
        if not et[1].startswith("rev_")
        and hasattr(data[et], "edge_attr")
        and data[et].edge_attr is not None
    }


if __name__ == "__main__":
    g, m = build_graph()
    print("nodes:", {nt: g[nt].num_nodes for nt in ["product", "keyword", "ip"]})
    print("edges dropped:", m["edges_dropped"])
    print("label pos/neg:", m["label_pos"], m["label_neg"])
    print("split:", int(g["product"].train_mask.sum()),
          int(g["product"].val_mask.sum()), int(g["product"].test_mask.sum()))
