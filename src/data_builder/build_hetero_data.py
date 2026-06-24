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


def graph_signature(data_dir: str = "data/processed/hin") -> str:
    """현재 HIN parquet 기반 그래프 지문.

    노드 id 목록(개수·내용·순서) + 엣지 행수를 해시. 04 노트북 재실행으로
    네트워크가 갱신되면 지문이 바뀌어, 옛 체크포인트 캐시를 자동 무효화한다.
    """
    import hashlib
    import pyarrow.parquet as pq

    pj = lambda f: os.path.join(data_dir, f)
    parts: List[str] = []
    for f, col, is_id in [("product_nodes_final.parquet", "ITEM_CD", True),
                          ("keyword_nodes_final.parquet", "keyword", False),
                          ("ip_nodes_final.parquet", "ip_name", False)]:
        s = pd.read_parquet(pj(f), columns=[col])[col]
        s = s.map(norm_id) if is_id else s.astype(str)
        h = hashlib.md5("|".join(s.tolist()).encode("utf-8")).hexdigest()[:12]
        parts.append(f"{f}={len(s)}:{h}")
    for f in ["product_keyword_edges_final.parquet", "ip_keyword_edges_final.parquet",
              "trend_keyword_edges_final.parquet", "product_ip_edges_final.parquet",
              "ip_ip_edges_final.parquet"]:
        parts.append(f"{f}={pq.read_metadata(pj(f)).num_rows}")
    return hashlib.md5("||".join(parts).encode("utf-8")).hexdigest()[:16]


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
    use_idf_keyword_weights: bool = False,
    idf_normalization: str = "max",
    add_2hop_edges: bool = False,
    hop2_kw_min_shared: int = 5,
    hop2_ip_min_shared: int = 2,
    hop2_kw_idf: bool = False,          # sim_kw를 IDF 가중 공유합으로 구축(일반어 노이즈 제거). True면 min_shared=float τ
    hop2_kw_idf_tau: float = 1.0,       # IDF 모드 임계(가중 공유합 ≥ τ). 노트북 idf_sim_sweep로 튜닝
    add_via_ip_edges: bool = False,
    add_ipip_kw_edges: bool = False,
    add_trend_kw_edges: bool = False,
) -> Tuple[HeteroData, Dict[str, object]]:
    """HIN parquet 로드 -> (HeteroData, maps).

    maps: product_ids/keyword_ids/ip_ids (idx->원본키), 역방향 dict 포함.
    """
    pj = lambda f: os.path.join(data_dir, f)
    pnodes = pd.read_parquet(pj("product_nodes_final.parquet"))
    knodes = pd.read_parquet(pj("keyword_nodes_final.parquet"))
    inodes = pd.read_parquet(pj("ip_nodes_final.parquet"))
    pk = pd.read_parquet(pj("product_keyword_edges_final.parquet"))
    ik = pd.read_parquet(pj("ip_keyword_edges_final.parquet"))
    tk = pd.read_parquet(pj("trend_keyword_edges_final.parquet"))
    pi = pd.read_parquet(pj("product_ip_edges_final.parquet"))
    ii = pd.read_parquet(pj("ip_ip_edges_final.parquet"))

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

    # P-K(has_kw) IDF 가중치 (선택) — 흔한 키워드 down-weight (EDA 05: gap 0.006→0.043).
    # df_k = 키워드별 등장 제품 수(= has_kw dst in-degree). 성공라벨 무관 → 누수 없음.
    if use_idf_keyword_weights:
        pk_ei = data["product", "has_kw", "keyword"].edge_index      # (2, E) [product, keyword]
        n_kw = len(kw_ids)
        df_k = torch.bincount(pk_ei[1], minlength=n_kw).clamp(min=1)  # (K,) 키워드별 제품 수
        idf = torch.log(float(len(prod_ids)) / df_k.float())         # (K,)
        if idf_normalization == "max":
            idf = idf / (idf.max() + 1e-8)                           # [0,1] 정규화 (Lift attr와 동일 스케일)
        data["product", "has_kw", "keyword"].edge_attr = idf[pk_ei[1]]  # (E,) 엣지별 IDF

    data["ip", "has_kw", "keyword"].edge_index, drops["ik"] = _edge(ik, "ip_name", "keyword", i2i, k2i)
    data["keyword", "trend_to", "keyword"].edge_index, drops["tk"] = _edge(tk, "src_keyword", "tgt_keyword", k2i, k2i)
    data["product", "has_ip", "ip"].edge_index, drops["pi"] = _edge(pi, "ITEM_CD", "ip_name", p2i, i2i, src_norm=True)
    data["ip", "has_ip", "ip"].edge_index, drops["ii"]      = _edge(ii, "src_ip", "tgt_ip", i2i, i2i)

    # 2홉 메타패스 엣지 사전 구성 (exp18 — 멀티홉 1-어텐션용).
    # P-K-P(공유 키워드 수≥thr) / P-I-P(공유 IP 수≥thr)를 product↔product 엣지로 추가 →
    # 1-layer 모델이 어텐션 1번으로 2홉(유사 제품)에 도달. 가중은 DiffMG 게이트가 학습.
    if add_2hop_edges:
        import scipy.sparse as sp

        def _hop2(ei, n_dst, min_shared, idf=None):
            """공유 dst 수(또는 IDF 가중 공유합) ≥ 임계인 제품쌍을 sim 엣지로.

            idf 주어지면 S = A·diag(idf)·Aᵀ = Σ_{공유 k} idf(k) (일반어는 idf≈0 → 노이즈 제거).
            """
            r = ei[0].numpy(); c = ei[1].numpy()
            A = sp.csr_matrix((np.ones(len(r), dtype=np.float32), (r, c)),
                              shape=(len(prod_ids), int(n_dst)))
            Aw = A.multiply(idf[None, :]).tocsr() if idf is not None else A   # 열(키워드) 가중
            S = (Aw @ A.T).tocoo()
            m = (S.data >= min_shared) & (S.row != S.col)
            return torch.tensor([S.row[m].tolist(), S.col[m].tolist()], dtype=torch.long)

        kw_idf = None
        if hop2_kw_idf:
            pk_ei = data["product", "has_kw", "keyword"].edge_index
            df_k = np.bincount(pk_ei[1].numpy(), minlength=len(kw_ids)).clip(min=1)
            kw_idf = np.log(float(len(prod_ids)) / df_k).astype(np.float32)     # idf(k)=log(P/df_k)
        data["product", "sim_kw", "product"].edge_index = _hop2(
            data["product", "has_kw", "keyword"].edge_index, len(kw_ids),
            hop2_kw_idf_tau if hop2_kw_idf else hop2_kw_min_shared, idf=kw_idf)
        data["product", "sim_ip", "product"].edge_index = _hop2(
            data["product", "has_ip", "ip"].edge_index, len(ip_ids), hop2_ip_min_shared)
        drops["sim_kw"] = int(data["product", "sim_kw", "product"].edge_index.size(1))
        drops["sim_ip"] = int(data["product", "sim_ip", "product"].edge_index.size(1))

    # 확장 메타패스 엣지 사전 계산 (pre-computed collapsed paths → product 1홉 도달)
    # has_kw_via_ip  (P-I-K):    A_PI @ A_IK         — IP가 가진 키워드를 제품에 직접 연결
    # has_kw_ipip    (P-I-IP-K): A_PI @ A_II @ A_IK  — IP-IP 경로 거쳐 연결
    # has_kw_trend   (P-K-K):    A_PK @ A_KK         — 트렌드 키워드 속성을 제품에 연결
    if add_via_ip_edges or add_ipip_kw_edges or add_trend_kw_edges:
        import scipy.sparse as sp

        nP, nI, nK = len(prod_ids), len(ip_ids), len(kw_ids)
        pi_ei = data["product", "has_ip", "ip"].edge_index.numpy()
        ik_ei = data["ip", "has_kw", "keyword"].edge_index.numpy()
        A_PI = sp.csr_matrix(
            (np.ones(pi_ei.shape[1], np.float32), (pi_ei[0], pi_ei[1])),
            shape=(nP, nI))
        A_IK = sp.csr_matrix(
            (np.ones(ik_ei.shape[1], np.float32), (ik_ei[0], ik_ei[1])),
            shape=(nI, nK))

        if add_via_ip_edges:
            M = (A_PI @ A_IK).tocoo()
            ei = torch.tensor([M.row.tolist(), M.col.tolist()], dtype=torch.long)
            data["product", "has_kw_via_ip", "keyword"].edge_index = ei
            drops["has_kw_via_ip"] = int(ei.size(1))

        if add_ipip_kw_edges:
            ii_ei = data["ip", "has_ip", "ip"].edge_index.numpy()
            A_II = sp.csr_matrix(
                (np.ones(ii_ei.shape[1], np.float32), (ii_ei[0], ii_ei[1])),
                shape=(nI, nI))
            M = (A_PI @ A_II @ A_IK).tocoo()
            ei = torch.tensor([M.row.tolist(), M.col.tolist()], dtype=torch.long)
            data["product", "has_kw_ipip", "keyword"].edge_index = ei
            drops["has_kw_ipip"] = int(ei.size(1))

        if add_trend_kw_edges:
            pk_ei = data["product", "has_kw", "keyword"].edge_index.numpy()
            tk_ei = data["keyword", "trend_to", "keyword"].edge_index.numpy()
            A_PK = sp.csr_matrix(
                (np.ones(pk_ei.shape[1], np.float32), (pk_ei[0], pk_ei[1])),
                shape=(nP, nK))
            A_KK = sp.csr_matrix(
                (np.ones(tk_ei.shape[1], np.float32), (tk_ei[0], tk_ei[1])),
                shape=(nK, nK))
            M = (A_PK @ A_KK).tocoo()
            ei = torch.tensor([M.row.tolist(), M.col.tolist()], dtype=torch.long)
            data["product", "has_kw_trend", "keyword"].edge_index = ei
            drops["has_kw_trend"] = int(ei.size(1))

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
        "graph_signature": graph_signature(data_dir),   # 캐시 무효화용 그래프 지문
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
