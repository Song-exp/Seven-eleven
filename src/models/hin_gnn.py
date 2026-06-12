"""HIN-GNN 조립 모델 — KGAT × HGT × DiffMG 융합.

한 층 = (1) DiffMG 관계 게이트 α_r  (2) HGT 타입 격리 메시지  (3) KGAT Bi-Interaction.
L 층 스택 = A^L 다중홉 전파. product 초기 피처는 content aggregation(콜드스타트 지원).

학습 신호: product 성공여부(이진). 산출물: 학습된 엣지 가중치(순회 추천용) + readout 분류.
수학적 차원만 다룬다. 표기: P/K/I = product/keyword/ip 노드 수, d = hidden.
"""
from typing import Dict, List, Tuple

import torch
import torch.nn as nn
from torch_geometric.utils import scatter

from .hgt_layer import HGTLayer
from .kgat_layer import KGATUpdate
from .diffmg_pruner import DiffMGRelationGate

EdgeType = Tuple[str, str, str]


def build_reverse_edges(
    edge_index_dict: Dict[EdgeType, torch.Tensor]
) -> Dict[EdgeType, torch.Tensor]:
    """(s, r, t) -> (t, 'rev_'+r, s) 역방향 엣지 생성 (양방향 메시지패싱용)."""
    rev: Dict[EdgeType, torch.Tensor] = {}
    for (s, r, t), ei in edge_index_dict.items():
        rev[(t, f"rev_{r}", s)] = ei.flip(0)
    return rev


def build_reverse_edge_attrs(
    edge_attr_dict: Dict[EdgeType, torch.Tensor]
) -> Dict[EdgeType, torch.Tensor]:
    """역방향 엣지에 동일한 attr 복사 (Lift는 방향 무관)."""
    return {(t, f"rev_{r}", s): attr for (s, r, t), attr in edge_attr_dict.items()}


class HINGNN(nn.Module):
    def __init__(
        self,
        node_types: List[str],
        edge_types: List[EdgeType],         # forward triple만 (역방향은 내부 생성)
        num_nodes: Dict[str, int],
        hidden_dim: int = 128,
        num_layers: int = 2,
        num_heads: int = 4,
        dropout: float = 0.3,
        use_diffmg: bool = True,
        diffmg_temperature: float = 1.0,
        product_aggr: str = "mean",
        use_has_promo: bool = True,
    ) -> None:
        super().__init__()
        self.node_types = node_types
        self.fwd_edge_types = edge_types
        self.all_edge_types = edge_types + [(t, f"rev_{r}", s) for (s, r, t) in edge_types]
        self.hidden_dim = hidden_dim
        self.product_aggr = product_aggr
        self.use_has_promo = use_has_promo
        self.use_diffmg = use_diffmg

        # 학습 임베딩 테이블: keyword, ip (product 제외 — content aggregation)
        self.keyword_emb = nn.Embedding(num_nodes["keyword"], hidden_dim)
        self.ip_emb = nn.Embedding(num_nodes["ip"], hidden_dim)
        nn.init.xavier_uniform_(self.keyword_emb.weight)
        nn.init.xavier_uniform_(self.ip_emb.weight)
        if use_has_promo:
            self.product_feat_lin = nn.Linear(2, hidden_dim)  # [has_promo, insta_mention_30d]

        # L 층: 각 층마다 DiffMG 게이트 + HGT + KGAT
        self.gates = nn.ModuleList(
            [DiffMGRelationGate(self.all_edge_types, diffmg_temperature) for _ in range(num_layers)]
            if use_diffmg else []
        )
        self.hgt = nn.ModuleList(
            [HGTLayer(hidden_dim, node_types, self.all_edge_types, num_heads, dropout)
             for _ in range(num_layers)]
        )
        self.kgat = nn.ModuleList(
            [KGATUpdate(hidden_dim, node_types, dropout) for _ in range(num_layers)]
        )

        # readout: product 임베딩 -> 성공 로짓
        self.head = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim, 1),
        )

    # --- product 초기 피처: 연결된 keyword/ip 임베딩 집계 ---
    def _init_product(
        self,
        edge_index_dict: Dict[EdgeType, torch.Tensor],
        has_promo: torch.Tensor = None,
        insta_m30: torch.Tensor = None,
    ) -> torch.Tensor:
        # Output: (P, d)
        device = self.keyword_emb.weight.device
        # product 노드 수는 (product, has_kw, keyword) / (product, has_ip, ip) 의 src 최대에 의존
        # → 호출부에서 num_product 를 보장하려 dim_size 를 명시 전달받는 게 안전하나,
        #   여기서는 has_promo 길이(있으면) 또는 edge 최대 인덱스+1 로 추정.
        num_product = (
            has_promo.size(0) if has_promo is not None
            else int(max(
                edge_index_dict[(("product", "has_kw", "keyword"))][0].max().item() + 1,
                1,
            ))
        )
        acc = torch.zeros(num_product, self.hidden_dim, device=device)  # (P, d)

        kw_et = ("product", "has_kw", "keyword")
        if kw_et in edge_index_dict:
            ei = edge_index_dict[kw_et]                      # (2, E) [product, keyword]
            kw_msg = self.keyword_emb(ei[1])                 # (E, d)
            acc = acc + scatter(kw_msg, ei[0], dim=0, dim_size=num_product,
                                reduce=self.product_aggr)    # (P, d)

        ip_et = ("product", "has_ip", "ip")
        if ip_et in edge_index_dict:
            ei = edge_index_dict[ip_et]                      # (2, E) [product, ip]
            ip_msg = self.ip_emb(ei[1])                      # (E, d)
            acc = acc + scatter(ip_msg, ei[0], dim=0, dim_size=num_product,
                                reduce=self.product_aggr)    # (P, d)

        if self.use_has_promo and has_promo is not None:
            p = has_promo.float()
            m = insta_m30.float() if insta_m30 is not None else torch.zeros_like(p)
            feat = torch.stack([p, m], dim=-1)          # (P, 2)
            acc = acc + self.product_feat_lin(feat)      # (P, d)
        return acc

    def forward(
        self,
        edge_index_dict: Dict[EdgeType, torch.Tensor],
        has_promo: torch.Tensor = None,
        insta_m30: torch.Tensor = None,
        num_product: int = None,
        edge_attr_dict: Dict[EdgeType, torch.Tensor] = None,
    ) -> torch.Tensor:
        # Input: forward triple edge_index_dict, has_promo (P,) optional
        # Output: product 성공 로짓 (P,)
        # 1) 초기 노드 표현
        x_dict: Dict[str, torch.Tensor] = {
            "keyword": self.keyword_emb.weight,              # (K, d)
            "ip": self.ip_emb.weight,                        # (I, d)
        }
        x_dict["product"] = self._init_product(edge_index_dict, has_promo, insta_m30)  # (P, d)

        # 2) 역방향 포함 전체 엣지 + edge_attr
        full_edges = dict(edge_index_dict)
        full_edges.update(build_reverse_edges(edge_index_dict))

        full_attrs = None
        if edge_attr_dict:
            full_attrs = dict(edge_attr_dict)
            full_attrs.update(build_reverse_edge_attrs(edge_attr_dict))

        # 3) L 층 전파: DiffMG 게이트 -> HGT 메시지(+Lift 가중치) -> KGAT 업데이트
        for layer_i in range(len(self.hgt)):
            rel_alpha = self.gates[layer_i]() if self.use_diffmg else None
            agg_dict = self.hgt[layer_i](x_dict, full_edges, rel_alpha, full_attrs)
            x_dict = self.kgat[layer_i](x_dict, agg_dict)               # (N, d) per type

        self._product_emb = x_dict["product"]                          # (P, d) 캐시(XAI)
        logits = self.head(x_dict["product"]).squeeze(-1)              # (P,)
        return logits

    # --- XAI / 추천 순회용 추출 ---
    @torch.no_grad()
    def relation_importance(self) -> List[Dict[str, float]]:
        """층별 DiffMG 관계 중요도 α_r (MD 대시보드용)."""
        if not self.use_diffmg:
            return []
        return [g.relation_importance() for g in self.gates]

    def last_edge_attention(self) -> Dict[str, torch.Tensor]:
        """마지막 층 HGT 엣지별 어텐션(평균 head). 순회 추천 엣지 가중치 소스."""
        return self.hgt[-1].last_attention

    def product_embeddings(self) -> torch.Tensor:
        """마지막 forward 의 product 임베딩 (P, d)."""
        return getattr(self, "_product_emb", None)
