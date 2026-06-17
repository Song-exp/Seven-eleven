"""KGAT (Knowledge Graph Attention Network) Bi-Interaction 업데이트.

역할: 집계된 이웃 메시지(agg)와 자기 표현(h)을 Bi-Interaction 으로 융합.
덧셈(+) 항과 원소곱(⊙) 항을 모두 포함해 고차 상호작용을 포착한다.

    h_new = LeakyReLU(W1 (h + agg)) + LeakyReLU(W2 (h ⊙ agg))

레이어를 L 회 스택하면 A^L 재귀 메시지 패싱 효과 → 신상품이 2~3홉 너머
히트 상품·트렌드 맥락을 흡수(Cold Start 극복).

수학적 차원만 다룬다. 표기: N = 노드 수, d = hidden.
"""
from typing import Dict, List

import torch
import torch.nn as nn


class KGATUpdate(nn.Module):
    """노드 타입별 Bi-Interaction 업데이트 (파라미터 격리)."""

    def __init__(
        self,
        hidden_dim: int,
        node_types: List[str],
        dropout: float = 0.3,
    ) -> None:
        super().__init__()
        self.w_add = nn.ModuleDict({nt: nn.Linear(hidden_dim, hidden_dim) for nt in node_types})
        self.w_mul = nn.ModuleDict({nt: nn.Linear(hidden_dim, hidden_dim) for nt in node_types})
        self.norm = nn.ModuleDict({nt: nn.LayerNorm(hidden_dim) for nt in node_types})
        self.act = nn.LeakyReLU(negative_slope=0.2)
        self.drop = nn.Dropout(dropout)

    def forward(
        self,
        h_dict: Dict[str, torch.Tensor],
        agg_dict: Dict[str, torch.Tensor],
    ) -> Dict[str, torch.Tensor]:
        # Input:  h_dict[nt] (N, d) , agg_dict[nt] (N, d)
        # Output: out_dict[nt] (N, d)
        out: Dict[str, torch.Tensor] = {}
        for nt, h in h_dict.items():
            agg = agg_dict[nt]                                   # (N, d)
            add_term = self.act(self.w_add[nt](h + agg))         # (N, d) 합 상호작용
            mul_term = self.act(self.w_mul[nt](h * agg))         # (N, d) 원소곱 상호작용
            z = self.drop(add_term + mul_term)                   # (N, d) Bi-Interaction
            out[nt] = self.norm[nt](z + h)                       # (N, d) 잔차 + 정규화
        return out
