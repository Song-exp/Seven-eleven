"""HGT (Heterogeneous Graph Transformer) 메시지 레이어.

역할: 이기종 엣지 타입별 독립 처리. 노드 타입별 Q/K/V 투영 + 엣지 타입별
W_att·W_msg 행렬로 파라미터를 격리하여 1/N 균등 혼합(over-smoothing)을 차단.

이 레이어는 '메시지 생성·집계'까지만 담당한다(타입 격리 어텐션).
노드 상태 갱신(Bi-Interaction)은 KGAT(kgat_layer.py)가 맡는다.

수학적 차원만 다룬다(데이터 전처리 컨텍스트 배제).
표기: N_s/N_t = src/dst 노드 수, E = 엣지 수, H = heads, d_k = hidden//H.
"""
import math
from typing import Dict, List, Tuple

import torch
import torch.nn as nn
from torch_geometric.utils import softmax, scatter

EdgeType = Tuple[str, str, str]


def _rk(et: EdgeType) -> str:
    """edge type tuple -> ParameterDict/ModuleDict 용 문자열 키."""
    return "__".join(et)


class HGTLayer(nn.Module):
    """타입 격리 어텐션 메시지 패싱 한 층.

    forward 는 목적지 노드 타입별 '집계된 이웃 메시지' dict 를 반환한다.
    (잔차/업데이트 없음 — KGAT 가 h 와 함께 Bi-Interaction 으로 갱신)
    """

    def __init__(
        self,
        hidden_dim: int,
        node_types: List[str],
        edge_types: List[EdgeType],
        num_heads: int = 4,
        dropout: float = 0.3,
    ) -> None:
        super().__init__()
        assert hidden_dim % num_heads == 0, "hidden_dim 은 num_heads 로 나눠떨어져야 함"
        self.hidden_dim = hidden_dim
        self.num_heads = num_heads
        self.d_k = hidden_dim // num_heads
        self.edge_types = edge_types
        self.sqrt_dk = math.sqrt(self.d_k)

        # 노드 타입별 Q/K/V 투영 (타입 격리)
        self.q_lin = nn.ModuleDict({nt: nn.Linear(hidden_dim, hidden_dim) for nt in node_types})
        self.k_lin = nn.ModuleDict({nt: nn.Linear(hidden_dim, hidden_dim) for nt in node_types})
        self.v_lin = nn.ModuleDict({nt: nn.Linear(hidden_dim, hidden_dim) for nt in node_types})
        # 목적지 타입별 메시지 통합 투영
        self.a_lin = nn.ModuleDict({nt: nn.Linear(hidden_dim, hidden_dim) for nt in node_types})

        # 엣지 타입별 관계 행렬 (heads, d_k, d_k) + prior scalar mu
        self.w_att = nn.ParameterDict()
        self.w_msg = nn.ParameterDict()
        self.mu = nn.ParameterDict()
        for et in edge_types:
            k = _rk(et)
            self.w_att[k] = nn.Parameter(torch.empty(num_heads, self.d_k, self.d_k))
            self.w_msg[k] = nn.Parameter(torch.empty(num_heads, self.d_k, self.d_k))
            self.mu[k] = nn.Parameter(torch.ones(num_heads))
            nn.init.xavier_uniform_(self.w_att[k])
            nn.init.xavier_uniform_(self.w_msg[k])

        self.drop = nn.Dropout(dropout)
        # XAI: 마지막 forward 의 엣지별 어텐션(평균 head) 저장
        self.last_attention: Dict[str, torch.Tensor] = {}

    def forward(
        self,
        x_dict: Dict[str, torch.Tensor],
        edge_index_dict: Dict[EdgeType, torch.Tensor],
        rel_alpha: Dict[EdgeType, torch.Tensor] = None,
        edge_attr_dict: Dict[EdgeType, torch.Tensor] = None,
    ) -> Dict[str, torch.Tensor]:
        # Input:  x_dict[nt] (N_nt, hidden) , edge_index_dict[et] (2, E)
        # Output: agg_dict[nt] (N_nt, hidden)  목적지 타입별 집계 이웃 메시지
        H, d_k = self.num_heads, self.d_k
        self.last_attention = {}

        # 목적지 타입별 누적 버퍼 (N_t, H, d_k)
        agg: Dict[str, torch.Tensor] = {
            nt: x_dict[nt].new_zeros(x_dict[nt].size(0), H, d_k) for nt in x_dict
        }

        for et, edge_index in edge_index_dict.items():
            s_type, _, t_type = et
            key = _rk(et)
            if key not in self.w_att:  # 역방향 등 미등록 관계는 별도 키 필요
                continue
            src, dst = edge_index[0], edge_index[1]              # (E,), (E,)

            # 노드 타입별 투영 후 head 분해
            k = self.k_lin[s_type](x_dict[s_type]).view(-1, H, d_k)   # (N_s, H, d_k)
            q = self.q_lin[t_type](x_dict[t_type]).view(-1, H, d_k)   # (N_t, H, d_k)
            v = self.v_lin[s_type](x_dict[s_type]).view(-1, H, d_k)   # (N_s, H, d_k)

            k_e = k[src]                                              # (E, H, d_k)
            q_e = q[dst]                                              # (E, H, d_k)
            v_e = v[src]                                              # (E, H, d_k)

            # 엣지 타입별 W_att 적용: (E,H,d_k) x (H,d_k,d_k) -> (E,H,d_k)
            k_e = torch.einsum("ehk,hkd->ehd", k_e, self.w_att[key])
            # 어텐션 스코어: (E,H)
            score = (q_e * k_e).sum(dim=-1) * self.mu[key] / self.sqrt_dk
            # 목적지 노드별 softmax (이 관계 내 incoming edge 기준)
            alpha = softmax(score, dst, num_nodes=x_dict[t_type].size(0))  # (E, H)
            alpha = self.drop(alpha)
            self.last_attention[key] = alpha.mean(dim=1).detach()    # (E,) head 평균

            # 엣지 타입별 W_msg 적용: (E,H,d_k) x (H,d_k,d_k) -> (E,H,d_k)
            msg = torch.einsum("ehk,hkd->ehd", v_e, self.w_msg[key])
            msg = msg * alpha.unsqueeze(-1)                          # (E, H, d_k)

            # Lift edge_attr 가중치 곱 — (E,) → (E, 1, 1) broadcast
            if edge_attr_dict is not None and et in edge_attr_dict:
                lift_w = edge_attr_dict[et].to(msg.device)           # (E,)
                msg = msg * lift_w.view(-1, 1, 1)

            # DiffMG 관계 게이트(α_r) 곱 — 스칼라 broadcast
            if rel_alpha is not None and et in rel_alpha:
                msg = msg * rel_alpha[et]

            # 목적지로 합산 집계
            agg[t_type] = agg[t_type] + scatter(
                msg, dst, dim=0, dim_size=x_dict[t_type].size(0), reduce="sum"
            )                                                        # (N_t, H, d_k)

        # head 결합 후 타입별 통합 투영
        out: Dict[str, torch.Tensor] = {}
        for nt, a in agg.items():
            a = a.reshape(a.size(0), self.hidden_dim)                # (N_t, hidden)
            out[nt] = self.a_lin[nt](a)                              # (N_t, hidden)
        return out
