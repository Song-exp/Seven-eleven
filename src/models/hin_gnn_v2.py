"""HIN-GNN v2 — 멀티태스크 (성공예측 + 동반구매 link-prediction).

설계: docs/final_model_leakfree_switch_plan.md §9-1.
동반구매를 **입력 엣지(누수)** 가 아니라 **보조 예측 타깃(비누수)** 으로 전환.
공유 임베딩이 content→동반구매 잠재력을 학습 → "궁합 좋은 속성 = 잘 팔림"의 적법한 forward 추론.

HINGNN을 상속: 주 forward(성공 로짓)는 그대로, aux 링크예측 헤드만 추가.
누수 차단: aux 양성/음성 쌍은 **train 제품에만** 한정(학습 스크립트가 보장).
"""
from __future__ import annotations

from typing import List, Tuple

import torch
import torch.nn as nn

from .hin_gnn import HINGNN

EdgeType = Tuple[str, str, str]


class HINGNNv2(HINGNN):
    """주 임무(성공) + 보조 임무(동반구매 link-pred) 멀티태스크."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        d = self.hidden_dim
        # aux 링크예측: bilinear  score(p,q) = proj(e_p)·e_q
        self.aux_proj = nn.Linear(d, d, bias=False)
        nn.init.xavier_uniform_(self.aux_proj.weight)

    def aux_link_logit(self, pairs: torch.Tensor) -> torch.Tensor:
        """pairs (2, E) 제품 인덱스 → 동반구매 링크 로짓 (E,).  forward 후 호출."""
        emb = self.product_embeddings()          # (P, d)  마지막 forward 캐시
        ep = self.aux_proj(emb[pairs[0]])        # (E, d)
        eq = emb[pairs[1]]                        # (E, d)
        return (ep * eq).sum(dim=-1)             # (E,)


def dropedge(edge_index_dict, p: float, generator: torch.Generator = None):
    """DropEdge — 학습 시 각 엣지를 확률 p로 무작위 드롭(GNN 정규화).  추론 시 p=0."""
    if p <= 0:
        return edge_index_dict
    out = {}
    for et, ei in edge_index_dict.items():
        E = ei.size(1)
        if E == 0:
            out[et] = ei
            continue
        keep = torch.rand(E, device=ei.device, generator=generator) >= p
        out[et] = ei[:, keep]
    return out
