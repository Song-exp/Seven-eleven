"""DiffMG (Differentiable Meta-Graph Search) 관계 게이트.

역할: 엣지 타입(관계)별 중요도를 '연속 변수' α_r 로 두고 역전파로 학습.
Softmax 기반 생존 경쟁 → 성공 기여 관계 가중치 증폭, 노이즈 관계 도태.
학습 후 α_r = MD 대시보드용 XAI 메타관계 중요도 맵.

수학적 차원만 다룬다.
표기: R = 관계(엣지 타입) 수.
"""
from typing import Dict, List, Tuple

import torch
import torch.nn as nn

EdgeType = Tuple[str, str, str]


class DiffMGRelationGate(nn.Module):
    """엣지 타입별 미분 가능한 연속 어텐션 가중치.

    forward 는 관계 -> 스칼라 α_r (softmax 정규화) dict 를 반환.
    HGTLayer.forward(..., rel_alpha=) 로 전달되어 관계별 메시지를 게이팅.
    """

    def __init__(self, edge_types: List[EdgeType], temperature: float = 1.0) -> None:
        super().__init__()
        self.edge_types = list(edge_types)
        self.temperature = temperature
        # 관계별 학습 로짓 (R,) — 연속 변수
        self.logits = nn.Parameter(torch.zeros(len(self.edge_types)))

    def forward(self, hard: bool = False) -> Dict[EdgeType, torch.Tensor]:
        # Input: (없음, 파라미터만) -> Output: {et: scalar α_r}, Σα_r = 1
        if hard:  # Gumbel-style 이산 선택(메타그래프 pruning) — 추론 분석용
            weights = torch.softmax(self.logits / self.temperature, dim=0)
            idx = weights.argmax()
            onehot = torch.zeros_like(weights)
            onehot[idx] = 1.0
            weights = onehot + weights - weights.detach()  # straight-through
        else:
            weights = torch.softmax(self.logits / self.temperature, dim=0)  # (R,)
        return {et: weights[i] for i, et in enumerate(self.edge_types)}

    @torch.no_grad()
    def relation_importance(self) -> Dict[str, float]:
        """XAI: 관계별 정규화 중요도 (사람이 읽는 값)."""
        weights = torch.softmax(self.logits / self.temperature, dim=0)
        return {"__".join(et): float(weights[i]) for i, et in enumerate(self.edge_types)}
