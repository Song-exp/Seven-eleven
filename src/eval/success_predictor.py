"""성공 예측 추론 + 평가지표.

EVAL 결정: PR-AUC 주지표 + AUC-ROC 병기, F1 은 best-threshold.
불균형(양성 23.6%)에서 PR-AUC 가 민감하므로 early stopping 기준으로 사용.
"""
from typing import Dict

import numpy as np
import torch
from sklearn.metrics import (
    average_precision_score,
    roc_auc_score,
    precision_recall_curve,
    f1_score,
)


def compute_metrics(y_true: np.ndarray, y_prob: np.ndarray) -> Dict[str, float]:
    """y_true (N,), y_prob (N,) -> {pr_auc, auc_roc, f1, threshold}."""
    out: Dict[str, float] = {}
    # 단일 클래스만 있으면 AUC 정의 불가 → nan 처리
    if len(np.unique(y_true)) < 2:
        return {"pr_auc": float("nan"), "auc_roc": float("nan"),
                "f1": float("nan"), "threshold": 0.5}
    out["pr_auc"] = float(average_precision_score(y_true, y_prob))
    out["auc_roc"] = float(roc_auc_score(y_true, y_prob))
    # best-threshold F1
    prec, rec, thr = precision_recall_curve(y_true, y_prob)
    f1s = 2 * prec * rec / (prec + rec + 1e-12)
    best = int(np.nanargmax(f1s))
    # precision_recall_curve: thr 길이는 prec/rec 보다 1 짧음
    out["threshold"] = float(thr[min(best, len(thr) - 1)])
    out["f1"] = float(f1_score(y_true, (y_prob >= out["threshold"]).astype(int)))
    return out


@torch.no_grad()
def predict_proba(
    model: torch.nn.Module,
    edge_index_dict: Dict,
    has_promo: torch.Tensor = None,
    edge_attr_dict: Dict = None,
    insta_m30: torch.Tensor = None,
) -> torch.Tensor:
    """product 성공 확률 (P,) 반환."""
    model.eval()
    logits = model(edge_index_dict, has_promo=has_promo, insta_m30=insta_m30, edge_attr_dict=edge_attr_dict)
    return torch.sigmoid(logits)


@torch.no_grad()
def evaluate_mask(
    model: torch.nn.Module,
    edge_index_dict: Dict,
    has_promo: torch.Tensor,
    y: torch.Tensor,
    mask: torch.Tensor,
    edge_attr_dict: Dict = None,
    insta_m30: torch.Tensor = None,
) -> Dict[str, float]:
    """주어진 마스크(노드 subset)에 대한 평가지표."""
    prob = predict_proba(model, edge_index_dict, has_promo, edge_attr_dict, insta_m30)
    yt = y[mask].cpu().numpy()
    yp = prob[mask].cpu().numpy()
    return compute_metrics(yt, yp)
