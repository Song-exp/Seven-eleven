"""v2 승격 결정 헬퍼 — 운영점(생존율 동기화) 비교 + artifact export.

operating_point: 예측양성률=base_rate 지점의 THR·P·R·F1 (exp47 0.666과 직접 비교).
export_v2: 체크포인트 + scores + metrics를 results dir로 저장 (서빙 후보 동결).
"""
from __future__ import annotations

import json
import os

import numpy as np
import pandas as pd
import torch


def operating_point(prob: np.ndarray, y: np.ndarray, base_rate: float = None) -> dict:
    """예측양성률을 base_rate(실제 생존율)로 맞춘 운영점의 THR·P·R·F1."""
    base = float(y.mean()) if base_rate is None else base_rate
    thr = float(np.quantile(prob, 1 - base))
    pred = (prob >= thr).astype(int)
    TP = int(((pred == 1) & (y == 1)).sum()); FP = int(((pred == 1) & (y == 0)).sum())
    FN = int(((pred == 0) & (y == 1)).sum())
    P = TP / (TP + FP) if TP + FP else 0.0
    R = TP / (TP + FN) if TP + FN else 0.0
    F1 = 2 * P * R / (P + R) if P + R else 0.0
    return dict(thr=round(thr, 4), predpos=round(float(pred.mean()), 4),
                precision=round(P, 4), recall=round(R, 4), f1=round(F1, 4),
                TP=TP, FP=FP, FN=FN)


def compare_models(v2_prob, v2_y, test_mask=None,
                   exp47_scores_path="experiments/results/exp47_no_copurchase/learned_product_scores.parquet"):
    """v2 vs exp47 운영점 비교 — **test-only**(held-out)가 기본.

    ⚠ full-set 비교는 train(70%) 과적합이 운영점 F1을 부풀려 더 과적합한 모델을 유리하게 만듦.
       모델별 자기 임계값(예측양성률=base)으로 test에서만 비교해야 공정.
    test_mask=None이면 full(비권장, 경고).
    """
    s47 = pd.read_parquet(exp47_scores_path)
    p47, y47 = s47["pred_success_prob"].values, s47["y_true"].values.astype(int)
    if test_mask is not None:
        op_v2 = operating_point(v2_prob[test_mask], v2_y[test_mask])
        op_47 = operating_point(p47[test_mask], y47[test_mask])
        scope = "TEST-only(held-out)"
    else:
        op_v2 = operating_point(v2_prob, v2_y); op_47 = operating_point(p47, y47)
        scope = "FULL(⚠train 오염)"
    df = pd.DataFrame({"exp47": op_47, "v2_sweepA": op_v2}).T
    df["scope"] = scope
    return df, op_v2, op_47


def export_v2(model, ctx, metrics: dict, out_dir: str, op: dict) -> str:
    """v2 체크포인트·scores·metrics 동결 (서빙 후보 results dir)."""
    os.makedirs(out_dir, exist_ok=True)
    cfg = dict(ctx["base_config"])
    cfg = json.loads(json.dumps(cfg))  # deep copy
    cfg["model"] = dict(cfg["model"]); cfg["model"]["hidden_dim"] = ctx["cfg"]["hidden_dim"]
    cfg["model"]["model_class"] = "HINGNNv2"
    cfg["graph"]["edge_types"] = [list(et) for et in ctx["edge_types"]]
    cfg["graph"]["include_offline_copurchase"] = False
    cfg["graph"]["include_quick_copurchase"] = False
    cfg["graph"]["add_basket_comp_edges"] = True
    cfg["graph"]["hop2_kw_idf"] = ctx["cfg"].get("hop2_kw_idf", False)        # C안 IDF sim_kw (engine 재로드 정합)
    cfg["graph"]["hop2_kw_idf_tau"] = ctx["cfg"].get("hop2_kw_idf_tau", 1.0)
    cfg["_note"] = "Model v2 sweepA — leak-free 멀티태스크. 서빙 로더는 HINGNNv2+basket_comp 어댑터 필요."
    torch.save({"model": model.state_dict(), "maps": ctx["maps"], "config": cfg},
               os.path.join(out_dir, "hin_gnn_best.pt"))
    pd.DataFrame({
        "ITEM_CD": ctx["maps"]["product_ids"], "ITEM_NM": ctx["maps"]["product_names"],
        "pred_success_prob": ctx["prob_full"], "y_true": ctx["y"],
    }).to_parquet(os.path.join(out_dir, "learned_product_scores.parquet"), index=False)
    with open(os.path.join(out_dir, "metrics.json"), "w", encoding="utf-8") as f:
        json.dump({k: {kk: round(float(vv), 4) for kk, vv in v.items()} for k, v in metrics.items()
                   if isinstance(v, dict)} | {"operating_point": op}, f, ensure_ascii=False, indent=2)
    with open(os.path.join(out_dir, "config_used.yaml"), "w", encoding="utf-8") as f:
        import yaml
        yaml.safe_dump(cfg, f, allow_unicode=True)
    return out_dir


def decision(metrics, op_v2, op_47, leak_free: bool, gap_target=0.10):
    """승격 판정: test PR-AUC 초과 + **test 운영점 F1 동등이상** + leak-free.

    op_v2/op_47는 **test-only** 운영점이어야 함(compare_models test_mask 전달).
    """
    pr_win = metrics["test"]["pr_auc"] > 0.5699
    f1_ok = op_v2["f1"] >= op_47["f1"] - 0.005       # test 운영점 F1 동등 이상
    gap_ok = metrics["gap"] < gap_target             # 참고용(필수 아님)
    promote = pr_win and f1_ok and leak_free
    return dict(promote=promote, pr_win=pr_win, f1_ok=f1_ok, leak_free=leak_free,
                gap_ok=gap_ok, gap=round(metrics["gap"], 3),
                verdict=("✅ 승격 권장" if promote else "⏸ 보류"))
