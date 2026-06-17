"""HIN-GNN 실험 오케스트레이션 유틸리티.

사용 패턴:
    from experiments.exp_utils import run_experiment, load_experiment, compare_experiments

    # 학습 + export 한번에
    results = run_experiment("experiments/configs/exp01_baseline.yaml", "exp01_baseline")

    # 결과 로드 (재실행 없이)
    results = load_experiment("exp01_baseline")

    # 여러 실험 비교
    df = compare_experiments(["exp01_baseline", "exp02_alpha_tuning", "exp03_complement_edges"])
"""
import copy
import json
import os
from typing import Dict, List, Optional

import pandas as pd
import yaml

RESULTS_ROOT = "experiments/results"
SAMPLE_SEEDS = ["마라", "로제", "흑임자", "단백질", "위스키", "딸기"]


# ─── config 유틸 ─────────────────────────────────────────────────────────────

def _load_config(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _apply_overrides(cfg: dict, overrides: Optional[Dict]) -> dict:
    """점(.)으로 구분된 중첩 키 패치. 예: {"train.lr_alpha": 0.02}"""
    if not overrides:
        return cfg
    cfg = copy.deepcopy(cfg)
    for key, val in overrides.items():
        keys = key.split(".")
        d = cfg
        for k in keys[:-1]:
            d = d[k]
        d[keys[-1]] = val
    return cfg


# ─── 핵심 함수 ───────────────────────────────────────────────────────────────

def run_experiment(
    config_path: str,
    exp_name: str,
    overrides: Optional[Dict] = None,
    force: bool = False,
) -> Dict:
    """config → train → eval → export.

    Parameters
    ----------
    config_path : str
        실험 기반 YAML 경로 (experiments/configs/expXX_*.yaml)
    exp_name : str
        실험 식별자. experiments/results/{exp_name}/ 에 결과 저장.
    overrides : dict, optional
        config 일부를 덮어쓸 키:값. 점 표기 사용 (예: {"train.lr_alpha": 0.02})
    force : bool
        True 이면 이미 결과가 있어도 재학습.

    Returns
    -------
    dict
        {"metrics": {split: {...}}, "rel_importance": [...], "recs": {seed: [...]}, "history": [...]}
    """
    from src.train.trainer import train
    from src.eval.export_results import export_experiment

    save_dir = os.path.join(RESULTS_ROOT, exp_name)
    ckpt_path = os.path.join(save_dir, "hin_gnn_best.pt")

    if os.path.exists(ckpt_path) and not force:
        print(f"[{exp_name}] 기존 체크포인트 발견 → export만 수행 (재학습 건너뜀). force=True로 재학습 가능.")
        results = export_experiment(ckpt_path, out_dir=save_dir, sample_seeds=SAMPLE_SEEDS)
        results["history"] = []
        return results

    cfg = _apply_overrides(_load_config(config_path), overrides)
    os.makedirs(save_dir, exist_ok=True)

    # 실제 사용된 config 보존 (재현용)
    with open(os.path.join(save_dir, "config_used.yaml"), "w", encoding="utf-8") as f:
        yaml.dump(cfg, f, allow_unicode=True, default_flow_style=False)

    # trainer는 yaml 경로를 받으므로 임시 파일 경유
    tmp_cfg = os.path.join(save_dir, "_tmp_config.yaml")
    with open(tmp_cfg, "w", encoding="utf-8") as f:
        yaml.dump(cfg, f, allow_unicode=True, default_flow_style=False)

    model, history, test_metrics = train(config_path=tmp_cfg, save_dir=save_dir)

    results = export_experiment(ckpt_path, out_dir=save_dir, sample_seeds=SAMPLE_SEEDS)
    results["history"] = history

    with open(os.path.join(save_dir, "metrics.json"), "w", encoding="utf-8") as f:
        json.dump(results["metrics"], f, ensure_ascii=False, indent=2)

    os.remove(tmp_cfg)
    print(f"[{exp_name}] 완료 → {save_dir}")
    return results


def load_experiment(exp_name: str) -> Dict:
    """experiments/results/{exp_name}/ 에서 결과 복원 (재학습 없이)."""
    save_dir = os.path.join(RESULTS_ROOT, exp_name)
    metrics_path = os.path.join(save_dir, "metrics.json")
    if not os.path.exists(metrics_path):
        raise FileNotFoundError(
            f"실험 결과 없음: {metrics_path}\n"
            f"먼저 run_experiment('{exp_name}') 를 실행하세요."
        )
    with open(metrics_path, encoding="utf-8") as f:
        metrics = json.load(f)

    rel_imp = None
    rel_path = os.path.join(save_dir, "relation_importance.json")
    if os.path.exists(rel_path):
        with open(rel_path, encoding="utf-8") as f:
            rel_imp = json.load(f)

    cfg = None
    cfg_path = os.path.join(save_dir, "config_used.yaml")
    if os.path.exists(cfg_path):
        with open(cfg_path, encoding="utf-8") as f:
            cfg = yaml.safe_load(f)

    return {
        "exp_name": exp_name,
        "metrics": metrics,
        "rel_importance": rel_imp,
        "cfg": cfg,
    }


def compare_experiments(exp_names: List[str]) -> pd.DataFrame:
    """여러 실험의 train/val/test 지표를 한 DataFrame으로 비교.

    Returns
    -------
    pd.DataFrame
        컬럼: exp, train_pr_auc, train_auc_roc, val_pr_auc, val_auc_roc, test_pr_auc, test_auc_roc, test_f1
    """
    rows = []
    for name in exp_names:
        try:
            res = load_experiment(name)
            row = {"exp": name}
            for split in ["train", "val", "test"]:
                m = res["metrics"].get(split, {})
                for metric in ["pr_auc", "auc_roc", "f1"]:
                    row[f"{split}_{metric}"] = round(m.get(metric, float("nan")), 4)
            rows.append(row)
        except FileNotFoundError:
            rows.append({"exp": name, "note": "결과 없음"})
    df = pd.DataFrame(rows)
    # 랜덤 기준선 표시용 행 추가
    baseline_row = {"exp": "[random baseline]", "test_pr_auc": 0.236}
    return pd.concat([df, pd.DataFrame([baseline_row])], ignore_index=True)


# ─── 시각화 ──────────────────────────────────────────────────────────────────

def plot_alpha_heatmap(exp_name: str, ax=None):
    """DiffMG α_r 층별 히트맵.

    균등(≈1/R) → 게이팅 미분화 상태. 값 차이가 클수록 관계 선택 작동.
    """
    import matplotlib.pyplot as plt
    import numpy as np

    res = load_experiment(exp_name)
    rel_imp = res.get("rel_importance")
    if not rel_imp:
        print(f"[{exp_name}] 관계 중요도 데이터 없음 (use_diffmg_gate=false?)")
        return

    relations = list(rel_imp[0].keys())
    matrix = np.array([[layer.get(r, 0.0) for r in relations] for layer in rel_imp])

    if ax is None:
        _, ax = plt.subplots(figsize=(max(10, len(relations) * 0.9), 1.5 + len(rel_imp)))

    im = ax.imshow(matrix, aspect="auto", cmap="YlOrRd", vmin=0.0, vmax=matrix.max())
    ax.set_xticks(range(len(relations)))
    ax.set_xticklabels(relations, rotation=45, ha="right", fontsize=8)
    ax.set_yticks(range(len(rel_imp)))
    ax.set_yticklabels([f"layer {i}" for i in range(len(rel_imp))])
    ax.set_title(f"DiffMG α_r (관계 중요도) — {exp_name}", fontsize=11)
    plt.colorbar(im, ax=ax, shrink=0.8)
    plt.tight_layout()
    return ax


def plot_training_curve(history: list, ax=None):
    """에폭별 val PR-AUC 학습 곡선."""
    import matplotlib.pyplot as plt

    if not history:
        print("history 없음 (load_experiment 에는 history 미저장)")
        return
    epochs = [r["epoch"] for r in history]
    pr_aucs = [r["pr_auc"] for r in history]

    if ax is None:
        _, ax = plt.subplots(figsize=(8, 4))
    ax.plot(epochs, pr_aucs, lw=1.5)
    ax.axhline(0.236, ls="--", color="gray", lw=1, label="random baseline")
    ax.set_xlabel("epoch")
    ax.set_ylabel("val PR-AUC")
    ax.set_title("학습 곡선")
    ax.legend()
    plt.tight_layout()
    return ax


def print_metrics_table(results: Dict):
    """metrics dict를 보기 좋게 출력."""
    print(f"\n{'split':<8} {'PR-AUC':>8} {'AUC-ROC':>9} {'F1@best':>9} {'threshold':>10}")
    print("-" * 48)
    for split in ["train", "val", "test"]:
        m = results["metrics"].get(split, {})
        print(f"{split:<8} {m.get('pr_auc', float('nan')):>8.4f} "
              f"{m.get('auc_roc', float('nan')):>9.4f} "
              f"{m.get('f1', float('nan')):>9.4f} "
              f"{m.get('threshold', float('nan')):>10.4f}")
    print(f"\n{'[random]':<8} {'0.2360':>8}")


def print_recommendations(results: Dict, seeds: Optional[List[str]] = None):
    """순회 추천 결과 출력."""
    recs = results.get("recs", {})
    for seed, lst in recs.items():
        if seeds and seed not in seeds:
            continue
        if lst is None:
            print(f"  {seed}: (그래프에 없음)")
        else:
            top = ", ".join(f"{kw}({sc:.4f})" for kw, sc in lst[:10])
            print(f"  {seed} → {top}")
