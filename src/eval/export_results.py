"""학습된 체크포인트 → 결과 영속화.

생성물:
  1) data/processed/hin/weighted_product_keyword_edges.parquet
        순회용 가중 네트워크 (product↔keyword, 학습된 어텐션 + product 성공확률)
  2) data/processed/hin/learned_product_scores.parquet
        product별 예측 성공확률 + 임베딩 norm (해석용)
  3) data/processed/hin/relation_importance.json
        층별 DiffMG α_r (XAI 메타관계 중요도)
  4) docs/hin_gnn_results.md
        지표(train/val/test) + α_r + 추천 샘플 리포트

실행: python -m src.eval.export_results
"""
import json
import os

import pandas as pd
import torch

from src.data_builder.build_hetero_data import build_graph, forward_edge_index_dict, forward_edge_attr_dict
from src.models.hin_gnn import HINGNN
from src.eval.success_predictor import evaluate_mask, predict_proba
from src.eval.recommend import (
    recommend_combinations,
    export_relation_importance,
    export_weighted_kw_edges,
)

CKPT = "checkpoints/hin_gnn_best.pt"
HIN_DIR = "data/processed/hin"
SAMPLE_SEEDS = ["마라", "로제", "흑임자", "단백질", "위스키", "딸기"]


def export_experiment(
    ckpt_path: str,
    out_dir: str,
    data_dir: str = "data/processed/hin",
    sample_seeds: list = None,
) -> dict:
    """체크포인트 → 지표·가중엣지·추천샘플 export. out_dir 에 결과 저장.

    반환: {"metrics": {split: {...}}, "rel_importance": [...], "recs": {seed: [...]}}
    """
    if sample_seeds is None:
        sample_seeds = SAMPLE_SEEDS
    dev = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    ckpt = torch.load(ckpt_path, weights_only=False)
    cfg, maps = ckpt["config"], ckpt["maps"]

    g = cfg["graph"]
    data, _ = build_graph(
        data_dir=data_dir,
        seed=cfg["split"]["seed"],
        ratios=tuple(cfg["split"]["ratios"]),
        include_offline_copurchase=g.get("include_offline_copurchase", False),
        include_quick_copurchase=g.get("include_quick_copurchase", False),
        lift_norm=g.get("lift_normalization", "log1p"),
        use_lift_weights=g.get("use_lift_weights", True),
    )
    eidx = {et: ei.to(dev) for et, ei in forward_edge_index_dict(data).items()}
    raw_attrs = forward_edge_attr_dict(data)
    eattr = {et: ea.to(dev) for et, ea in raw_attrs.items()} if raw_attrs else None
    hp = data["product"].has_promo.to(dev)
    im30 = data["product"].insta_mention_30d.to(dev)
    y = data["product"].y.to(dev)
    model = _rebuild(cfg, data, dev)
    model.load_state_dict(ckpt["model"])
    model.eval()

    metrics = {
        "train": evaluate_mask(model, eidx, hp, y, data["product"].train_mask.to(dev), eattr, insta_m30=im30),
        "val":   evaluate_mask(model, eidx, hp, y, data["product"].val_mask.to(dev),   eattr, insta_m30=im30),
        "test":  evaluate_mask(model, eidx, hp, y, data["product"].test_mask.to(dev),  eattr, insta_m30=im30),
    }

    os.makedirs(out_dir, exist_ok=True)
    wdf = export_weighted_kw_edges(model, eidx, maps, hp, eattr)
    wdf.to_parquet(os.path.join(out_dir, "weighted_product_keyword_edges.parquet"), index=False)

    prob = predict_proba(model, eidx, hp, eattr, insta_m30=im30).cpu().numpy()
    emb = model.product_embeddings().detach().cpu()
    pdf = pd.DataFrame({
        "ITEM_CD": maps["product_ids"],
        "ITEM_NM": maps["product_names"],
        "pred_success_prob": prob,
        "y_true": y.cpu().numpy().astype(int),
        "emb_norm": emb.norm(dim=1).numpy(),
    })
    pdf.to_parquet(os.path.join(out_dir, "learned_product_scores.parquet"), index=False)

    rel_imp = export_relation_importance(model)
    with open(os.path.join(out_dir, "relation_importance.json"), "w", encoding="utf-8") as f:
        json.dump(rel_imp, f, ensure_ascii=False, indent=2)

    recs = {}
    for s in sample_seeds:
        recs[s] = recommend_combinations(model, eidx, maps, [s], hp, eattr, top_k=10) \
            if s in maps["k2i"] else None

    _write_md(cfg, metrics, rel_imp, recs, wdf, pdf,
              md_path=os.path.join(out_dir, "report.md"))
    return {"metrics": metrics, "rel_importance": rel_imp, "recs": recs}


def _rebuild(cfg, data, dev):
    m = cfg["model"]
    model = HINGNN(
        cfg["graph"]["node_types"], [tuple(e) for e in cfg["graph"]["edge_types"]],
        {"product": data["product"].num_nodes, "keyword": data["keyword"].num_nodes,
         "ip": data["ip"].num_nodes},
        m["hidden_dim"], m["num_layers"], m["num_heads"], m["dropout"],
        m["use_diffmg_gate"], m["diffmg_temperature"],
        cfg["node_feat"]["product_aggr"], cfg["node_feat"]["use_has_promo_feature"],
    ).to(dev)
    return model


def main():
    dev = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    ckpt = torch.load(CKPT, weights_only=False)
    cfg, maps = ckpt["config"], ckpt["maps"]

    data, _ = build_graph(seed=cfg["split"]["seed"], ratios=tuple(cfg["split"]["ratios"]))
    eidx = {et: ei.to(dev) for et, ei in forward_edge_index_dict(data).items()}
    hp = data["product"].has_promo.to(dev)
    im30 = data["product"].insta_mention_30d.to(dev)
    y = data["product"].y.to(dev)
    model = _rebuild(cfg, data, dev)
    model.load_state_dict(ckpt["model"]); model.eval()

    # --- 지표 ---
    metrics = {
        "train": evaluate_mask(model, eidx, hp, y, data["product"].train_mask.to(dev), insta_m30=im30),
        "val": evaluate_mask(model, eidx, hp, y, data["product"].val_mask.to(dev), insta_m30=im30),
        "test": evaluate_mask(model, eidx, hp, y, data["product"].test_mask.to(dev), insta_m30=im30),
    }

    # --- 1) 가중 네트워크 ---
    os.makedirs(HIN_DIR, exist_ok=True)
    wdf = export_weighted_kw_edges(model, eidx, maps, hp)
    wpath = os.path.join(HIN_DIR, "weighted_product_keyword_edges.parquet")
    wdf.to_parquet(wpath, index=False)

    # --- 2) product 성공확률 + 임베딩 norm ---
    prob = predict_proba(model, eidx, hp, insta_m30=im30).cpu().numpy()
    emb = model.product_embeddings().detach().cpu()
    pdf = pd.DataFrame({
        "ITEM_CD": maps["product_ids"],
        "ITEM_NM": maps["product_names"],
        "pred_success_prob": prob,
        "y_true": y.cpu().numpy().astype(int),
        "emb_norm": emb.norm(dim=1).numpy(),
    })
    pdf.to_parquet(os.path.join(HIN_DIR, "learned_product_scores.parquet"), index=False)

    # --- 3) 관계 중요도 ---
    rel_imp = export_relation_importance(model)
    with open(os.path.join(HIN_DIR, "relation_importance.json"), "w", encoding="utf-8") as f:
        json.dump(rel_imp, f, ensure_ascii=False, indent=2)

    # --- 4) 추천 샘플 ---
    recs = {}
    for s in SAMPLE_SEEDS:
        if s in maps["k2i"]:
            recs[s] = recommend_combinations(model, eidx, maps, [s], hp, top_k=10)
        else:
            recs[s] = None

    _write_md(cfg, metrics, rel_imp, recs, wdf, pdf)  # docs/hin_gnn_results.md (기본)
    print("saved:", wpath)
    print("metrics:", {k: round(v["pr_auc"], 4) for k, v in metrics.items()})


def _write_md(cfg, metrics, rel_imp, recs, wdf, pdf, md_path="docs/hin_gnn_results.md"):
    L = []
    L.append("# HIN-GNN 학습 결과 리포트\n")
    L.append("> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`")
    L.append("> 재현: `python -m src.eval.export_results`\n")
    m = cfg["model"]
    L.append("## 1. 설정 (표준 프리셋)\n")
    L.append(f"- hidden_dim={m['hidden_dim']}, num_layers={m['num_layers']}, "
             f"num_heads={m['num_heads']}, dropout={m['dropout']}, diffmg={m['use_diffmg_gate']}")
    L.append(f"- loss=weighted BCE(pos_weight={cfg['label']['pos_weight']}), "
             f"split={cfg['split']['ratios']} (계층화), optim=Adam(W lr={cfg['train']['lr_w']}, "
             f"α lr={cfg['train']['lr_alpha']}), early stop=val {cfg['eval']['primary_metric']}\n")

    L.append("## 2. 성능 지표\n")
    L.append("| split | PR-AUC | AUC-ROC | F1@best | threshold |")
    L.append("|---|---|---|---|---|")
    for k in ["train", "val", "test"]:
        v = metrics[k]
        L.append(f"| {k} | {v['pr_auc']:.4f} | {v['auc_roc']:.4f} | {v['f1']:.4f} | {v['threshold']:.4f} |")
    L.append("\n> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.\n")

    L.append("## 3. DiffMG 관계 중요도 α_r (XAI)\n")
    for i, ri in enumerate(rel_imp):
        top = sorted(ri.items(), key=lambda x: -x[1])
        L.append(f"**layer {i}**")
        for k, vv in top:
            L.append(f"- `{k}`: {vv:.4f}")
        L.append("")
    L.append("> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.\n")

    L.append("## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)\n")
    for s, r in recs.items():
        if r is None:
            L.append(f"- **{s}**: (그래프에 없음 — 정규화로 다른 형태일 수 있음)")
        else:
            L.append(f"- **{s}** → " + ", ".join(f"{kw}({sc:.5f})" for kw, sc in r))
    L.append("")

    L.append("## 5. 영속화 산출물\n")
    L.append(f"- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 ({len(wdf):,}행)")
    L.append(f"- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm ({len(pdf):,}행)")
    L.append("- `data/processed/hin/relation_importance.json` — 층별 α_r")
    L.append("- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치\n")

    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(L))


if __name__ == "__main__":
    main()
