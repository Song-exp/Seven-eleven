"""HIN-GNN 이중 최적화(Bi-level) 학습 루프.

src/train/.claude-rules.md 강제 규칙:
  - 네트워크 가중치 W 와 구조 가중치 α(DiffMG) 의 Optimizer 를 명확히 분리.
  - Step 1 (train_step): Train 데이터로 W 업데이트.
  - Step 2 (val_step):  Validation 데이터로 α 업데이트.
  - 두 업데이트의 backward/step 이 서로 간섭하지 않도록 격리(각 단계 전 양쪽 grad zero).

config(configs/train_config.yaml) 에서 모든 하이퍼파라미터 로드. 하드코딩 금지.
"""
import os
from typing import Dict

import torch
import torch.nn as nn

import yaml

from src.models.hin_gnn import HINGNN
from src.data_builder.build_hetero_data import build_graph, forward_edge_index_dict, forward_edge_attr_dict
from src.eval.success_predictor import evaluate_mask


def load_config(path: str = "configs/train_config.yaml") -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _split_params(model: HINGNN):
    """DiffMG α 파라미터와 그 외 W 파라미터 분리."""
    alpha_ids = {id(p) for g in model.gates for p in g.parameters()}
    alpha_params = [p for g in model.gates for p in g.parameters()]
    w_params = [p for p in model.parameters() if id(p) not in alpha_ids]
    return w_params, alpha_params


def train(config_path: str = "configs/train_config.yaml", save_dir: str = "checkpoints"):
    cfg = load_config(config_path)
    dev = ("cuda" if torch.cuda.is_available() else "cpu") \
        if cfg["train"]["device"] == "auto" else cfg["train"]["device"]
    device = torch.device(dev)

    # --- 그래프 로드 ---
    g = cfg["graph"]
    data, maps = build_graph(
        data_dir=g.get("data_dir", "data/processed/hin"),
        seed=cfg["split"]["seed"],
        ratios=tuple(cfg["split"]["ratios"]),
        include_offline_copurchase=g.get("include_offline_copurchase", False),
        include_quick_copurchase=g.get("include_quick_copurchase", False),
        lift_norm=g.get("lift_normalization", "log1p"),
        use_lift_weights=g.get("use_lift_weights", True),
        use_idf_keyword_weights=g.get("use_idf_keyword_weights", False),
        idf_normalization=g.get("idf_normalization", "max"),
        add_2hop_edges=g.get("add_2hop_edges", False),
        hop2_kw_min_shared=g.get("hop2_kw_min_shared", 5),
        hop2_ip_min_shared=g.get("hop2_ip_min_shared", 2),
        add_via_ip_edges=g.get("add_via_ip_edges", False),
        add_ipip_kw_edges=g.get("add_ipip_kw_edges", False),
        add_trend_kw_edges=g.get("add_trend_kw_edges", False),
    )
    edge_index_dict = {et: ei.to(device) for et, ei in forward_edge_index_dict(data).items()}
    raw_attrs = forward_edge_attr_dict(data)
    edge_attr_dict = {et: ea.to(device) for et, ea in raw_attrs.items()} if raw_attrs else None
    has_promo = data["product"].has_promo.to(device)
    insta_m30 = data["product"].insta_mention_30d.to(device)
    y = data["product"].y.to(device)
    train_mask = data["product"].train_mask.to(device)
    val_mask = data["product"].val_mask.to(device)

    # --- 모델 ---
    m = cfg["model"]
    model = HINGNN(
        node_types=cfg["graph"]["node_types"],
        edge_types=[tuple(e) for e in cfg["graph"]["edge_types"]],
        num_nodes={"product": data["product"].num_nodes,
                   "keyword": data["keyword"].num_nodes,
                   "ip": data["ip"].num_nodes},
        hidden_dim=m["hidden_dim"], num_layers=m["num_layers"],
        num_heads=m["num_heads"], dropout=m["dropout"],
        use_diffmg=m["use_diffmg_gate"], diffmg_temperature=m["diffmg_temperature"],
        product_aggr=cfg["node_feat"]["product_aggr"],
        use_has_promo=cfg["node_feat"]["use_has_promo_feature"],
        readout_hop_mode=m.get("readout_hop_mode", "final"),
    ).to(device)

    # --- 이중 Optimizer 분리 ---
    w_params, alpha_params = _split_params(model)
    t = cfg["train"]
    opt_w = torch.optim.Adam(w_params, lr=t["lr_w"], weight_decay=t["weight_decay_w"])
    opt_a = torch.optim.Adam(alpha_params, lr=t["lr_alpha"], weight_decay=t["weight_decay_alpha"]) \
        if alpha_params else None

    pos_weight = torch.tensor([cfg["label"]["pos_weight"]], device=device)
    criterion = nn.BCEWithLogitsLoss(pos_weight=pos_weight)

    best_metric, best_state, patience = -1.0, None, 0
    history = []
    for epoch in range(1, t["epochs"] + 1):
        model.train()

        # === Step 1: W 업데이트 (train 데이터) ===
        opt_w.zero_grad()
        if opt_a:
            opt_a.zero_grad()
        logits = model(edge_index_dict, has_promo=has_promo, insta_m30=insta_m30, edge_attr_dict=edge_attr_dict)
        loss_w = criterion(logits[train_mask], y[train_mask])
        loss_w.backward()
        opt_w.step()                                                # W 만 갱신

        # === Step 2: α 업데이트 (val 데이터) ===
        if opt_a:
            opt_w.zero_grad()
            opt_a.zero_grad()
            logits = model(edge_index_dict, has_promo=has_promo, insta_m30=insta_m30, edge_attr_dict=edge_attr_dict)
            loss_a = criterion(logits[val_mask], y[val_mask])
            loss_a.backward()
            opt_a.step()                                            # α 만 갱신

        # === 평가 / early stopping (val PR-AUC) ===
        val_m = evaluate_mask(model, edge_index_dict, has_promo, y, val_mask, edge_attr_dict, insta_m30=insta_m30)
        loss_w_val = float(loss_w.detach())
        history.append({"epoch": epoch, "loss_w": loss_w_val, **val_m})
        cur = val_m[cfg["eval"]["primary_metric"]]
        if cur > best_metric:
            best_metric, patience = cur, 0
            best_state = {k: v.detach().cpu().clone() for k, v in model.state_dict().items()}
        else:
            patience += 1
        if epoch % 10 == 0 or epoch == 1:
            print(f"[{epoch:03d}] loss_w={loss_w_val:.4f} "
                  f"val_pr_auc={val_m['pr_auc']:.4f} val_auc={val_m['auc_roc']:.4f}")
        if patience >= t["early_stopping_patience"]:
            print(f"early stop @ {epoch} (best {cfg['eval']['primary_metric']}={best_metric:.4f})")
            break

    if best_state is not None:
        model.load_state_dict(best_state)
    os.makedirs(save_dir, exist_ok=True)
    torch.save({"model": model.state_dict(), "maps": maps, "config": cfg},
               os.path.join(save_dir, "hin_gnn_best.pt"))

    # 최종 test 평가 (학습과 동일하게 edge_attr_dict 적용 — train/test 일관성)
    test_mask = data["product"].test_mask.to(device)
    test_m = evaluate_mask(model, edge_index_dict, has_promo, y, test_mask, edge_attr_dict, insta_m30=insta_m30)
    print("TEST:", {k: round(v, 4) for k, v in test_m.items()})
    return model, history, test_m


if __name__ == "__main__":
    train()
