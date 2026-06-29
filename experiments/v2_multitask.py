"""Model v2 — 멀티태스크 + 키워드 보완 엣지 + 정규화 (leak-free 장바구니 복원).

설계: docs/final_model_leakfree_switch_plan.md §9.
  · 접근 2: 동반구매 = 보조 link-prediction 타깃 (train 쌍만 → 누수 차단)
  · 접근 3: keyword__basket_comp__keyword 보완 엣지 (graph 구조 주입)
  · §9-6: dropout/weight_decay/hidden_dim/DropEdge/aux λ 정규화

실행: python -m experiments.v2_multitask            (스모크: EPOCHS 작게)
      python -m experiments.v2_multitask --full     (풀 학습)
스윕: CFG 딕셔너리 조정.
"""
from __future__ import annotations

import argparse
import os
from collections import defaultdict

import numpy as np
import torch
import torch.nn as nn

from src.data_builder.build_hetero_data import build_graph, forward_edge_index_dict, norm_id
from src.models.hin_gnn_v2 import HINGNNv2, dropedge
from src.eval.success_predictor import evaluate_mask, predict_proba

BASE_EXP = "experiments/results/exp47_no_copurchase"
BASKET_COMP = "data/processed/hin/keyword_basket_comp_edges.parquet"
KW_KW = ("keyword", "basket_comp", "keyword")

# --- v2 정규화·멀티태스크 설정 (§9-6 스윕 대상) ---
CFG = dict(
    hidden_dim=48,          # 64→48 용량↓
    dropout=0.45,           # 0.3→0.45
    weight_decay=0.002,     # 0.0005→0.002
    dropedge=0.2,           # GNN 정규화
    aux_lambda=0.5,         # 멀티태스크 보조 손실 가중 (=정규화)
    basket_comp_support=3,  # 보완 엣지 지지도 하한
    lr=0.005, lr_alpha=0.02,
    epochs=25, patience=15,
    hop2_kw_idf=False,      # C안: sim_kw를 IDF 가중 공유합으로 구축(일반어 노이즈 제거)
    hop2_kw_idf_tau=12.0,   # IDF 임계 τ (noteook idf_sim_sweep 튜닝: τ≈12에서 deg~12·노이즈 96%↓)
)


def _load_base_config():
    ckpt = torch.load(os.path.join(BASE_EXP, "hin_gnn_best.pt"), weights_only=False, map_location="cpu")
    return ckpt["config"]


def _add_basket_comp_edges(eidx, k2i, support, train_only_df=None):
    import pandas as pd
    if train_only_df is not None:
        df = train_only_df                       # 누수 재검증: train 제품쌍만으로 빌드한 보완엣지
    elif os.path.exists(BASKET_COMP):
        df = pd.read_parquet(BASKET_COMP)
    else:
        print("  (basket_comp 엣지 없음 — build_basket_comp_edges 먼저 실행)")
        return eidx, 0
    df = df[df["support"] >= support]
    s = df["src_keyword"].astype(str).map(k2i)
    t = df["tgt_keyword"].astype(str).map(k2i)
    ok = s.notna() & t.notna()
    ei = torch.tensor([s[ok].astype(int).tolist(), t[ok].astype(int).tolist()], dtype=torch.long)
    eidx = dict(eidx); eidx[KW_KW] = ei.to(next(iter(eidx.values())).device)
    return eidx, int(ei.size(1))


def _build_aux_pairs(data, maps, device, seed=42):
    """동반구매 양성 쌍 (train 제품만) + 동수 랜덤 음성.  누수 차단 핵심."""
    import pandas as pd
    p2i = maps["p2i"]
    train_mask = data["product"].train_mask.numpy()
    train_ids = {maps["product_ids"][i] for i in np.where(train_mask)[0]}
    pos = set()
    for nm in ["offline_commerce_edge_lift_pair_out.csv", "quick_commerce_edge_lift_pair_out.csv"]:
        fp = os.path.join("data/processed", nm)
        if not os.path.exists(fp):
            continue
        df = pd.read_csv(fp, encoding="utf-8-sig")
        for a, b in zip(df["상품코드_A"].map(norm_id), df["상품코드_B"].map(norm_id)):
            if a in train_ids and b in train_ids and a in p2i and b in p2i:
                pos.add((p2i[a], p2i[b]))
    pos = list(pos)
    rng = np.random.default_rng(seed)
    train_idx = np.where(train_mask)[0]
    posset = set(pos)
    neg = []
    while len(neg) < len(pos):
        a, b = int(rng.choice(train_idx)), int(rng.choice(train_idx))
        if a != b and (a, b) not in posset and (b, a) not in posset:
            neg.append((a, b))
    pairs = torch.tensor(pos + neg, dtype=torch.long).T.to(device)
    labels = torch.cat([torch.ones(len(pos)), torch.zeros(len(neg))]).to(device)
    return pairs, labels, len(pos)


def run(full=False, cfg_override=None, basket_train_only=False, tag="v2"):
    cfg = dict(CFG)
    if cfg_override:
        cfg.update(cfg_override)
    if full:
        cfg["epochs"], cfg["patience"] = 200, 30
    base = _load_base_config()
    g = base["graph"]
    dev = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    data, maps = build_graph(
        seed=base["split"]["seed"], ratios=tuple(base["split"]["ratios"]),
        include_offline_copurchase=False, include_quick_copurchase=False,  # leak-free
        use_lift_weights=False, use_idf_keyword_weights=False,
        add_2hop_edges=True, hop2_kw_min_shared=g["hop2_kw_min_shared"], hop2_ip_min_shared=g["hop2_ip_min_shared"],
        hop2_kw_idf=cfg["hop2_kw_idf"], hop2_kw_idf_tau=cfg["hop2_kw_idf_tau"],
        add_via_ip_edges=True, add_ipip_kw_edges=True, add_trend_kw_edges=True,
    )
    eidx = {et: ei.to(dev) for et, ei in forward_edge_index_dict(data).items()}
    # 누수 재검증(§9-3): basket_comp을 train 제품쌍만으로 재빌드
    tdf = None
    if basket_train_only:
        from src.data_builder.build_basket_comp_edges import build_basket_comp_edges
        train_ids = {maps["product_ids"][i] for i in np.where(data["product"].train_mask.numpy())[0]}
        tdf = build_basket_comp_edges(min_support=cfg["basket_comp_support"], restrict_ids=train_ids, save=False)
    eidx, n_bc = _add_basket_comp_edges(eidx, maps["k2i"], cfg["basket_comp_support"], train_only_df=tdf)
    edge_types = list(eidx.keys())
    hp = data["product"].has_promo.to(dev); im = data["product"].insta_mention_30d.to(dev)
    y = data["product"].y.to(dev)
    tr = data["product"].train_mask.to(dev); va = data["product"].val_mask.to(dev); te = data["product"].test_mask.to(dev)
    aux_pairs, aux_lab, n_pos = _build_aux_pairs(data, maps, dev)

    print(f"v2 graph: +basket_comp 엣지 {n_bc:,} | aux 동반구매 양성쌍(train) {n_pos:,} | hidden={cfg['hidden_dim']}")

    m = base["model"]
    model = HINGNNv2(
        node_types=base["graph"]["node_types"], edge_types=edge_types,
        num_nodes={"product": data["product"].num_nodes, "keyword": data["keyword"].num_nodes, "ip": data["ip"].num_nodes},
        hidden_dim=cfg["hidden_dim"], num_layers=m["num_layers"], num_heads=m["num_heads"],
        dropout=cfg["dropout"], use_diffmg=m["use_diffmg_gate"], diffmg_temperature=m["diffmg_temperature"],
        product_aggr=base["node_feat"]["product_aggr"], use_has_promo=base["node_feat"]["use_has_promo_feature"],
    ).to(dev)

    alpha_ids = {id(p) for gt in model.gates for p in gt.parameters()}
    w_params = [p for p in model.parameters() if id(p) not in alpha_ids]
    a_params = [p for gt in model.gates for p in gt.parameters()]
    opt_w = torch.optim.Adam(w_params, lr=cfg["lr"], weight_decay=cfg["weight_decay"])
    opt_a = torch.optim.Adam(a_params, lr=cfg["lr_alpha"], weight_decay=0.001) if a_params else None
    crit = nn.BCEWithLogitsLoss(pos_weight=torch.tensor([base["label"]["pos_weight"]], device=dev))
    aux_crit = nn.BCEWithLogitsLoss()
    gen = torch.Generator(device=dev)

    best, best_state, pat = -1.0, None, 0
    for ep in range(1, cfg["epochs"] + 1):
        model.train()
        opt_w.zero_grad()
        if opt_a:
            opt_a.zero_grad()
        ei_drop = dropedge(eidx, cfg["dropedge"], gen)
        logits = model(ei_drop, has_promo=hp, insta_m30=im)
        loss_main = crit(logits[tr], y[tr])
        loss_aux = aux_crit(model.aux_link_logit(aux_pairs), aux_lab)   # 멀티태스크
        (loss_main + cfg["aux_lambda"] * loss_aux).backward()
        opt_w.step()

        if opt_a:                                                       # α: val 주임무
            opt_w.zero_grad(); opt_a.zero_grad()
            logits = model(eidx, has_promo=hp, insta_m30=im)
            crit(logits[va], y[va]).backward()
            opt_a.step()

        vm = evaluate_mask(model, eidx, hp, y, va, insta_m30=im)
        if vm["pr_auc"] > best:
            best, pat = vm["pr_auc"], 0
            best_state = {k: v.detach().cpu().clone() for k, v in model.state_dict().items()}
        else:
            pat += 1
        if ep % 5 == 0 or ep == 1:
            print(f"[{ep:03d}] main={loss_main.item():.4f} aux={loss_aux.item():.4f} val_pr={vm['pr_auc']:.4f}")
        if pat >= cfg["patience"]:
            print(f"early stop @ {ep}"); break

    if best_state:
        model.load_state_dict(best_state)
    tm = evaluate_mask(model, eidx, hp, y, te, insta_m30=im)
    vm = evaluate_mask(model, eidx, hp, y, va, insta_m30=im)
    trm = evaluate_mask(model, eidx, hp, y, tr, insta_m30=im)
    gap = trm["pr_auc"] - vm["pr_auc"]
    print("─" * 60)
    print(f"[{tag}{' basket_train_only' if basket_train_only else ''}] "
          f"train_pr={trm['pr_auc']:.4f} val_pr={vm['pr_auc']:.4f} test_pr={tm['pr_auc']:.4f} "
          f"test_auc={tm['auc_roc']:.4f} test_f1={tm['f1']:.4f} | gap={gap:.3f}")
    print(f"  cfg: dropout={cfg['dropout']} dropedge={cfg['dropedge']} hidden={cfg['hidden_dim']} "
          f"wd={cfg['weight_decay']} aux_λ={cfg['aux_lambda']}")
    print(f"  vs exp47: test 0.5699 / gap 0.215 | vs v2-base: test 0.6003 / gap 0.190")
    with torch.no_grad():
        prob_full = predict_proba(model, eidx, hp, insta_m30=im).cpu().numpy()
    ctx = dict(model=model, eidx=eidx, hp=hp, im=im, y=y.cpu().numpy().astype(int),
               train_mask=data["product"].train_mask.numpy(), val_mask=data["product"].val_mask.numpy(),
               test_mask=data["product"].test_mask.numpy(), maps=maps, base_config=base, cfg=cfg,
               prob_full=prob_full, edge_types=edge_types, data=data)
    return model, dict(train=trm, val=vm, test=tm, gap=gap), ctx


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", action="store_true")
    run(full=ap.parse_args().full)
