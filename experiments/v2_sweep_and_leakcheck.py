"""v2 §9-3 누수 재검증 + §9-6 정규화 스윕.

실행: python -m experiments.v2_sweep_and_leakcheck
  [A] 구조 검증: aux train-only 확인 + basket_comp full vs train-only 엣지 중복도
  [B] 행동 검증: train-only basket로 재학습 → test PR-AUC가 v2-base(0.600)와 유사하면 누수 없음
  [C] 정규화 스윕: 더 강한 dropout/dropedge/hidden↓/aux_λ↑ 로 gap<0.10 시도
"""
from __future__ import annotations

import numpy as np

from src.data_builder.build_hetero_data import build_graph
from src.data_builder.build_basket_comp_edges import build_basket_comp_edges
from experiments.v2_multitask import run, _build_aux_pairs
import torch


def structural_leak_check():
    print("=" * 70, "\n[A] 구조 누수 검증 (§9-3)")
    data, maps = build_graph(
        include_offline_copurchase=False, include_quick_copurchase=False,
        add_2hop_edges=True, hop2_kw_min_shared=3, hop2_ip_min_shared=1,
        add_via_ip_edges=True, add_ipip_kw_edges=True, add_trend_kw_edges=True)
    dev = torch.device("cpu")
    # aux 양성쌍이 100% train인지
    pairs, labels, n_pos = _build_aux_pairs(data, maps, dev)
    train_mask = data["product"].train_mask.numpy()
    pa = pairs.numpy()
    pos_pairs = pa[:, : n_pos]
    all_train = bool(train_mask[pos_pairs[0]].all() and train_mask[pos_pairs[1]].all())
    print(f"  aux 양성쌍 {n_pos}개 — 전부 train? {all_train}  (False면 누수)")

    # basket_comp: full vs train-only 엣지 집합 비교
    train_ids = {maps["product_ids"][i] for i in np.where(train_mask)[0]}
    full = build_basket_comp_edges(min_support=3, save=False)
    tonly = build_basket_comp_edges(min_support=3, restrict_ids=train_ids, save=False)
    sf = {(r.src_keyword, r.tgt_keyword) for r in full.itertuples()}
    st = {(r.src_keyword, r.tgt_keyword) for r in tonly.itertuples()}
    jac = len(sf & st) / len(sf | st) if (sf | st) else 0
    print(f"  basket_comp(support≥3): full {len(sf)} / train-only {len(st)} / Jaccard {jac:.3f}")
    print(f"  → Jaccard 높으면 구조가 test 제품에 거의 무의존(누수 희석). test-전용 엣지 {len(sf - st)}개")
    return dict(aux_train_only=all_train, jaccard=round(jac, 3), full=len(sf), train_only=len(st))


def main():
    leak = structural_leak_check()

    print("=" * 70, "\n[B] 행동 누수 검증: train-only basket 재학습")
    _, r_leak, _ = run(full=True, basket_train_only=True, tag="leakcheck")

    print("=" * 70, "\n[C] 정규화 스윕 (§9-6)")
    configs = [
        ("sweepA_reg++", dict(dropout=0.5, dropedge=0.35, hidden_dim=32, aux_lambda=1.0)),
        ("sweepB_reg+++", dict(dropout=0.5, dropedge=0.4, hidden_dim=32, weight_decay=0.004, aux_lambda=1.0)),
    ]
    sweep = {}
    for tag, ov in configs:
        _, r, _ = run(full=True, cfg_override=ov, tag=tag)
        sweep[tag] = r

    print("=" * 70, "\n[요약]")
    print(f"  구조검증: aux_train_only={leak['aux_train_only']} basket Jaccard={leak['jaccard']}")
    print(f"  v2-base           : test 0.6003 gap 0.190")
    print(f"  leakcheck(train만): test {r_leak['test']['pr_auc']:.4f} gap {r_leak['gap']:.3f}  "
          f"→ v2-base와 유사하면 누수 없음")
    for tag, r in sweep.items():
        print(f"  {tag:16s}: test {r['test']['pr_auc']:.4f} gap {r['gap']:.3f}")


if __name__ == "__main__":
    main()
