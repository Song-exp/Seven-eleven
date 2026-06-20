"""v2_promotion_decision.ipynb 생성기 — A안(v2 승격) 결정 노트북.

학습(sweepA) → 운영점 F1 비교 → 누수 요약 → 승격 판정 → (승격 시) artifact export.
실행: python -m experiments._build_promotion_notebook
"""
import os
import nbformat as nbf

NB = "experiments/notebooks/v2_promotion_decision.ipynb"
md = lambda s: nbf.v4.new_markdown_cell(s)
code = lambda s: nbf.v4.new_code_cell(s)
cells = []

cells.append(md(r"""# v2 승격 결정 노트북 (A안) — 운영점 비교로 최종 판단

> 계획: `docs/final_model_leakfree_switch_plan.md` §9-4 승격 기준.
> 흐름: **sweepA 학습 → 운영점(생존율 동기화) F1 vs exp47 → 누수 요약 → 승격 판정 → (승격 시) artifact export**

```
승격 기준 (§9-4):
  (a) test PR-AUC > exp47(0.570)          ← 랭킹 품질
  (b) 운영점 F1 ≥ exp47(0.666)            ← MD 합격선 실제 성능  ★이 노트북의 핵심
  (c) leak-free                            ← train-only basket Δ≈0
  (d) gap < 0.10 (참고)                    ← 과적합
→ (a)&(b)&(c) 충족 시 승격 권장.
```"""))

cells.append(code(r"""import os, sys, json
ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
if ROOT not in sys.path: sys.path.insert(0, ROOT)
os.chdir(ROOT)
import numpy as np, pandas as pd
from experiments.v2_multitask import run, CFG
from experiments.v2_promote import operating_point, compare_models, export_v2, decision
from experiments.v2_sweep_and_leakcheck import structural_leak_check
SWEEPA = dict(dropout=0.5, dropedge=0.35, hidden_dim=32, aux_lambda=1.0)
print("sweepA cfg:", SWEEPA)"""))

cells.append(md("## 1. v2-sweepA 학습 (leak-free 멀티태스크 + 정규화)"))
cells.append(code(r"""model, metrics, ctx = run(full=True, cfg_override=SWEEPA, tag="sweepA")
print("train/val/test PR-AUC:", {k: round(metrics[k]['pr_auc'],4) for k in ['train','val','test']}, "| gap:", round(metrics['gap'],3))"""))

cells.append(md("## 2. ★ 운영점 비교 (생존율 23.8% 동기화) — v2 vs exp47"))
cells.append(code(r"""cmp_df, op_v2, op_47 = compare_models(ctx['prob_full'], ctx['y'])
display(cmp_df[['thr','predpos','precision','recall','f1','TP','FP','FN']])
print(f"운영점 F1:  exp47 {op_47['f1']}  vs  v2-sweepA {op_v2['f1']}   (Δ {op_v2['f1']-op_47['f1']:+.4f})")
print(f"test PR-AUC: exp47 0.5699  vs  v2-sweepA {metrics['test']['pr_auc']:.4f}   (Δ {metrics['test']['pr_auc']-0.5699:+.4f})")"""))

cells.append(md("## 3. 누수 재검증 요약 (§9-3)"))
cells.append(code(r"""leak = structural_leak_check()
# 행동 검증(train-only basket)은 v2_sweep_and_leakcheck.py에서 실측: test 0.5921 ≈ full 0.6003 (Δ0.008)
leak_free = leak['aux_train_only'] and (leak['jaccard'] >= 0.5)   # 구조 + (사전 행동검증 Δ0.008)
print(f"aux 100% train: {leak['aux_train_only']} | basket Jaccard: {leak['jaccard']} | 행동검증(사전): train-only 0.592 ≈ full 0.600")
print("→ leak-free:", leak_free)"""))

cells.append(md("## 4. 승격 판정"))
cells.append(code(r"""dec = decision(metrics, op_v2, op_47, leak_free=leak_free)
print(json.dumps(dec, ensure_ascii=False, indent=2))
print()
print("="*50)
print(f"  판정: {dec['verdict']}")
print(f"  (a) PR-AUC 초과: {dec['pr_win']} | (b) 운영점 F1 동등이상: {dec['f1_ok']} | (c) leak-free: {dec['leak_free']} | (d) gap<0.10: {dec['gap_ok']}({dec['gap']})")
print("="*50)"""))

cells.append(md("## 5. (승격 시) artifact export + 서빙 전환 안내"))
cells.append(code(r"""OUT = "experiments/results/v2_sweepA"
if dec['promote']:
    export_v2(model, ctx, metrics, OUT, op_v2)
    print("✅ export 완료 →", OUT)
    for fn in sorted(os.listdir(OUT)): print("   ", fn)
    print("\n[서빙 전환 절차]")
    print(" 1) src/eval/md/engine.py 및 serve.py 가 HINGNNv2+basket_comp 그래프를 로드하도록 어댑터 필요")
    print("    (engine._rebuild → HINGNNv2, build_graph에 keyword_basket_comp_edges 주입)")
    print(" 2) SERVING_EXP = 'v2_sweepA' / EngineConfig.exp_dir 변경")
    print(" 3) md_prescription_pipeline.ipynb 재실행 → 처방 캐시 재생성")
else:
    print("⏸ 보류 — exp47 유지. 기준 미달 항목:",
          [k for k in ['pr_win','f1_ok','leak_free'] if not dec[k]])"""))

nb = nbf.v4.new_notebook(cells=cells)
nb.metadata["kernelspec"] = {"display_name": "Python 3", "language": "python", "name": "python3"}
os.makedirs(os.path.dirname(NB), exist_ok=True)
with open(NB, "w", encoding="utf-8") as f:
    nbf.write(nb, f)
print("wrote", NB, "cells:", len(cells))
