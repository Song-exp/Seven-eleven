"""md_prescription_pipeline.ipynb 생성기.

전역 객체(eng/g1/g2/pe/scoreboard)를 셀 간 유기적으로 바인딩하여
단일추론 → 차분행렬 → 처방엔진 → 검증·KPI → 자동저장 전 과정을 한 노트북으로.
실행: python -m src.eval.md._build_notebook
"""
import os
import nbformat as nbf

NB_PATH = "experiments/notebooks/md_prescription_pipeline.ipynb"

md = lambda s: nbf.v4.new_markdown_cell(s)
code = lambda s: nbf.v4.new_code_cell(s)
cells = []

cells.append(md(r"""# MD 처방 파이프라인 — 단일추론 → 차분행렬 → 처방 → 검증·KPI (exp47 leak-free)

> 계획: `docs/eda_channel_prescription_plan.md` (Plan 2) · `docs/final_model_leakfree_switch_plan.md` (Plan 1)
> 모듈: `src/eval/md/{engine,tasks,prescription,validate}.py`

```
[Stage 1] 단일 추론 캐시        eng = MDEngine().run_single_inference()  →  prob·y·att24·Mass·장부(full/train)
    │                            (원칙 0: 전체 forward 1회. 이후 슬라이싱/조건부 집계)
    ▼
[Stage 2] 집계·차분 행렬        g1 = stage_g1_macro(eng)      성공망 vs 실패망 (A_diff±)
    │                            g2 = stage_g2_channel(eng)    POS 독점망 vs 인스타 독점망
    │                            cell_4a/4b/4c                  혼동행렬 구조 진단 (within-channel)
    ▼
[Stage 3] 처방 엔진 바인딩      pe = MDPrescriptionEngine(eng, g2)  →  get_md_prescription(seed)
    │                            §5: 장바구니·A빔·B anti·C 포화/신흥·E 견고성·F 소생
    ▼
[Stage 4] 검증·KPI 스코어보드   validate.run_all(eng, pe)     Tier1 순열·Tier2 holdout·Tier4 개입·Tier5 시너지·Precision@K
    │
    ▼
[Stage 5] 자동 저장             OUT_DIR/{inference_cache.npz, ledgers.parquet, engine_meta.json, scoreboard.json, figures/}
```
전역 객체 `eng·g1·g2·pe·scoreboard` 가 셀 간 메모리로 유기적으로 연결됩니다."""))

cells.append(code(r"""# --- Setup ---
import os, sys, json
ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))  # repo root (notebooks/ → ../../)
if ROOT not in sys.path: sys.path.insert(0, ROOT)
os.chdir(ROOT)

import numpy as np, pandas as pd
import matplotlib.pyplot as plt
plt.rcParams["font.family"] = "Malgun Gothic"; plt.rcParams["axes.unicode_minus"] = False

from src.eval.md.engine import MDEngine, EngineConfig
from src.eval.md import tasks as T
from src.eval.md.prescription import MDPrescriptionEngine, get_md_prescription
from src.eval.md import validate as V
print("repo:", ROOT)"""))

cells.append(md(r"""## 🔀 모델 선택 — exp47 vs v2_sweepA

| | exp47 (현 서빙) | v2_sweepA (검증 우세) |
|---|---|---|
| 구조 | HINGNN, copurchase 제거 | HINGNNv2(멀티태스크) + basket_comp 엣지 |
| 누수 | leak-free | leak-free |
| test PR-AUC | 0.570 | **0.606** |
| test 운영점 F1 | 0.544 | **0.583** |
| 과적합 gap | 0.215 | **0.135** |

`MODEL` 한 줄만 바꿔 두 모델 중 선택. 출력은 모델별 폴더로 분리 저장됩니다."""))

cells.append(code(r"""# ── 모델 선택 (여기만 바꾸면 됨) ──
MODEL = "v2_sweepA"      # "v2_sweepA"(최종 채택)  또는  "exp47"

ENGINE_CFG = EngineConfig.exp47() if MODEL == "exp47" else EngineConfig.v2_sweepA()
OUT_DIR = f"experiments/results/md_prescription/{MODEL}"
FIG_DIR = os.path.join(OUT_DIR, "figures"); os.makedirs(FIG_DIR, exist_ok=True)
print(f"선택 모델: {MODEL}  | exp_dir={ENGINE_CFG.exp_dir}  | THR={ENGINE_CFG.thr}  | out={OUT_DIR}")"""))

# ---- Stage 1
cells.append(md("## Stage 1 — 단일 추론 캐시 (원칙 0: 전체 forward 1회)"))
cells.append(code(r"""eng = MDEngine(ENGINE_CFG).run_single_inference()
eng.build_mass()
lg_full  = eng.build_ledger("full")
lg_train = eng.build_ledger("train")
c = eng.cache
print(f"[{MODEL}] is_v2={getattr(eng,'is_v2',False)}  | P/K/I = {c['P']}/{c['K']}/{c['I']}  | base={c['base_rate']:.3f}  | THR={eng.cfg.thr}")
print(f"att 관계 {len(c['att'])}종 | prob 평균 {c['prob'].mean():.3f}")
print(f"장부(full)  killer={len(lg_full.killer)} mine={len(lg_full.mine)} hub={len(lg_full.hub)}")
print(f"장부(train) killer={len(lg_train.killer)} mine={len(lg_train.mine)} hub={len(lg_train.hub)}")"""))

cells.append(code(r"""# Stage 1 대시보드: 혼동행렬 × 채널 + 오분류 확신도 + 장부 규모
diag = T.cell_4a(eng)
fig, ax = plt.subplots(1, 3, figsize=(16, 4))
ct = diag["crosstab"].drop(index="All", errors="ignore").drop(columns="All", errors="ignore")
ct.plot(kind="bar", stacked=True, ax=ax[0]); ax[0].set_title(f"혼동행렬 × 채널 (THR={eng.cfg.thr})"); ax[0].set_xlabel("")
ax[1].hist(diag["prob_err"], bins=30, color="#d9534f", alpha=.8); ax[1].axvline(eng.cfg.thr, ls="--", c="k")
ax[1].set_title(f"오분류 확신도 (경계선비율 {diag['borderline_frac']:.0%})"); ax[1].set_xlabel("pred_success_prob")
ax[2].bar(["killer","mine","hub"], [len(lg_full.killer),len(lg_full.mine),len(lg_full.hub)], color=["#5cb85c","#d9534f","#777"])
ax[2].set_title("마스터 장부 규모 (full)")
plt.tight_layout(); plt.savefig(os.path.join(FIG_DIR,"stage1_dashboard.png"), dpi=110, bbox_inches="tight"); plt.show()"""))

# ---- Stage 2
cells.append(md("## Stage 2 — 집계·차분 행렬 (거시 G-1 / 미시 G-2) + 혼동행렬 구조 진단"))
cells.append(code(r"""g1   = T.stage_g1_macro(eng)                 # 성공망 vs 실패망
g2   = T.stage_g2_channel(eng, "track1")     # 전 채널 POS vs 인스타 (처방 주 신호원)
g2b  = T.stage_g2_channel(eng, "track2")     # 세븐 한정 검증 트랙
print("G-1 A_diff+ nnz:", int((g1.A_diff_plus>0).sum()), "| A_diff- nnz:", int((g1.A_diff_minus>0).sum()))
print("G-2(track1) POS독점망 nnz:", int((g2.A_diff_pos>0).sum()), "| 인스타독점망 nnz:", int((g2.A_diff_insta>0).sum()))"""))

cells.append(code(r"""# Cell 4b — 분면별 1-hop 위상 대조 (within-channel)
display(T.cell_4b(eng, within_channel=True))
# Cell 4c — 극단 오분류 Top
ext = T.cell_4c(eng, top_k=8)
print("── 가장 억울한 FN (저prob 성공) ──"); display(ext["FN"])
print("── 가장 황당한 FP (고prob 실패) ──"); display(ext["FP"])"""))

# ---- Stage 3
cells.append(md("## Stage 3 — 처방 엔진 바인딩 (§5: 장바구니·A~F)"))
cells.append(code(r"""pe = MDPrescriptionEngine(eng, g2, universe="full")
for seed in ["마라","로제","단백질","흑임자","위스키"]:
    print("="*70); print(get_md_prescription(pe, seed))"""))

# ---- Stage 4
cells.append(md("## Stage 4 — 실시간 검증 · KPI 스코어보드 (Tier1·2·4·5 + Precision@K)"))
cells.append(code(r"""scoreboard = V.run_all(eng, pe, light=True)   # light=False면 순열 500회·표본 확대
rows = []
def _row(name, val, passed, note):
    rows.append(dict(검증=name, 값=val, 통과=("✅" if passed else "⚠️"), 비고=note))
t1=scoreboard["tier1_permutation"]; _row("Tier1 순열귀무", f"{t1['pass_rate']:.0%}", t1['pass_rate']>=0.7, f"killer {t1['n_killer']}개 중 95pct 초과")
t2=scoreboard["tier2_holdout"];    _row("Tier2 holdout", f"PRAUC {t2['test_prauc']} / base {t2['base']}", t2['passed'], f"CI_lo {t2['ci_lo']}")
t4=scoreboard["tier4_intervention"];_row("Tier4 개입(faithful)", f"killer {t4['killer_pos_rate']} vs rand {t4['random_pos_rate']}", t4['passed'], f"mine 비상승 {t4['mine_nonpos_rate']}")
t5=scoreboard["tier5_synergy"];    _row("Tier5 조합시너지", f"초가법 {t5['superadd_rate']}", t5['superadd_rate']>0.0, f"n={t5['n']}")
bk=scoreboard["backtest_precision_at_k"]; _row("KPI Precision@K", f"P@k {bk['precision_at_k']} / hit {bk['hit_rate']}", bk['precision_at_k']>0.0, f"n={bk['n']} (랜덤≈0.5%)")
board = pd.DataFrame(rows); display(board)"""))

cells.append(code(r"""# KPI 스코어보드 시각화
fig, ax = plt.subplots(figsize=(9,3.2))
labels=["Tier1\n순열","Tier2\nholdout","Tier4\n개입","Tier5\n시너지","KPI\nP@K"]
vals=[t1['pass_rate'], t2['test_prauc']/max(t2['base'],1e-9)-1, t4['killer_pos_rate'], t5['superadd_rate'], bk['hit_rate']]
cols=["#5cb85c" if p else "#f0ad4e" for p in [t1['pass_rate']>=0.7,t2['passed'],t4['passed'],t5['superadd_rate']>0,bk['precision_at_k']>0]]
ax.bar(labels, vals, color=cols); ax.set_title("검증·KPI 스코어보드 (정규화 지표)"); ax.axhline(0, c="k", lw=.5)
plt.tight_layout(); plt.savefig(os.path.join(FIG_DIR,"stage4_scoreboard.png"), dpi=110, bbox_inches="tight"); plt.show()"""))

# ---- Stage 5
cells.append(md("## Stage 5 — 제안 파일 구조로 자동 저장"))
cells.append(code(r"""# 캐시·장부·메타 저장 (engine.save_cache) + 스코어보드 + 처방 샘플 + 그림
eng.save_cache(OUT_DIR)
with open(os.path.join(OUT_DIR,"scoreboard.json"),"w",encoding="utf-8") as f:
    json.dump(scoreboard, f, ensure_ascii=False, indent=2, default=lambda o: o.tolist() if hasattr(o,'tolist') else str(o))
with open(os.path.join(OUT_DIR,"prescriptions_sample.txt"),"w",encoding="utf-8") as f:
    for seed in ["마라","로제","단백질","흑임자","위스키"]:
        f.write("="*70+"\n"+get_md_prescription(pe, seed)+"\n")
board.to_csv(os.path.join(OUT_DIR,"scoreboard.csv"), index=False, encoding="utf-8-sig")

print("저장 완료 →", OUT_DIR)
for root,_,files in os.walk(OUT_DIR):
    for fn in sorted(files):
        p=os.path.join(root,fn); print(f"  {os.path.relpath(p,OUT_DIR):40s} {os.path.getsize(p):>10,} B")"""))

nb = nbf.v4.new_notebook(cells=cells)
nb.metadata["kernelspec"] = {"display_name": "Python 3", "language": "python", "name": "python3"}
os.makedirs(os.path.dirname(NB_PATH), exist_ok=True)
with open(NB_PATH, "w", encoding="utf-8") as f:
    nbf.write(nb, f)
print("wrote", NB_PATH, "cells:", len(cells))
