"""C안 — 키워드 IDF 정규화 sim_kw 로 v2 재학습 → experiments/results/v2_sim_idf.

sim 엣지 영향력 EDA(S5)의 C안. v2_sweepA(A)와 **학습 하이퍼파라미터를 동일**하게 두고
sim_kw 구축만 IDF(S=A·diag(idf)·Aᵀ≥τ)로 바꿔 *그래프 변경의 순효과*만 비교한다.

τ는 노트북 `sim_diag.idf_sim_sweep` 미리보기로 고른 값(기본 12 — deg~12·노이즈 96%↓).

실행:
    python -m experiments.train_sim_idf            # τ=12
    python -m experiments.train_sim_idf --tau 16   # τ 바꿔 재학습

학습 후: 노트북 `eda/sim_edge_influence/sim_edge_influence.ipynb` §S5 셀이
experiments/results/v2_sim_idf 를 자동 로드해 A vs C(PR-AUC·gap·sim_ip 변별·전이ablation) 비교.
"""
import argparse
import os
import sys

sys.path.insert(0, os.getcwd())
from experiments.v2_multitask import run                       # noqa: E402
from experiments.v2_promote import export_v2, operating_point  # noqa: E402

OUT_DIR = "experiments/results/v2_sim_idf"
# v2_sweepA(A)와 동일한 학습 레시피 (v2_sweep_and_leakcheck 'sweepA_reg++') — 공정 비교용
SWEEPA = dict(dropout=0.5, dropedge=0.35, hidden_dim=32, aux_lambda=1.0)


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--tau", type=float, default=12.0, help="IDF sim_kw 임계 (노트북 sweep로 튜닝)")
    ap.add_argument("--out", default=OUT_DIR)
    args = ap.parse_args()

    override = {**SWEEPA, "hop2_kw_idf": True, "hop2_kw_idf_tau": args.tau}
    print(f"[C안 학습] IDF sim_kw τ={args.tau} | 레시피=sweepA | out={args.out}")
    model, metrics, ctx = run(full=True, cfg_override=override, tag=f"v2_sim_idf(τ{args.tau:g})")
    op = operating_point(ctx["prob_full"], ctx["y"])
    export_v2(model, ctx, metrics, args.out, op)
    print(f"\n✅ 저장: {args.out}/hin_gnn_best.pt")
    print(f"   test PR-AUC {metrics['test']['pr_auc']:.4f} | gap {metrics['gap']:.3f}")
    print("   → 노트북 §S5 셀 Run 하면 A vs C 자동 비교 (이미 VARIANTS에 경로 등록됨)")


if __name__ == "__main__":
    main()
