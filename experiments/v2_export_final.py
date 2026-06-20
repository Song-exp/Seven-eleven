"""v2-sweepA 학습 + artifact 동결 → experiments/results/v2_sweepA/ (서빙 후보).

실행: python -m experiments.v2_export_final
"""
from experiments.v2_multitask import run
from experiments.v2_promote import operating_point, export_v2

SWEEPA = dict(dropout=0.5, dropedge=0.35, hidden_dim=32, aux_lambda=1.0)
OUT = "experiments/results/v2_sweepA"


def main():
    model, metrics, ctx = run(full=True, cfg_override=SWEEPA, tag="export")
    te = ctx["test_mask"]
    op_test = operating_point(ctx["prob_full"][te], ctx["y"][te])   # held-out 운영점
    export_v2(model, ctx, metrics, OUT, op_test)
    print("=" * 60)
    print("✅ export →", OUT)
    print("  test PR-AUC:", round(metrics["test"]["pr_auc"], 4), "| gap:", round(metrics["gap"], 3))
    print("  test 운영점 F1:", op_test["f1"], "(exp47 test 0.544)")
    import os
    for fn in sorted(os.listdir(OUT)):
        print("   ", fn)


if __name__ == "__main__":
    main()
