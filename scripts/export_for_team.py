"""
팀원 전달용 최소 파일 세트 복사 스크립트.

실행:
    # 코드만 복사 (데이터는 별도 전달)
    python scripts/export_for_team.py --dest ../7eleven_share

    # 코드 + HIN parquet 포함
    python scripts/export_for_team.py --dest ../7eleven_share --include-data
"""
import argparse
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# ── 코드 파일 ─────────────────────────────────────────────────────
CODE_FILES = [
    # 노트북
    "eda/notebooks/04_hin_graph_builder.ipynb",
    "experiments/notebooks/methodA_relation_gating.ipynb",
    # 실험 유틸 + config
    "experiments/exp_utils.py",
    "experiments/configs/exp01_baseline.yaml",
    "experiments/configs/exp02_alpha_tuning.yaml",
    "experiments/configs/exp03_complement_edges.yaml",
    "experiments/configs/exp06_both_copurchase.yaml",
    "experiments/configs/exp07_copurchase_binary.yaml",
    # 기본 학습 config
    "configs/train_config.yaml",
    # src
    "src/__init__.py",
    "src/train/__init__.py",
    "src/train/trainer.py",
    "src/eval/__init__.py",
    "src/eval/export_results.py",
    "src/eval/success_predictor.py",
    "src/eval/recommend.py",
    "src/data_builder/__init__.py",
    "src/data_builder/build_hetero_data.py",
    "src/models/__init__.py",
    "src/models/hin_gnn.py",
    "src/models/hgt_layer.py",
    "src/models/kgat_layer.py",
    "src/models/diffmg_pruner.py",
]

# ── 데이터 파일 (--include-data 시 복사) ─────────────────────────
DATA_FILES = [
    # 04 노트북 출력물 (HIN 그래프)
    "data/processed/hin/product_nodes.parquet",
    "data/processed/hin/keyword_nodes.parquet",
    "data/processed/hin/ip_nodes.parquet",
    "data/processed/hin/product_keyword_edges.parquet",
    "data/processed/hin/ip_keyword_edges.parquet",
    "data/processed/hin/trend_keyword_edges.parquet",
    "data/processed/hin/product_ip_edges.parquet",
    # exp03 보완재 엣지
    "data/processed/complement_lift_pairs.csv",
    # exp06·07 동반구매 엣지
    "data/processed/offline_commerce_edge_lift_pair_out.csv",
    "data/processed/quick_commerce_edge_lift_pair_out.csv",
]


def copy_file(src: Path, dst: Path):
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dest", required=True, help="복사 대상 폴더 경로")
    parser.add_argument("--include-data", action="store_true",
                        help="HIN parquet + CSV 데이터 파일도 복사")
    args = parser.parse_args()

    dest = Path(args.dest).resolve()
    print(f"대상 폴더: {dest}\n")

    targets = CODE_FILES + (DATA_FILES if args.include_data else [])

    ok, skip = [], []
    for rel in targets:
        src = ROOT / rel
        if not src.exists():
            skip.append(rel)
            continue
        copy_file(src, dest / rel)
        ok.append(rel)

    print(f"[복사 완료] {len(ok)}개")
    for f in ok:
        print(f"  OK  {f}")

    if skip:
        print(f"\n[없는 파일] {len(skip)}개 - 건너뜀")
        for f in skip:
            print(f"  XX  {f}")

    if not args.include_data:
        print("\n[안내] 데이터 파일은 포함되지 않았습니다.")
        print("  포함하려면: python scripts/export_for_team.py --dest <경로> --include-data")
        print("  데이터 없이 실행하려면 04_hin_graph_builder.ipynb를 먼저 돌려 hin/ 폴더를 생성하세요.")


if __name__ == "__main__":
    main()
