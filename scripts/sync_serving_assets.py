"""배포 번들(serving_assets/) 동기화 — 백엔드가 읽는 모델·데이터 산출물만 추려 복사.

Railway 배포는 git 추적 파일만 가져가는데 모델/데이터는 .gitignore 대상이므로,
필요한 최소 파일을 serving_assets/(추적됨)로 복사해 둔다. Dockerfile이 이를 런타임 경로로 되돌린다.

재학습(hin_gnn_best.pt)·재export(weighted_*.parquet)·재빌드(HIN _final) 후 이 스크립트를 다시 실행.

    python -m scripts.sync_serving_assets

대상은 serve.py(_data) + combo_serve(MDEngine/build_hetero_data)가 실제로 읽는 파일과 일치.
"""
import os
import shutil

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DST = os.path.join(ROOT, "serving_assets")

# (소스 상대경로, serving_assets 내 하위폴더)
MODEL_DIR = "experiments/results/v2_sweepA"
MODEL_FILES = [
    "learned_product_scores.parquet",
    "weighted_product_keyword_edges.parquet",
    "weighted_ip_keyword_edges.parquet",
    "weighted_product_ip_edges.parquet",
    "weighted_trend_keyword_edges.parquet",
    "relation_importance.json",
    "hin_gnn_best.pt",
]
HIN_DIR = "data/processed/hin"
HIN_FILES = [
    "product_nodes_final.parquet",
    "keyword_nodes_final.parquet",
    "ip_nodes_final.parquet",
    "product_keyword_edges_final.parquet",
    "ip_keyword_edges_final.parquet",
    "trend_keyword_edges_final.parquet",
    "product_ip_edges_final.parquet",
    "ip_ip_edges_final.parquet",
    "keyword_basket_comp_edges.parquet",   # v2 basket_comp 주입(_inject_basket_comp) 필수
    "keyword_final.csv",
]
PROCESSED_FILES = [
    "data/processed/pos_product_features.parquet",
    "data/processed/trend_keywords.parquet",
]


def _copy(src_rel: str, dst_sub: str) -> None:
    src = os.path.join(ROOT, src_rel)
    if not os.path.exists(src):
        print(f"  [MISSING] {src_rel}")
        return
    dst_dir = os.path.join(DST, dst_sub)
    os.makedirs(dst_dir, exist_ok=True)
    shutil.copy2(src, os.path.join(dst_dir, os.path.basename(src)))
    print(f"  [OK] {src_rel} -> serving_assets/{dst_sub}/")


def main() -> None:
    print("배포 번들 동기화 (serving_assets/)")
    for f in MODEL_FILES:
        _copy(os.path.join(MODEL_DIR, f), "model")
    for f in HIN_FILES:
        _copy(os.path.join(HIN_DIR, f), "hin")
    for f in PROCESSED_FILES:
        _copy(f, "processed")
    print("완료. 변경분을 커밋·푸시하면 Railway가 재배포합니다.")


if __name__ == "__main__":
    main()
