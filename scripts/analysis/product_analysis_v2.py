import polars as pl
import os

# 1. 경로 설정 (현재 작업 디렉토리 기준 절대 경로 확보)
current_dir = os.path.dirname(os.path.abspath(__file__))
b2_path = os.path.join(current_dir, 'data/processed/B2_POS_SALE.parquet')
b4_path = os.path.join(current_dir, 'data/raw/B4_ITEM_DV_INFO.csv')

def run_analysis():
    # 데이터 존재 여부 최종 확인
    if not os.path.exists(b2_path):
        print(f"❌ B2 파일을 찾을 수 없습니다: {b2_path}")
        return
    if not os.path.exists(b4_path):
        print(f"❌ B4 파일을 찾을 수 없습니다: {b4_path}")
        return

    print("🔍 데이터 로드 중 (스키마 확인)...")
    # B2의 실제 컬럼명 확인 (가장 흔한 에러 원인)
    b2_lazy = pl.scan_parquet(b2_path)
    b2_columns = b2_lazy.collect_schema().names()
    print(f"B2 컬럼 목록: {b2_columns}")

    # '상품코드' 또는 '상품 코드'가 있는지 확인
    join_key = '상품코드'
    if '상품 코드' in b2_columns:
        join_key = '상품 코드'
        print(f"⚠️ B2에서 '상품 코드'(띄어쓰기 있음)를 발견했습니다. 이를 조인 키로 사용합니다.")
    elif '상품코드' not in b2_columns and 'ITEM_CD' in b2_columns:
        join_key = 'ITEM_CD'
        print(f"⚠️ B2에서 'ITEM_CD'를 조인 키로 사용합니다.")

    # 2. 마스터 데이터 로드 (B4)
    # B4_ITEM_DV_INFO.csv: ITEM_CD, ITEM_NM, ITEM_LRDV_NM, ITEM_MDDV_NM, ITEM_SMDV_NM
    b4_df = pl.read_csv(b4_path).rename({"ITEM_CD": join_key})

    print("📊 데이터 결합 및 분석 중...")
    # 3. 조인 및 집계
    analysis = (
        b2_lazy.join(b4_df.lazy(), on=join_key, how="inner")
        .group_by("ITEM_MDDV_NM")
        .agg([
            pl.col("판매금액").sum().alias("총매출액"),
            pl.col("판매수량").sum().alias("총판매량"),
            pl.col("ITEM_NM").n_unique().alias("상품다양성")
        ])
        .sort("총매출액", descending=True)
        .collect()
    )

    print("\n" + "="*80)
    print("🏆 [중분류별 매출 분석 결과 (Top 10)]")
    print("="*80)
    print(analysis.head(10))
    print("="*80)
    print("\n✅ 분석이 완료되었습니다.")

if __name__ == "__main__":
    run_analysis()
