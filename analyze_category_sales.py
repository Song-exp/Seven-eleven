import polars as pl
import pandas as pd
import matplotlib.pyplot as plt

# 1. 데이터 로드
b2_path = 'data/processed/B2_POS_SALE.parquet'
b4_path = 'data/raw/B4_ITEM_DV_INFO.csv'

print("🔍 데이터 로드 중...")
# B2 (판매): Lazy로 스캔
b2_lazy = pl.scan_parquet(b2_path)

# B4 (마스터): CSV 로드
b4_df = pl.read_csv(b4_path)

# 2. 컬럼명 정렬 및 조인
# B2의 '상품코드'와 B4의 'ITEM_CD'를 맞춤
# B2의 컬럼 확인을 위해 스키마 확인 (GEMINI.md 기준: '상품코드')
b4_df = b4_df.rename({"ITEM_CD": "상품코드"})

# 3. 조인 및 집계
print("📊 카테고리별 매출 집계 중...")
category_analysis = (
    b2_lazy.join(b4_df.lazy(), on="상품코드", how="inner")
    .group_by("ITEM_MDDV_NM") # 중분류명
    .agg([
        pl.col("판매금액").sum().alias("총매출액"),
        pl.col("판매수량").sum().alias("총판매수량"),
        pl.col("ITEM_NM").n_unique().alias("상품수")
    ])
    .sort("총매출액", descending=True)
    .collect()
)

# 4. 결과 출력 (상위 10개)
print("\n--- [상위 10개 중분류별 매출 현황] ---")
print(category_analysis.head(10))

# 5. 간단한 리포트 (Pandas 변환 후 저장 가능)
# category_analysis.to_pandas().to_csv('category_sales_report.csv', index=False)
print("\n✅ 분석 완료: 카테고리별 매출 통계가 집계되었습니다.")
