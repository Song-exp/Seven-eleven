import polars as pl
import pandas as pd
import os

# 1. 데이터 경로 설정
b2_path = 'data/processed/B2_POS_SALE.parquet'
b4_path = 'data/processed/B4_CLEAN_FOOD_ITEM.parquet'

# 2. 데이터 로드
print("🔍 데이터 로딩 및 결합 중...")
# B2 (판매): Lazy로 스캔
b2_lazy = pl.scan_parquet(b2_path)

# B4 (마스터): Parquet 우선, 실패 시 CSV 로드 (ITEM_CD: str 강제)
try:
    if os.path.exists(b4_path):
        b4_df = pl.read_parquet(b4_path)
    else:
        b4_df = pl.read_csv('data/raw/B4_ITEM_DV_INFO.csv', schema_overrides={"ITEM_CD": pl.String})
except Exception as e:
    print(f"⚠️ B4 데이터 로드 중 오류 발생: {e}")
    b4_df = pl.read_csv('data/raw/B4_ITEM_DV_INFO.csv', ignore_errors=True, infer_schema_length=10000)

# 컬럼명 통일 (B4: ITEM_CD, B2: 상품코드)
if "ITEM_CD" in b4_df.columns:
    b4_df = b4_df.rename({"ITEM_CD": "상품코드"})

# 3. 상품별 총매출 집계 (전체 기간)
item_sales = (
    b2_lazy.group_by("상품코드")
    .agg(pl.col("판매금액").sum().alias("상품별총매출"))
    .collect()
)

# 4. 마스터 정보와 결합 (중분류명 포함)
df_merged = item_sales.join(b4_df, on="상품코드", how="inner")

# 5. 중분류별 파레토 분석 수행
print("📊 중분류별 파레토 분석 시작...")
results = []
middle_categories = df_merged["ITEM_MDDV_NM"].unique().to_list()

for cat in middle_categories:
    if cat is None: continue
    
    # 해당 중분류 필터링 및 매출 순 정렬
    df_cat = df_merged.filter(pl.col("ITEM_MDDV_NM") == cat).sort("상품별총매출", descending=True)
    
    total_items = len(df_cat)
    if total_items < 5: continue # 데이터가 너무 적은 카테고리 제외
    
    total_sales = df_cat["상품별총매출"].sum()
    if total_sales == 0: continue
    
    # 누적 매출액 및 누적 비중 계산
    df_cat = df_cat.with_columns([
        pl.col("상품별총매출").cum_sum().alias("누적매출액"),
        (pl.col("상품별총매출").cum_sum() / total_sales * 100).alias("누적매출비중_pct")
    ])
    
    # 매출 80%를 차지하는 상품 수 및 비중 계산
    top_80_items_df = df_cat.filter(pl.col("누적매출비중_pct") <= 80)
    top_80_count = len(top_80_items_df)
    top_80_share_pct = (top_80_count / total_items * 100)
    
    # 상위 20% 상품이 차지하는 매출 비중 계산
    top_20_threshold = int(total_items * 0.2)
    if top_20_threshold == 0: top_20_threshold = 1
    top_20_sales_share = (df_cat.head(top_20_threshold)["상품별총매출"].sum() / total_sales * 100)
    
    results.append({
        "중분류명": cat,
        "전체상품수": total_items,
        "매출80%차지_상품수": top_80_count,
        "매출80%차지_상품비중(%)": top_80_share_pct,
        "상위20%상품_매출비중(%)": top_20_sales_share,
        "총매출액": total_sales
    })

# 6. 결과 데이터프레임 생성 및 정렬 (상위 20% 상품의 매출 비중이 높은 순 = 집중도가 높은 순)
df_pareto = pl.DataFrame(results).sort("상위20%상품_매출비중(%)", descending=True)

# 7. 결과 저장 및 출력
output_path = 'eda/ipynb/yumi/pareto_analysis_middle_category.xlsx'
df_pareto.to_pandas().to_excel(output_path, index=False)

print(f"\n✅ 파레토 분석 완료! 결과 저장됨: {output_path}")
print("\n🏆 [매출 집중도(상위 20%의 매출 비중) 상위 15개 중분류]")
print(df_pareto.select(["중분류명", "전체상품수", "상위20%상품_매출비중(%)", "매출80%차지_상품비중(%)"]).head(15))
