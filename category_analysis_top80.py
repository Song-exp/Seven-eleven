import polars as pl
import pandas as pd
import os

# 1. 데이터 경로 설정
ROOT = os.getcwd()
B2_PATH = os.path.join(ROOT, 'data/processed/B2_POS_SALE.parquet')
B4_PATH = os.path.join(ROOT, 'data/processed/B4_CLEAN_FOOD_ITEM.parquet')
OUTPUT_PATH = os.path.join(ROOT, 'eda/ipynb/yumi/category_top80_analysis.xlsx')

def run_top80_category_analysis():
    print("🔍 데이터를 로드하고 분석을 준비합니다...")
    
    # 데이터 로드 (Lazy 모드)
    b2_lazy = pl.scan_parquet(B2_PATH)
    
    try:
        if os.path.exists(B4_PATH):
            b4_df = pl.read_parquet(B4_PATH)
        else:
            b4_df = pl.read_csv(os.path.join(ROOT, 'data/raw/B4_ITEM_DV_INFO.csv'), schema_overrides={"ITEM_CD": pl.String})
    except Exception as e:
        print(f"⚠️ B4 로드 오류: {e}")
        b4_df = pl.read_csv(os.path.join(ROOT, 'data/raw/B4_ITEM_DV_INFO.csv'), ignore_errors=True, infer_schema_length=10000)

    # 컬럼명 통일 및 타입 변환
    if "ITEM_CD" in b4_df.columns:
        b4_df = b4_df.rename({"ITEM_CD": "상품코드"})
    b4_df = b4_df.with_columns(pl.col("상품코드").cast(pl.Utf8))

    # 2. 상품별 총매출 집계
    print("📊 상품별 총매출 집계 중...")
    item_sales = (
        b2_lazy.group_by("상품코드")
        .agg(pl.col("판매금액").sum().alias("상품별총매출"))
        .collect()
        .with_columns(pl.col("상품코드").cast(pl.Utf8))
    )

    # 3. 마스터 정보와 결합 (중분류명, 상품명 포함)
    df_merged = item_sales.join(b4_df, on="상품코드", how="inner")

    # 4. 중분류별 80% 핵심 상품 분석
    print("📊 중분류별 매출 80% 기여 상품 리스트업 시작...")
    results = []
    middle_categories = df_merged["ITEM_MDDV_NM"].unique().to_list()

    for cat in middle_categories:
        if cat is None: continue
        
        # 해당 중분류 필터링 및 매출 순 정렬
        df_cat = df_merged.filter(pl.col("ITEM_MDDV_NM") == cat).sort("상품별총매출", descending=True)
        
        total_items = len(df_cat)
        if total_items == 0: continue
        
        total_sales = df_cat["상품별총매출"].sum()
        if total_sales == 0: continue
        
        # 누적 비중 계산
        df_cat = df_cat.with_columns([
            (pl.col("상품별총매출").cum_sum() / total_sales * 100).alias("누적비중")
        ])
        
        # 매출 80% 안에 드는 상품들 필터링
        # (마지막 상품이 80%를 살짝 넘기더라도 포함시키기 위해 80% 이하인 행 + 바로 다음 행 하나까지 고려 가능하지만, 여기선 깔끔하게 80.5% 기준으로 잡습니다)
        df_top80 = df_cat.filter(pl.col("누적비중") <= 80.5)
        
        # 만약 필터링된 상품이 하나도 없다면(첫 상품이 이미 80%를 넘는 경우), 첫 번째 상품을 포함
        if len(df_top80) == 0:
            df_top80 = df_cat.head(1)
            
        top80_item_names = df_top80["ITEM_NM"].to_list()
        top80_count = len(top80_item_names)
        
        results.append({
            "중분류명": cat,
            "전체상품수": total_items,
            "상위80%상품수": top80_count,
            "상위80%상품비중(%)": round((top80_count / total_items * 100), 2),
            "매출80%기여_상품리스트": ", ".join(top80_item_names),
            "카테고리총매출": total_sales
        })

    # 5. 결과 저장
    df_final = pl.DataFrame(results).sort("카테고리총매출", descending=True)
    
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df_final.to_pandas().to_excel(OUTPUT_PATH, index=False)

    print(f"\n✅ 분석 완료! 결과가 엑셀로 저장되었습니다: {OUTPUT_PATH}")
    print("\n🏆 [매출 상위 10개 중분류의 80% 핵심 상품 현황]")
    print(df_final.select(["중분류명", "전체상품수", "상위80%상품수", "상위80%상품비중(%)"]).head(10))

if __name__ == "__main__":
    run_top80_category_analysis()
