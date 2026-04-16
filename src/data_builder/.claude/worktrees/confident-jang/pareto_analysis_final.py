import polars as pl
import pandas as pd
import os

# 1. 데이터 경로 설정 (상대 경로 및 절대 경로 고려)
ROOT = os.getcwd()
B2_PATH = os.path.join(ROOT, 'data/processed/B2_POS_SALE.parquet')
B4_PATH = os.path.join(ROOT, 'data/processed/B4_CLEAN_FOOD_ITEM.parquet')
OUTPUT_PATH = os.path.join(ROOT, 'eda/ipynb/yumi/pareto_analysis_with_items.xlsx')

def run_pareto_analysis():
    print(f"🔍 데이터 경로 확인:\n- B2: {B2_PATH}\n- B4: {B4_PATH}")
    
    if not os.path.exists(B2_PATH):
        print(f"❌ 판매 데이터 파일을 찾을 수 없습니다: {B2_PATH}")
        # 대안 경로 확인 (B2_FOOD_POS_SALE.parquet)
        alt_b2 = os.path.join(ROOT, 'data/processed/B2_FOOD_POS_SALE.parquet')
        if os.path.exists(alt_b2):
            print(f"💡 대안 파일을 사용합니다: {alt_b2}")
            global b2_lazy
            b2_lazy = pl.scan_parquet(alt_b2)
        else:
            return
    else:
        b2_lazy = pl.scan_parquet(B2_PATH)

    # B4 (마스터) 로드
    try:
        if os.path.exists(B4_PATH):
            b4_df = pl.read_parquet(B4_PATH)
        else:
            print(f"⚠️ {B4_PATH}가 없어 CSV로 로드 시도합니다.")
            b4_df = pl.read_csv(os.path.join(ROOT, 'data/raw/B4_ITEM_DV_INFO.csv'), schema_overrides={"ITEM_CD": pl.String})
    except Exception as e:
        print(f"⚠️ B4 로드 오류: {e}")
        b4_df = pl.read_csv(os.path.join(ROOT, 'data/raw/B4_ITEM_DV_INFO.csv'), ignore_errors=True, infer_schema_length=10000)

    # 컬럼명 통일 및 타입 변환
    if "ITEM_CD" in b4_df.columns:
        b4_df = b4_df.rename({"ITEM_CD": "상품코드"})
    b4_df = b4_df.with_columns(pl.col("상품코드").cast(pl.Utf8))

    # 2. 상품별 총매출 집계
    print("📊 상품별 매출 집계 중...")
    item_sales = (
        b2_lazy.group_by("상품코드")
        .agg(pl.col("판매금액").sum().alias("상품별총매출"))
        .collect()
        .with_columns(pl.col("상품코드").cast(pl.Utf8))
    )

    # 3. 마스터 정보와 결합
    df_merged = item_sales.join(b4_df, on="상품코드", how="inner")

    # 4. 중분류별 파레토 분석
    print("📊 중분류별 파레토 분석 및 상품 리스트 추출 시작...")
    results = []
    middle_categories = df_merged["ITEM_MDDV_NM"].unique().to_list()

    for cat in middle_categories:
        if cat is None: continue
        
        df_cat = df_merged.filter(pl.col("ITEM_MDDV_NM") == cat).sort("상품별총매출", descending=True)
        total_items = len(df_cat)
        if total_items < 5: continue
        
        total_sales = df_cat["상품별총매출"].sum()
        if total_sales == 0: continue
        
        df_cat = df_cat.with_columns([
            (pl.col("상품별총매출").cum_sum() / total_sales * 100).alias("누적비중")
        ])
        
        top_80_df = df_cat.filter(pl.col("누적비중") <= 80.5) 
        top_80_count = len(top_80_df)
        if top_80_count == 0: top_80_count = 1
        
        top_20_idx = max(1, int(total_items * 0.2))
        top_20_contribution = (df_cat.head(top_20_idx)["상품별총매출"].sum() / total_sales * 100)
        
        # 핵심 상품명 리스트 (상위 10개)
        top_item_names = ", ".join(df_cat.head(10)["ITEM_NM"].to_list())
        
        results.append({
            "중분류명": cat,
            "전체상품수": total_items,
            "매출80%차지_상품수": top_80_count,
            "매출80%차지_상품비중(%)": round((top_80_count / total_items * 100), 2),
            "상위20%상품_매출비중(%)": round(top_20_contribution, 2),
            "핵심상품리스트(Top10)": top_item_names,
            "카테고리총매출": total_sales
        })

    if not results:
        print("⚠️ 분석 결과가 비어 있습니다.")
        return

    df_pareto = pl.DataFrame(results).sort("상위20%상품_매출비중(%)", descending=True)
    
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df_pareto.to_pandas().to_excel(OUTPUT_PATH, index=False)

    print(f"\n✅ 분석 완료! 저장 경로: {OUTPUT_PATH}")
    print("\n🏆 [매출 집중도 상위 10개 중분류 및 핵심 상품]")
    print(df_pareto.select([
        "중분류명", "상위20%상품_매출비중(%)", "매출80%차지_상품비중(%)", "핵심상품리스트(Top10)"
    ]).head(10))

if __name__ == "__main__":
    run_pareto_analysis()
