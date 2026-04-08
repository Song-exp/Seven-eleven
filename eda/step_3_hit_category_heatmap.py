import polars as pl
import plotly.express as px
import plotly.graph_objects as go
import os

# [Copy-Paste Block for Jupyter Notebook]
# Step 3: 히트 상품(Top 20%)의 카테고리 밀집도 및 히트맵 분석

def run_hit_category_heatmap(b2_path, b4_path):
    print(f"--- [분석 시작] 히트 상품의 카테고리별 밀집도 및 히트맵 분석 ---")
    
    # 1. 데이터 로드 (Lazy)
    b2_lazy = pl.scan_parquet(b2_path)
    b4_lazy = pl.scan_parquet(b4_path)
    
    # 2. 담배/주류 제외 필터링 (Step 2-1 기준 유지)
    exclude_cats = ["담배", "맥주", "와인", "양주", "소주/전통주"]
    b4_filtered = b4_lazy.filter(~pl.col("대분류명").is_in(exclude_cats))
    
    # 3. 신제품 추출 및 4주차 누적 매출 계산
    launch_dates = (
        b2_lazy.group_by("상품코드")
        .agg(pl.col("판매일자").min().alias("launch_dt"))
    )
    
    npd_sales = (
        b2_lazy.join(launch_dates, on="상품코드")
        .with_columns(
            (pl.col("판매일자").str.to_date("%Y%m%d") - pl.col("launch_dt").str.to_date("%Y%m%d")).dt.total_days().alias("days_after")
        )
        .filter(pl.col("days_after") <= 28)
        .group_by("상품코드")
        .agg(pl.col("판매금액").sum().alias("cum_4w"))
        .filter(pl.col("cum_4w") > 0)
    )
    
    # 4. 히트 상품(Top 20%) 정의
    # 전체 NPD 중 상위 20% 임계값 산출
    threshold_4w = npd_sales.select(pl.col("cum_4w").quantile(0.8)).collect().item()
    
    hit_items = (
        npd_sales.filter(pl.col("cum_4w") >= threshold_4w)
        .select("상품코드")
    )
    
    # 5. 히트 상품과 카테고리 정보 결합
    hit_master = (
        hit_items.join(b4_filtered, on="상품코드", how="inner")
        .group_by(["대분류명", "중분류명"])
        .agg(pl.len().alias("hit_count"))
        .collect()
        .sort("hit_count", descending=True)
    )
    
    # 6. 시각화: 히트맵 (Heatmap)
    # 대분류 vs 중분류 구조로 Pivot
    pivot_df = hit_master.pivot(
        on="중분류명",
        index="대분류명",
        values="hit_count"
    ).to_pandas().set_index("대분류명").fillna(0)
    
    fig = px.imshow(
        pivot_df,
        labels=dict(x="중분류명", y="대분류명", color="히트 상품 수"),
        x=pivot_df.columns,
        y=pivot_df.index,
        color_continuous_scale="Reds",
        title="신제품 히트 상품(Top 20%) 카테고리 밀집도 히트맵",
        aspect="auto"
    )
    
    fig.update_layout(
        xaxis_title="중분류명",
        yaxis_title="대분류명",
        template="plotly_white",
        height=600
    )
    
    fig.show()
    
    # 7. 결과 요약
    print(f"\n[히트 상품 카테고리 집중도 분석 결과]")
    print(f" - 분석 대상 히트 상품 수: {hit_items.collect().height:,} 개")
    print(f" - 히트 상품이 가장 많이 포진된 Top 5 중분류:")
    print(hit_master.head(5))
    
    return hit_master

# 실행부
B2_PATH = "../../data/processed/B2_POS_SALE_CLEANED.parquet"
B4_PATH = "../../data/processed/B4_ITEM_DV_INFO.parquet"

if os.path.exists(B2_PATH) and os.path.exists(B4_PATH):
    hit_heatmap_res = run_hit_category_heatmap(B2_PATH, B4_PATH)
else:
    print("필요한 데이터 파일이 없습니다.")
