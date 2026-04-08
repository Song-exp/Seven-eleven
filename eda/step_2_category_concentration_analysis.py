import polars as pl
import plotly.express as px
import plotly.graph_objects as go
import os

# [Copy-Paste Block for Jupyter Notebook]
# Step 2: 카테고리별 매출 쏠림도 및 신제품(NPD) 출시 빈도 분석

def run_category_concentration_analysis(b2_path, b4_path):
    print(f"--- [분석 시작] 카테고리별 매출 비중 및 NPD 출시 패턴 분석 ---")
    
    # 1. 데이터 로드 및 병합 (Lazy)
    b2_lazy = pl.scan_parquet(b2_path)
    b4_lazy = pl.scan_parquet(b4_path)
    
    # B2 데이터에 B4 분류 정보 결합
    # B4의 상품코드가 String인지 확인 후 Join
    sales_master = b2_lazy.join(b4_lazy, on="상품코드", how="inner")
    
    # 2. 대분류별 매출 비중 및 NPD 수 집계
    # 신제품 정의: 전체 기간 중 최초 등장한 상품 (Step 1 로직 활용)
    launch_dates = (
        b2_lazy.group_by("상품코드")
        .agg(pl.col("판매일자").min().alias("launch_dt"))
    )
    
    # 카테고리별 지표 산출
    cat_stats = (
        sales_master.join(launch_dates, on="상품코드")
        .group_by(["대분류명", "중분류명"])
        .agg([
            pl.col("판매금액").sum().alias("total_sales"),
            pl.col("상품코드").n_unique().alias("total_items"),
            pl.col("상품코드").filter(pl.col("launch_dt") >= "20250101").n_unique().alias("npd_count") # 25년 이후 신제품 기준
        ])
        .collect()
        .sort("total_sales", descending=True)
    )
    
    # 매출 비중(%) 계산
    total_revenue = cat_stats["total_sales"].sum()
    cat_stats = cat_stats.with_columns(
        (pl.col("total_sales") / total_revenue * 100).alias("sales_share_pct")
    )
    
    # 3. 시각화 1: 중분류별 매출 비중 vs NPD 수 (Dual Axis)
    top_n = 20 # 상위 20개 중분류
    top_cat = cat_stats.head(top_n)
    
    fig = go.Figure()
    
    # 매출 비중 (Bar)
    fig.add_trace(go.Bar(
        x=top_cat["중분류명"],
        y=top_cat["sales_share_pct"],
        name="매출 비중 (%)",
        marker_color='rgb(55, 83, 109)'
    ))
    
    # NPD 수 (Line)
    fig.add_trace(go.Scatter(
        x=top_cat["중분류명"],
        y=top_cat["npd_count"],
        name="신제품(NPD) 수",
        yaxis="y2",
        mode='lines+markers',
        line=dict(color='firebrick', width=3)
    ))
    
    fig.update_layout(
        title=f"상위 {top_n}개 중분류별 매출 비중 및 신제품 출시 현황",
        xaxis_title="중분류명",
        yaxis=dict(title="매출 비중 (%)"),
        yaxis2=dict(title="신제품(NPD) 수", overlaying="y", side="right"),
        template="plotly_white",
        legend=dict(x=0.8, y=0.9)
    )
    
    fig.show()
    
    # 4. 롱테일(Long-tail) 분석 결과 요약
    print("\n[카테고리 쏠림도(Long-tail) 분석 결과]")
    top_5_share = cat_stats.head(5)["sales_share_pct"].sum()
    print(f" - 상위 5개 중분류의 매출 합계 비중: {top_5_share:.2f}%")
    
    # 신제품 집중도 분석
    npd_density = (cat_stats.filter(pl.col("total_items") > 0)
                  .with_columns((pl.col("npd_count") / pl.col("total_items")).alias("npd_density")))
    
    top_density = npd_density.sort("npd_density", descending=True).head(5)
    print("\n[신제품 출시 밀도가 높은 카테고리 (Top 5)]")
    print(top_density.select(["대분류명", "중분류명", "npd_density"]))
    
    return cat_stats

# 실행부
B2_PATH = "../../data/processed/B2_POS_SALE_CLEANED.parquet"
B4_PATH = "../../data/processed/B4_ITEM_DV_INFO.parquet"

if os.path.exists(B2_PATH) and os.path.exists(B4_PATH):
    cat_analysis_res = run_category_concentration_analysis(B2_PATH, B4_PATH)
else:
    print("필요한 데이터 파일이 없습니다.")
