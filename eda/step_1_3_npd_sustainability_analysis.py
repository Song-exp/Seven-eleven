import polars as pl
import plotly.express as px
import plotly.graph_objects as go
import os

# [Copy-Paste Block for Jupyter Notebook]
# Step 1 추가: NPD 지속성(Sustainability) 및 히트 상품 전이 분석

def run_npd_sustainability_analysis(b2_path):
    print(f"--- [분석 시작] 신제품 초기 흥행의 지속성(Sustainability) 검증 ---")
    
    # 1. 데이터 로드 및 누적 매출 계산 (Lazy)
    b2_lazy = pl.scan_parquet(b2_path)
    
    # 출시일 산출 및 주차별 매출 집계 로직
    b2_with_date = b2_lazy.with_columns(
        pl.col("판매일자").str.to_date("%Y%m%d").alias("sale_dt")
    )
    
    launch_dates = (
        b2_with_date
        .group_by("상품코드")
        .agg(pl.col("sale_dt").min().alias("launch_dt"))
    )
    
    # 1주차(7일) 및 4주차(28일) 매출 계산
    # 주차별 '순수' 매출 증분을 보기 위해 1w와 4w를 각각 독립적으로 집계
    npd_sales = (
        b2_with_date
        .join(launch_dates, on="상품코드")
        .with_columns(
            (pl.col("sale_dt") - pl.col("launch_dt")).dt.total_days().alias("days_after")
        )
        .group_by("상품코드")
        .agg([
            pl.col("판매금액").filter(pl.col("days_after") <= 7).sum().alias("sales_1w"),
            pl.col("판매금액").filter(pl.col("days_after") <= 28).sum().alias("sales_4w")
        ])
        .filter(pl.col("sales_4w") > 0)
        .collect()
    )
    
    # 2. 히트 상품(Top 20%) 정의 및 라벨링
    threshold_1w = npd_sales.select(pl.col("sales_1w").quantile(0.8)).item()
    threshold_4w = npd_sales.select(pl.col("sales_4w").quantile(0.8)).item()
    
    npd_labeled = npd_sales.with_columns([
        (pl.col("sales_1w") >= threshold_1w).alias("is_hit_1w"),
        (pl.col("sales_4w") >= threshold_4w).alias("is_hit_4w")
    ])
    
    # 3. 지속성(Retention) 지표 산출
    # 1주차 히트 상품 중 4주차에도 히트인 비율
    hit_1w_count = npd_labeled.filter(pl.col("is_hit_1w")).height
    sustained_hit_count = npd_labeled.filter(pl.col("is_hit_1w") & pl.col("is_hit_4w")).height
    retention_rate = (sustained_hit_count / hit_1w_count * 100) if hit_1w_count > 0 else 0
    
    print(f"\n[지속성 분석 결과]")
    print(f" - 1주차 히트 상품 수: {hit_1w_count:,} 개 (임계값: {threshold_1w:,.0f}원)")
    print(f" - 4주차 히트 유지 수: {sustained_hit_count:,} 개 (임계값: {threshold_4w:,.0f}원)")
    print(f" - 성공 유지율(Retention Rate): {retention_rate:.2f}%")
    
    # 4. 시각화: 1w vs 4w 매출 상관관계 산점도
    fig = px.scatter(
        npd_labeled.to_pandas(),
        x="sales_1w",
        y="sales_4w",
        color="is_hit_4w",
        log_x=True,
        log_y=True,
        hover_data=["상품코드"],
        title="NPD 초기 성과(1w) vs 최종 성과(4w) 상관관계 분석",
        labels={"sales_1w": "1주차 누적 매출 (Log)", "sales_4w": "4주차 누적 매출 (Log)"},
        template="plotly_white",
        color_discrete_map={True: "red", False: "blue"}
    )
    
    # 가이드라인 추가
    fig.add_vline(x=threshold_1w, line_dash="dash", line_color="gray", annotation_text="1w Hit Line")
    fig.add_hline(y=threshold_4w, line_dash="dash", line_color="gray", annotation_text="4w Hit Line")
    
    fig.show()
    
    # 5. 전이 행렬(Transition Matrix) 시각화 준비
    print("\n[성공 상태 전이 행렬 (Transition Matrix)]")
    transition = npd_labeled.group_by(["is_hit_1w", "is_hit_4w"]).agg(pl.len().alias("count")).sort(["is_hit_1w", "is_hit_4w"])
    print(transition)
    
    return npd_labeled

# 실행부
B2_PATH = "../../data/processed/B2_POS_SALE_CLEANED.parquet"
if os.path.exists(B2_PATH):
    npd_analysis_res = run_npd_sustainability_analysis(B2_PATH)
else:
    print(f"파일을 찾을 수 없습니다: {B2_PATH}")
