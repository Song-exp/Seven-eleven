import polars as pl
import plotly.express as px
import plotly.graph_objects as go
import os

# [Copy-Paste Block for Jupyter Notebook]
# Step 4-2: 신제품(NPD)의 바스켓 견인 효과 분석 (수정 버전)

def run_npd_basket_impact_analysis(b2_path):
    print(f"--- [분석 시작] 신제품(NPD)이 장바구니 크기에 미치는 영향 분석 ---")
    
    # 1. 데이터 로드 (Lazy)
    b2_lazy = pl.scan_parquet(b2_path)
    
    # 2. 신제품(NPD) 식별 (최초 등장일 기준)
    # 데이터의 전체 기간 중 초기 일부를 제외한 나머지를 NPD로 정의하기 위해 출시일 산출
    launch_dates = (
        b2_lazy.group_by("상품코드")
        .agg(pl.col("판매일자").min().alias("launch_dt"))
        .collect()
    )
    
    # 데이터셋의 최소 날짜 확인 후, 그 이후에 등장한 것을 NPD로 간주 (유동적 기준)
    min_date = launch_dates["launch_dt"].min()
    # 예: 데이터 시작일로부터 7일 이후에 처음 나타난 상품을 NPD로 정의
    # (실제 비즈니스 로직에 따라 "20250101" 등으로 고정 가능)
    npd_threshold = "20250101" 
    
    npd_list = launch_dates.filter(pl.col("launch_dt") >= npd_threshold)["상품코드"].to_list()
    npd_set = set(npd_list)
    
    print(f" - 전체 고유 상품 수: {len(launch_dates):,} 개")
    print(f" - 식별된 신제품(NPD) 수: {len(npd_set):,} 개 (기준일: {npd_threshold})")
    
    if len(npd_set) == 0:
        print("경고: 신제품으로 분류된 상품이 없습니다. 기준일(npd_threshold)을 조정하세요.")
        return

    # 3. 영수증 단위 집계 및 NPD 포함 여부 체크
    # 대용량 처리를 위해 필요한 컬럼만 선택
    receipt_data = (
        b2_lazy.with_columns([
            pl.col("판매시간").str.slice(0, 4).alias("판매분")
        ])
        .group_by(["점포코드", "POS번호", "판매일자", "판매분", "거래번호"])
        .agg([
            pl.col("상품코드").alias("items"),
            pl.col("판매금액").sum().alias("receipt_amt"),
            pl.col("상품코드").n_unique().alias("basket_size")
        ])
        .collect()
    )
    
    # NPD 포함 여부 플래그 생성 (최적화된 방식)
    receipt_data = receipt_data.with_columns(
        pl.col("items").map_elements(lambda x: any(item in npd_set for item in x), return_dtype=pl.Boolean).alias("has_npd")
    )
    
    # 4. 그룹별 비교 통계
    comparison = (
        receipt_data.group_by("has_npd")
        .agg([
            pl.len().alias("receipt_count"),
            pl.col("basket_size").mean().alias("avg_basket_size"),
            pl.col("receipt_amt").mean().alias("avg_atv")
        ])
        .sort("has_npd")
    )
    
    print("\n[NPD 포함 여부에 따른 구매 행태 비교]")
    print(comparison)
    
    # 5. 안전한 지표 계산 및 시각화
    if comparison.height < 2:
        print("\n비교 불가: 한 쪽 그룹(NPD 포함 혹은 미포함)에 데이터가 없습니다.")
        return

    # 지표 추출
    normal_atv = comparison.filter(pl.col("has_npd") == False)["avg_atv"][0]
    npd_atv = comparison.filter(pl.col("has_npd") == True)["avg_atv"][0]
    lift = ((npd_atv / normal_atv) - 1) * 100
    
    print(f"\n인사이트: 신제품 포함 시 객단가 변화율(Lift): {lift:.2f}%")
    
    # 시각화
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=["일반 영수증", "신제품 포함 영수증"],
        y=comparison["avg_basket_size"],
        text=comparison["avg_basket_size"].round(2),
        textposition='auto',
        marker_color=['gray', 'red']
    ))
    
    fig.update_layout(
        title="신제품(NPD) 포함 여부에 따른 평균 장바구니 크기 비교",
        yaxis_title="평균 구매 상품 수 (SKU)",
        template="plotly_white"
    )
    fig.show()
    
    return receipt_data

# 실행부
B2_PATH = "../../data/processed/B2_POS_SALE_CLEANED.parquet"
if os.path.exists(B2_PATH):
    impact_res = run_npd_basket_impact_analysis(B2_PATH)
else:
    print(f"파일을 찾을 수 없습니다: {B2_PATH}")
