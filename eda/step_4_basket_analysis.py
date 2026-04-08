import polars as pl
import plotly.express as px
import plotly.graph_objects as go
import os

# [Copy-Paste Block for Jupyter Notebook]
# Step 4: 영수증 단위 요약 (Basket Size & ATV 분석)

def run_basket_analysis(b2_path):
    print(f"--- [분석 시작] 영수증(Session) 단위 소비 패턴 분석 ---")
    
    # 1. 데이터 로드 (Lazy)
    b2_lazy = pl.scan_parquet(b2_path)
    
    # 2. 영수증(Session) 식별자 생성 및 집계
    # 지시사항: 점포 + POS + 일자 + 시간(분 단위) 
    # 판매시간(HHMMSS)에서 앞 4자리(HHMM)만 추출하여 분 단위 세션 생성
    receipt_stats = (
        b2_lazy.with_columns([
            pl.col("판매시간").str.slice(0, 4).alias("판매분")
        ])
        .group_by(["점포코드", "POS번호", "판매일자", "판매분", "거래번호"]) # 거래번호를 포함하여 무결성 강화
        .agg([
            pl.col("상품코드").n_unique().alias("basket_size"),
            pl.col("판매금액").sum().alias("receipt_amt"),
            pl.col("판매수량").sum().alias("total_qty")
        ])
        .collect()
    )
    
    # 3. 글로벌 지표 계산
    total_receipts = len(receipt_stats)
    avg_basket_size = receipt_stats["basket_size"].mean()
    avg_atv = receipt_stats["receipt_amt"].mean()
    
    # 단일 상품 결제 비율
    single_item_receipts = receipt_stats.filter(pl.col("basket_size") == 1).height
    single_rate = (single_item_receipts / total_receipts) * 100
    
    print(f"\n[영수증 통계 요약]")
    print(f" - 전체 분석 영수증 수: {total_receipts:,} 건")
    print(f" - 평균 장바구니 크기(Basket Size): {avg_basket_size:.2f} 개")
    print(f" - 평균 객단가(ATV): {avg_atv:,.0f} 원")
    print(f" - 단일 상품 결제 비중: {single_rate:.2f}% ({single_item_receipts:,} 건)")
    
    # 4. 시각화: Basket Size 분포 (Histogram)
    # 10개 이상의 대형 장바구니는 '10+'로 묶어서 시각화 가독성 확보
    receipt_stats = receipt_stats.with_columns(
        pl.when(pl.col("basket_size") >= 10).then(10).otherwise(pl.col("basket_size")).alias("basket_size_clipped")
    )
    
    dist_data = receipt_stats.group_by("basket_size_clipped").agg(pl.len().alias("count")).sort("basket_size_clipped")
    
    fig = px.bar(
        dist_data.to_pandas(),
        x="basket_size_clipped",
        y="count",
        title="장바구니 크기(Basket Size) 분포",
        labels={"basket_size_clipped": "구매 상품 수 (SKU)", "count": "영수증 수"},
        text_auto='.2s',
        template="plotly_white"
    )
    fig.update_layout(xaxis=dict(tickmode='linear', tick0=1, dtick=1))
    fig.show()
    
    # 5. 비즈니스 인사이트 도출을 위한 구간 분석
    print("\n[장바구니 구간별 객단가]")
    atv_by_size = (
        receipt_stats.group_by("basket_size_clipped")
        .agg(pl.col("receipt_amt").mean().alias("avg_receipt_amt"))
        .sort("basket_size_clipped")
    )
    print(atv_by_size)
    
    return receipt_stats

# 실행부
B2_PATH = "../../data/processed/B2_POS_SALE_CLEANED.parquet"
if os.path.exists(B2_PATH):
    basket_res = run_basket_analysis(B2_PATH)
else:
    print(f"파일을 찾을 수 없습니다: {B2_PATH}")
