import polars as pl
import plotly.express as px
import plotly.graph_objects as go
import os

# [Copy-Paste Block for Jupyter Notebook]
# Step 4-3: 시간대별 카테고리 동반 구매(Temporal Affinity) 분석

def run_temporal_affinity_analysis(b2_path, b4_path):
    print(f"--- [분석 시작] 시간대별 소비 맥락 및 카테고리 궁합 분석 ---")
    
    # 1. 데이터 로드 (Lazy)
    b2_lazy = pl.scan_parquet(b2_path)
    b4_lazy = pl.scan_parquet(b4_path)
    
    # 2. 시간대(Time Slot) 정의
    # 06-10: Morning, 11-14: Lunch, 15-18: Afternoon, 19-22: Evening, 23-05: Night
    b2_timed = b2_lazy.with_columns([
        pl.col("판매시간").str.slice(0, 2).cast(pl.Int32).alias("hour")
    ]).with_columns([
        pl.when(pl.col("hour").is_between(6, 10)).then(pl.lit("Morning"))
        .when(pl.col("hour").is_between(11, 14)).then(pl.lit("Lunch"))
        .when(pl.col("hour").is_between(15, 18)).then(pl.lit("Afternoon"))
        .when(pl.col("hour").is_between(19, 22)).then(pl.lit("Evening"))
        .otherwise(pl.lit("Night")).alias("time_slot")
    ])
    
    # 3. 카테고리 정보 결합 및 세션화 (담배 제외)
    sales_master = b2_timed.join(b4_lazy.filter(pl.col("대분류명") != "담배"), on="상품코드", how="inner")
    
    sessions = (
        sales_master.group_by(["점포코드", "POS번호", "판매일자", "거래번호", "time_slot"])
        .agg(pl.col("중분류명").unique())
        .filter(pl.col("중분류명").list.len() >= 2) # 동반 구매만
        .collect()
    )
    
    # 4. 시간대별 카테고리 쌍 빈도 계산
    exploded = sessions.with_row_index("session_id").explode("중분류명")
    
    pairs = (
        exploded.join(exploded, on="session_id")
        .filter(pl.col("중분류명") < pl.col("중분류명_right"))
        .group_by(["time_slot", "중분류명", "중분류명_right"])
        .agg(pl.len().alias("pair_count"))
        .sort(["time_slot", "pair_count"], descending=[False, True])
    )
    
    # 시간대별 Top 5 추출
    top_temporal_pairs = pairs.group_by("time_slot").head(5).sort("time_slot")
    
    # 5. 시각화: 시간대별 주요 동반 구매 조합 (Faceted Bar Chart)
    # 가독성을 위해 "카테고리 A - 카테고리 B" 문자열 생성
    top_temporal_pairs = top_temporal_pairs.with_columns(
        (pl.col("중분류명") + " + " + pl.col("중분류명_right")).alias("pair_name")
    )
    
    fig = px.bar(
        top_temporal_pairs.to_pandas(),
        x="pair_count",
        y="pair_name",
        color="time_slot",
        facet_col="time_slot",
        facet_col_wrap=2,
        orientation="h",
        title="시간대별 주요 카테고리 동반 구매 조합 (Top 5)",
        labels={"pair_count": "동반 구매 빈도", "pair_name": "카테고리 조합"},
        template="plotly_white",
        height=800
    )
    
    # Facet 타이틀 정리
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
    fig.show()
    
    print("\n[시간대별 최상위 조합 분석 결과]")
    print(top_temporal_pairs.select(["time_slot", "pair_name", "pair_count"]))
    
    return top_temporal_pairs

# 실행부
B2_PATH = "../../data/processed/B2_POS_SALE_CLEANED.parquet"
B4_PATH = "../../data/processed/B4_ITEM_DV_INFO.parquet"

if os.path.exists(B2_PATH) and os.path.exists(B4_PATH):
    temp_affinity_res = run_temporal_affinity_analysis(B2_PATH, B4_PATH)
else:
    print("필요한 데이터 파일이 없습니다.")
