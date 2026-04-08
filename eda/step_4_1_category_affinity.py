import polars as pl
import plotly.express as px
import os

# [Copy-Paste Block for Jupyter Notebook]
# Step 4-1: 카테고리 동반 구매(Affinity) 분석 - "무엇과 무엇을 같이 사는가?"

def run_category_affinity_analysis(b2_path, b4_path):
    print(f"--- [분석 시작] 카테고리 간 동반 구매(Affinity) 분석 ---")
    
    # 1. 데이터 로드 및 병합 (Lazy)
    b2_lazy = pl.scan_parquet(b2_path)
    b4_lazy = pl.scan_parquet(b4_path)
    
    # 2. 영수증 세션 정의 및 카테고리 정보 결합
    # 담배/주류 제외 (분석 왜곡 방지)
    exclude_cats = ["담배", "맥주", "와인", "양주", "소주/전통주"]
    b4_filtered = b4_lazy.filter(~pl.col("대분류명").is_in(exclude_cats))
    
    # 영수증 단위로 묶기 (세션 ID 생성)
    # 메모리 효율을 위해 장바구니 크기가 2 이상인 세션만 타겟팅
    session_data = (
        b2_lazy.join(b4_filtered, on="상품코드", how="inner")
        .with_columns([
            pl.col("판매시간").str.slice(0, 4).alias("판매분")
        ])
        .select(["점포코드", "POS번호", "판매일자", "판매분", "거래번호", "중분류명"])
        .group_by(["점포코드", "POS번호", "판매일자", "판매분", "거래번호"])
        .agg(pl.col("중분류명").unique()) # 한 영수증 내 동일 카테고리 중복 제거
        .filter(pl.col("중분류명").list.len() >= 2) # 동반 구매만 필터링
        .collect()
    )
    
    # 3. 카테고리 쌍(Pair) 생성 및 빈도 계산
    # 리스트 형태의 카테고리들을 조합(Combination)으로 변환
    # 대용량 처리를 위해 폭발(Explode) 후 Self-Join 방식 활용
    exploded = session_data.with_row_index("session_id").explode("중분류명")
    
    pairs = (
        exploded.join(exploded, on="session_id")
        .filter(pl.col("중분류명") < pl.col("중분류명_right")) # 중복 및 자기 자신 제거
        .group_by(["중분류명", "중분류명_right"])
        .agg(pl.len().alias("pair_count"))
        .sort("pair_count", descending=True)
    )
    
    # 4. 시각화: 상위 30개 동반 구매 조합 히트맵
    top_pairs = pairs.head(30)
    
    pivot_df = top_pairs.pivot(
        on="중분류명_right",
        index="중분류명",
        values="pair_count"
    ).to_pandas().set_index("중분류명").fillna(0)
    
    fig = px.imshow(
        pivot_df,
        labels=dict(x="카테고리 B", y="카테고리 A", color="동반 구매 건수"),
        title="주요 카테고리별 동반 구매(Affinity) 히트맵 (담배/주류 제외)",
        color_continuous_scale="Viridis",
        aspect="auto"
    )
    
    fig.update_layout(template="plotly_white", height=700)
    fig.show()
    
    # 5. 결과 요약
    print("\n[최고의 궁합: 동반 구매 Top 10 조합]")
    print(pairs.head(10))
    
    return pairs

# 실행부
B2_PATH = "../../data/processed/B2_POS_SALE_CLEANED.parquet"
B4_PATH = "../../data/processed/B4_ITEM_DV_INFO.parquet"

if os.path.exists(B2_PATH) and os.path.exists(B4_PATH):
    affinity_res = run_category_affinity_analysis(B2_PATH, B4_PATH)
else:
    print("필요한 데이터 파일이 없습니다.")
