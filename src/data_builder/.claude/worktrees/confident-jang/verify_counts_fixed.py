import polars as pl
import os

# 1. 파일 경로 (현재 프로젝트 기준)
B2_PATH = 'data/processed/B2_POS_SALE.parquet'
B4_PATH = 'data/raw/B4_ITEM_DV_INFO.csv'

def check_stats():
    print("🔍 [데이터 수치 검증 시작]")
    
    # [1] B4 상품 마스터 검증
    print("\n--- B4 (상품 마스터) 검증 ---")
    if not os.path.exists(B4_PATH):
        print(f"❌ B4 파일을 찾을 수 없습니다: {B4_PATH}")
        return
        
    b4_df = pl.read_csv(B4_PATH)
    total_b4 = len(b4_df)
    print(f"원본 B4 행 수: {total_b4:,} 건 (목표: 159,075)")

    # 식품 필터링 기준 (비식품류 제외)
    exclude_list = ['서비스', '담배', '비식품', '잡화', '기타']
    b4_food = b4_df.filter(~pl.col("ITEM_LRDV_NM").is_in(exclude_list))
    
    food_b4_count = len(b4_food)
    print(f"필터링 후 B4 행 수: {food_b4_count:,} 건 (목표: 92,357)")

    # [2] B2 판매 데이터 검증
    print("\n--- B2 (판매 데이터) 검증 ---")
    if not os.path.exists(B2_PATH):
        print(f"❌ B2 파일을 찾을 수 없습니다: {B2_PATH}")
        return
        
    b2_lazy = pl.scan_parquet(B2_PATH)
    total_b2 = b2_lazy.select(pl.len()).collect().item()
    print(f"B2 전체 행 수: {total_b2:,} 건 (목표: 101,399,891)")

    # [3] 식품 전용 POS 데이터 (B4 식품 마스터와 조인)
    print("📊 식품 데이터 필터링(Join) 중... (데이터가 커서 수 초가 걸릴 수 있습니다)")
    food_pos_count = (
        b2_lazy.join(
            b4_food.lazy().select(["ITEM_CD"]), 
            left_on="상품코드", 
            right_on="ITEM_CD", 
            how="inner"
        )
        .select(pl.len())
        .collect()
        .item()
    )
    print(f"식품 전용 POS 행 수: {food_pos_count:,} 건 (목표: 72,044,884)")

    print("\n" + "="*50)
    print(f"✅ 최종 확인 완료!")
    print("="*50)

if __name__ == "__main__":
    check_stats()
