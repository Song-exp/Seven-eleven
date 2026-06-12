import polars as pl
import os

# 1. 경로 설정
B2_PATH = 'data/processed/B2_POS_SALE.parquet'
B4_RAW_PATH = 'data/raw/B4_ITEM_DV_INFO.csv'
OUTPUT_B2_PATH = 'data/processed/B2_FOOD_POS_SALE.parquet'
OUTPUT_B4_PATH = 'data/processed/B4_CLEAN_FOOD_ITEM.parquet'

# 2. 선정된 식품 카테고리 리스트
food_categories = [
    "디저트", "빵", "간식", "미반", "냉장", "신선", "즉석음료", "즉석 식품", 
    "유음료", "조리빵", "전통주", "면", "과자", "조미료/건물", "음료", 
    "냉동", "맥주", "안주", "건강/기호식품", "커피/차", "아이스크림", 
    "가공식품", "양주와인"
]

def run_preprocessing():
    print(f"🚀 [최종 전처리] 선정된 {len(food_categories)}개 카테고리 기반 데이터 추출 시작")

    if not os.path.exists(B2_PATH):
        print(f"❌ B2 파일을 찾을 수 없습니다: {B2_PATH}")
        return

    # --- [Step 1] B4 식품 마스터 필터링 ---
    print("\n--- [1] B4 식품 마스터 필터링 중... ---")
    b4_df = pl.read_csv(B4_RAW_PATH, schema_overrides={"ITEM_CD": pl.String})
    
    # 제공된 리스트에 포함된 대분류만 필터링
    b4_food = b4_df.filter(pl.col("ITEM_LRDV_NM").is_in(food_categories))
    
    food_b4_count = len(b4_food)
    print(f"✅ B4 식품 마스터 행 수: {food_b4_count:,} 건 (목표: 92,357)")

    # --- [Step 2] B2 식품 판매 데이터 필터링 ---
    print("\n--- [2] B2 식품 판매 데이터 추출 중... (대용량 데이터라 시간이 소요됩니다) ---")
    b2_lazy = pl.scan_parquet(B2_PATH)
    
    # Inner Join을 통해 식품 데이터만 추출
    b2_food_lazy = b2_lazy.join(
        b4_food.lazy().select(["ITEM_CD"]), 
        left_on="상품코드", 
        right_on="ITEM_CD", 
        how="inner"
    )
    
    # 최종 결과 수집
    b2_food = b2_food_lazy.collect()
    food_pos_count = len(b2_food)
    print(f"✅ B2 식품 판매 데이터 행 수: {food_pos_count:,} 건 (목표: 72,044,884)")

    # --- [Step 3] 결과 저장 ---
    print("\n💾 데이터 저장 중...")
    if not os.path.exists('data/processed'):
        os.makedirs('data/processed')
        
    b2_food.write_parquet(OUTPUT_B2_PATH)
    b4_food.write_parquet(OUTPUT_B4_PATH)
    
    print(f"\n✅ 전처리 완료!")
    print(f"- 저장 파일 1: {OUTPUT_B2_PATH}")
    print(f"- 저장 파일 2: {OUTPUT_B4_PATH}")

if __name__ == "__main__":
    run_preprocessing()
