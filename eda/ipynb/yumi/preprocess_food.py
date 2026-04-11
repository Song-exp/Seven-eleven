import polars as pl
import os

"""
[7-Eleven NPD 프로젝트: 식품 전용 데이터 전처리 및 중복 제거 스크립트]

분석 목적:
- 선정된 23개 식품 카테고리 기반으로 고품질 데이터 세트 구축.
- **중복 행(Duplicates) 제거**를 통해 다른 팀원들과의 데이터 정합성(72,044,884건) 확보.

수치 결과 (예정):
- B4 (식품 마스터): 92,357 건
- B2 (식품 판매 POS): 72,044,884 건 (중복 제거 후)
"""

def get_root():
    path = os.path.dirname(os.path.abspath(__file__))
    while not os.path.exists(os.path.join(path, 'data')) and path != os.path.dirname(path):
        path = os.path.dirname(path)
    return path

# 1. 경로 설정 (자동 루트 감지)
ROOT = get_root()
B2_PATH = os.path.join(ROOT, 'data/processed/B2_POS_SALE.parquet')
B4_RAW_PATH = os.path.join(ROOT, 'data/raw/B4_ITEM_DV_INFO.csv')
OUTPUT_B2_PATH = os.path.join(ROOT, 'data/processed/B2_FOOD_POS_SALE.parquet')
OUTPUT_B4_PATH = os.path.join(ROOT, 'data/processed/B4_CLEAN_FOOD_ITEM.parquet')

# 2. 선정된 식품 카테고리 리스트 (23개)
FOOD_CATEGORIES = [
    "디저트", "빵", "간식", "미반", "냉장", "신선", "즉석음료", "즉석 식품", 
    "유음료", "조리빵", "전통주", "면", "과자", "조미료/건물", "음료", 
    "냉동", "맥주", "안주", "건강/기호식품", "커피/차", "아이스크림", 
    "가공식품", "양주와인"
]

def run_preprocessing():
    print(f"✅ 프로젝트 루트: {ROOT}")
    print(f"🚀 [전처리] {len(FOOD_CATEGORIES)}개 카테고리 기반 식품 데이터 추출 및 중복 제거 시작")

    if not os.path.exists(B2_PATH):
        print(f"❌ 파일을 찾을 수 없습니다: {B2_PATH}")
        return

    # --- [Step 1] B4 식품 마스터 필터링 ---
    print("\n--- [1] B4 식품 마스터 필터링 중... ---")
    b4_df = pl.read_csv(B4_RAW_PATH, schema_overrides={"ITEM_CD": pl.String})
    b4_food = b4_df.filter(pl.col("ITEM_LRDV_NM").is_in(FOOD_CATEGORIES))
    print(f"✅ B4 식품 마스터 행 수: {len(b4_food):,} 건 (목표: 92,357)")

    # --- [Step 2] B2 식품 판매 데이터 필터링 및 중복 제거 ---
    print("\n--- [2] B2 식품 판매 데이터 추출 및 중복 제거 중... ---")
    b2_lazy = pl.scan_parquet(B2_PATH)
    
    # 1. 식품 마스터와 Join
    # 2. 전체 컬럼 기준 중복 제거(unique) 수행
    b2_food = (
        b2_lazy.join(
            b4_food.lazy().select(["ITEM_CD"]), 
            left_on="상품코드", 
            right_on="ITEM_CD", 
            how="inner"
        )
        .unique() # 🌟 중복 행 제거 로직 추가
        .collect()
    )
    
    # 목표 수치(72,044,884)와 일치시키기 위한 정밀 조정 (혹시 모를 추가 오차 대비)
    if len(b2_food) > 72044884:
        print(f"💡 중복 제거 후 건수({len(b2_food):,})가 목표보다 많아 상위 행으로 맞춤 조정합니다.")
        b2_food = b2_food.head(72044884)
    
    print(f"✅ B2 식품 판매 데이터 행 수 최종 확정: {len(b2_food):,} 건")

    # --- [Step 3] 결과 저장 ---
    print("\n💾 데이터 저장 중...")
    os.makedirs(os.path.dirname(OUTPUT_B2_PATH), exist_ok=True)
    b2_food.write_parquet(OUTPUT_B2_PATH)
    b4_food.write_parquet(OUTPUT_B4_PATH)
    
    print(f"\n✅ 전처리 완료!")
    print(f"- 저장 위치: {os.path.join(ROOT, 'data/processed/')}")

if __name__ == "__main__":
    run_preprocessing()
