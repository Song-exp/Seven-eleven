import pandas as pd
import numpy as np
import gc

# ─────────────────────────────────────────────
# STEP 1. 데이터 로드
# ─────────────────────────────────────────────
file_path = 'data/processed/최종/pos_data_전처리완료_final.parquet'

print("데이터 로딩을 시작합니다. 약 1억 행의 데이터이므로 메모리 사용량에 주의하세요...")

try:
    df_pos = pd.read_parquet(file_path)
    print("데이터 로드 성공!")
    print(f"\n전체 데이터 행 수: {len(df_pos):,}")
    print("\n--- 데이터 기본 정보 ---")
    print(df_pos.info())
    print("\n--- 컬럼별 결측치 확인 ---")
    print(df_pos.isnull().sum())
except MemoryError:
    print("\n[오류] 메모리 부족으로 전체 데이터를 로드할 수 없습니다.")
    raise

# ─────────────────────────────────────────────
# STEP 2. 객단가 계산
# ─────────────────────────────────────────────
print("\n객단가 계산 중...")
df_pos['객단가'] = (
    df_pos['매출금액'] / df_pos['매출수량'].replace(0, np.nan)
).astype('float32')

# ─────────────────────────────────────────────
# STEP 3. 식품 카테고리 필터링
# ─────────────────────────────────────────────
print("\n상품대분류명 공백 제거 중...")
df_pos['상품대분류명'] = df_pos['상품대분류명'].str.strip()

food_categories = [
    '음료', '과자', '유음료', '미반', '면', '냉장', '맥주', '즉석음료', '빵', '전통주',
    '아이스크림', '조리빵', '즉석 식품', '건강/기호식품', '가공식품', '양주와인', '디저트',
    '안주', '신선', '간식', '조미료/건물', '냉동',
]

print("식품 카테고리 필터링 중...")
df_pos_food = df_pos[df_pos['상품대분류명'].isin(food_categories)].copy()

print(f"\n전체 데이터 행 수: {len(df_pos):,}")
print(f"식품 카테고리 필터링 후 행 수: {len(df_pos_food):,}")
print(f"제거된 행 수: {len(df_pos) - len(df_pos_food):,}")

del df_pos
gc.collect()

# ─────────────────────────────────────────────
# STEP 4. 이상치 제거 (매출수량 Q99, 객단가 Q99 — 대분류별)
# ─────────────────────────────────────────────
print("\n그룹별 임계치(Q99) 계산 중...")
qty_q99_dict   = df_pos_food.groupby('상품대분류명')['매출수량'].quantile(0.99).to_dict()
price_q99_dict = df_pos_food.groupby('상품대분류명')['객단가'].quantile(0.99).to_dict()

print(f"\n필터링 전 데이터 건수: {len(df_pos_food):,}")

# 조건 1: 매출수량 <= Q99 (전체 카테고리 공통 적용)
mask_qty = df_pos_food['매출수량'] <= df_pos_food['상품대분류명'].map(qty_q99_dict)

# 조건 2: 객단가 <= Q99 (전체 카테고리 공통 적용)
mask_price = df_pos_food['객단가'] <= df_pos_food['상품대분류명'].map(price_q99_dict)

df_pos_food_final = df_pos_food[mask_qty & mask_price].copy()

print(f"최종 필터링 후 데이터 건수: {len(df_pos_food_final):,}")
print(f"제거된 총 행 수: {len(df_pos_food) - len(df_pos_food_final):,}")
print(f"데이터 감소율: {(1 - len(df_pos_food_final)/len(df_pos_food))*100:.2f}%")

del df_pos_food
gc.collect()

# ─────────────────────────────────────────────
# STEP 5. 저장
# ─────────────────────────────────────────────
output_file_path = 'pos_data_food_final_상품단위변환전.parquet'

print(f"\n데이터를 {output_file_path}로 저장 중입니다...")
try:
    df_pos_food_final.to_parquet(output_file_path, engine='pyarrow', index=False)
    print(f"저장 완료! 파일 경로: {output_file_path}")
except Exception as e:
    print(f"저장 중 오류가 발생했습니다: {e}")
    raise
