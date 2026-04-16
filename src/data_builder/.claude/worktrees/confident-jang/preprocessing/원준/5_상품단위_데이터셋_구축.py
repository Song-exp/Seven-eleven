# -*- coding: utf-8 -*-
"""
5_상품단위_데이터셋_구축.py
──────────────────────────────────────────────────────────────────
입력: pos_data_food_final_상품단위변환전.parquet  (약 7,000만 행)
출력: pos_data_food_final_상품단위.parquet        (상품 단위, 26 컬럼)

설계 원칙
  1. 메모리 효율 최우선 – 필요 컬럼만 로드, 중간 결과 즉시 삭제
  2. 대/중/소분류명 집계 전 strip() 적용 (공백으로 인한 중복 방지)
  3. 조기 매출 피처 – 완전 관측 불가(연도 말 출시) 시 NaN
  4. 타깃 변수 미포함 (추후 별도 추가 예정)
──────────────────────────────────────────────────────────────────
최종 26개 컬럼
  [식별자]       상품코드, 상품명
  [카테고리]     상품대분류명, 상품중분류명, 상품소분류명
  [기간]         첫_판매일, 마지막_판매일, 판매_지속기간(일), 판매_활성_월수
  [규모]         총_매출수량, 총_매출금액, 객단가, 총_거래건수, 취급_점포수
  [조기 매출]    출시후_{3/7/30}일_{매출액/수량}_{합계/평균}  (9개)
  [패턴]         야간_매출_비율, 주말_매출_비율, 피크_시간대
──────────────────────────────────────────────────────────────────
"""

import gc
from pathlib import Path

import numpy as np
import pandas as pd

# ──────────────────────────────────────────────────────────────
# 0. 경로 설정
# ──────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]
BASE_DIR = PROJECT_ROOT / 'data' / 'processed' / '최종'

INPUT_FILE  = BASE_DIR / 'df_전처리완료.parquet' # 이전 단계의 출력 파일명에 맞춤
OUTPUT_FILE = BASE_DIR / 'pos_data_food_final_상품단위.csv'

# 관측 종료일 (2025년 마지막 날)
OBS_END = pd.Timestamp('2025-12-31')

print('=' * 60)
print('상품 단위 데이터셋 구축 시작')
print('=' * 60)


# ──────────────────────────────────────────────────────────────
# STEP 1. 필요 컬럼만 선택적 로드 + 타입 최적화
# ──────────────────────────────────────────────────────────────
print('\n[STEP 1] 데이터 로딩 (필요 컬럼만 선택)...')

LOAD_COLS = [
    '상품코드',
    '상품명',
    '상품대분류명', '상품중분류명', '상품소분류명',
    '매출수량', '매출금액',
    '점포코드',      # 취급_점포수 계산
    '판매시간_dt',   # 첫/마지막 판매일, 조기 매출
    '판매월',        # 판매_활성_월수 (Int8 — 이미 저장됨)
    '판매시간대',    # 야간_매출_비율
    '판매요일',      # 주말_매출_비율
]

df = pd.read_parquet(INPUT_FILE, columns=LOAD_COLS)
print(f'  로드 완료: {len(df):,} 행  |  '
      f'메모리: {df.memory_usage(deep=True).sum() / 1e9:.2f} GB')

# ── 타입 최적화 ──────────────────────────────────────────────
df['매출수량'] = df['매출수량'].astype('float32')
df['매출금액'] = df['매출금액'].astype('float32')

# 대/중/소분류명: strip() 선행 후 category 변환
#   → 앞뒤 공백으로 인한 중복 카테고리 생성 방지 + 메모리 절약
for col in ['상품대분류명', '상품중분류명', '상품소분류명']:
    df[col] = df[col].str.strip().astype('category')

print(f'  타입 최적화 후 메모리: {df.memory_usage(deep=True).sum() / 1e9:.2f} GB')


# ──────────────────────────────────────────────────────────────
# STEP 2. 기본 집계 피처 (단일 groupby)
# ──────────────────────────────────────────────────────────────
print('\n[STEP 2] 기본 집계 피처 계산...')

agg = df.groupby('상품코드', sort=False).agg(
    상품명         = ('상품명',        'first'),
    상품대분류명   = ('상품대분류명',  'first'),
    상품중분류명   = ('상품중분류명',  'first'),
    상품소분류명   = ('상품소분류명',  'first'),
    첫_판매일      = ('판매시간_dt',   'min'),
    마지막_판매일  = ('판매시간_dt',   'max'),
    총_매출수량    = ('매출수량',      'sum'),
    총_매출금액    = ('매출금액',      'sum'),
    총_거래건수    = ('매출금액',      'count'),
    취급_점포수    = ('점포코드',      'nunique'),
    판매_활성_월수 = ('판매월',        'nunique'),
).reset_index()

# 파생 컬럼
agg['판매_지속기간(일)'] = (
    agg['마지막_판매일'] - agg['첫_판매일']
).dt.days.astype('int16')

agg['객단가'] = (
    agg['총_매출금액'] / agg['총_매출수량'].replace(0, np.nan)
).astype('float32')

print(f'  기본 집계 완료: {len(agg):,} 개 상품')


# ──────────────────────────────────────────────────────────────
# STEP 3. 시간대 패턴 피처
# ──────────────────────────────────────────────────────────────
print('\n[STEP 3] 시간대 패턴 피처 계산...')

# ── 3-1. 야간 매출 비율 (22시 ~ 05시) ────────────────────────
print('  3-1. 야간 매출 비율 (22~05시)...')
night_mask   = (df['판매시간대'] >= 22) | (df['판매시간대'] <= 5)
night_series = (
    df.loc[night_mask, ['상품코드', '매출금액']]
    .groupby('상품코드', sort=False)['매출금액']
    .sum()
    .rename('_야간매출')
)
del night_mask; gc.collect()

# join: agg['상품코드'] → night_series.index 기반 매핑
agg = agg.join(night_series, on='상품코드', how='left')
# 야간 판매가 없는 상품 → 비율 0 (NaN → 0으로 처리)
agg['야간_매출_비율'] = (
    agg['_야간매출'].fillna(0) / agg['총_매출금액'].replace(0, np.nan)
).astype('float32')
agg.drop(columns=['_야간매출'], inplace=True)
del night_series; gc.collect()

# ── 3-2. 주말 매출 비율 (토, 일) ──────────────────────────────
print('  3-2. 주말 매출 비율...')
weekend_mask   = df['판매요일'].isin(['토', '일'])
weekend_series = (
    df.loc[weekend_mask, ['상품코드', '매출금액']]
    .groupby('상품코드', sort=False)['매출금액']
    .sum()
    .rename('_주말매출')
)
del weekend_mask; gc.collect()

agg = agg.join(weekend_series, on='상품코드', how='left')
agg['주말_매출_비율'] = (
    agg['_주말매출'].fillna(0) / agg['총_매출금액'].replace(0, np.nan)
).astype('float32')
agg.drop(columns=['_주말매출'], inplace=True)
del weekend_series; gc.collect()

# ── 3-3. 피크 시간대 (판매 건수가 가장 많은 시간대) ───────────
print('  3-3. 피크 시간대...')
peak_hour = (
    df.groupby(['상품코드', '판매시간대'], sort=False)
    .size()
    .reset_index(name='_cnt')
    .sort_values('_cnt', ascending=False)   # 건수 내림차순 → 최빈 시간대가 first
    .groupby('상품코드', sort=False)['판매시간대']
    .first()
    .rename('피크_시간대')
)
agg = agg.join(peak_hour, on='상품코드', how='left')
del peak_hour; gc.collect()

print('  시간대 패턴 피처 완료')


# ──────────────────────────────────────────────────────────────
# STEP 4. 조기 매출 트렌드 피처
#
#   전략 (메모리 효율):
#     ① df에 _경과일 컬럼 직접 추가 (복사 없이 in-place)
#     ② 3 / 7 / 30일 윈도우를 순차 처리 → 각 서브셋 즉시 삭제
#     ③ 완전 관측 불가 상품 (관측 종료일 초과) → NaN
#     ④ 모든 윈도우 처리 후 df 전체 삭제
#
#   완전 관측 가능 조건 (각 윈도우 n일):
#     첫_판매일 + (n-1) ≤ OBS_END  ↔  첫_판매일 ≤ OBS_END - (n-1)
#   예) 3일 윈도우: 첫_판매일 ≤ 2025-12-29
#       7일 윈도우: 첫_판매일 ≤ 2025-12-25
#      30일 윈도우: 첫_판매일 ≤ 2025-12-02
# ──────────────────────────────────────────────────────────────
print('\n[STEP 4] 조기 매출 트렌드 피처 계산...')

# 상품별 첫_판매일을 df에 매핑 → 경과일 계산
print('  경과일 컬럼 생성 중...')
first_sale_map  = agg.set_index('상품코드')['첫_판매일']
df['_첫판매일'] = df['상품코드'].map(first_sale_map)
df['_경과일']   = (df['판매시간_dt'] - df['_첫판매일']).dt.days
df.drop(columns=['_첫판매일'], inplace=True)
del first_sale_map; gc.collect()

# 완전 관측 가능 여부 플래그 (상품 단위)
agg['_obs3']  = agg['첫_판매일'] <= (OBS_END - pd.Timedelta(days=2))
agg['_obs7']  = agg['첫_판매일'] <= (OBS_END - pd.Timedelta(days=6))
agg['_obs30'] = agg['첫_판매일'] <= (OBS_END - pd.Timedelta(days=29))

WINDOWS = [
    (3,  '3일',  '_obs3'),
    (7,  '7일',  '_obs7'),
    (30, '30일', '_obs30'),
]

for n_days, label, obs_col in WINDOWS:
    print(f'  출시 후 {label} 피처 계산...')

    # 해당 윈도우 내 거래만 필터링
    mask = (df['_경과일'] >= 0) & (df['_경과일'] < n_days)
    sub  = df.loc[mask, ['상품코드', '매출금액', '매출수량']]

    # 집계
    early = sub.groupby('상품코드', sort=False).agg(
        **{
            f'출시후_{label}_매출액_합계':  ('매출금액', 'sum'),
            f'출시후_{label}_매출수량_합계': ('매출수량', 'sum'),
        }
    ).reset_index()
    early[f'출시후_{label}_매출액_평균'] = (
        early[f'출시후_{label}_매출액_합계'] / n_days
    ).astype('float32')

    del sub, mask; gc.collect()

    # 기본 집계 테이블에 merge (left join → 미판매 상품 NaN)
    agg = agg.merge(early, on='상품코드', how='left')
    del early; gc.collect()

    # 완전 관측 불가 상품 → NaN 마스킹
    not_obs = ~agg[obs_col]
    for col in [f'출시후_{label}_매출액_합계',
                f'출시후_{label}_매출수량_합계',
                f'출시후_{label}_매출액_평균']:
        agg.loc[not_obs, col] = np.nan

# 임시 플래그 컬럼 삭제
agg.drop(columns=['_obs3', '_obs7', '_obs30'], inplace=True)

# 원본 df 전체 삭제 (메모리 반환)
del df; gc.collect()
print('  조기 매출 트렌드 피처 완료')


# ──────────────────────────────────────────────────────────────
# STEP 5. 컬럼 순서 정리 및 저장
# ──────────────────────────────────────────────────────────────
print('\n[STEP 5] 최종 정리 및 저장...')

FINAL_COLS = [
    # 식별자
    '상품코드', '상품명',
    # 카테고리
    '상품대분류명', '상품중분류명', '상품소분류명',
    # 기간
    '첫_판매일', '마지막_판매일', '판매_지속기간(일)', '판매_활성_월수',
    # 거래 규모
    '총_매출수량', '총_매출금액', '객단가', '총_거래건수', '취급_점포수',
    # 조기 매출 트렌드 (합계 / 평균 / 수량)
    '출시후_3일_매출액_합계',   '출시후_7일_매출액_합계',   '출시후_30일_매출액_합계',
    '출시후_3일_매출액_평균',   '출시후_7일_매출액_평균',   '출시후_30일_매출액_평균',
    '출시후_3일_매출수량_합계', '출시후_7일_매출수량_합계', '출시후_30일_매출수량_합계',
    # 시간대 패턴
    '야간_매출_비율', '주말_매출_비율', '피크_시간대',
]

df_product = agg[FINAL_COLS].copy()
del agg; gc.collect()

# ── 결과 요약 출력 ────────────────────────────────────────────
print(f'\n  상품 수   : {len(df_product):,}')
print(f'  컬럼 수   : {len(df_product.columns)}')

print('\n  [ 컬럼별 데이터타입 ]')
print(df_product.dtypes.to_string())

print('\n  [ 결측치 현황 ]')
null_counts = df_product.isnull().sum()
has_null = null_counts[null_counts > 0]
if len(has_null) > 0:
    print(has_null.to_string())
else:
    print('  결측치 없음')

# ── 저장 ─────────────────────────────────────────────────────
# encoding='utf-8-sig': BOM 포함 UTF-8 → Excel에서 한글 깨짐 방지
df_product.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
print(f'\n  저장 완료 → {OUTPUT_FILE}')

print('\n' + '=' * 60)
print('상품 단위 데이터셋 구축 완료')
print('=' * 60)
