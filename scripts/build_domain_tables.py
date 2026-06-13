"""
세븐일레븐 도메인 테이블 빌드 스크립트
docs/data_schema.md 참조

출력:
  - data/processed/seven_eleven_product_master.parquet  (브릿지)
  - data/processed/pos_product_features.parquet         (POS 판매 피처)
  - data/processed/final_product_keywords.parquet       (POS 키워드/속성)
  - instagram_engagement_with_keywords.parquet          (인스타, 원본 그대로)

이 스크립트는 각 도메인 테이블을 검증하고 현황을 출력합니다.
실제 빌드는 eda/notebooks/01_pos_feature_engineering.ipynb 에서 수행합니다.
"""
import io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import pandas as pd
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
PROC = BASE / 'data' / 'processed'


def load_and_report(label, path, key_col=None):
    df = pd.read_parquet(path) if str(path).endswith('.parquet') else pd.read_csv(path, encoding='utf-8-sig')
    print(f'\n[{label}] {path.name}  →  {df.shape[0]:,}행 × {df.shape[1]}컬럼')
    if key_col:
        null_key = df[key_col].isna().sum()
        dup_key = df.duplicated(subset=key_col).sum()
        print(f'  키({key_col}): null {null_key}개 / 중복 {dup_key}개')
    return df


# ── 1. 브릿지 ────────────────────────────────────────────────────────────────
bridge = load_and_report(
    '브릿지', PROC / 'seven_eleven_product_master.parquet', key_col='ITEM_CD'
)
print('  소스별:', bridge['소스'].value_counts().to_dict())
print('  NPD:', bridge['is_npd'].sum(), '/ 전체', len(bridge))


# ── 2. POS 도메인 ─────────────────────────────────────────────────────────────
pos = load_and_report(
    'POS 피처', PROC / 'pos_product_features.parquet', key_col='ITEM_CD'
)
print(f'  첫판매일 범위: {pos["첫판매일"].min()} ~ {pos["첫판매일"].max()}')
print(f'  프로모션 있음: {pos["has_promo_30d"].sum():,}개')

kw = load_and_report(
    'POS 키워드', PROC / 'final_product_keywords.parquet', key_col='상품코드'
)
kw_filled = kw['최종키워드'].apply(lambda x: isinstance(x, (list, type(x))) and len(x) > 0).sum()
print(f'  키워드 있음: {kw_filled:,}개')
print(f'  브랜드 있음: {kw["브랜드"].notna().sum():,}개')

# POS 두 테이블 커버리지 교차 확인
pos_ids = set(pos['ITEM_CD'])
kw_ids  = set(kw['상품코드'])
print(f'\n  [POS 커버리지 교차]')
print(f'  pos_product_features에만: {len(pos_ids - kw_ids)}개')
print(f'  final_product_keywords에만: {len(kw_ids - pos_ids)}개')
print(f'  공통: {len(pos_ids & kw_ids):,}개')


# ── 3. 인스타 도메인 ──────────────────────────────────────────────────────────
insta = load_and_report(
    '인스타', PROC / 'instagram_engagement_with_keywords.parquet', key_col='정규화명'
)
seven = insta[insta['편의점명'] == '세븐일레븐']
print(f'  세븐일레븐: {len(seven):,}행 / 제품 {seven["정규화명"].nunique():,}개')
print(f'  언급일 범위: {seven["언급일"].min()} ~ {seven["언급일"].max()}')

# 브릿지 ↔ 인스타 커버리지 확인
bridge_insta_nms = set(bridge['인스타_정규화명'].dropna())
seven_nms        = set(seven['정규화명'].dropna())
print(f'\n  [인스타 커버리지]')
print(f'  브릿지에 있고 인스타 데이터에도 있음: {len(bridge_insta_nms & seven_nms):,}개')
print(f'  브릿지에 있으나 인스타 데이터 없음:  {len(bridge_insta_nms - seven_nms):,}개')


# ── 최종 요약 ─────────────────────────────────────────────────────────────────
print('\n' + '='*60)
print('도메인 테이블 현황 요약')
print('='*60)
print(f'브릿지:         {len(bridge):,}개 제품 (ITEM_CD × 인스타_정규화명 매핑)')
print(f'POS 피처:       {len(pos):,}개 (NPD only, ITEM_CD 기준)')
print(f'POS 키워드:     {len(kw):,}개 (NPD only, 상품코드 기준)')
print(f'인스타 게시물:  {len(seven):,}행 / {seven["정규화명"].nunique():,}개 제품 (세븐일레븐)')
print()
print('join 방법:')
print('  브릿지 × POS    →  ITEM_CD')
print('  브릿지 × 인스타  →  인스타_정규화명 = 정규화명')
print('  POS 피처 × 키워드 → ITEM_CD = 상품코드')
