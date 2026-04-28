"""
build_master.py
───────────────────────────────────────────────────────────────────────
마스터 데이터셋 2종 (product_master, ip_master) 구축 스크립트.

입력:
  - final_product_keywords.parquet  : 기존 상품 베이스
  - 찐최종뉴뉴_community_1hop_mapping_final.csv : 커뮤니티 트렌드

출력:
  - data/product_master_dataset.parquet
  - data/ip_master_dataset.parquet
"""

import ast
import os
import pandas as pd

# ── 경로 설정 ──────────────────────────────────────────────────────────
BASE_PARQUET = "final_product_keywords.parquet"
TREND_CSV    = "찐최종뉴뉴_community_1hop_mapping_final.csv"
OUT_DIR      = "data"
os.makedirs(OUT_DIR, exist_ok=True)


# ── 유틸: 문자열로 저장된 리스트를 파이썬 list로 변환 ─────────────────
def safe_list(val):
    """NaN, None, 문자열 리스트 모두 처리해 항상 list 반환."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return []
    if isinstance(val, list):
        return val
    try:
        parsed = ast.literal_eval(val)
        return parsed if isinstance(parsed, list) else [parsed]
    except Exception:
        return [v.strip() for v in str(val).split(',') if v.strip()]


# ═══════════════════════════════════════════════════════════════════════
# 0. 원본 데이터 로딩
# ═══════════════════════════════════════════════════════════════════════
print("[0] 원본 데이터 로딩...")
base_df  = pd.read_parquet(BASE_PARQUET)
trend_df = pd.read_csv(TREND_CSV, encoding='utf-8-sig')

# 컬럼명 정규화 (앞뒤 공백 제거)
base_df.columns  = base_df.columns.str.strip()
trend_df.columns = trend_df.columns.str.strip()

# 필수 컬럼 확인
print(f"  base_df  : {base_df.shape}  |  컬럼: {base_df.columns.tolist()}")
print(f"  trend_df : {trend_df.shape} |  컬럼: {trend_df.columns.tolist()}")

# base_df의 attributes 컬럼을 list 형태로 정규화
base_df['attributes'] = base_df['attributes'].apply(safe_list)
base_df['categories'] = base_df['categories'].apply(safe_list)


# ═══════════════════════════════════════════════════════════════════════
# Step 1-A. 트렌드 속성 집계 (ITEM_NM 기준, 상품명 정규화로 매핑)
# ═══════════════════════════════════════════════════════════════════════
print("\n[1-A] 트렌드 속성 집계...")

# (정규화_제품명, 정규화_속성) 쌍의 빈도 집계
trend_freq = (
    trend_df
    .groupby(['정규화_제품명', '정규화_속성'])
    .size()
    .reset_index(name='freq')
)

# 상품별 트렌드 속성 목록 (중복 없는 리스트)
trend_attrs_by_prod = (
    trend_freq
    .groupby('정규화_제품명')['정규화_속성']
    .apply(list)
    .reset_index()
    .rename(columns={'정규화_속성': '트렌드_속성'})
)

# 상품별 트렌드 속성 빈도 dict  →  {"달콤함": 5, "바삭함": 3, ...}
trend_freq_by_prod = (
    trend_freq
    .groupby('정규화_제품명')
    .apply(lambda df: dict(zip(df['정규화_속성'], df['freq'])))
    .reset_index()
    .rename(columns={0: '트렌드_속성_빈도'})
)

# 상품별 linked_ips (중복 없는 인플루언서 목록)
linked_ips_by_prod = (
    trend_df
    .groupby('정규화_제품명')['인플루언서명']
    .apply(lambda x: list(x.dropna().unique()))
    .reset_index()
    .rename(columns={'인플루언서명': 'linked_ips'})
)


# ═══════════════════════════════════════════════════════════════════════
# Step 1-B. Product Master 구축
# ═══════════════════════════════════════════════════════════════════════
print("[1-B] Product Master 구축...")

prod = base_df.copy()

# 트렌드 데이터 병합 (정규화_제품명 ↔ ITEM_NM 매핑)
prod = prod.merge(trend_attrs_by_prod,  left_on='ITEM_NM', right_on='정규화_제품명', how='left').drop(columns='정규화_제품명', errors='ignore')
prod = prod.merge(trend_freq_by_prod,   left_on='ITEM_NM', right_on='정규화_제품명', how='left').drop(columns='정규화_제품명', errors='ignore')
prod = prod.merge(linked_ips_by_prod,   left_on='ITEM_NM', right_on='정규화_제품명', how='left').drop(columns='정규화_제품명', errors='ignore')

# NaN → 빈 구조로 채우기
prod['트렌드_속성']      = prod['트렌드_속성'].apply(lambda x: x if isinstance(x, list) else [])
prod['트렌드_속성_빈도'] = prod['트렌드_속성_빈도'].apply(lambda x: x if isinstance(x, dict) else {})
prod['linked_ips']       = prod['linked_ips'].apply(lambda x: x if isinstance(x, list) else [])

# 속성 분리: 일반_속성 = Set A - Set B
prod['일반_속성'] = prod.apply(
    lambda r: list(set(r['attributes']) - set(r['트렌드_속성'])),
    axis=1
)

# 최종 스키마 선택
product_master = prod[[
    'ITEM_CD', 'ITEM_NM', 'p_price', 'p_cap',
    'is_survived', 'categories',
    '일반_속성', '트렌드_속성', '트렌드_속성_빈도',
    'event_name', 'linked_ips'
]].copy()

print(f"  product_master : {product_master.shape}")
print(product_master.head(3).to_string())


# ═══════════════════════════════════════════════════════════════════════
# Step 2. IP Master 구축
# ═══════════════════════════════════════════════════════════════════════
print("\n[2] IP Master 구축...")

# ip별 게시글 날짜 (중복 없는 리스트)
ip_dates = (
    trend_df
    .groupby('인플루언서명')['게시글_날짜']
    .apply(lambda x: list(x.dropna().astype(str).unique()))
    .reset_index()
    .rename(columns={'인플루언서명': 'ip_name', '게시글_날짜': 'post_dates'})
)

# ip별 트렌드 속성 + 빈도 dict
ip_trend_attrs = (
    trend_df
    .groupby('인플루언서명')['정규화_속성']
    .apply(lambda x: list(x.dropna().unique()))
    .reset_index()
    .rename(columns={'인플루언서명': 'ip_name', '정규화_속성': '트렌드_속성'})
)

# ip별 트렌드 속성 빈도 dict
ip_trend_freq = (
    trend_df
    .groupby(['인플루언서명', '정규화_속성'])
    .size()
    .reset_index(name='freq')
    .groupby('인플루언서명')
    .apply(lambda df: dict(zip(df['정규화_속성'], df['freq'])))
    .reset_index()
    .rename(columns={'인플루언서명': 'ip_name', 0: '트렌드_속성_빈도'})
)

ip_master = ip_dates.merge(ip_trend_attrs, on='ip_name', how='left')
ip_master = ip_master.merge(ip_trend_freq,  on='ip_name', how='left')

# 일반_속성: 현재 Base IP 데이터 없으므로 빈 리스트로 초기화
ip_master['일반_속성'] = [[] for _ in range(len(ip_master))]

# NaN 처리
ip_master['트렌드_속성']      = ip_master['트렌드_속성'].apply(lambda x: x if isinstance(x, list) else [])
ip_master['트렌드_속성_빈도'] = ip_master['트렌드_속성_빈도'].apply(lambda x: x if isinstance(x, dict) else {})

# 최종 스키마
ip_master = ip_master[[
    'ip_name', 'post_dates', '일반_속성', '트렌드_속성', '트렌드_속성_빈도'
]].copy()

print(f"  ip_master : {ip_master.shape}")
print(ip_master.head(3).to_string())


# ═══════════════════════════════════════════════════════════════════════
# Step 3. 저장
# ═══════════════════════════════════════════════════════════════════════
print("\n[3] Parquet 저장...")

prod_out = os.path.join(OUT_DIR, "product_master_dataset.parquet")
ip_out   = os.path.join(OUT_DIR, "ip_master_dataset.parquet")

product_master.to_parquet(prod_out, index=False)
ip_master.to_parquet(ip_out,        index=False)

print(f"  ✔ {prod_out}")
print(f"  ✔ {ip_out}")
print("\n[완료] 마스터 데이터셋 구축 완료.")
