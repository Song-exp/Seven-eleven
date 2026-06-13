# ─────────────────────────────────────────────────────────────────────────────
# 01b_matching_diagnostics.ipynb — build-blog-with-pos
#
# blog_keywords_with_pos.parquet 재생성
#
# 입력:
#   blog_keywords_processed.parquet  (상품명 기준 기존 블로그 데이터)
#   blog_keywords_new_crawl.parquet  (batch_blog_new_crawl.py 출력, 있으면 적용)
#   pos_product_features.parquet     (ITEM_CD ↔ ITEM_NM 매핑)
#
# 처리:
#   1. blog_proc 상품명 → ITEM_CD 매핑 (정확 일치 → 정규화명 일치 순)
#   2. blog_keywords_new_crawl 병합 (신규 크롤링 결과, 동일 ITEM_CD면 우선)
#
# 출력:
#   blog_keywords_with_pos.parquet   (ITEM_CD | ITEM_NM | 정규화명 | 키워드)
# ─────────────────────────────────────────────────────────────────────────────

import ast
import re
import numpy as np
import pandas as pd
from pathlib import Path

BASE_DIR = Path(os.path.abspath(os.path.join(os.getcwd(), '..', '..')))
PROC_DIR = BASE_DIR / 'data' / 'processed'

BLOG_PROC_PATH  = PROC_DIR / 'blog_keywords_processed.parquet'
NEW_CRAWL_PATH  = PROC_DIR / 'blog_keywords_new_crawl.parquet'
POS_PATH        = PROC_DIR / 'pos_product_features.parquet'
OUTPUT_PATH     = PROC_DIR / 'blog_keywords_with_pos.parquet'

# ── 유틸 ─────────────────────────────────────────────────────────────────────
def _norm_id(x):
    try: return str(int(float(x)))
    except: return str(x)

def _norm_name(s):
    """브랜드 접두사 제거 + 특수문자·공백 제거 + 소문자."""
    s = re.sub(r'^[^)]+\)', '', str(s)).strip()
    s = re.sub(r'[\s!&\(\)\[\].·★☆▶▷]', '', s).lower()
    return s

def _to_list(v):
    if isinstance(v, (list, np.ndarray)): return list(v)
    try: return ast.literal_eval(str(v))
    except: return []

def _has_kw(v):
    return len(v) > 0 if isinstance(v, (list, np.ndarray)) else False

def _union_kws(series):
    seen, result = set(), []
    for kws in series:
        for kw in (_to_list(kws) or []):
            if kw not in seen:
                seen.add(kw)
                result.append(kw)
    return result

# ── 로드 ─────────────────────────────────────────────────────────────────────
pos       = pd.read_parquet(POS_PATH)
blog_proc = pd.read_parquet(BLOG_PROC_PATH)

pos['ITEM_CD'] = pos['ITEM_CD'].apply(_norm_id)

print(f'pos_product_features : {len(pos):,}개 NPD')
print(f'blog_keywords_processed: {len(blog_proc):,}개 상품명')

# ── ITEM_CD 룩업 (pos 기준) ──────────────────────────────────────────────────
name_to_id = {str(r.ITEM_NM).strip(): _norm_id(r.ITEM_CD) for _, r in pos.iterrows()}
norm_to_id = {_norm_name(r.ITEM_NM): _norm_id(r.ITEM_CD) for _, r in pos.iterrows()}
id_to_nm   = {_norm_id(r.ITEM_CD): str(r.ITEM_NM).strip() for _, r in pos.iterrows()}

# ── Part 1: blog_proc → ITEM_CD 매핑 ────────────────────────────────────────
rows, no_match = [], 0
for _, row in blog_proc.iterrows():
    name = str(row['상품명']).strip()
    kws  = _to_list(row['확정키워드_정제'])
    item_cd = name_to_id.get(name) or norm_to_id.get(_norm_name(name))
    if item_cd is None:
        no_match += 1
        continue
    rows.append({
        'ITEM_CD':  item_cd,
        'ITEM_NM':  id_to_nm.get(item_cd, name),
        '정규화명': name,
        '키워드':   kws,
    })

df_from_proc = pd.DataFrame(rows)

# 동일 ITEM_CD 다중 매핑 시 키워드 합집합
df_from_proc = (
    df_from_proc
    .groupby('ITEM_CD', as_index=False)
    .agg(ITEM_NM=('ITEM_NM', 'first'),
         정규화명=('정규화명', 'first'),
         키워드=('키워드', _union_kws))
)

print(f'\n[Part 1] blog_proc 매핑')
print(f'  성공: {len(df_from_proc)}개  |  미매칭: {no_match}개')

# ── Part 2: 신규 크롤링 결과 병합 ────────────────────────────────────────────
if NEW_CRAWL_PATH.exists():
    new_crawl = pd.read_parquet(NEW_CRAWL_PATH)
    new_crawl['ITEM_CD'] = new_crawl['ITEM_CD'].apply(_norm_id)
    new_crawl['키워드']  = new_crawl['확정키워드_정제'].apply(_to_list)
    new_crawl['정규화명'] = new_crawl['ITEM_NM']
    new_crawl = new_crawl[['ITEM_CD', 'ITEM_NM', '정규화명', '키워드']]

    kw_filled = new_crawl['키워드'].apply(_has_kw).sum()
    print(f'\n[Part 2] 신규 크롤링 결과 로드: {len(new_crawl)}개')
    print(f'  키워드 있음: {kw_filled}개 / 없음: {len(new_crawl) - kw_filled}개')

    # concat 후 drop_duplicates: 동일 ITEM_CD면 new_crawl(keep='last') 우선
    combined = pd.concat([df_from_proc, new_crawl], ignore_index=True)
    result   = combined.drop_duplicates(subset='ITEM_CD', keep='last').reset_index(drop=True)
else:
    print(f'\n[Part 2] blog_keywords_new_crawl.parquet 없음 → blog_proc 기반만 사용')
    result = df_from_proc

# ── 저장 ─────────────────────────────────────────────────────────────────────
result.to_parquet(OUTPUT_PATH, index=False, engine='pyarrow')

filled = result['키워드'].apply(_has_kw).sum()
print(f'\n=== blog_keywords_with_pos.parquet 재생성 완료 ===')
print(f'  총 {len(result):,}개 ITEM_CD')
print(f'  키워드 있음: {filled:,}개 / 없음: {len(result) - filled:,}개')
print(f'  저장: {OUTPUT_PATH}')
