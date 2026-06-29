"""keyword_final.csv 확정 편집 (대시보드 연결 Phase 1, 옵션 B).

규칙:
  · killer 中 ❌가짜(강등) → tag=neutral (뱃지 제거, 그래프엔 잔류)
  · mine → tag=mine 유지, include=Y (빨강 회피 뱃지; 추천 제외는 serve._kpk_next에서)
  · killer(✅확실/❓불명확/🔶조건부)·매개 → 유지
  · 추가 컬럼: raw성공률·wilson·발표등급 (killer 신뢰도 차등 표기용)
백업: keyword_final.csv.bak
"""
import shutil
import numpy as np
import pandas as pd

FP = 'data/processed/hin/keyword_final.csv'
BASE = 0.238

df = pd.read_csv(FP)

# ── raw 직접성공률 · Wilson (model-free) ──
edges = pd.read_parquet('data/processed/hin/product_keyword_edges_final.parquet')
pn = pd.read_parquet('data/processed/hin/product_nodes_final.parquet')[['ITEM_CD', '성공여부']]
m = edges.merge(pn, on='ITEM_CD', how='left')
g = m.groupby('keyword')['성공여부'].agg(rn='count', rs=lambda x: (x == '성공').sum())
raw = (g['rs'] / g['rn'])


def wil(s, n, z=1.96):
    if n == 0:
        return np.nan
    p = s / n
    return (p + z*z/(2*n) - z*np.sqrt(p*(1-p)/n + z*z/(4*n*n))) / (1 + z*z/n)


wilson = {k: wil(s, n) for k, s, n in zip(g.index, g['rs'], g['rn'])}
df['raw성공률'] = df['keyword'].map(raw).round(3)
df['wilson'] = df['keyword'].map(wilson).round(3)

# ── 발표등급 (killer만) ──
def disp_tier(r):
    if r['tag'] != 'killer':
        return ''
    if r['판정'] == '가짜(강등)':
        return '강등'
    if '조건부' in str(r['판정']):
        return '조건부(IP)'
    if '불명확' in str(r['판정']):
        return '보류'
    rw, wl = r['raw성공률'], r['wilson']
    if pd.notna(rw) and pd.notna(wl) and rw >= 0.5 and wl >= BASE:
        return '강증거'
    if pd.notna(rw) and (rw >= 0.4 or (pd.notna(wl) and wl >= BASE)):
        return '보통증거'
    return '모델신호'


df['발표등급'] = df.apply(disp_tier, axis=1)

# ── tag 확정: ❌가짜 killer → neutral 강등 ──
fake = (df['tag'] == 'killer') & (df['판정'] == '가짜(강등)')
df.loc[fake, 'tag'] = 'neutral'

# ── include: 전부 Y (옵션 B = mine도 그래프 잔류) ──
df['include'] = 'Y'

shutil.copy(FP, FP + '.bak')
df.to_csv(FP, index=False, encoding='utf-8-sig')

print('백업: %s.bak' % FP)
print('가짜→neutral 강등:', int(fake.sum()))
print('최종 tag 분포:')
print(df['tag'].value_counts().to_string())
print()
print('killer 발표등급 분포:')
print(df[df.tag == 'killer']['발표등급'].value_counts().to_string())
print()
print('서빙 처방 키워드(killer+매개):', ((df.tag == 'killer') | (df.tag == '매개')).sum())
print('mine(회피 뱃지, 추천 제외):', (df.tag == 'mine').sum())
