"""키워드별 성공/실패 제품 예시(매출 or 좋아요 상위) md 생성.

대상: keyword_final.csv 의 killer/mine/매개 (❌가짜 제외) — keyword_frequency_purity_groups.md와 동일 풀.
metric: 세븐=POS sales_30d_amt(매출) / CU·GS25=인스타 좋아요합 / 세븐 인스타전용=브릿지 좋아요(fallback).
"""
import pandas as pd
import numpy as np


def norm_id(x):
    try:
        return str(int(float(x)))
    except Exception:
        return str(x)


kf = pd.read_csv('data/processed/hin/keyword_final.csv')
kf = kf[(kf.tag.isin(['killer', 'mine', '매개'])) & (kf['판정'] != '가짜(강등)')].copy()

pn = pd.read_parquet('data/processed/hin/product_nodes_final.parquet')
edges = pd.read_parquet('data/processed/hin/product_keyword_edges_final.parquet')
pos = pd.read_parquet('data/processed/pos_product_features.parquet')
ins = pd.read_parquet('data/processed/instagram_engagement_with_keywords.parquet')

# ---- metric_map: product_nodes ITEM_CD -> (value, unit) ----
metric = {}  # ITEM_CD -> (val, unit)

# 1) 세븐 POS 매출
pos_sales = {norm_id(r.ITEM_CD): float(r.sales_30d_amt) for r in pos.itertuples()}
for cd in pn[pn['편의점명'] == '세븐일레븐']['ITEM_CD']:
    v = pos_sales.get(norm_id(cd))
    if v is not None and v > 0:
        metric[cd] = (v, '매출')

# 2) CU/GS25 인스타 좋아요합 (synthetic id = 편의점명_정규화명)
cg = ins[ins['편의점명'].isin(['CU', 'GS25'])]
likes_cg = cg.groupby(['편의점명', '정규화명'])['좋아요 수'].sum()
for (conv, norm), v in likes_cg.items():
    sid = f'{conv}_{norm}'
    metric.setdefault(sid, (float(v), '좋아요'))

# 3) 세븐 인스타전용 fallback (POS 매출 없는 세븐) — 브릿지로 정규화명 매핑 후 좋아요합
try:
    bridge = pd.read_parquet('data/processed/seven_eleven_product_master.parquet')
    bmap = {norm_id(cd): nm for cd, nm in zip(bridge['ITEM_CD'], bridge['인스타_정규화명'])
            if pd.notna(nm)}
    sev = ins[ins['편의점명'] == '세븐일레븐']
    likes_sev = sev.groupby('정규화명')['좋아요 수'].sum().to_dict()
    for cd in pn[pn['편의점명'] == '세븐일레븐']['ITEM_CD']:
        if cd in metric:
            continue
        nm = bmap.get(norm_id(cd))
        if nm and nm in likes_sev:
            metric[cd] = (float(likes_sev[nm]), '좋아요')
except Exception as e:
    print('bridge fallback skipped:', e)

# ---- product meta ----
meta = pn.set_index('ITEM_CD')[['ITEM_NM', '편의점명', '성공여부']].to_dict('index')


def fmt(v, unit):
    if unit == '매출':
        if v >= 1e8:
            return f'{v/1e8:.1f}억'
        if v >= 1e4:
            return f'{v/1e4:.0f}만'
        return f'{int(v)}'
    return f'{int(v):,}♥'


def examples(kw, topn=5):
    cds = edges.loc[edges.keyword == kw, 'ITEM_CD'].tolist()
    succ, fail = [], []
    for cd in cds:
        m = meta.get(cd)
        if not m:
            continue
        val, unit = metric.get(cd, (None, None))
        sort_key = val if val is not None else -1
        row = (sort_key, m['ITEM_NM'], val, unit)
        (succ if m['성공여부'] == '성공' else fail).append(row)
    succ.sort(key=lambda r: -r[0])
    fail.sort(key=lambda r: -r[0])

    def render(rows):
        out = []
        for _, nm, val, unit in rows[:topn]:
            tag = f'({fmt(val, unit)})' if val is not None else '(—)'
            out.append(f'{nm}{tag}')
        return ' · '.join(out) if out else '—'
    return len(succ), len(fail), render(succ), render(fail)


L = []
w = lambda s='': L.append(s)
w('# 키워드별 성공/실패 제품 예시 (매출·좋아요 상위)')
w()
w('> 모델 HIN-GNN(v2_sweepA) · 생성 2026-06-23 · 짝 문서: `keyword_frequency_purity_groups.md`')
w('> 대상: killer · mine · 매개 (❌가짜 제외). 각 키워드의 **직접 보유 제품**을 매출/좋아요 순 상위 5개씩.')
w('> metric: **세븐=POS 30일 매출**(억/만원) · **CU/GS25=인스타 좋아요합**(♥) · 세븐 인스타전용=브릿지 좋아요(♥). `(—)`=metric 없음.')
w()
w('---')
w()

ORDER = [('killer', 'KILLER — 성공 특이', False), ('mine', 'MINE — 실패 특이', True),
         ('매개', '매개 — 보편 증폭', False)]
for tag, title, asc in ORDER:
    sub = kf[kf.tag == tag].sort_values('purity', ascending=asc)
    w(f'## {title}  (n={len(sub)})')
    w()
    for _, r in sub.iterrows():
        ns, nf, es, ef = examples(r.keyword)
        grade = f' {r.grade}' if pd.notna(r.grade) else ''
        w(f'### {r.keyword}  (빈도 {int(r["빈도"])} / purity {r.purity:.2f} / {r["판정"]}{grade})')
        w(f'- **성공 {ns}개**: {es}')
        w(f'- **실패 {nf}개**: {ef}')
        w()

open('docs/keyword_examples_by_metric.md', 'w', encoding='utf-8').write('\n'.join(L))
print('written docs/keyword_examples_by_metric.md  lines:', len(L))
print('metric coverage: %d products' % len(metric))
