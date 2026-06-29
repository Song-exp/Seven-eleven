"""키워드별 근거(왜 성공/실패/매개) + 예시상품 → keyword_evidence.json.

대시보드 브리핑 패널에서 tag 키워드 클릭 시 "왜 이 신호인지 + 그 키워드 든 실제 상품"을 보여주기 위한 룩업.
metric: 세븐=POS 30일 매출 / CU·GS25=인스타 좋아요합 / 세븐 인스타전용=브릿지 좋아요.
"""
import json
import numpy as np
import pandas as pd

BASE = 0.238


def norm_id(x):
    try:
        return str(int(float(x)))
    except Exception:
        return str(x)


kf = pd.read_csv('data/processed/hin/keyword_final.csv')
kf = kf[kf.tag.isin(['killer', 'mine', '매개'])].copy()

edges = pd.read_parquet('data/processed/hin/product_keyword_edges_final.parquet')
pn = pd.read_parquet('data/processed/hin/product_nodes_final.parquet')[['ITEM_CD', 'ITEM_NM', '편의점명', '성공여부']]
pos = pd.read_parquet('data/processed/pos_product_features.parquet')
ins = pd.read_parquet('data/processed/instagram_engagement_with_keywords.parquet')

# metric_map: ITEM_CD → (값, 단위)
metric = {}
pos_sales = {norm_id(r.ITEM_CD): float(r.sales_30d_amt) for r in pos.itertuples()}
for cd in pn[pn['편의점명'] == '세븐일레븐']['ITEM_CD']:
    v = pos_sales.get(norm_id(cd))
    if v and v > 0:
        metric[cd] = (v, '매출')
cg = ins[ins['편의점명'].isin(['CU', 'GS25'])]
for (conv, nm), v in cg.groupby(['편의점명', '정규화명'])['좋아요 수'].sum().items():
    metric.setdefault(f'{conv}_{nm}', (float(v), '좋아요'))
try:
    bridge = pd.read_parquet('data/processed/seven_eleven_product_master.parquet')
    bmap = {norm_id(c): nm for c, nm in zip(bridge['ITEM_CD'], bridge['인스타_정규화명']) if pd.notna(nm)}
    likes_sev = ins[ins['편의점명'] == '세븐일레븐'].groupby('정규화명')['좋아요 수'].sum().to_dict()
    for cd in pn[pn['편의점명'] == '세븐일레븐']['ITEM_CD']:
        if cd not in metric:
            nm = bmap.get(norm_id(cd))
            if nm and nm in likes_sev:
                metric[cd] = (float(likes_sev[nm]), '좋아요')
except Exception as e:
    print('bridge skip:', e)

meta = pn.set_index('ITEM_CD')[['ITEM_NM', '성공여부']].to_dict('index')


def fmt(v, unit):
    if unit == '매출':
        return ('%.1f억' % (v/1e8)) if v >= 1e8 else ('%d만' % (v/1e4)) if v >= 1e4 else str(int(v))
    return '%s좋아요' % format(int(v), ',')


def examples(kw, n=3):
    cds = edges.loc[edges.keyword == kw, 'ITEM_CD'].tolist()
    succ, fail = [], []
    for cd in cds:
        mm = meta.get(cd)
        if not mm:
            continue
        val, unit = metric.get(cd, (None, None))
        row = (val if val is not None else -1, mm['ITEM_NM'], (fmt(val, unit) if val is not None else None))
        (succ if mm['성공여부'] == '성공' else fail).append(row)
    succ.sort(key=lambda r: -r[0]); fail.sort(key=lambda r: -r[0])
    pack = lambda rows: [{'name': nm, 'metric': mt} for _, nm, mt in rows[:n]]
    return pack(succ), pack(fail), len([1 for cd in cds if meta.get(cd, {}).get('성공여부') == '성공']), \
        len([1 for cd in cds if meta.get(cd, {}).get('성공여부') == '실패'])


def why(r):
    raw = r['raw성공률']; d = r['delta']; dp = r['delta_pos']; mult = (raw / BASE) if pd.notna(raw) else None
    if r['tag'] == 'killer':
        s = '성공 신호'
        if pd.notna(raw):
            s += ' · 이 속성을 단 제품의 %d%%가 성공(평균 24%%의 %.1f배)' % (round(raw*100), mult)
        if pd.notna(d):
            s += ' · 넣으면 성공확률 %+.1f%%p' % (d*100)
        return s + (' [%s]' % r['발표등급'] if pd.notna(r.get('발표등급')) and r['발표등급'] else '')
    if r['tag'] == 'mine':
        s = '실패 신호(회피)'
        if pd.notna(raw):
            s += ' · 단 제품의 %d%%만 성공(평균 24%% 이하)' % round(raw*100)
        if pd.notna(d):
            s += ' · 넣으면 성공확률 %+.1f%%p' % (d*100)
        return s
    # 매개
    s = '보편 강화제'
    if pd.notna(raw):
        s += ' · 혼자선 단 제품 %d%% 성공으로 약하나' % round(raw*100)
    if pd.notna(dp):
        s += ' 어디 붙여도 +(%d%% 캐리어에서 성공확률↑)' % round(dp*100)
    return s


TAG_KO = {'killer': '성공 키워드', 'mine': '지뢰(회피)', '매개': '매개(보편 강화)'}
ev = {}
for _, r in kf.iterrows():
    succ, fail, ns, nf = examples(r.keyword)
    ev[r.keyword] = {
        'tag': r.tag, 'tag_ko': TAG_KO[r.tag], 'why': why(r),
        'n_succ': ns, 'n_fail': nf, 'succ': succ, 'fail': fail,
    }

json.dump(ev, open('data/processed/hin/keyword_evidence.json', 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
print('written data/processed/hin/keyword_evidence.json :', len(ev), '키워드')
for k in ['고창', '마라', '즉석']:
    if k in ev:
        print('\n[%s] %s' % (k, ev[k]['why']))
        print('  성공:', ' / '.join('%s(%s)' % (x['name'], x['metric']) for x in ev[k]['succ']))
        print('  실패:', ' / '.join('%s(%s)' % (x['name'], x['metric']) for x in ev[k]['fail']))
