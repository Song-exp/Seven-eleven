"""KBO IP 제품 전체 — 키워드별 타입(killer/매개/mine/일반) 표시 md 생성. 보고서 §10.5 부록."""
import pandas as pd


def nid(x):
    try:
        return str(int(float(x)))
    except Exception:
        return str(x)


kf = pd.read_csv('data/processed/hin/keyword_final.csv')
tagm = dict(zip(kf.keyword, kf.tag))
pn = pd.read_parquet('data/processed/hin/product_nodes_final.parquet')
pos = pd.read_parquet('data/processed/pos_product_features.parquet')
ins = pd.read_parquet('data/processed/instagram_engagement_with_keywords.parquet')
ie = pd.read_parquet('data/processed/hin/product_ip_edges_final.parquet')
kbo = set(ie[ie.ip_name.str.contains('KBO', na=False)]['ITEM_CD'])

metric = {}
ps = {nid(r.ITEM_CD): float(r.sales_30d_amt) for r in pos.itertuples()}
for cd in pn[pn['편의점명'] == '세븐일레븐']['ITEM_CD']:
    v = ps.get(nid(cd))
    if v and v > 0:
        metric[cd] = (v, '매출')
for (c, nm), v in ins[ins['편의점명'].isin(['CU', 'GS25'])].groupby(['편의점명', '정규화명'])['좋아요 수'].sum().items():
    metric.setdefault(f'{c}_{nm}', (float(v), '좋아요'))


def fmt(cd):
    v = metric.get(cd)
    if not v:
        return '—'
    a, u = v
    return ('%.1f억' % (a/1e8)) if u == '매출' and a >= 1e8 else ('%d만' % (a/1e4)) if u == '매출' else '%s♥' % format(int(a), ',')


MARK = {'killer': '🟢', '매개': '🟣', 'mine': '🔴', 'neutral': '⚪'}
rows = []
for r in pn.itertuples():
    if r.ITEM_CD not in kbo:
        continue
    kws = list(r.키워드_final) if r.키워드_final is not None else []
    rows.append((str(r.ITEM_NM), (r.성공여부 == '성공'), metric.get(r.ITEM_CD, (0, ''))[0], fmt(r.ITEM_CD), kws))
rows.sort(key=lambda x: (-x[1], -x[2]))


def render(kws):
    out = []
    for k in kws:
        t = tagm.get(k, 'neutral')
        out.append('%s%s' % (MARK[t], k))
    return ' '.join(out)


def counts(kws):
    t = [tagm.get(k, 'neutral') for k in kws]
    return t.count('killer'), t.count('매개'), t.count('mine'), t.count('neutral')


L = []
w = lambda s='': L.append(s)
w('# KBO IP 제품 — 키워드 타입 전체 분해')
w()
w('> 보고서 `keyword_combination_strategy_report.md` §10.5 부록 · 생성 2026-06-23')
w('> 같은 KBO IP 제품을 키워드 단위로 타입 표시. IP 고정 상태에서 레시피(강한 killer + mine 회피)가 성패를 가름.')
w()
w('**범례: 🟢 killer(성공)  ·  🟣 매개(보편 강화)  ·  🔴 mine(지뢰)  ·  ⚪ 일반**')
w('각 제품: `(killer수/매개수/mine수/일반수)`')
w()
w('---')
w()
ns = sum(1 for r in rows if r[1])
w('## 성공 제품 (%d개)' % ns)
w()
for nm, s, _, m, kws in rows:
    if not s:
        continue
    nk, nme, nmi, nn = counts(kws)
    w('**● %s** (%s) — `k%d/매%d/지%d/일%d`' % (nm, m, nk, nme, nmi, nn))
    w('  %s' % render(kws))
    w()
w('---')
w()
nf = sum(1 for r in rows if not r[1])
w('## 실패 제품 (%d개)' % nf)
w()
for nm, s, _, m, kws in rows:
    if s:
        continue
    nk, nme, nmi, nn = counts(kws)
    w('**✗ %s** (%s) — `k%d/매%d/지%d/일%d`' % (nm, m, nk, nme, nmi, nn))
    w('  %s' % render(kws))
    w()

open('docs/kbo_keyword_breakdown.md', 'w', encoding='utf-8').write('\n'.join(L))
print('written docs/kbo_keyword_breakdown.md  · 성공 %d / 실패 %d' % (ns, nf))
