"""purity 0.45 완화 후보에 Δprob(인과) 검증 — 몇 개가 진짜 killer로 통과하나."""
import numpy as np
import pandas as pd

from src.eval.md.engine import MDEngine, EngineConfig
from src.eval.md.inspector import evidence_table
from src.eval.md.review import conditional_delta, raw_freq, solo_success, solo_wilson, NOISE

# ── 후보 재선정 (purity 0.45~0.50, 미태그, WoE>0, support_succ≥3, freq≥3, hub아님) ──
sc = pd.read_csv('experiments/results/md_prescription/keyword_scores_full.csv')
kf = pd.read_csv('data/processed/hin/keyword_final.csv')[['keyword', 'tag']].rename(columns={'tag': 'final_tag'})
import pyarrow.parquet as pq  # noqa
edges = pd.read_parquet('data/processed/hin/product_keyword_edges_final.parquet')
pn = pd.read_parquet('data/processed/hin/product_nodes_final.parquet')[['ITEM_CD', '성공여부']]
m = edges.merge(pn, on='ITEM_CD', how='left')
g = m.groupby('keyword')['성공여부'].agg(freq='count', s=lambda x: (x == '성공').sum()).reset_index()
t = sc.merge(g, on='keyword', how='left').merge(kf, on='keyword', how='left')
t['final_tag'] = t['final_tag'].fillna('neutral')
NS, NF = 1197, 3836
with np.errstate(divide='ignore'):
    t['woe'] = np.log(((t.support_succ / NS) / (t.support_fail / NF)).replace([np.inf, -np.inf], np.nan))
cur = set(t[t.final_tag == 'killer'].keyword)
cand = t[(t.purity >= 0.45) & (t.purity < 0.5) & (t.woe > 0) & (t.support_succ >= 3)
         & (t.freq >= 3) & (t.tag != 'hub') & (~t.keyword.isin(cur))]['keyword'].tolist()
print('후보:', cand)

# ── 엔진 로드 + Δprob ──
eng = MDEngine(EngineConfig.v2_sweepA()).run_single_inference()
eng.build_mass(); eng.build_ledger("full")

ev = evidence_table(eng, cand).set_index('keyword')
rows = []
for kw in cand:
    ki = eng.seed_to_idx(kw)
    d = float(ev.loc[kw, 'delta_prob_mean'])
    cond, ip = (np.nan, None)
    if d < -NOISE:
        cond, ip = conditional_delta(eng, ki)
    if d > NOISE:
        verdict = 'killer_확실'
    elif d >= -NOISE:
        verdict = 'killer_불명확'
    elif not np.isnan(cond) and cond > NOISE:
        verdict = f'조건부killer(IP:{ip})'
    else:
        verdict = '가짜(미달)'
    rows.append(dict(keyword=kw, 빈도=raw_freq(eng, ki),
                     raw성공률=solo_success(eng, ki), wilson=solo_wilson(eng, ki),
                     delta=round(d, 4), delta_pos=ev.loc[kw, 'delta_prob_pos_rate'],
                     delta_cond=round(cond, 4) if not np.isnan(cond) else None, 판정=verdict))
res = pd.DataFrame(rows).sort_values('delta', ascending=False)
print()
print(res.to_string(index=False))
print()
print('통과(확실):', (res['판정'] == 'killer_확실').sum(),
      '/ 조건부:', res['판정'].str.startswith('조건부').sum(),
      '/ 불명확:', (res['판정'] == 'killer_불명확').sum(),
      '/ 미달:', (res['판정'] == '가짜(미달)').sum())
res.to_csv('experiments/results/md_prescription/purity045_candidates_deltaprob.csv', index=False, encoding='utf-8-sig')
print('saved → experiments/results/md_prescription/purity045_candidates_deltaprob.csv')
