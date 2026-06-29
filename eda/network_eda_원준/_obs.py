# -*- coding: utf-8 -*-
import nbformat as nbf
nb = nbf.read('keyword_cluster_eda.ipynb', as_version=4)
D = [c for c in nb.cells if c.cell_type=='code' and 'model_robustness_report.md' in c.source][0]
old = r"RW('\n**(a) 오차가 큰 군집은 *주제(군집) 탓*이 아니라 *성공률이 높아서*다**\n')"
new = (old + "\n"
       + r"""RW(f'먼저 **관찰**: 군집마다 예측 오차가 다르게 나타난다 — 군집 평균 오차(|예측−실제|)가 가장 작은 {fitdf.iloc[0][\"군집명\"]} {fitdf.iloc[0][\"MAE\"]:.3f}부터 가장 큰 {fitdf.iloc[-1][\"군집명\"]} {fitdf.iloc[-1][\"MAE\"]:.3f}까지 벌어진다(기술 보고서 §5). 그렇다면 *어떤 군집은 왜 더 부정확한가* — 주제가 어려워서인가? **아니다, 성공률로 거의 다 설명된다**:\n')""")
assert old in D.source, 'a header missing'
D.source = D.source.replace(old, new)
for c in nb.cells:
    if c.cell_type=='code': compile(c.source,'<cell>','exec')
nbf.write(nb,'keyword_cluster_eda.ipynb'); print('(a) 관찰 lead-in 추가')
