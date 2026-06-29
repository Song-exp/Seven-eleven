"""대시보드가 추천하는 키워드 조합 문구 전체 덤프 — 전역 트렌드별.

각 트렌드 → 출발 속성(trendAttrs) → recommend_bundle(일관 묶음)·recommend_paths(꿀조합).
mine(지뢰) 키워드가 추천에 섞이는지 전역 검증.
"""
from src.eval import serve

d = serve._data()
mine = d.get("mine", set())
trend_attrs = {t: [a for a in attrs if a in d["graph_kw"]]
               for t, attrs in d["trend_attrs"].items()}
trend_attrs = {t: a for t, a in trend_attrs.items() if a}

L = ['# 대시보드 키워드 조합 추천 문구 — 전역 덤프', '',
     '> 생성 2026-06-23 · 모델 v2_sweepA · 트렌드 %d개' % len(trend_attrs),
     '> 각 트렌드: 출발속성 → **일관 묶음**(recommend_bundle) · **꿀조합**(recommend_paths).',
     '> mine(지뢰)은 추천에서 제외됨 — 본 덤프로 전역 검증.', '', '---', '']

rec_leak = 0       # mine이 '추천'으로 등장 (0이어야 정상)
seed_mine = 0      # mine이 트렌드 자기 속성(시드)으로 등장 (정상 — 추천 아님)
total_combos = 0
for t in sorted(trend_attrs):
    seeds = trend_attrs[t][:3]
    seedset = set(seeds)
    bundle = serve.recommend_bundle(seeds)
    paths = serve.recommend_paths(seeds, n_paths=2)
    combo_kw = set(bundle) | {k for p, _ in paths for k in p}
    for kw in combo_kw & mine:
        if kw in seedset:
            seed_mine += 1
        else:
            rec_leak += 1
    total_combos += 1
    seed_lbl = ', '.join(('🔴' + s if s in mine else s) for s in seeds)
    L.append('## %s' % t)
    L.append('- 출발 속성: %s%s' % (seed_lbl, ' (🔴=지뢰 카테고리)' if seedset & mine else ''))
    L.append('- 일관 묶음: %s' % (' + '.join(bundle) if bundle else '—'))
    if paths:
        for i, (p, sc) in enumerate(paths, 1):
            L.append('- 꿀조합 %d: %s' % (i, ' → '.join(p)))
    L.append('')

L.insert(7, '> **mine 추천 누출: %d건** (0이어야 정상) · mine이 트렌드 자기속성(시드)으로만 등장: %d건(추천 아님, 🔴 회피 표시) · 조합 트렌드 %d개'
         % (rec_leak, seed_mine, total_combos))
L.insert(8, '')
leak = rec_leak

open('docs/dashboard_recommendations_dump.md', 'w', encoding='utf-8').write('\n'.join(L))
print('written docs/dashboard_recommendations_dump.md')
print('트렌드:', len(trend_attrs), '| mine 누출:', leak)
