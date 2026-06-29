"""killer 발표 신뢰도 등급(강증거/보통/모델신호/보류/조건부) md 생성.

키워드 풀은 그대로(keyword_final.csv), raw 직접성공률·Wilson 하한을 덧붙여 발표 등급만 부여.
base 0.238 앵커: raw≥0.5(≈2.1×base·상위 8%) ∧ Wilson≥base(우연 아님) ∧ Δ확실 = 강증거.
"""
import pandas as pd
import numpy as np

BASE = 0.238

edges = pd.read_parquet('data/processed/hin/product_keyword_edges_final.parquet')
pn = pd.read_parquet('data/processed/hin/product_nodes_final.parquet')[['ITEM_CD', '성공여부']]
kf = pd.read_csv('data/processed/hin/keyword_final.csv')

m = edges.merge(pn, on='ITEM_CD', how='left')
g = m.groupby('keyword')['성공여부'].agg(n='count', s=lambda x: (x == '성공').sum())
g['raw'] = g['s'] / g['n']


def wil(s, n, z=1.96):
    p = s / n
    return (p + z*z/(2*n) - z*np.sqrt(p*(1-p)/n + z*z/(4*n*n))) / (1 + z*z/n)


g['wilson'] = [wil(s, n) for s, n in zip(g['s'], g['n'])]
g = g.reset_index()

k = kf[(kf.tag == 'killer') & (kf['판정'] != '가짜(강등)')].merge(g, on='keyword', how='left')


def tier(r):
    if '조건부' in str(r['판정']):
        return '조건부(IP)'
    if '불명확' in str(r['판정']):
        return '보류(Δ노이즈)'
    if r['raw'] >= 0.5 and r['wilson'] >= BASE:
        return '강증거'
    if r['raw'] >= 0.4 or r['wilson'] >= BASE:
        return '보통증거'
    return '모델신호(raw약함)'


k['등급'] = k.apply(tier, axis=1)

L = []
w = lambda s='': L.append(s)
w('# killer 발표 신뢰도 등급 — 성공률 해석 레이어')
w()
w('> 모델 HIN-GNN(v2_sweepA) · 생성 2026-06-23 · 짝 문서: `keyword_frequency_purity_groups.md` · `keyword_examples_by_metric.md`')
w('> **키워드 풀은 안 바뀐다.** killer/mine/매개 분류는 그대로(killer 89·mine 70·매개 79, ❌가짜 제외).')
w('> 여기에 **raw 직접성공률 + Wilson 하한**을 덧붙여 *발표 신뢰도 등급*만 추가한다. 성공률 기준으로 새로 들어오거나 빠지는 키워드는 실질 0개.')
w()
w('---')
w()
w('## 0. 왜 이 레이어가 필요한가')
w()
w('killer는 purity(어텐션)·Δprob(인과)로 뽑혔지만, **직접 제품의 raw 성공률은 별개**다. ')
w('예: `족발`은 purity 0.75·Δprob>0로 killer지만 직접 제품은 3성공/3실패(raw 0.50). ')
w('MD 발표에서 "성공 키워드"라며 50/50 제품을 보여주면 설득력이 약하다. → **raw 성공률로 발표 등급을 나눠** 헤드라인용과 잠재신호용을 구분한다.')
w()
w('## 1. base rate = 0.238 — 모든 기준의 앵커')
w()
w('| 정의 | 값 |')
w('|---|---|')
w('| 글로벌 base = 전체 제품 성공률 (1197/5033) | **0.238** |')
w('| 키워드별 직접성공률 평균 (빈도≥10) | 0.236 (≈ 일치) |')
w()
w('- **"보통 한 키워드에 붙은 제품은 4개 중 1개(~24%)만 성공한다"** = base. 두 정의(글로벌·키워드별평균)가 0.238로 수렴.')
w('- 빈도≥5 키워드 직접성공률 분포: 중앙값 0.22, **90 percentile = 0.50** → raw>0.5는 **전체 키워드 상위 8%**.')
w()
w('## 2. 세 증거 축 — 서로 다른 질문')
w()
w('| 지표 | 답하는 질문 | 성격 |')
w('|---|---|---|')
w('| **raw 직접성공률** | 그 키워드 단 제품이 실제로 성공했나 | 실증 (발표 노출값, 교란 포함) |')
w('| **Wilson 하한** | 그 실증이 표본 우연 아닌가 | 통계 (소표본 보정) |')
w('| **Δprob** | 넣어서 성공확률을 올렸나 | 인과 (교란 제거, 기존 판정) |')
w()
w('### 2.5 Wilson 하한이란')
w()
w('**"표본이 작은 걸 감안한, 믿을 수 있는 최소 성공률."** 비율(proportion)의 95% 신뢰구간 하한(Wilson score interval, 1927).')
w()
w('성공률 50%라도 *2번 중 1번*과 *100번 중 50번*은 신뢰도가 다르다. 단순 성공률은 둘 다 0.50으로 같지만, '
  'Wilson 하한은 **표본이 적으면 값을 base 쪽으로 깎아내려** "이 정도는 진짜 보장된다"는 보수적 하한을 준다.')
w()
w('```')
w('        p̂ + z²/2n − z·√( p̂(1−p̂)/n + z²/4n² )')
w('하한 = ─────────────────────────────────────────     (p̂=s/n, z=1.96=95%)')
w('                  1 + z²/n')
w('```')
w()
w('| 키워드 | 성공/제품 | 단순 성공률 | Wilson 하한 |')
w('|---|---|---|---|')
w('| 공주 | 1/1 | 1.00 | **0.21** (n=1이라 폭락) |')
w('| 족발 | 3/6 | 0.50 | **0.19** (n=6, base 아래) |')
w('| 하와이 | 10/12 | 0.83 | **0.55** (n 충분, 덜 깎임) |')
w()
w('- **어디 쓰이나**: Reddit·Amazon 리뷰 랭킹("리뷰 적은데 100%" 거품 방지), A/B 테스트 전환율, 임상시험 발생률 — '
  '**소표본·극단 비율(0%·100% 근처)**에서 표준 도구. 단순 Wald 구간은 그런 경우 음수·폭0이 나와 못 씀.')
w('- **우리 용처**: 키워드 성공률도 공주(1/1)·족발(3/6)처럼 소표본·극단이라 동일 논리로 거품 제거.')
w()
w('## 3. 임계점과 의미')
w()
w('| 지표 | 컷 | 의미 |')
w('|---|---|---|')
w('| raw 직접성공률 | **≥ 0.50** | base의 약 2.1배 · 단 제품의 **과반 성공** · 전체 키워드 상위 8% |')
w('| | 0.40 ~ 0.50 | base의 1.7~2.1배 (과반 미만) |')
w('| | ≤ 0.238 | base 이하 = raw 신호 없음 |')
w('| Wilson 하한 | **≥ 0.238 (base)** | 표본이 작아도 95% 비관적으로 base 초과 = 우연 아님 |')
w('| | < 0.238 | raw가 높아도 소표본이라 base와 구분 불가 (예: 족발 3/6, W=0.19) |')
w('| Δprob | > +0.01 | 인과 유발 확인 (확실) |')
w()
w('## 4. 발표 등급 (3축 합성)')
w()
w('| 등급 | 규칙 | n | 발표 표현 |')
w('|---|---|---|---|')
order = ['강증거', '보통증거', '모델신호(raw약함)', '보류(Δ노이즈)', '조건부(IP)']
rules = {
    '강증거': 'raw≥0.5 ∧ Wilson≥base ∧ Δ확실',
    '보통증거': 'Δ확실 ∧ (raw≥0.4 ∨ Wilson≥base) ∧ ¬강증거',
    '모델신호(raw약함)': 'Δ확실 ∧ raw<0.4 ∧ Wilson<base',
    '보류(Δ노이즈)': '판정=불명확(Δ±0.01)',
    '조건부(IP)': '판정=조건부',
}
expr = {
    '강증거': '**헤드라인 "성공 키워드"** (실증·통계·인과 3박자)',
    '보통증거': '처방 가능, **"보장" 표현 자제**',
    '모델신호(raw약함)': '**"모델 포착 잠재신호"**로만 (raw는 base 수준)',
    '보류(Δ노이즈)': '발표 제외 권장',
    '조건부(IP)': '**"IP 콜라보 전용"** (단독 처방 금지)',
}
for t in order:
    n = (k['등급'] == t).sum()
    w('| %s | `%s` | %d | %s |' % (t, rules[t], n, expr[t]))
w()
w('> **"보장" 금지**: raw 0.5는 "과반 성공 = 평범한 키워드의 2배"이지 성공 보장이 아니다.')
w()
w('## 5. 발표 핵심 문장')
w()
w('> 평범한 키워드는 단 제품의 **4개 중 1개(~24%, base)**만 성공한다. ')
w('> **강증거 killer**는 단 제품의 **과반(≥50%)이 성공** — 전체 키워드 상위 8%에 들면서, ')
w('> 그 성공률이 소표본 우연이 아니고(Wilson≥base), 넣었을 때 실제로 성공확률을 올린다(Δprob>0). ')
w('> = **실증·통계·인과 세 증거가 모두 통과한 키워드.**')
w()
w('---')
w()
w('## 6. 등급별 키워드 전수')
w()
for t in order:
    sub = k[k['등급'] == t].sort_values('raw', ascending=False)
    if len(sub) == 0:
        continue
    w('### %s  (n=%d)' % (t, len(sub)))
    w()
    parts = [f'{r.keyword}(빈도{int(r["n"])}/raw{r.raw:.2f}/W{r.wilson:.2f}/Δ{r.delta:+.3f})' for _, r in sub.iterrows()]
    w('- ' + ', '.join(parts))
    w()

open('docs/killer_evidence_tiers.md', 'w', encoding='utf-8').write('\n'.join(L))
print('written docs/killer_evidence_tiers.md  lines:', len(L))
print(k['등급'].value_counts().to_string())
