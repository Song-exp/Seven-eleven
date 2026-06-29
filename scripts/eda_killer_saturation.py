"""killer 무더기 = 포화 실증 — 성공 담보 키워드를 쌓아도 시너지 자체는 안 좋아진다.

실험 (모델 재추론, 가상노드 배치):
 A. 누적 스택: killer를 1→N개 무작위로 쌓을 때 평균 성공확률 → 평탄화(diminishing) 확인
 B. 한계기여: n번째 killer 추가의 Δ → 0 으로 감쇠
 C. 헤드룸 대조: 같은 killer를 (약한 빈 베이스) vs (killer m개 더미) 에 추가했을 때 한계기여 비교
    → 강한 더미에 얹으면 ≈0 (포화), 빈 베이스엔 +  (= 시너지는 '약한 걸 살리는 것'이지 강한 것끼리의 합이 아님)
"""
import numpy as np
import pandas as pd
from src.eval.md.engine import MDEngine, EngineConfig

eng = MDEngine(EngineConfig.v2_sweepA()).run_single_inference()
kf = pd.read_csv('data/processed/hin/keyword_final.csv')
killers = [k for k in kf[kf.tag == 'killer'].keyword]
K = [(k, eng.seed_to_idx(k)) for k in killers]
K = [(k, i) for k, i in K if i is not None]
names = [k for k, _ in K]
idx = [i for _, i in K]
rng = np.random.default_rng(0)

s0 = eng.score_concept([])
print('빈 베이스 성공확률 s0 = %.3f  (killer 후보 %d개)' % (s0, len(idx)))

# 단독 lift (빈 베이스에 killer 1개)
singles = eng.score_concept_batch([[i] for i in idx]) - s0
print('killer 단독 lift(빈 베이스): 평균 %+.3f / 중앙 %+.3f' % (singles.mean(), np.median(singles)))
print()

# ── A. 누적 스택: 무작위 n개 killer 쌓기 (R회 평균) ──
print('=== A. killer n개 쌓을 때 평균 성공확률 (무작위 R=40 평균) ===')
NMAX, R = 14, 40
curve = []
for n in range(1, NMAX + 1):
    concepts = [list(rng.choice(idx, n, replace=False)) for _ in range(R)]
    sc = eng.score_concept_batch(concepts, chunk_size=64)
    curve.append((n, float(sc.mean())))
prev = s0
print('%3s %10s %12s' % ('n', '평균prob', 'n번째 한계Δ'))
for n, p in curve:
    print('%3d %10.3f %12.4f' % (n, p, p - prev))
    prev = p
print()

# ── C. 헤드룸 대조: killer를 (빈 베이스) vs (killer m개 더미) 에 추가 ──
print('=== C. 같은 killer 추가의 한계기여 — 베이스 강도별 ===')
print('베이스(killer m개)에 새 killer 1개 추가 시 평균 Δ (held-out killer 20개 평균, 베이스 10회 평균)')
print('%14s %12s' % ('베이스 killer수', '추가 killer Δ'))
for m in [0, 1, 2, 4, 6, 8, 10]:
    deltas = []
    for _ in range(10):
        if m == 0:
            base = []
        else:
            base = list(rng.choice(idx, m, replace=False))
        baseset = set(base)
        cand = [i for i in idx if i not in baseset]
        addk = rng.choice(cand, min(20, len(cand)), replace=False)
        base_s = eng.score_concept(base)
        with_s = eng.score_concept_batch([base + [int(a)] for a in addk])
        deltas.append(float((with_s - base_s).mean()))
    print('%14d %12.4f' % (m, np.mean(deltas)))
print()

# ── 단독강도 ↔ 한계기여(시너지) 음의 상관 재현 ──
# 강한 killer(단독 lift 큰)일수록 더미에 얹을 때 추가 여지 작음
strong_base = list(rng.choice(idx, 6, replace=False))
bs = eng.score_concept(strong_base)
held = [i for i in idx if i not in set(strong_base)]
add_on_strong = eng.score_concept_batch([strong_base + [int(i)] for i in held]) - bs
single_held = eng.score_concept_batch([[int(i)] for i in held]) - s0
r = np.corrcoef(single_held, add_on_strong)[0, 1]
print('단독강도(빈베이스 lift) vs 강한더미에 얹은 한계기여 상관 = %.3f' % r)
print('(음수일수록: 강한 killer일수록 더미 위 추가여지 적음 = 포화)')
