# 성공 조합 — 강한 키워드는 보완재가 아니라 대체재 (포화)

- 모델: v2_sweepA | 날짜: 2026-06-21 | 대상: 조합 마이닝 방법론
- 도구: `combo.combo_beam`(포화 노출), `combo.seed_partners`(인과 파트너표), `inspector.evidence_table`
- 발단: combo_mining_plan Step1(모델-가이드 빔) 구현 → 깊은 빔·초가산성 selector가 degenerate.

## 관찰 1 — 가상 올스타 노드는 ~2키워드에서 포화
`score_concept(S)` 단계별 marginal (마라 최상위 조합):
```
+누들:    +0.7711
+짭조름함: +0.0387
+콜라보:   +0.0001   ← 완전 포화
```
→ depth≥3 조합은 noise. "3~4hop 성공 DNA 레일"은 허상.

## 관찰 2 — synergy(초가산성)와 절대 score가 반대로 움직임
- 고score 조합(0.83) → **logit synergy −10** (강+강 = 대체재, 1+1<2)
- 양수 synergy 조합 → **score 0.03** (약+약, 절대 성공확률 바닥)
- killer 쌍 synergy는 +0.03 수준이나 score_pair도 0.01대 (약한 키워드끼리만 보완)

## 관찰 3 — headroom Δprob는 깨끗이 판별 (포화 없음)
보통 제품(prob<0.8)에 [seed+X] 추가 시 Δ — 마라:
```
누들 +0.091 · 짭조름함 +0.044 · 콜라보 +0.033 · 라면 +0.027 · 쫄깃함 +0.026  (보강)
소스 −0.013 · 탕 −0.016 · 중식 −0.017 · 우동 −0.017 · 고단백 −0.021         (anti)
```

## 해석
- 모델은 **강한 키워드를 보완재가 아니라 대체재로** 본다(수확체감·포화). "조합이 성공을 만든다(superadditive)"는 가설은 *강한 키워드 영역에서* 기각.
- 진짜 구조 = **강한 단일 앵커(누들·마라) + 방향 맞는 보조**. MD 액션은 "앵커에 어떤 보조를 붙이고(Δ>0) 무엇을 피하나(Δ<0)"이지 "몇 개를 쌓나"가 아님.
- 따라서 selector는 **headroom Δprob**(비포화·인과), synergy는 **진단 라벨**(보완/대체)로만.

## 방법론 반영 (combo_mining_plan 정정)
- 채택: `seed_partners`(인과 파트너표) = 조합 확정의 핵심.
- 강등: `combo_beam`(예시 생성용 보조, depth≤2), `mine_pairs` synergy = 진단.
- 폐기: 3~4hop 깊은 빔 + superadditivity selector.

## 일반화 가설
"앵커 + 보조" 구조라면, 같은 보조 키워드가 **어떤 앵커에 붙냐에 따라** Δ가 바뀔 것(문맥 의존). [[2026-06-21_고창-modifier-vs-base]]의 carrier-base 효과와 같은 축 — 다음: `seed_partners`의 base를 바꿔(마라 vs 로제) 보조의 Δ가 뒤집히는지 확인.
