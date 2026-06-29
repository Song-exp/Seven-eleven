# 키워드 분류 판별기준 (as-built)

> 학습된 HIN-GNN(v2_sweepA)로 키워드를 **3개 출력 유형**(killer·mine·매개)으로 추리는 기준.
> hub(일반어)는 **출력이 아니라 분류용 내부 게이트/기준선**.
> 도구: **`src/eval/md/classify.py`(라이브 분류, A안)** ← 이 기준을 코드화. `review.py`(질적 검토표) · 노트북 `experiments/notebooks/keyword_finalization.ipynb`.
>
> **★ A안 (2026-06-24): 라이브 재계산.** 정적 `keyword_final.csv` 대신 `classify.classify_keywords_live(eng)`가 **모델 로드 시 1회** 이 기준을 그대로 돌려 태그를 산출하고 엔진에 캐시한다 → **데이터/모델을 갈아끼우면 마커가 자동으로 따라온다.** 대시보드 combo 경로(torch)가 이 태그를 노드에 부착(한 키워드 클릭=전역 분류 1층). 노이즈 플로어 Δ=±0.01은 pair synergy(2층)와 **단일 기준선**으로 통일.

---

## 0. 출력 유형 = 3개 (hub는 내부용)

| 유형 | MD 의미 | 축 |
|---|---|---|
| **killer** | "넣어라" — 성공 유발 | 성공 특이성 + 인과 유발 |
| **mine** | "피해라" — 실패 신호 | 실패 특이성 + 인과 악재 |
| **매개** | "아무 데나 넣으면 +" — 보편 강화제 | 보편 리프트 (특이성과 직교) |
| ~~hub~~ | (출력 X) | **내부 게이트·기준선**: killer를 일반어로부터 가르고, purity 해석의 baseline |

> hub는 keyword_final.csv 서빙 태그로 내보내지 않는다(=neutral 취급). 역할은 ① 간식·디저트 같은 범용어가 killer로 새는 것 차단 ② "killer=백본보다 성공특이"의 기준선.

---

## 1. 공통 지표 (단일 추론 1회 + 라벨 — 재추론 없음)

```
base_rate     = n_succ/(n_succ+n_fail) = 0.238
Score_succ(k) = Σ_4경로 Σ_{성공제품 p} att(p,k)        # has_kw·via_ip·ipip·trend
Score_fail(k) = Σ_4경로 Σ_{실패제품 p} att(p,k)
purity(k)     = Score_succ / (Score_succ + Score_fail)   # base 0.238 기준
Hub_Score(k)  = Σ_경로 Σ_p att(p,k)·Mass[p]              # Mass=z(sim_kw deg)+z(sim_ip deg)
att_lift(k)   = (Score_succ/n_succ)/(Score_fail/n_fail)  # 모델 관점, 희소성 편향 O
WoE(k)        = ln( (supp_s/n_succ)/(supp_f/n_fail) )    # 통계 관점, 희소성 편향 X
빈도(k)        = 직접 has_kw 제품 수 (우회경로 supp 아님 = 실제 증거량)
단독성공률(k)  = mean( y[k 보유 제품] )                   # raw, model-free
단독_wilson(k) = 단독성공률의 Wilson 95% 하한            # 소표본 페널티
```

## 2. 검증 지표 (개입 = 재추론, **모델 로드 시 1회 라이브** — `classify.py`가 배치로 계산)

```
Δprob(k)        = 평균_캐리어[ score(캐리어+k) − score(캐리어) ]   # 인과 유발 (그래프에 k 추가 후 재forward)
Δprob_cond(k|IP)= score([k]|IP) − score([]|IP)                  # IP 조건부 (공룡류 구제)
delta_pos(k)    = Δprob>0 인 캐리어 비율                         # 보편성 (1.0=무조건)
```

> **왜 재추론?** Δprob는 *반사실*("k를 넣으면?")이라 단일 추론에 그 상태가 없다. 관찰(purity·단독성공률)은 *교란*(공룡=IP 후광)을 못 가르고, 개입(캐리어 고정+k만 토글)만 순수 기여를 분리한다. GNN 비선형이라 분석적 계산 불가.

---

## 3. 유형별 판별기준

### killer — 성공 특이 + 인과 유발
```
[분류·관찰]  빈도≥3 ∧ supp_s≥3 ∧ purity≥0.50 ∧ Score_succ≥Q75 ∧ WoE>0 ∧ supp_f>0
[검증·인과]  Δprob>0  (Δ≤0 → 조건부(Δcond>0) 또는 가짜 강등)
```
- **근거 축**: purity↑(성공특이) · WoE>0(통계신호) · **Δprob>0(유발)** · 단독성공률↑(강한 base)
- **판정 tier** (노이즈밴드 ±0.01): 확실(Δ>+0.01) / 불명확(±0.01) / 조건부(Δ≤0∧Δcond>0) / 가짜(Δ<−0.01∧Δcond≤0)
- **근거문**: `purity {p}(성공특이)·WoE {w:+}·Δ {d:+}(유발✓)·단독 {s}({강/중/약}base)`

### mine — 실패 특이 + 인과 악재 (killer의 대칭)
```
[분류·관찰]  빈도≥3 ∧ supp_f≥3 ∧ purity≤0.15 ∧ Score_fail≥Q75 ∧ WoE<0 ∧ supp_s>0
[검증·인과]  Δprob<0  (Δ≥0 → 통계만, 의심)
```
- **근거 축**: purity↓(실패특이) · WoE<0(통계 실패신호) · **Δprob<0(악재)**
- **잡히는 종류**: 건강·기능성 컨셉(다이어트·운동·칼로리) / 주류 니치(와인·사케·하이볼) / 무거운 식사·국물(찌개·짜장·육수·마라) / 레드오션 향·맛(마라·향신료·산미) / 무드 추상어(행복·휴식)
- **근거문**: `purity {p}(실패특이)·WoE {w:+}·Δ {d:+}(악재✓)`

### 매개 — 보편 증폭 (killer/mine과 직교)
```
[분류·관찰]  빈도≥5 ∧ supp_all≥10 ∧ supp_all<400(메가허브 제외) ∧ 단독성공률<0.45 ∧ ~killer ∧ ~mine
[검증·인과]  delta_pos≥0.7(거의 전캐리어 +) ∧ Δprob_mean>0.04
```
- **근거 축**: 단독성공률↓(혼자 약함) · **delta_pos↑(어디든 +)** · purity 균형(성공특이 아님)
- **잡히는 종류**: 포맷(즉석·삼각) / 맛질감(짭조름함·감칠맛·쫄깃함) / 기능(저당·당충전) / 콜라보(허쉬·쿠앤크) / 굿즈(캐릭터·키링)
- **근거문**: `단독 {s}(혼자 {약/중})·delta_pos {dp}(전캐리어 +)·Δ {d:+}(보편리프트)·purity 균형`

### hub — 내부 게이트 (출력 X)
```
빈도≥3 ∧ supp_all≥3 ∧ |purity−0.238|<0.15 ∧ Hub_Score≥Q80 ∧ att_lift<1.5 ∧ |WoE|<0.5 ∧ ~killer ∧ ~mine
```
- 역할: killer false-positive 차단(간식·디저트) + purity baseline. **서빙 태그 아님(neutral 출력)**.

---

## 4. 빈도 보정 (소표본 거품 제거) — 단일추론

소표본은 극단값을 쉽게 얻어 유리하다: 공주·뷰티(직접 제품 1개)가 단독성공률 1.0·purity 1.0.

| 보정 | 방법 | 효과 |
|---|---|---|
| **빈도 floor** | killer/mine `빈도≥3`, 매개 `빈도≥5` | n=1~2 명백한 거품 제거 (공주·뷰티·행복 등) |
| **Wilson 하한** | `단독_wilson` 컬럼 | 소표본 자동 페널티 (공주 1.0→0.21, 하와이 0.83→0.55) |

- **빈도** = 직접 has_kw 제품 수. `supp`(우회 IP·트렌드 경로 포함)는 직접 1개여도 3 이상이 될 수 있어 부족 → 직접 빈도로 게이트.
- n=3(공룡)은 빈도로 안 자르고 **인과(Δprob 조건부)**가 판단 — 빈도+인과 역할 분담.

---

## 5. 개수와 균형 (참고)

| | 빈도보정 전 | 빈도≥3 | + Δprob 검증 |
|---|---|---|---|
| killer | 48 | 33 | ~20 확실 + 조건부 |
| mine | 84 | 70 | (Δprob<0 검증 시 추가 정제) |
| 매개 | — | ~28 | (delta_pos 자체가 검증) |

> **killer < mine 은 구조적**: base 0.238 = 제품 76%가 실패 → "실패하는 길"이 "성공하는 길"보다 많다. 비율 ~1:2는 정상. killer=좁은 처방 shortlist / mine=포괄 블랙리스트로 역할이 달라 비대칭이 적절.

---

## 6. 3층 필터 요약

```
1. WoE 분류        (관찰, 단일추론)   ← purity·WoE·Score·att_lift
2. + 빈도 보정     (관찰, 단일추론)   ← 빈도 floor·Wilson (소표본 거품)
3. + Δprob 검증    (개입, 재추론)     ← 유발(killer)·악재(mine)·보편(매개)
```
- 1·2 = 싸다(엣지 카운트). 3 = 비싸다(재추론) — **모델 로드 시 1회 라이브**, 후보(killer/mine) Δprob은 공유 헤드룸 표본 1배치로 축약(`classify._batch_delta`, warmup ~수십 초 → 캐시).
- **공주(n=1)** = 소표본 → 2가 잡음. **공룡(Δ≤0)** = 교란 승객 → 3이 잡음. 둘 다 필요.

> 상세 인사이트: [[2026-06-22_synergy-headroom-단독강도역전]] (synergy ⟂ 단독강도 / killer×killer 포화) · 카톡 md `2026-06-22_keyword-ledger-param-verification-woe-filter` (파라미터 검증).
