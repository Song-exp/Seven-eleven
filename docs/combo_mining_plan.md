# 성공 조합 마이닝 — 최종 개선 계획 (as-decided 2026-06-21)

> 목적: 학습된 HIN-GNN(v2_sweepA)에서 **"널리 쓰이면서 흥행을 견인하는 정예 키워드 조합"**을 모델-가이드로 탐색하고,
> 그 조합의 1-hop 맥락을 타입별로 부착해 **MD 처방용 서브네트워크**로 시각화·해석한다.
> 모델: `v2_sweepA` (현 서빙, THR 0.7757). 엔진 프리미티브: `score_concept`/`delta_prob` (가상 제품 노드, `has_kw`(+`has_ip`)만 연결 → leak-free).
> 설계 배경/교정: 이 문서 §6 체크리스트. 상위 가이드: [md_prescription_system_guide](md_prescription_system_guide.md) · [v2 전환](v2_serving_transition.md).

> ### 🔴 실측 정정 (2026-06-21) — 깊은 빔·초가산성 selector 폐기
> 구현·실측 결과 **가상 올스타 노드 `score_concept(S)`는 ~2키워드에서 포화**(마라→누들 +0.77 → +짭조름함 +0.04 → +콜라보 **+0.0001**). 따라서 **3~4hop 깊은 조합 빔은 무의미**하고, **초가산성(synergy)을 *선택 기준*으로 쓰면 강한 키워드끼리 대체재(substitute)라 음수만 남아** 절대 성공확률 0.03짜리만 골라짐. → Track2 "조합 superadditive" 가설은 이 데이터에서 **대체로 기각**, 구조는 "강한 단일 앵커 + 보조".
> **채택된 엔진 = `seed_partners`** (headroom Δprob 인과 파트너표, 비포화). `combo_beam`은 예시 생성용 보조(depth≤2). `synergy`(logit)는 selector가 아니라 **보완재 vs 대체재 진단 라벨**로만. 상세: [findings/2026-06-21_조합-포화-대체재](findings/2026-06-21_조합-포화-substitution.md).

---

## 0. 한눈에 — 결정된 파이프라인

```
Seed (예: #마라)
 │
 │ ① prefilter   partner_beam (어텐션 메타패스) → 후보 키워드군 추림 (연산량 절감용, 채점 아님)
 │
 │ ② combo beam  ★신규 — score_concept 기반 모델-가이드 빔
 │     목적함수 = set-level Δprob × log(support)
 │     제약     = support ≥ τ (지지도 바닥)  ·  headroom(prob<0.8)에서 계산  ·  초가산성(synergy) 주입
 │     → 3~4중 정예 키워드 조합 (널리 쓰이며 시너지 검증된 레일)
 ▼
 │ ③ 1-hop 맥락 부착  (타입별 분리 · 각 원소 Δprob로 인과 가중)
 │     · IP / trend   = GNN 어텐션 1-hop (has_kw_via_ip / has_kw_ipip / has_kw_trend)
 │     · 매대 / 퀵     = raw Lift 레이어 (basket_partners, 제품 레벨 조인) ← 어텐션 아님
 ▼
 │ ④ 채널 역할 분리  (g2 A_diff, track1)  POS 독점망→[제품 내실]  /  인스타 독점망→[마케팅 카피]
 │ ⑤ 앵커 vs 카멜레온  contrib ANOVA (carrier-base 통제)
 ▼
서브네트워크 시각화 (타입별 정규화·색)  +  MD 처방문
```

핵심 원칙: **탐색(②)은 인과(Δprob), 맥락(③)은 연관 지도이되 Δprob로 가중, 채널(④)은 액션 축, 검증(Track2)은 인과.**

---

## 1. Step 1 — combo beam (★신규 핵심 엔진)

naive 어텐션 워크는 rare 편향으로 폐기. **그러나 순수 Δprob 빔도 rare 편향을 재현한다**(Δprob ⟂ purity ⟂ rarity). 따라서 빈도를 목적함수에 명시적으로 넣는다.

### 목적함수
```
다음 파트너 X 선택:  argmax_X  f(set ∪ {X})
  f(S) = Δprob_headroom(S) · log(1 + support_min(S))
  s.t.  support(X) ≥ τ        (예: τ = MIN_SUPPORT = 3, 정예성 원하면 ↑)
        X ∉ S
Δprob_headroom(S) = mean over p∈{prob<0.8, S 미보유}  [ score_concept(kw(p) ∪ S) − score_concept(kw(p)) ]
```
- **support 바닥·가중**: "시장에서 널리 쓰이며 견인"을 보장 (rare·과적합 조합 배제).
- **headroom 계산**: prob 포화 제품에선 Δ가 기계적으로 눌리므로 `prob<0.8` 제품에서만 평균 (tier4 보정과 동일).
- **set-level**: marginal이 아니라 *집합 전체* Δprob로 채점 → greedy 근시 완화.

### 탐색 전략
- **빔 폭 ≥ 10** (greedy 1폭은 시너지쌍 놓침 — XOR형).
- **초가산성 주입**: 확장 후보를 marginal Δ가 아니라 `synergy(S, X) = Δprob(S∪X) − Δprob(S) − Δprob(X)` 가 양인 쪽으로 가산점 → 빔과 Track2 검증이 같은 방향.
- 깊이 3~4 hop에서 정지.
- prefilter(`partner_beam`)로 후보를 수백 개로 줄인 뒤 빔 채점(전체 K=2k 풀 score_concept는 비쌈).

### 제안 시그니처 (신규 `src/eval/md/combo.py` 또는 prescription.py 확장)
```python
def combo_beam(eng, seed_idx, depth=3, beam_width=10, support_floor=3,
               headroom_thr=0.8, synergy_bonus=1.0, prefilter_k=200) -> list[ComboPath]:
    """모델-가이드 다단계 조합 빔. 반환: 조합 시퀀스 + set Δprob + support_min + synergy."""
```

---

## 2. Step 2 — 1-hop 맥락 부착 (타입별 · Δprob 가중)

조합은 '단어 이름'일 뿐. 기획 깊이를 위해 1-hop 이웃을 **타입별로 분리**해 붙인다. **단, 1-hop = 연관(맥락 지도)** 이므로 각 맥락 원소의 *인과 무게는 Δprob로 별도 산출*한다 (띠부씰·공룡 함정 차단).

| 맥락 | 소스 | ⚠ 주의 |
|---|---|---|
| IP 후광 | GNN 어텐션 `has_kw_via_ip`(#10)·`has_kw_ipip`(#11) reverse lookup | IP "견인" 주장은 `score_concept`(IP 유/무) Δprob로 검증 |
| 트렌드 연쇄 | GNN 어텐션 `has_kw_trend`(#12) | 동일 |
| **매대/퀵 동반구매** | **raw Lift 레이어** (`basket_partners` / `complement_lift_pairs.csv`, 제품 레벨 조인) | **GNN 어텐션 아님** — `co_offline`/`co_quick`(#6/#7)은 누수로 forward에서 제거됨. basket_comp(키워드↔키워드)는 모델 내부에 이미 학습돼 별도 부착 불필요 |

출력: 조합 키워드별 `{타입: [(이웃, 어텐션_or_Lift, Δprob_인과)]}`.

---

## 3. Step 3 — 서브네트워크 융합 (★시각화 전용)

3~4 경로의 1-hop 데이터를 하나의 그래프로 합쳐 본다. **단 이종 엣지 가중치는 합산·비교 불가**(HGT 타입별 어텐션 스케일 상이 + Lift는 다른 단위).

- 융합은 **시각화 목적만**: 엣지를 **타입별 정규화 + 색 구분**으로 표시. 단일 가중 인접행렬로 뭉쳐 centrality 등 연산을 직접 돌리지 않는다.
- "중심성/독점망" 류 정량은 **타입 일관된 채널 A_diff 서브그래프(`g2 track1`)** 위에서만 계산.

---

## 4. Track 2 — 인과 검증

### 초가산성 (Superadditivity)
```
Synergy = Δprob(A+B) − [Δprob(A) + Δprob(B)]
```
- **null 필요**: 랜덤 키워드쌍 synergy 분포(순열) 대비 유의한지 (단순 nonzero는 무의미).
- **ceiling 보정**: headroom(prob<0.8) 제품에서 계산.
- 기존 `validate.tier5_synergy` 확장.

### contrib 분산분해 (앵커 vs 카멜레온)
```
contrib(k | carrier p) = score_concept(kw(p)) − score_concept(kw(p) − {k})
앵커형  = contrib 분산 작음 (문맥 무관 고정)
카멜레온= contrib 분산 큼 (문맥 의존)
```
- ⚠ **고창 발견 적용**: contrib 분산은 '문맥'이 아니라 **캐리어 base 포화**에서도 커진다(빈츠 0.9→contrib≈0). co-keyword로만 그룹핑하면 carrier-saturation을 context로 **오귀속**.
- → **carrier base prob를 통제**: 2-way (base구간 × co-keyword) 또는 partial correlation. 기존 `inspector.keyword_context_breakdown`/`keyword_disentangle` 확장.

---

## 5. Step 4~5 + 출력

- **채널 역할 분리** (`g2 = stage_g2_channel(eng,"track1")`의 A_diff): 파트너가 POS 독점망 가중 높음 → **[제품 속성 내실]**, 인스타 독점망 높음 → **[마케팅 카피]**. *연관이지만 정당* — 채널은 인과 주장이 아니라 **액션 축**(매출 데이터 vs 소셜 텍스트).
- **처방문 조립**: "성공 견인=Δprob / 어디 쓸지=채널 / 안전성=앵커·카멜레온" 세 축을 분리해 서술.

예시 출력:
> "#마라 기획안은 성공 특이망 레일 + 초가산성(synergy>0, support 충분)으로 [기획 승인]. 파트너 #스낵은 POS 매출 독점망 가중 → 제형·얼얼한 맛으로 내실. 짱구IP는 인스타 반응 독점망 중심성 최고 → 론칭 카피·소셜 비주얼 집중. (#마라=카멜레온형: 캐리어 base 받쳐줄 때만 견인)"

---

## 6. ⚠ 핵심 제약 체크리스트 (교정 6건)

| # | 함정 | 고침 |
|---|---|---|
| 1 | Δprob 빔도 rare 편향 재현 (Δ⟂purity⟂rarity) | 목적함수에 `support` 바닥·가중 명시 |
| 2 | greedy 빔이 시너지쌍 놓침 (XOR형) | 빔 폭≥10 + set-level Δ + synergy 주입 |
| 3 | 동반구매를 GNN 어텐션으로 오인 | #6/#7은 forward에 없음 → raw Lift 레이어 제품 조인 |
| 4 | 1-hop reverse lookup = 연관 | 각 맥락 원소를 Δprob로 인과 가중 |
| 5 | 이종 엣지 가중치 합산 | 융합=시각화 전용(타입별 정규화·색), 정량은 A_diff |
| 6 | contrib 분산 = 문맥? (실은 carrier 포화) | base prob 통제(2-way/partial) |

---

## 7. 구현 맵 (재사용 vs 신규) · 순서

| 단계 | 재사용 | 신규 |
|---|---|---|
| ① prefilter | `prescription.partner_beam` | — |
| ② combo beam | `engine.score_concept`/`delta_prob` | **`combo_beam`** (support·synergy·set-level) |
| ③ 맥락 부착 | 어텐션 캐시, `prescription.basket_partners` | **`context_attach`** (타입 분리 + Lift 조인 + Δprob 가중) |
| ④ 채널 | `tasks.stage_g2_channel` (A_diff) | 처방문 조립부 |
| Track2 초가산성 | `validate.tier5_synergy` | null(순열) + headroom 보정 |
| Track2 ANOVA | `inspector.keyword_context_breakdown`/`disentangle` | base-통제 분산분해 |
| 시각화 | `docs/network_viz` 도구 | 타입별 색·정규화 융합 뷰 |

**구현 순서 (phase)**
1. **Phase 1 — `combo_beam`** (Step 1 진짜 엔진). 가장 먼저. 나머지가 이 출력을 입력으로 받음.
2. Phase 2 — `context_attach` (Step 2~3, 타입 분리 + Lift + Δ가중).
3. Phase 3 — Track2 검증 강화 (초가산성 null·headroom / ANOVA base-통제).
4. Phase 4 — 처방문 조립 + 서브네트워크 시각화.
5. 노트북 셀로 결선 (`md_prescription_pipeline.ipynb` Stage 3.5 신설).

---

## 8. 산출물
- `src/eval/md/combo.py`: `combo_grow`(메인)·`seed_partners`·`mine_pairs`·`combo_beam`(보조)·`export_combo_final`.
- `src/eval/md/subnet.py`: 서브네트워크 + 상호작용 (§9).
- 노트북: `keyword_finalization.ipynb` §3.7(조합)·§3.8(서브네트) — 키워드별 결론을 한 곳에서.
- 발견은 `docs/findings/`에 누적.

---

## 9. as-built — 메타패스 전개 + 서브네트워크 생성 로직 (2026-06-21 구현)

### 9-1. 메타패스 전개 = `combo.combo_grow` (개방형 인과 전진)
고정 depth 없음. 데이터가 레일 깊이를 결정.
```
레일 S = [seed]
매 hop:  seed_partners(base=S) → headroom Δ 최대 후보 X
  margin(X|S) ≥ ε  → 합격. S∪{X}. 라벨: synergy = margin(X|S) − margin(X|{seed})
                     synergy>0 ★찐보완재 / ≤0 강한보조. 다음 hop.
  top margin < ε   → Early Stop (전부 대체/포화) → 경로 폐쇄.
  margin<0 후보     → Bypass Registry(잠식 블랙리스트, 사유·support 기록).
```
- margin = **headroom Δ**(가상 올스타 노드 아님 — 포화 회피). synergy = **효과수정**(레일이 X를 증폭하나).
- 실측(마라): 마라➔누들(강한보조)➔짭조름함(★찐보완재)➔콜라보➔라면. Bypass: 야식·매콤·바삭(잠식).

### 9-2. 서브네트워크 생성 = `subnet.build_subnetwork`
레일을 척추로, 각 마디에 **1-hop 문맥을 타입별 분리**해 부착 (이종 가중치 융합 금지).
| 문맥 | 소스 엣지 | 비고 |
|---|---|---|
| 척추(rail) | combo_grow 레일 | 엣지=causal margin, 라벨=찐보완재/강한보조 |
| IP | `('ip','has_kw','keyword')` 어텐션 | IP만 **2-hop**(시그니처 키워드)까지 |
| 트렌드 | `('keyword','trend_to','keyword')` 어텐션 | |
| 바스켓 | `('keyword','basket_comp','keyword')` 어텐션 | v2 키워드↔키워드 보완(제품조인 불필요) |
| 잠식(anti) | Bypass Registry | 회색 점선 "피할 것" |
- 1-hop 기본 + IP만 2-hop (깊이는 레일이 책임 → 문맥은 얕게, over-smoothing 방지).
- 반환: `dict(nodes[{id,type,label}], edges[{src,tgt,type,weight}])`. 노드 id = `kw:{idx}`/`ip:{idx}`.
- `draw_subnetwork`: matplotlib 타입색 인라인(레일 빨강·IP 보라·트렌드 주황·바스켓 청록·잠식 회색).

### 9-3. 상호작용 3종 (대시보드 클릭 = 이 함수들)
| 함수 | 질문 | 방법 |
|---|---|---|
| `recommend_within(start, pool)` | 시작 노드에 뭘 붙일까 | pool 노드를 `margin(C\|{start})` headroom Δ로 랭킹 |
| `pair_synergy(a, b)` | 둘이 보완? 대체? | `margin(b\|a) − margin(b\|∅)` >0 ✅보완 / <0 ⚠잠식 |
| `best_path(a, b, subnet)` | 둘을 잇는 최적 경로 | 서브네트 **구조 경로**(BFS) → `score_concept`로 재랭킹 + 끝점 시너지. 없으면 **글루 노드**(`argmax min(syn(a,C),syn(C,b))`) 제안 |
- **분리 원칙**: 패스=구조(어떻게 연결), 시너지=인과(붙이면 좋나). 한 숫자로 합치지 않음.

### 9-4. 대시보드 연결 (구현 완료)
- **오프라인 선계산**(`scripts.export_combo_dashboard`): 서브네트 구조 + 노드쌍 시너지 + 노드별 추천 → `Dashboard/combo_data.js`. 서버 없이 즉시(임원 데모용).
- **동적 라이브**(`src/eval/combo_serve.py` + `api.py /combo`): MDEngine 싱글톤 지연로드 → 아무 키워드나 ~1-2s(같은 시드 재요청 0s). 선계산과 **같은 `build_seed`** 호출 → 캐시·라이브 byte 일치.
- **프론트**: `dashboard.html`이 오프라인 캐시 미스 시 `/combo` fetch 후 적재(`ensureComboSeed`). `file://`(데모)면 건너뜀 → 데모는 구운 시드만, 서버 서빙(MD 실무)이면 동적. 2노드 클릭 = 시너지 룩업 + BFS 패스(JS, 가벼움).

### 9-5. ★ 배치 가속 (2026-06-22) — 동적화의 전제
`engine.score_concept_batch`: 가상 제품 N개를 한 그래프에 동시 추가 → 1-forward로 N개 prob.
제품 노드가 학습 임베딩 없이 content-aggregation이고 readout이 product→keyword 역전파를 안 써
**단일 forward와 drift=0**(실측). `_ConceptCache.warm`이 `seed_partners`/`combo_grow`/`pair_synergy`/
`recommend_within`의 평가 집합을 일괄 선계산 → **combo_grow 85s → ~1.1s(x77)**. 청크는 정확성과
무관(속도만), 기본 64. 이 가속이 라이브 `/combo`와 EDA 라이브 드릴다운을 동시에 가능케 함.

> 노트북 검토 위치: `keyword_finalization.ipynb` §3.7(레일·Bypass) → §3.8(서브네트 그림 + 추천/시너지/패스). §3.8c drift 검증 셀.
