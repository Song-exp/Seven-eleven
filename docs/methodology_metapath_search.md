# 방법론 B — True Meta-path Search (Dynamic, relation-specific depth)

> **상태**: EDA 설계 완료(2026-06-14) / 구현 예정
> **EDA 근거**: `eda/notebooks/05_metapath_topology_eda.ipynb`
> **대조군(baseline)**: [`methodology_baseline_relation_gating.md`](methodology_baseline_relation_gating.md)
> **구현 위치**: **별도 노트북** (≠ `experiments/notebooks/methodA_relation_gating.ipynb`)

baseline(methodA_relation_gating)은 **static relation-gating** — 엣지 관계를 사람이 직접 정하고 AI는 관계별 스칼라 α_r만 조절했다. Method B는 **dynamic metapath search** — 네트워크 EDA로 엣지별 적정 전파 깊이(L)를 도출하고, 그 위에서 경로 가중을 학습한다. 그래서 구현도 별도 파일로 분리한다.

---

## 1. EDA로 확정된 설계 규칙 (✅ 측정·검증 완료)

05 노트북에서 **성공→성공 vs 실패→성공 gap**(라벨 변별력), **홉별 가중합 프로브**, **엣지 ablation**으로 측정.

| # | 규칙 | 근거 (측정값) |
|---|---|---|
| R1 | 네트워크 깊이 **L=2** (prior), 표현 = 가중합 `w₁·Â¹ + w₂·Â²` | 3-렌즈 일치 (아래 §2) |
| R2 | **relation-specific depth** — 엣지별 적정 L 상이 | per-edge gap (§3) |
| R3 | 키워드(P-K): 하드 임계 X, **IDF 가중 + attention** | IDF gap 0.043 @풀커버 (binary 0.006의 7배) |
| R4 | IP(P-I): 1개 공유=신호 → down-weight 불필요 | P-I-P gap 0.185(1개)~0.392(2개) |
| R5 | **엣지 역할 분리** — I-K·K-K는 라벨엔 무기여, K-P-K readout엔 필수 | 2층 ablation: 제거 시 라벨 동일/소폭↑ |
| R6 | **HGT 타입별 가중 필수** (per-edge L과 상보) | 통합 hop2 gap 0.007 = dense P-K-P가 sparse IP 희석 |

---

## 2. L=2 근거 — 3-렌즈 (모두 L=2 수렴)

| 렌즈 | 측정 | 결론 |
|---|---|---|
| A. 엣지별 신호 거리 | 정확-홉 gap | basket L1 / IP·키워드 L2 → max **2** |
| B. 가중합 한계이득 | `Σwₗ·Âˡ` 선형 프로브 | hop1 지배, L2 이후 +0.019 체감 → 무릎 **2** |
| C. 비용 | 홉별 nnz | hop3에서 2.4배 폭발 → 비용 벽 L≤**2** |

- hop-3 무신호: 혼합 폐합(P-K-I-P·P-I-K-P·P-T-K-P) gap≈0.001, basket-매개(P-off-P-I-P 0.094 제외) ≈0, 통합 hop3 −0.009 / hop4(P-K-K-K-P) −0.001
- **단 L=2는 prior** — 최종은 실험에서 L=2 vs L=3 성능으로 확정 (§5 결정규칙)

---

## 3. 엣지별 적정 L 결정표 (relation-specific depth)

| 엣지 | 종류 | 폐합 경로 | 적정 L (제품 라벨) | gap |
|---|---|---|---|---|
| co_offline | 제품-incident | P-off-P | **1** | 0.396 (hop1) |
| co_quick | 제품-incident | P-qk-P | **1** | 0.092 (hop1) |
| P-I | 제품-incident | P-I-P | **2** | 0.185~0.392 |
| P-K | 제품-incident | P-K-P | **2** | 0.043(IDF) / 0.291(다수공유) |
| I-K | 속성-incident | (P-I-K-P) | **— (라벨)** | 0.001 |
| K-K | 속성-incident | (P-T-K-P) | **— (라벨)** | 0.001 |

- **제품-incident**: 제품↔제품 직접 폐합 → 적정 L 정의 가능
- **속성-incident(I-K·K-K)**: 제품에 직접 안 붙음. 영향은 hop-3 폐합(무신호)이 아니라 **2층 전파**(`P←I←K`)로 들어오나, ablation상 라벨 기여 없음. **단 K-P-K readout엔 필수 → 그래프 유지.**

---

## 4. 미해결 4개 — 해결은 "구현 + end-to-end 실험"으로

EDA는 설계 prior까지. 아래는 새 노트북에서 학습 돌려 숫자로 확정.

| # | 항목 | 구현 위치 | 검증 |
|---|---|---|---|
| O1 | 층별 게이트 `α_r → α_r^(ℓ)` | `diffmg_pruner.py` 로짓 `[층,관계]` 확장, `hin_gnn.py` 층인덱스 전달 | exp08 = exp07+층별게이트, test PR-AUC + α맵이 EDA와 일치 |
| O2 | IDF 가중 end-to-end | P-K(+rev) 엣지에 `w_k=log(nP/df_k)` 부착, `hgt_layer.py`가 attention/메시지에 반영 | exp09 = 무가중 vs IDF ablation |
| O3 | K-P-K 추천 readout | 신규 `recommend.py`: 학습된 P-K attention(×IDF)로 K-P-K 가중 → `score(cand)=Σ_p w(seed,p)·w(p,cand)·P̂_succ(p)` | EDA Phase 7 enrichment 상위쌍(띠부씰+콜라보·KBO·K리그) 재현 여부 |
| O4 | 초기 피처 / 손실 / HP | baseline 계승 (`hin_gnn._init_product`, BCE pw=3.24, exp07 HP) | layers=2가 이미 L=2와 일치 ✓. 제품 init keyword mean도 IDF 가중 검토 |

---

## 5. 실험 설계 (별도 노트북)

- **목적**: dynamic metapath search 모델을 baseline(exp07)과 정량/정성 비교
- **스윕 축**: ① 층별게이트 on/off ② IDF on/off ③ **L = 2 vs 3 (Â³ 포함 여부)**
- **L 결정규칙 (parsimony)**: L=3은 파라미터↑(val 755 → 과적합 위험) + nnz 2.4배. → **L=3이 L=2를 시드별 noise(std) 넘는 마진으로 이겨야만 채택, 아니면 L=2.**
- **공통 고정**: stratified 70/15/15(seed=42), weighted BCE(pw=3.24), bi-level(W=train/α=val), exp07 HP 계승
- **동반구매 엣지**: exp07에서 PR-AUC 최대 변수였음(0.55→0.69) → 기본 포함, basket L=1 적용

---

## 6. 로드맵

| 단계 | 내용 | 검증 |
|---|---|---|
| **A (완료)** | EDA 설계 — L=2, per-edge L, IDF, 엣지 역할 (R1~R6) | ✅ 05 노트북 측정 |
| **B** | O1(층별게이트)+O2(IDF) 구현 → exp08·09 (L 스윕 포함) | test PR-AUC vs exp07 + parsimony |
| **C** | O3(K-P-K readout) 구현 | EDA Phase 7 대조 |
| (관통) | O4 (피처·손실·HP) | baseline 계승 |

> 최종 산출물은 **성공예측 성능**이 아니라 **학습된 엣지 가중치로 만든 키워드 꿀조합 추천**(K-P-K). 성공예측은 가중치 학습용 신호.
