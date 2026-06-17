# 방법론 Baseline — 관계별 가중치 조절 (Relation Gating)

> **상태**: 현재 구현 완료 / **Baseline 동결(freeze)**
> **구현 위치**: `src/models/`, `experiments/notebooks/methodA_relation_gating.ipynb`
> **다음 단계**: [`methodology_metapath_search.md`](methodology_metapath_search.md) — True Meta-path Search 재설계 (EDA 설계 완료 2026-06-14)

이 문서는 현재 코드가 채택한 **현실적 타협안**인 *관계별 가중치 조절(Relation Gating)* 방법론을 동결 기준점으로 정리한다. 향후 학술적 재설계(Method B)의 성능·서사 비교 대조군이다.

---

## 1. 한 줄 요약

> **사람이 단일 메타패스 레일을 고정으로 깔고, AI는 그 위에서 개별 관계(Relation)의 스칼라 볼륨 α_r만 조절한다.**

원논문(DiffMG)의 "경로(메타패스) 조합 자동 탐색"이 아니라, **고정 경로 위 관계 단위 게이팅**으로 축소된 구조다. 이 축소가 baseline의 정체성이자 한계다.

---

## 2. 아키텍처: KGAT × HGT × DiffMG 융합 (현재 구현 기준)

```
입력 그래프 (HeteroData, 노드 3종 / 엣지 4종 + 역방향)
        ↓
[product 초기 표현]  ← keyword/ip 임베딩 mean-집계 + has_promo + insta_mention_30d
        ↓                                              (콜드스타트 지원)
[L 층 반복 (기본 2층)]
  ┌─ DiffMGRelationGate : 관계 타입별 스칼라 α_r = softmax(logits/temp), Σα_r = 1
  ├─ HGTLayer           : 노드타입별 Q/K/V + 엣지타입별 W_att·W_msg per head, dst softmax
  └─ KGATUpdate         : Bi-Interaction  LeakyReLU(W₁(h+agg)) + LeakyReLU(W₂(h⊙agg)) + residual
        ↓
[Readout head]  →  product 성공확률 (이진 분류, BCEWithLogits + pos_weight)
```

| 컴포넌트 | 파일 | 역할 |
|---|---|---|
| `HINGNN` | `src/models/hin_gnn.py` | 전체 조립 (초기 피처 → L층 → readout) |
| `HGTLayer` | `src/models/hgt_layer.py` | 이기종 엣지 타입별 어텐션 메시지 |
| `KGATUpdate` | `src/models/kgat_layer.py` | Bi-Interaction 노드 업데이트 |
| `DiffMGRelationGate` | `src/models/diffmg_pruner.py` | 관계 중요도 α_r 미분 학습 |
| 학습 루프 | `src/train/trainer.py` | Bi-level: W는 train / α는 val |

---

## 3. 세 논문 — 원형 대비 실제 구현 정합성

| 항목 | 원논문 | 현재 구현 | 판정 |
|---|---|---|---|
| KGAT Bi-Interaction | `LeakyReLU(W₁(h+agg))+LeakyReLU(W₂(h⊙agg))` | 동일 (+residual, +LayerNorm) | **충실** |
| KGAT 고차 전파 | 인접행렬 attention propagation | **L층 스택**(A^L 명시 계산 X, sparse scatter) | 등가 구현 |
| KGAT KG임베딩(TransR) | 있음 | **없음** (순수 분류로 단순화) | 단순화 |
| HGT 타입별 어텐션 | meta-relation별 W_att/W_msg + μ prior | 동일 | **충실** |
| HGT RTE(상대 시간) | 있음 | **없음** | 미구현 |
| **DiffMG 메타패스 탐색** | **경로 후보 공간에서 NAS 탐색 → 이산 메타그래프** | **관계별 전역 softmax 스칼라** (경로 조합 없음) | **괴리** |
| DiffMG Bi-level 최적화 | W=train / α=val | 동일 (`trainer.py` Step1/Step2) | **충실** |

> **핵심 괴리**: docs/model_architecture.md가 서술한 "꿀조합 메타패스를 모델이 스스로 발굴"은 현재 코드로 뒷받침되지 않는다. DiffMG가 **relation-level gating**으로 축소되어 있고, 추천 경로는 config에 `meta_path: [keyword, product, keyword]`로 **수동 고정**돼 있다.

---

## 4. 메타패스 설정 — 단일 경로 완전 고정

- 추천/순회 경로: **`[키워드(속성·트렌드) → 상품 → 키워드]`** 단일 패스 고정 (`experiments/configs/*.yaml`의 `recommend.meta_path`)
- AI가 하는 일: 이 레일 위에서 관계별 α_r **볼륨만** 조절
- AI가 못 하는 일: 어떤 경로 조합(`[상품→속성→IP→상품]`, `[상품→영수증→상품]` 등)이 좋은지 **탐색·선택**

---

## 5. 병목 — α_r 미분화 현상

양방향 포함 약 8개 관계에 softmax를 적용 → 균등값 **1/8 ≈ 0.125** 부근으로 분산되어, 진짜 '성공 방정식' 속성과 '노이즈' 속성을 칼로 자르듯 도태시키지 못하고 모두 흐릿하게 생존한다.

- **증상 관측**: exp01에서 α_r 균등 분포 확인 → exp02(temperature 1.0→0.5, lr_α 0.005→0.02)의 직접 동기
- **완화 시도**: temperature 하향 + lr_α 인상으로 softmax 집중도를 올렸으나, **관계 단위** 게이팅의 구조적 한계(경로를 못 가름)는 잔존
- 이 병목이 Method B(경로 단위 α_path pruning) 전환의 근거다

---

## 6. 실험 구성 (exp01~exp07, `methodA_relation_gating.ipynb`)

모든 실험 **공통 고정**: 노드 3종, HGT hidden=128/layers=2/heads=4/dropout=0.3, DiffMG 게이트 ON, weighted BCE(pos_weight=3.24), stratified split 70/15/15(seed=42), Adam lr_w=0.005, early stop patience=30(val PR-AUC).

변형은 **① DiffMG α 튜닝 ② 동반구매 엣지 ③ Lift 가중 여부** 3축에서만 발생하며, **누적(incremental)** 구조다.

| 실험 | α (lr_α / temp) | 추가 엣지 | Lift 가중 | test PR-AUC | test AUC-ROC |
|---|---|---|---|---|---|
| **exp01** Baseline | 0.005 / 1.0 | 없음 | — | 0.5265 | 0.7725 |
| **exp02** α 튜닝 | 0.02 / 0.5 | 없음 | — | 0.5275 | 0.7699 |
| **exp03** 보완재 | 0.02 / 0.5 | complement (968쌍, Lift≥3.0) | log1p | 0.5464 | 0.7883 |
| **exp06** 동반구매 Lift | 0.02 / 0.5 | co_offline + co_quick | Lift(log1p) | 0.6598 | 0.8294 |
| **exp07** 동반구매 Binary | 0.02 / 0.5 | co_offline + co_quick | off (binary) | **0.6877** | **0.8424** |

> 랜덤 기준선: PR-AUC=0.236 (양성 23.6%) / AUC-ROC=0.500

**관측 2가지**
1. 동반구매 엣지 추가가 PR-AUC를 0.55→0.66~0.69로 끌어올린 최대 변수 (exp03→exp06/07).
2. **Binary(exp07) > Lift 가중(exp06)** — "Lift 값 자체보다 엣지 존재 여부가 더 안정적 신호"라는 exp07 가설과 일치.

---

## 7. 그래프 데이터 (학습 입력)

> 수량은 04 노트북 Run 시점에 따라 변동. 최신 기준은 `docs/graph_and_model_guide.md` 참조.

| 구분 | 타입 | 비고 |
|---|---|---|
| 노드 | product / keyword / ip 3종 | product에 promo 18종 원핫 + insta_mention_30d |
| 엣지(forward) | `product→keyword`, `ip→keyword`, `keyword→keyword`(trend), `product→ip` | + 동반구매 시 `co_offline`, `co_quick` |
| 역방향 | 위 전부 `rev_*` 자동 생성 (`build_reverse_edges`) | softmax 대상 관계 수 ≈ 2배 |

생성: `eda/notebooks/04_hin_graph_builder.ipynb` → Run All → `data/processed/hin/*.parquet`

---

## 8. Baseline로 동결하는 이유

- 데이터 파이프라인·평가 지표·재현 환경이 안정화되어 **대조군으로서 신뢰 가능**
- Method B(경로 탐색 재설계)의 성능 향상과 "AI가 경로를 스스로 발굴" 서사를 **정량/정성 비교**할 기준점 필요
- exp01~exp07 결과는 `experiments/results/{exp_name}/`에 동결 보존

---

## 9. 한계 명시 (Method B가 해결할 항목)

| # | 한계 | Method B 대응 |
|---|---|---|
| L1 | 단일 메타패스 수동 고정 | 다중 후보 메타패스 동적 개방 |
| L2 | 관계 단위 α_r 미분화 (≈0.125) | 경로 단위 α_path + pruning |
| L3 | "경로 발굴" 서사 미충족 | 이기종 A^L 행렬곱으로 경로 수학적 도출 |
| L4 | A^L 명시 계산 부재 | 서브 인접행렬 분할 + 거듭제곱 (홉 수 L=2 EDA 확정 2026-06-14, `methodology_metapath_search.md` §2) |
```
