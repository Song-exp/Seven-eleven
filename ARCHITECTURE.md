# 7-Eleven NPD Framework — 시스템 아키텍처

## 1. 전체 파이프라인

```
data/raw/                    (Read-Only)
    B2_POS_SALE
    B3_OLN_ODR_DLVR
    B4_ITEM_DV_INFO
    B5_MNM_DATA
        │
        ▼
data/processed/*.parquet     (Read-Only)
    ├── B2_POS_SALE.parquet
    ├── B4_ITEM_DV_INFO.parquet
    ├── B5_MNM_DATA.parquet
    └── PRODUCT_FULL_CONTEXT.parquet
        │
        ├─── src/data_builder/final_data_aggregator.py   (POS + B4 + B5 통합)
        ├─── src/data_builder/keyword_extractor.py        (Ollama → 키워드 5-10개)
        ├─── src/data_builder/attribute_inferrer.py       (Ollama → 속성 7종)
        └─── src/data_builder/batch_processor.py          (위 두 단계 체크포인트 오케스트레이션)
                │
                ▼
        data/graph_objects/
        ├── src/data_builder/node_extractor.py    (4 노드 타입 생성)
        └── src/data_builder/edge_generator.py    (엣지 생성)
                │
                ▼
        src/models/
        ├── hgt_layer.py       (Heterogeneous Graph Transformer)
        ├── kgat_layer.py      (Knowledge Graph Attention Network)
        └── diffmg_pruner.py   (Differentiable Meta-Graph, 구조 탐색)
                │
                ▼
        src/train/trainer.py   (Bi-level Optimization)
                │
                ▼
        src/eval/
        ├── success_predictor.py    (신상품 성공 확률 예측)
        └── cannibalization.py      (기존 상품 잠식 분석)
```

---

## 2. HIN 그래프 구조

### 노드 타입 (4가지 고정)

| 노드 타입 | 소스 데이터 | 초기 피처 | 비고 |
|-----------|------------|----------|------|
| `상품 (product)` | B4_ITEM_DV_INFO | 상품명, 카테고리 임베딩 | KPI 레이블도 이 노드에 |
| `속성 (attribute)` | attribute_inferrer 출력 | 맛·온도·식감·편의성·건강·용도·패키징 | KPI 포함 금지 |
| `트렌드/IP (trend)` | keyword_extractor 출력 + 외부 MCP | 트렌드 키워드 임베딩 | KPI 포함 금지 |
| `영수증 (receipt)` | B2_POS_SALE | 구매 시간·채널·금액 집계 | KPI 계산용 보조 노드 |

> **데이터 누수 방지**: KPI(성공 여부)는 오직 `상품` 노드의 타겟 레이블로만 사용. `속성`·`트렌드` 노드 피처에 절대 포함 금지.

### 허용 엣지 타입

| 엣지 | 방향 | 소스 |
|------|------|------|
| `상품 → 속성` | 단방향 | attribute_inferrer |
| `상품 → 트렌드` | 단방향 | keyword_extractor |
| `영수증 → 상품` | 단방향 | B2_POS_SALE (구매 이력) |
| `속성 ↔ 속성` | 양방향 (선택) | 속성 간 공출현 |

> 임의의 노드 타입·엣지 추가 금지 — 연산 폭발 방지.

---

## 3. 모델 레이어

### 3-1. HGT Layer (`src/models/hgt_layer.py`)

Heterogeneous Graph Transformer. 노드/엣지 타입별 독립 파라미터 투영.

```
Input:
  x_dict  : { node_type: Tensor (N_type, d_in) }
  edge_index_dict : { (src, rel, dst): Tensor (2, E) }

핵심 규칙:
  - 노드 타입마다 별도 W_Q, W_K, W_V 파라미터 (파라미터 격리)
  - 엣지 타입마다 별도 W_rel 파라미터
  - Multi-head attention: d_head = d_model // num_heads

Output:
  out_dict : { node_type: Tensor (N_type, d_out) }
```

### 3-2. KGAT Layer (`src/models/kgat_layer.py`)

Knowledge Graph Attention Network. Bi-Interaction 융합.

```
Input:
  x      : Tensor (N, d)
  edge_index : Tensor (2, E)
  edge_weight : Tensor (E,)   # 어텐션 가중치

Bi-Interaction 수식:
  e_add  = W1 * (x_i + x_j)          # 덧셈 성분
  e_prod = W2 * (x_i ⊙ x_j)          # 내적 성분
  e_out  = LeakyReLU(e_add + e_prod)  # 융합

Output:
  out : Tensor (N, d_out)
```

> `+` 와 `⊙` 두 성분이 반드시 모두 포함되어야 함.

### 3-3. DiffMG Pruner (`src/models/diffmg_pruner.py`)

Differentiable Meta-Graph. 메타 경로 구조를 학습으로 탐색.

```
파라미터 종류:
  W (네트워크 가중치) : 각 GNN 레이어 파라미터
  α (구조 가중치)     : 메타 경로 중요도 스칼라

출력:
  pruned_graph : 중요도 낮은 엣지 타입 제거된 그래프
  α_scores     : { edge_type: float } 각 경로 중요도
```

---

## 4. 학습 루프 (`src/train/trainer.py`)

Bi-level Optimization — W와 α를 서로 다른 데이터로 업데이트.

```python
for epoch in epochs:
    # Step 1: Train data → W 업데이트
    loss_train = train_step(train_data)
    loss_train.backward()
    optimizer_W.step()          # W optimizer만 step
    optimizer_W.zero_grad()

    # Step 2: Validation data → α 업데이트  (Step 1과 완전 격리)
    loss_val = val_step(val_data)
    loss_val.backward()
    optimizer_alpha.step()      # α optimizer만 step
    optimizer_alpha.zero_grad()
```

> `loss.backward()`와 `optimizer.step()`이 W/α 간에 절대 혼용되지 않아야 함.

---

## 5. 평가 (`src/eval/`)

| 모듈 | 목적 | 입력 |
|------|------|------|
| `success_predictor.py` | 신상품 성공 확률 (0~1) | 상품 노드 임베딩 |
| `cannibalization.py` | 기존 상품 매출 잠식 위험도 | 상품 간 유사도 행렬 |

> 평가 단계에서만 KPI 레이블 사용 허용.

---

## 6. LLM 파이프라인 (`src/data_builder/`)

로컬 Ollama 기반 — API 비용 없이 속성/키워드 추출.

```
상품명 (B4_ITEM_DV_INFO.상품명)
    │
    ├── keyword_extractor.py  (모델: gemma4:e4b)
    │       → 트렌드 키워드 5-10개
    │
    └── attribute_inferrer.py  (모델: gemma4:26b)
            → 속성 7종: 맛 / 온도 / 식감 / 편의성 / 건강 / 용도 / 패키징

batch_processor.py: 위 두 단계를 체크포인트 기반으로 오케스트레이션
  - 중단 시 재시작 가능 (fault tolerance)
  - 완료 여부: data/processed/keywords/ 확인
```

---

## 7. 코드 제약 요약

| 구역 | 핵심 제약 |
|------|----------|
| `data/raw/`, `data/processed/` | Read-Only, 직접 수정 금지 |
| `src/data_builder/` | KPI를 속성/트렌드 피처에 포함 금지, 노드 타입 4종 고정 |
| `src/models/` | forward 상단에 텐서 shape 주석 필수, 파일당 300줄 이하 |
| `src/train/` | W optimizer와 α optimizer 완전 분리 |
| `src/eval/` | KPI 레이블 사용은 이 구역에서만 허용 |
| 전체 | PyTorch + 승인된 GNN 라이브러리만 사용, GPU 우선 |
