# HIN-GNN 구현 구조 (KGAT × HGT × DiffMG 융합)

> 연구 논리·선행연구는 [`model_architecture.md`](model_architecture.md), 학습 의사결정은
> [`training_decisions.md`](training_decisions.md), 하이퍼파라미터는 `configs/train_config.yaml` 참조.
> 이 문서는 **실제 구현된 코드 구조**를 정리한다. (2026-06-04 구현)

---

## 1. 파일 맵

| 파일 | 역할 | 핵심 |
|---|---|---|
| `src/models/hgt_layer.py` | HGT 타입 격리 어텐션 메시지 | 노드타입별 Q/K/V + 엣지타입별 $W_{att}\cdot W_{msg}$ |
| `src/models/kgat_layer.py` | KGAT Bi-Interaction 업데이트 | $\sigma(W_1(h{+}agg))+\sigma(W_2(h{\odot}agg))$ |
| `src/models/diffmg_pruner.py` | DiffMG 관계 게이트 | 엣지타입별 연속 $\alpha_r$ (softmax) |
| `src/models/hin_gnn.py` | 3개 조립 + readout + XAI | content-aggregation 콜드스타트, L층 스택 |
| `src/data_builder/build_hetero_data.py` | parquet 7종 → `HeteroData` + split | id 맵, norm_id, 계층화 split |
| `src/train/trainer.py` | 이중 최적화(Bi-level) 학습 루프 | W(train) / α(val) optimizer 분리 |
| `src/eval/success_predictor.py` | 추론 + 평가지표 | PR-AUC·AUC-ROC·F1@best |
| `src/eval/recommend.py` | 순회 키워드 조합 추천 | 메타패스 α×성공 스코어 |

---

## 2. 그래프 사양 (실측)

**노드 3종** — `product` 5,143 / `keyword` 3,540(`is_trend_keyword` 플래그) / `ip` 288

**엣지 4종 (forward triple, 역방향 자동 생성)** — 모두 id 매핑 드롭 0

| triple | 행수 |
|---|---|
| `(product, has_kw, keyword)` | 33,761 |
| `(ip, has_kw, keyword)` | 1,744 |
| `(keyword, trend_to, keyword)` | 5,146 |
| `(product, has_ip, ip)` | 472 |

역방향은 `(t, rev_r, s)` 로 등록되어 **독립 파라미터**($W$)를 가짐(이기종성 유지).
`complement(product↔product)` 은 v1 제외(ablation 예정), `substitute` 미구현.

**타겟** `성공여부`: 성공 1,214 / 실패 3,929 (양성 23.6%).

---

## 3. 한 층(layer)의 수학 — 융합 방식

각 층은 세 메커니즘을 순차 결합한다 (표기: $H$=heads, $d_k$=hidden/H, $E$=엣지 수).

### (1) DiffMG 관계 게이트 — `diffmg_pruner.py`
관계별 학습 로짓 $\ell \in \mathbb{R}^R$ → $\alpha_r = \mathrm{softmax}(\ell/\tau)_r$ (연속 변수, $\sum_r\alpha_r{=}1$).
역전파로 성공 기여 관계 증폭, 노이즈 도태. `hard=True` 면 straight-through로 단일 관계 선택(메타그래프 pruning).

### (2) HGT 타입 격리 메시지 — `hgt_layer.py`
엣지 $(s\xrightarrow{r}t)$ 마다:
- $K=W^K_{type(s)}h_s,\ Q=W^Q_{type(t)}h_t,\ V=W^V_{type(s)}h_s$ → head 분해 $(N,H,d_k)$
- 어텐션 $\ \text{score}_e = (Q_{dst} \cdot (W^{att}_r K_{src})) \cdot \mu_r / \sqrt{d_k}\ $ → 목적지별 softmax → $\alpha^{HGT}_e\ (E,H)$
- 메시지 $\ m_e = (W^{msg}_r V_{src}) \cdot \alpha^{HGT}_e \cdot \alpha_r\ $ ($\alpha_r$=DiffMG 게이트)
- 목적지로 합산 집계 → 타입별 통합 투영 $\to agg_t\ (N_t, d)$

> **구현 단순화**: 정통 HGT 의 *관계 통합* softmax 대신 **관계별 softmax 후 DiffMG 게이팅 합산**.
> 관계 게이트를 깔끔히 분리하기 위함 — 추후 cross-relation softmax 로 교체 가능.

### (3) KGAT Bi-Interaction 업데이트 — `kgat_layer.py`
$$h' = \mathrm{LeakyReLU}(W_1(h+agg)) + \mathrm{LeakyReLU}(W_2(h\odot agg))$$
드롭아웃 → 잔차($+h$) → LayerNorm. 덧셈·원소곱 동시 융합으로 고차 상호작용 포착.

**L층 스택 = $A^L$ 재귀 전파** → 신상품이 2~3홉 너머 히트상품·트렌드 맥락 흡수(Cold Start).

---

## 4. 콜드스타트 — product content aggregation

product 는 ID 임베딩이 **없다**. 초기 피처를 연결 노드로부터 합성:
$$h^0_{product} = \mathrm{aggr}_{kw}\big(E_{keyword}\big) + \mathrm{aggr}_{ip}\big(E_{ip}\big) + W_{promo}\cdot\text{has\_promo}$$
`aggr` = mean(기본)|sum. keyword·ip 만 학습 임베딩 테이블 보유.
→ 매출 기록 없는 가상 신상품도 **키워드 집합만 주면** 즉시 초기화 가능(데이터 누수 없음: KPI 미사용).

---

## 5. Forward 흐름 (`HINGNN.forward`)

```
x = {keyword: E_kw, ip: E_ip, product: content_aggregation(edges, has_promo)}
full_edges = forward_edges ∪ reverse_edges
for layer in 1..L:
    α_r   = DiffMG_gate[layer]()                       # 관계 게이트
    agg   = HGT[layer](x, full_edges, rel_alpha=α_r)   # 타입격리 메시지
    x     = KGAT[layer](x, agg)                        # Bi-Interaction
logits = MLP_head(x['product'])                        # (P,) 성공 로짓
```

---

## 6. 이중 최적화 학습 (`trainer.py`)

`src/train/.claude-rules.md` 강제 — W 와 α(DiffMG) optimizer 분리:
- **Step 1 (train)**: `loss_train = BCE(logits[train_mask])` → `opt_w.step()` (W 만)
- **Step 2 (val)**: `loss_val = BCE(logits[val_mask])` → `opt_a.step()` (α 만)
- 각 step 전 양쪽 grad zero → backward/step 격리.
- Loss: `BCEWithLogitsLoss(pos_weight≈3.24)`. Early stopping: val PR-AUC, patience 30.
- 최종 `checkpoints/hin_gnn_best.pt` 저장(model state + maps + config), test 평가 출력.

---

## 7. 평가 (`success_predictor.py`)
`compute_metrics(y_true, y_prob)` → PR-AUC(주) + AUC-ROC + F1@best-threshold.

## 8. 순회 추천 (`recommend.py`) — 활용 단계
학습된 가중치만 사용. 시드 키워드 $k_s$ 에 대해 메타패스 $k_s\to product\to k_t$:
$$\text{score}(k_t|k_s)=\sum_{j:\,k_s,k_t\in kw(j)} \underbrace{att(j,k_s)}_{\text{엣지 }\alpha}\cdot \underbrace{p_{success}(j)}_{\text{성공확률}}\cdot att(j,k_t)$$
- `recommend_combinations(...)` → top_k (키워드, 점수)
- `export_relation_importance(model)` → 층별 $\alpha_r$ (XAI MD 대시보드)
- `export_weighted_kw_edges(...)` → 가중 product↔keyword 엣지 CSV(오프라인 순회)

---

## 9. 실행 방법

```bash
# 사전: PyTorch + PyG + scikit-learn + pyyaml 설치된 환경 (프로젝트 루트에서 실행)
python -m src.data_builder.build_hetero_data   # 로더 단독 점검 (노드/엣지/split 출력)
python -m src.train.trainer                    # 학습 → checkpoints/hin_gnn_best.pt
```
```python
# 추천 사용 예
from src.data_builder.build_hetero_data import build_graph, forward_edge_index_dict
from src.models.hin_gnn import HINGNN
from src.eval.recommend import recommend_combinations
# (모델 로드 후)
recommend_combinations(model, eidx, maps, seed_keywords=["마라"], top_k=20)
```

---

## 10. 알려진 단순화 / TODO
- HGT softmax 관계별 처리(§3 (2) 박스) — cross-relation 통합 softmax 로 교체 여지
- 콜드스타트 *그래프 주입 API* 미구현(현재 content-aggregation 함수만 존재) — `COLD_START` 게이트
- `complement` 엣지 ablation 도입, `substitute`(Jaccard) 엣지 미구현
- 다중 타겟(생존기간·판매궤적 클러스터) 미구현 — 현행 이진 성공 라벨
- 실행 검증은 torch 환경 필요(개발 venv 에 torch 미설치 — 컴파일·컬럼·드롭률·split 만 사전 검증 완료)
