# HIN 그래프 데이터 & 모델 실행 가이드

## 0. 사전 준비 — LLM 서버 (Ollama)

`src/eval/serve.py` (대시보드 RAG) 및 `src/data_builder/extract_ip_kw_candidates.py` (IP 키워드 추출)는 Ollama gemma4:e4b를 사용한다. 스크립트 실행 전 WSL에서 먼저 띄워야 한다.

### Ollama 시작 (WSL 터미널)

```bash
OLLAMA_MODELS=/mnt/c/Users/송정현/.ollama/models OLLAMA_HOST=0.0.0.0:11435 ollama serve
```

`Listening on [::]:11435` 로그가 찍히면 준비 완료. 이 창은 그대로 두고 다른 터미널에서 스크립트를 실행한다.

> 또는 PowerShell에서 `.\scripts\start_ollama.ps1` 실행해도 동일.

### 연결 구조

```
[Python 스크립트 / FastAPI]  →  localhost:11435  →  [WSL Ollama]  →  RTX 5060 (CUDA)
```

스크립트는 HTTP 요청만 보내므로 Windows / WSL 어디서 실행해도 무관하다. GPU는 Ollama 서버가 담당한다.

---

## 1. 최종 그래프 데이터

저장 위치: `data/processed/hin/`

### 노드 (3종)

| 파일 | 노드 타입 | 수량 | 주요 컬럼 |
|---|---|---|---|
| `product_nodes.parquet` | 제품 | 5,161개 (세븐일레븐 2,996 / CU 1,162 / GS25 1,003) | `ITEM_CD`, `ITEM_NM`, `편의점명`, `성공여부`, `첫_등장일`, `insta_mention_30d`, `키워드_final`, `promo_*` × 18 |
| `keyword_nodes.parquet` | 키워드 | 3,345개 (트렌드 포함 370개) | `keyword`, `is_trend_keyword`, `추출_속성` |
| `ip_nodes.parquet` | IP/브랜드 | 282개 | `ip_name`, `키워드_final` |

### 엣지 (4종)

| 파일 | 관계 | 수량 |
|---|---|---|
| `product_keyword_edges.parquet` | 제품 → 키워드 | 41,335행 |
| `ip_keyword_edges.parquet` | IP → 키워드 | 1,671행 |
| `trend_keyword_edges.parquet` | 트렌드키워드 → 속성키워드 | 2,303행 |
| `product_ip_edges.parquet` | 제품 → IP | 1,291행 |

### 생성 방법

그래프 데이터를 처음부터 재생성하려면:

```
eda/notebooks/04_hin_graph_builder.ipynb  →  Run All
```

소스 입력 파일 (`data/processed/hin/`):
- `product_final_keywords.csv` — 제품별 확정 키워드
- `product_ip_mapping.xlsx` — IP 매핑 (Sheet: 제품_IP_매핑, IP별_키워드)
- `product_promo_keywords.csv` — 프로모션 유형
- `keyword_eda_final.csv` — 키워드 정규화 검수 결과

---

## 2. 네트워크 모델 구조

### 아키텍처: KGAT × HGT × DiffMG 융합

```
입력 그래프 (HeteroData)
        ↓
[product 초기 표현]  ← keyword/ip 임베딩 집계 + has_promo + insta_mention_30d
        ↓
[L 층 반복 (기본 2층)]
  ┌─ DiffMGRelationGate  : 관계 타입별 중요도 α_r 학습 (softmax 경쟁)
  ├─ HGTLayer            : 타입별 독립 가중치 행렬로 메시지 생성
  └─ KGATUpdate          : Bi-Interaction으로 노드 표현 업데이트
        ↓
[Readout head]  →  product 성공확률 (이진 분류)
```

> 컴포넌트별 파일·코드 레벨 수식·forward 흐름·학습 루프는 **[부록 A. 구현 상세](#부록-a-구현-상세-코드-레벨)** 참조.

### 입력 피처 요약

- **product**: keyword/ip 임베딩 mean-aggregation + `has_promo`(0/1) + `insta_mention_30d`
- **keyword**: 학습 임베딩 (Xavier 초기화)
- **ip**: 학습 임베딩 (Xavier 초기화)
- **타겟 레이블**: `성공여부` — 세븐일레븐 POS 파레토 80% 기준 / CU·GS25 인스타 좋아요합 기준

---

## 3. 실험 실행 방법

### 방법 A — 통합 노트북 (권장)

```
experiments/notebooks/methodA_relation_gating.ipynb
```

커널 재시작 → Run All 하면 exp01~exp07 순차 학습 후 비교표 출력.

| 실험 | 핵심 변경점 | 비고 |
|---|---|---|
| exp01 Baseline | hidden=128, layers=2, lr_α=0.005, temp=1.0 | 비교 기준 |
| exp02 α 튜닝 | lr_α=0.02, temperature=0.5 | α_r 분화 유도 |
| exp03 보완재 엣지 | complement_edges=true | 동반구매 관계 추가 |
| exp06 동반구매 Lift | co_offline+co_quick, Lift 가중치 | Lift 신호 기여 측정 |
| exp07 동반구매 Binary | Lift 가중치 없이 엣지 존재만 | Lift 값 기여 검증 |

특정 실험만 재실행: 해당 셀에서 `force=True` 주석 해제 후 실행.

### 방법 B — CLI 단독 학습

```bash
# 프로젝트 루트에서 실행
python -m src.train.trainer
# 기본 config: configs/train_config.yaml

# 특정 실험 config 지정 (exp_utils.run_experiment 경유)
python -c "
from experiments.exp_utils import run_experiment
run_experiment('experiments/configs/exp01_baseline.yaml', 'exp01_baseline', force=True)
"
```

### 방법 C — 결과만 export (재학습 없이)

```bash
python -m src.eval.export_results
# 기본: checkpoints/hin_gnn_best.pt 로드
# 출력: experiments/results/{exp_name}/ 에 metrics.json, relation_importance.json, report.md
```

---

## 4. 실험 결과 읽는 법

각 실험 결과는 `experiments/results/{exp_name}/` 에 저장됩니다.

```
exp01_baseline/
├── config_used.yaml             # 재현용 하이퍼파라미터
├── metrics.json                 # train/val/test PR-AUC, AUC-ROC, F1
├── relation_importance.json     # 층별 DiffMG α_r (관계 중요도)
├── report.md                    # 종합 보고서
├── training_curve.png           # 학습 곡선
└── alpha_heatmap.png            # α_r 히트맵
```

핵심 지표: **test PR-AUC** (랜덤 기준선 0.236 대비 개선 여부로 판단)

### 관계 중요도 해석 (`relation_importance.json`)

```json
[
  { "product-has_kw-keyword": 0.32, "ip-has_kw-keyword": 0.18, ... },  // layer 0
  { "product-has_kw-keyword": 0.28, ... }                               // layer 1
]
```

α_r 값이 1/관계수(≈0.125 for 8관계)에 가까우면 분화 미흡 → exp02처럼 temperature 낮춰서 재실험.

---

## 5. 파이프라인 전체 흐름

```
[데이터 준비]
  eda/notebooks/00_product_keyword_pipeline.ipynb   키워드 소스 전처리
  eda/notebooks/01_pos_feature_engineering.ipynb    POS 피처 계산
  eda/notebooks/01b_matching_diagnostics.ipynb      매칭 QA + 성공 라벨 생성
        ↓
[HIN 그래프 빌드]
  eda/notebooks/04_hin_graph_builder.ipynb
  → data/processed/hin/*.parquet  (노드 3종 + 엣지 4종)
        ↓
[모델 학습 & 실험]
  experiments/notebooks/methodA_relation_gating.ipynb
  → experiments/results/{exp_name}/
        ↓
[추론 & XAI]
  src/eval/export_results.py      가중 엣지·예측확률·α_r export
  src/eval/visualize_network.py   키워드 네트워크 시각화
  src/eval/recommend.py           시드 키워드 기반 조합 추천
```

---

# 부록 A. 구현 상세 (코드 레벨)

> 연구 논리·선행연구는 [`model_architecture.md`](model_architecture.md), 학습 의사결정은 [`training_decisions.md`](training_decisions.md), 하이퍼파라미터는 `configs/train_config.yaml` 참조.
> 이 부록은 **실제 구현된 코드 구조**를 정리한다. (2026-06-04 구현, 구 `hin_gnn_implementation.md` 통합)
> 노드·엣지 수량은 빌드별로 다르므로 **본문 §1을 정본**으로 본다(아래 §A.2 수치는 통합 시점 실측 스냅샷).

## A.1 파일 맵

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

## A.2 그래프 사양 (통합 시점 실측 스냅샷)

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

## A.3 한 층(layer)의 수학 — 융합 방식

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

## A.4 콜드스타트 — product content aggregation

product 는 ID 임베딩이 **없다**. 초기 피처를 연결 노드로부터 합성:
$$h^0_{product} = \mathrm{aggr}_{kw}\big(E_{keyword}\big) + \mathrm{aggr}_{ip}\big(E_{ip}\big) + W_{promo}\cdot\text{has\_promo}$$
`aggr` = mean(기본)|sum. keyword·ip 만 학습 임베딩 테이블 보유.
→ 매출 기록 없는 가상 신상품도 **키워드 집합만 주면** 즉시 초기화 가능(데이터 누수 없음: KPI 미사용).

## A.5 Forward 흐름 (`HINGNN.forward`)

```
x = {keyword: E_kw, ip: E_ip, product: content_aggregation(edges, has_promo)}
full_edges = forward_edges ∪ reverse_edges
for layer in 1..L:
    α_r   = DiffMG_gate[layer]()                       # 관계 게이트
    agg   = HGT[layer](x, full_edges, rel_alpha=α_r)   # 타입격리 메시지
    x     = KGAT[layer](x, agg)                        # Bi-Interaction
logits = MLP_head(x['product'])                        # (P,) 성공 로짓
```

## A.6 이중 최적화 학습 (`trainer.py`)

`src/train/.claude-rules.md` 강제 — W 와 α(DiffMG) optimizer 분리:
- **Step 1 (train)**: `loss_train = BCE(logits[train_mask])` → `opt_w.step()` (W 만)
- **Step 2 (val)**: `loss_val = BCE(logits[val_mask])` → `opt_a.step()` (α 만)
- 각 step 전 양쪽 grad zero → backward/step 격리.
- Loss: `BCEWithLogitsLoss(pos_weight≈3.24)`. Early stopping: val PR-AUC, patience 30.
- 최종 `checkpoints/hin_gnn_best.pt` 저장(model state + maps + config), test 평가 출력.

## A.7 평가 (`success_predictor.py`)
`compute_metrics(y_true, y_prob)` → PR-AUC(주) + AUC-ROC + F1@best-threshold.

## A.8 순회 추천 (`recommend.py`) — 활용 단계
학습된 가중치만 사용. 시드 키워드 $k_s$ 에 대해 메타패스 $k_s\to product\to k_t$:
$$\text{score}(k_t|k_s)=\sum_{j:\,k_s,k_t\in kw(j)} \underbrace{att(j,k_s)}_{\text{엣지 }\alpha}\cdot \underbrace{p_{success}(j)}_{\text{성공확률}}\cdot att(j,k_t)$$
- `recommend_combinations(...)` → top_k (키워드, 점수)
- `export_relation_importance(model)` → 층별 $\alpha_r$ (XAI MD 대시보드)
- `export_weighted_kw_edges(...)` → 가중 product↔keyword 엣지 CSV(오프라인 순회)

```python
# 로더 단독 점검 + 추천 사용 예 (프로젝트 루트)
python -m src.data_builder.build_hetero_data   # 노드/엣지/split 출력
```
```python
from src.data_builder.build_hetero_data import build_graph, forward_edge_index_dict
from src.models.hin_gnn import HINGNN
from src.eval.recommend import recommend_combinations
# (모델 로드 후)
recommend_combinations(model, eidx, maps, seed_keywords=["마라"], top_k=20)
```

## A.9 알려진 단순화 / TODO
- HGT softmax 관계별 처리(§A.3 (2) 박스) — cross-relation 통합 softmax 로 교체 여지
- 콜드스타트 *그래프 주입 API* 미구현(현재 content-aggregation 함수만 존재) — `COLD_START` 게이트
- `complement` 엣지 ablation 도입, `substitute`(Jaccard) 엣지 미구현
- 다중 타겟(생존기간·판매궤적 클러스터) 미구현 — 현행 이진 성공 라벨
- 실행 검증은 torch 환경 필요(개발 venv 에 torch 미설치 — 컴파일·컬럼·드롭률·split 만 사전 검증 완료)
