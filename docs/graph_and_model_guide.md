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

| 컴포넌트 | 파일 | 역할 |
|---|---|---|
| `HINGNN` | `src/models/hin_gnn.py` | 전체 조립 모델 |
| `HGTLayer` | `src/models/hgt_layer.py` | 이기종 엣지 타입별 어텐션 |
| `KGATUpdate` | `src/models/kgat_layer.py` | 다중홉 Bi-Interaction 업데이트 |
| `DiffMGRelationGate` | `src/models/diffmg_pruner.py` | 관계 중요도 α_r 미분 학습 |

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
