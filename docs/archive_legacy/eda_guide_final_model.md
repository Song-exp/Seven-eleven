# 최종 모델 EDA 가이드 — 팀원용

> ⚠️ **구버전 / 폐기 모델 (exp41, 동반구매 누수 포함) — 현행 아님.**
> 현행 서빙은 **v2_sweepA**. 이 가이드의 모델·수치는 exp41 기준이라 운영과 다르다.
> 현행 EDA·처방 진입점: [`md_prescription_system_guide.md`](../md_prescription_system_guide.md) · [`eda_channel_prescription_plan.md`](../eda_channel_prescription_plan.md).

> **대상 모델**: `exp41_trend_kw3_ip1`  
> test PR-AUC **0.6959** / val-test gap **0.033** / 랜덤 대비 **2.95배**  
> 상세 스펙: `final_model_summary.md`

---

## 0. 환경 준비

```bash
# 프로젝트 루트에서
.venv\Scripts\activate        # Windows
source .venv/bin/activate     # Mac/Linux
```

**체크포인트·산출물 경로** (이미 저장됨, 재학습 불필요)

| 파일 | 위치 |
|---|---|
| 모델 가중치 | `experiments/results/exp41_trend_kw3_ip1/hin_gnn_best.pt` |
| 제품 예측 점수 | `experiments/results/exp41_trend_kw3_ip1/learned_product_scores.parquet` |
| 가중 제품-키워드 엣지 | `experiments/results/exp41_trend_kw3_ip1/weighted_product_keyword_edges.parquet` |
| 관계 중요도 α_r | `experiments/results/exp41_trend_kw3_ip1/relation_importance.json` |

---

## 1. 노트북 실행 순서

### Step 1 — 실험 결과 확인 (선택)

```
experiments/notebooks/methodB_edge_ablation.ipynb
```

- **Run All** → 12셀 격자 결과, Phase A/B 비교 표, α 히트맵 출력
- 학습을 새로 돌리지 않음 — 저장된 metrics.json을 읽는 것뿐

---

### Step 2 — 질적 분석 (핵심 EDA)

```
experiments/notebooks/methodB_qualitative_analysis.ipynb
```

**볼 수 있는 것**:
- 시드 키워드(예: 마라, 로제, 흑임자)로부터 모델이 추천하는 키워드 조합
- K-P-K walk 결과 — "이 키워드 조합을 가진 히트 제품은 무엇인가"
- 모델 품질 정성 판정 (추천 조합이 실제 트렌드와 맞는지)

**실행 방법**: Run All (serve.py가 exp41을 자동으로 읽음)

**팀원이 집중할 포인트**:
- Phase 1 출력 테이블: 시드별 추천 조합 top-6 검토
- "이 조합이 기획 관점에서 말이 되는가?" 정성 판단 후 메모

---

### Step 3 — 네트워크 구조 EDA

```
experiments/notebooks/methodB_network_walk_eda.ipynb
```

**볼 수 있는 것**:
- 성공 vs 실패 제품의 키워드 분포 차이
- 허브 키워드 (많은 제품과 연결된 키워드) 목록
- 트렌드 키워드 포함 비율이 성공/실패 그룹에서 다른지
- Confusion Matrix — 모델이 실제로 맞히는 구간

**실행 방법**: Run All

---

### Step 4 — 대시보드 서빙

```bash
python -m uvicorn src.eval.api:app --port 8000
```

브라우저에서 `http://localhost:8000` 접속

- `/health` → `{"serving_exp": "exp41_trend_kw3_ip1"}` 확인 후 사용
- 트렌드 키워드 입력 → 추천 키워드 조합 + 경유 제품 네트워크 시각화

---

## 2. 모델 출력 해석 요령

### 예측 점수 (`learned_product_scores.parquet`)

```python
import pandas as pd
scores = pd.read_parquet("experiments/results/exp41_trend_kw3_ip1/learned_product_scores.parquet")
# 컬럼: ITEM_CD, pred_success_prob, emb_norm
# pred_success_prob >= 0.5552 → 성공 예측 (test 최적 threshold)
```

### 관계 중요도 α_r (`relation_importance.json`)

```python
import json
alpha = json.load(open("experiments/results/exp41_trend_kw3_ip1/relation_importance.json"))[0]
sorted(alpha.items(), key=lambda x: -x[1])[:5]
# → co_offline(0.131) > rev_co_offline(0.100) > rev_co_quick(0.093) > co_quick(0.079) > sim_ip(0.069)
```

**해석 주의**: 대부분 관계의 α ≈ 0.023 (균등 수렴). 오프라인/퀵커머스 동반구매만 유의미하게 분화된 상태. 나머지 관계(키워드·IP 경로)는 학습이 더 필요하거나 `lr_alpha`/`temperature` 조정으로 분화 가능.

### 가중 키워드 엣지 (`weighted_product_keyword_edges.parquet`)

```python
wedges = pd.read_parquet("experiments/results/exp41_trend_kw3_ip1/weighted_product_keyword_edges.parquet")
# 컬럼: ITEM_CD, keyword, weight
# weight = 모델이 해당 제품-키워드 연결에 부여한 학습 가중치
```

---

## 3. 남은 EDA 작업 목록

| 우선순위 | 작업 | 노트북/위치 | 비고 |
|---|---|---|---|
| ★★★ | 질적 분석 결과 정리 — 추천 조합 OK/NG 판정 | `methodB_qualitative_analysis` Run All | MD에게 공유할 기획 근거 |
| ★★★ | 대시보드 시연 데이터 검증 | `api.py` 서버 띄운 뒤 직접 조작 | 시드 10개 이상 테스트 |
| ★★ | 성공/실패 서브네트워크 비교 | `methodB_network_walk_eda` Run All | 허브 키워드 목록 확보 |
| ★★ | α_r 분화 개선 시도 | `lr_alpha` 0.02→0.05, `temperature` 0.5→0.3 | 선택 사항 — 현재 성능 충분 |
| ★ | all_experiments.ipynb 최신화 | exp41 포함 여부 확인 후 Run All | |

---

## 4. serve.py 교체 완료 확인

`src/eval/serve.py` 상단:

```python
SERVING_EXP = "exp41_trend_kw3_ip1"   # 최종 모델 확정
```

이 줄이 이미 변경됨 — 별도 수정 불필요.
