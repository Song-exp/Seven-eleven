# v2 서빙 어댑터 전환 기록 (2026-06-21)

> 무엇이 바뀌었나 — exp47(현 서빙) 외에 **Model v2(`v2_sweepA`)를 EDA·처방 파이프라인에서 선택·로드**할 수 있게 한 변경 기록.
> 관련: [모델 전환·확정](final_model_leakfree_switch_plan.md) · [EDA·처방](eda_channel_prescription_plan.md) · 실험 대장 §2.5.

---

## 0. TL;DR
- **두 모델을 한 파이프라인에서 명시적으로 선택** 가능해짐: `EngineConfig.exp47()` / `EngineConfig.v2_sweepA()`.
- 엔진이 체크포인트 `config.model.model_class`로 **자동 분기**(HINGNN vs HINGNNv2) + v2면 `basket_comp` 엣지 주입.
- EDA 노트북 상단에 **모델 선택 셀**(`MODEL = "exp47" | "v2_sweepA"`) 추가, 출력은 모델별 폴더 분리.
- **현 서빙 기본값은 여전히 exp47** (안전). v2는 검증된 우세 후보로 *선택 로드 가능* 상태.

---

## 1. 왜 (배경)
held-out 공정 비교에서 v2가 우세(▼)인데, exp47만 로드 가능하면 v2를 실제로 써볼 수 없었음. → 두 모델을 **동일 EDA·처방 코드로 비교·전환**할 수 있게 어댑터를 추가.

| held-out(test) | exp47 | v2_sweepA |
|---|---|---|
| PR-AUC | 0.570 | **0.606~0.608** |
| 운영점 F1 | 0.544 | 0.55~0.583 (⚠노이즈) |
| 과적합 gap | 0.215 | **0.135** |
| leak-free | ✅ | ✅ |

> ⚠ **정직한 단서**: test셋이 작아(~180 성공) **운영점 F1은 run마다 0.55~0.583로 흔들림**. v2의 견고한 우위는 **PR-AUC(+0.036)·gap(−0.08)**이고, 운영점 우위는 마진/노이즈. "v2가 분명히 낫다"가 아니라 "v2가 leak-free면서 랭킹·일반화는 확실히 낫고, 운영점은 동등~소폭 우세"가 정확한 표현.

---

## 2. 무엇이 바뀌었나 (코드 변경)

### `src/eval/md/engine.py`
| 변경 | 내용 |
|---|---|
| `EngineConfig.exp47()` / `.v2_sweepA()` | 모델 프리셋 classmethod (exp_dir·THR 자동). v2 THR=0.7757. |
| `EngineConfig.basket_comp_support=3` | v2 basket_comp 엣지 지지도 하한(학습과 일치). |
| `run_single_inference` 분기 | `config.model.model_class=="HINGNNv2"` 또는 `add_basket_comp_edges` 면 v2 경로. |
| `_inject_basket_comp()` | `keyword_basket_comp_edges.parquet`(support≥3) → `('keyword','basket_comp','keyword')` eidx 주입. |
| `_rebuild_v2()` | `HINGNNv2`를 `config.graph.edge_types`(basket_comp 포함)로 재구성. |
| `self.is_v2` 플래그 | 캐시/노트북에서 모델 구분용. |

> exp47 경로는 **무변경**(기존 `_rebuild`=HINGNN 그대로). v2는 추가 분기일 뿐 기존 동작 영향 없음.

### `experiments/results/v2_sweepA/` (신규 — 동결 artifact)
`v2_export_final.py`로 학습·동결: `hin_gnn_best.pt`(HINGNNv2 state) / `config_used.yaml`(model_class=HINGNNv2, edge_types+basket_comp, hidden=32) / `learned_product_scores.parquet` / `metrics.json`.

### `experiments/notebooks/md_prescription_pipeline.ipynb`
- **모델 선택 셀 추가**: `MODEL = "exp47"  # 또는 "v2_sweepA"` 한 줄로 전환.
- `ENGINE_CFG = EngineConfig.exp47()/.v2_sweepA()`, `OUT_DIR=…/{MODEL}` (출력 분리).
- Stage 1에 `is_v2` 표기.

### `experiments/v2_promote.py`
- `compare_models`에 **`test_mask` 인자 추가 → test-only 운영점이 기본**. (full-set은 train 과적합으로 더 과적합한 모델을 유리하게 만드는 오류 → §3 교훈)

---

## 3. 핵심 교훈 (이번 전환에서 확정된 원칙)
1. **운영점 비교는 반드시 test-only.** full-set 운영점 F1은 train(70%) 과적합을 흡수해 *더 과적합한 모델*을 유리하게 만든다(1차 오판의 원인: exp47 full 0.666 vs test 0.544).
2. **PR-AUC와 운영점은 다른 것.** 평균 랭킹(PR-AUC)이 좋아도 배포 임계값 한 점에선 질 수 있다. 둘 다 봐야 한다.
3. **작은 test셋의 운영점 F1은 노이즈.** 단일 run으로 결론 금지 — PR-AUC·gap 같은 안정 지표로 교차 확인.

---

## 3.5. ⚠ 모델을 바꾸면 무엇이 바뀌나 (실측)

노트북 `MODEL`을 exp47↔v2로 바꿔도 **키워드 장부는 거의 안 바뀝니다**. 실측:
- `has_kw` 어텐션: exp47 vs v2 **상관 1.00**(최대차 0.0006) → 사실상 동일.
- `prob`(제품별 예측): **상관 0.91, 최대차 0.72** → 확실히 다름.
- killer/mine/hub 장부: **Jaccard 1.00**(완전 동일).

**이유**: 어텐션은 키워드 기준 softmax라 **그래프 구조가 지배**(학습 가중치 거의 무관, rare-keyword 편향). 따라서 `Score_succ ≈ 키워드별 성공제품 비율` = **데이터 속성**이라 모델 불변. 반면 prob은 제품 임베딩(basket_comp·멀티태스크·hidden 차이)으로 크게 달라짐.

| 구성요소 | 모델 의존? | 근거 |
|---|---|---|
| killer/mine/hub 장부 | ❌ 거의 불변 | 어텐션=구조 지배 |
| A_diff 행렬 (Stage 2) | ❌ 거의 불변 | has_kw 어텐션 기반 |
| 승인/반려 판정 | ❌ 불변 | 장부 기반 |
| 혼동행렬 (Stage 0.5) | ✅ 바뀜 | prob + THR |
| 파트너 추천 (메타패스 빔) | ✅ 바뀜 | `p_success(j)` 가중 |
| anti-partner · 소생 (B·F) | ✅ 바뀜 | Δprob 개입 |

> **함의**: MD에게 주는 핵심 어휘(장부·A_diff)는 모델 선택에 안정적. 모델의 가치는 장부가 아니라 **제품 점수·동적 추천(파트너·개입)**에 있음. (메모리 "성공예측은 가중치 학습용 신호"와 정합)

---

## 4. 사용법
```python
from src.eval.md.engine import MDEngine, EngineConfig
eng = MDEngine(EngineConfig.exp47()).run_single_inference()      # 현 서빙
eng = MDEngine(EngineConfig.v2_sweepA()).run_single_inference()  # v2 (HINGNNv2+basket_comp 자동)
```
노트북: 상단 `MODEL` 한 줄 변경 → 전체 EDA·처방·검증이 그 모델로 재실행, 결과는 `…/md_prescription/{MODEL}/`에 분리 저장.

---

## 5. 아직 안 된 것 (범위 밖)
- **FastAPI 서빙(`src/eval/serve.py`)은 미적용.** serve.py는 자체 로직이 많아 별도 어댑터 필요. (현재 `SERVING_EXP="exp47_no_copurchase"` 유지)
- **기본 서빙 모델 교체는 미실행.** v2를 "기본"으로 바꾸려면: ① test셋 확대/교차검증으로 운영점 우위 재확인 → ② `EngineConfig` 기본값·serve.py 포인터 변경. 현재는 **선택 로드만 가능**(안전한 상태).
