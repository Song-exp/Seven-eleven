# 최종 모델 요약 — exp41_trend_kw3_ip1

> ⚠️ **구버전 / 폐기 모델 (exp41, 동반구매 누수 포함) — 현행 아님.**
> 현행 서빙은 **v2_sweepA**(held-out PR-AUC 0.608, leak-free). 아래 수치(PR-AUC 0.6959 등)는 누수가 포함된 exp41 기준이라 운영과 다르다.
> 현행 근거: [`final_evolution_report.md`](../final_evolution_report.md) · [`final_model_leakfree_switch_plan.md`](../final_model_leakfree_switch_plan.md) · [`md_prescription_system_guide.md`](../md_prescription_system_guide.md).

> 생성일: 2026-06-20  
> 체크포인트: `checkpoints/hin_gnn_best.pt`  
> 실험 config: `experiments/configs/exp41_trend_kw3_ip1.yaml`

---

## 1. 모델 식별

| 항목 | 값 |
|---|---|
| 실험명 | `exp41_trend_kw3_ip1` |
| 선정 근거 | 12셀 격자 실험 1위 · Phase A(h) · Phase B(kw) 확정 후 최고 성능 |
| 랜덤 베이스라인 PR-AUC | 0.236 (양성 비율 23.6%) |

---

## 2. HIN 그래프 구조

### 2-1. 노드 타입

| 노드 | 수량 | 초기화 방식 |
|---|---|---|
| `product` | 5,033 | content_aggregation (키워드 mean + 프로모션 원핫 18종) |
| `keyword` | 2,039 | random embedding |
| `ip` | 281 | random embedding |

### 2-2. 엣지 타입 (Forward 12 + Reverse 12 = 24 관계)

| 엣지 타입 | 경로 | 설명 |
|---|---|---|
| `product → has_kw → keyword` | 직접 | 제품-키워드 직접 연결 |
| `ip → has_kw → keyword` | 직접 | IP-키워드 직접 연결 |
| `keyword → trend_to → keyword` | 직접 | 트렌드 키워드 → 속성 키워드 |
| `product → has_ip → ip` | 직접 | 제품-IP 직접 연결 |
| `ip → has_ip → ip` | 직접 | IP-IP 연결 |
| `product → co_offline → product` | 직접 | 오프라인 영수증 동반구매 (Lift 기반) |
| `product → co_quick → product` | 직접 | 퀵커머스 영수증 동반구매 (Lift 기반) |
| `product → sim_kw → product` | 2-hop | 공유 키워드 ≥ 3개인 제품 쌍 |
| `product → sim_ip → product` | 2-hop | 공유 IP ≥ 1개인 제품 쌍 |
| `product → has_kw_via_ip → keyword` | 2-hop | P→IP→K (A_PI @ A_IK) |
| `product → has_kw_ipip → keyword` | 3-hop | P→IP→IP→K (A_PI @ A_II @ A_IK) |
| `product → has_kw_trend → keyword` | 2-hop | P→K→K (A_PK @ A_KK, 트렌드 속성 경유) |

> 역방향(rev_) 엣지 12종은 `hin_gnn.py`에서 자동 추가. config에 별도 명시 불필요.

### 2-3. 그래프 밀도 파라미터

| 파라미터 | 값 | 설명 |
|---|---|---|
| `hop2_kw_min_shared` | **3** | sim_kw 엣지 생성 최소 공유 키워드 수 |
| `hop2_ip_min_shared` | **1** | sim_ip 엣지 생성 최소 공유 IP 수 |
| `lift_normalization` | log1p | 동반구매 Lift 정규화 |
| `use_idf_keyword_weights` | false | 키워드 IDF 가중 미사용 |
| `use_lift_weights` | false | Lift 값 엣지 가중 미사용 |

---

## 3. 모델 아키텍처

| 파라미터 | 값 |
|---|---|
| 아키텍처 | HGT (Heterogeneous Graph Transformer) + DiffMG gate |
| `hidden_dim` | **64** |
| `num_layers` | 1 |
| `num_heads` | 4 |
| `dropout` | 0.3 |
| `use_diffmg_gate` | true |
| `diffmg_temperature` | 0.5 |
| `readout_hop_mode` | final |

> **DiffMG gate**: 24개 관계 타입에 Softmax 어텐션 가중치 α_r을 학습.  
> 경로 중요도를 미분 가능하게 학습 → XAI 해석 근거로 직결.

---

## 4. 학습 설정

| 파라미터 | 값 |
|---|---|
| optimizer | Adam |
| `lr_w` (모델 가중치) | 0.005 |
| `lr_alpha` (DiffMG 게이트) | 0.02 |
| `weight_decay_w` | 0.0005 |
| `weight_decay_alpha` | 0.001 |
| `epochs` | 200 |
| early stopping metric | val_pr_auc |
| early stopping patience | 30 |
| `device` | auto (CUDA 우선) |

### 레이블 설정

| 파라미터 | 값 |
|---|---|
| 손실 함수 | Weighted BCE |
| `pos_weight` | 3.24 |
| 양성 클래스 | `성공여부 == '성공'` |

### 데이터 분할

| 파라미터 | 값 |
|---|---|
| 비율 | train 70 / val 15 / test 15 |
| scheme | stratified |
| stratify_by | `[편의점명, 성공여부]` |
| seed | 42 |

---

## 5. 성능 지표

| Split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| Train | 0.7528 | 0.9056 | 0.6897 | 0.5996 |
| Val | 0.7288 | 0.8625 | 0.6469 | 0.7535 |
| **Test** | **0.6959** | **0.8734** | **0.6528** | **0.5552** |

- **val-test gap**: 0.033 (기준 0.040 이하 → 과적합 없음)
- **랜덤 대비 배율**: 0.6959 / 0.236 ≈ **2.95×**

---

## 6. DiffMG 관계 중요도 α_r (Layer 0)

> Softmax 정규화 값. 균등 기준 = 1/24 ≈ 0.0417.

| 관계 타입 | α_r | 해석 |
|---|---|---|
| `product → co_offline → product` | **0.1314** | ★ 오프라인 동반구매가 가장 강한 신호 |
| `product ← co_offline ← product` | 0.0998 | 역방향도 유효 |
| `product ← co_quick ← product` | 0.0929 | 퀵커머스 역방향 |
| `product → co_quick → product` | 0.0787 | 퀵커머스 순방향 |
| `product → sim_ip → product` | 0.0690 | IP 유사 제품 쌍 |
| `product ← sim_ip ← product` | 0.0686 | IP 유사 역방향 |
| `product → sim_kw → product` | 0.0492 | 키워드 유사 제품 쌍 |
| `product ← sim_kw ← product` | 0.0400 | 키워드 유사 역방향 |
| 나머지 16개 관계 | ≈ 0.0231 | 균등 수준 (게이트 미분화 상태) |

> **주의**: α_r의 많은 관계가 ≈ 1/R(균등)에 수렴.  
> `lr_alpha` 상향 또는 `diffmg_temperature` 하향 후 재학습 시 분화 가능성 있음.

---

## 7. 추천 시스템 설정

| 파라미터 | 값 |
|---|---|
| `meta_path` | keyword → product → keyword |
| `score_fn` | hybrid_alpha_success |
| `top_k` | 20 |
| `beam_width` | 50 |

---

## 8. 영속화 산출물

| 파일 | 설명 |
|---|---|
| `checkpoints/hin_gnn_best.pt` | 학습된 모델 가중치 |
| `data/processed/hin/weighted_product_keyword_edges.parquet` | 가중 네트워크 (37,333행) |
| `data/processed/hin/learned_product_scores.parquet` | 제품별 예측확률 + 임베딩 norm (5,033행) |
| `data/processed/hin/relation_importance.json` | 층별 α_r |
| `experiments/results/exp41_trend_kw3_ip1/metrics.json` | 성능 지표 |
| `experiments/results/exp41_trend_kw3_ip1/config_used.yaml` | 실험 당시 실제 사용 config |
