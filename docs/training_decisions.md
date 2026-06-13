# HIN-GNN 학습 의사결정 로그

> Decision Gate Protocol 기록. 각 결정은 `configs/train_config.yaml` 의 키와 1:1 대응.

## [NODE_FEAT] product 노드 초기 피처
- **결정**: 콘텐츠 집계 (content aggregation)
- **내용**: product 초기 벡터 = 연결된 keyword/ip 임베딩의 mean + `has_promo_30d` 1차원 결합. keyword·ip만 학습 임베딩 테이블 보유.
- **이유**: 활용 시나리오(시드 키워드 → 고가중치 엣지 순회 → 조합 추천)에서 키워드 의미가 자산으로 남아야 순회 품질이 좋음. 또한 매출 기록 없는 가상 신상품도 키워드 집합만으로 초기화 가능 → 진정한 Cold Start 지원. ID 임베딩은 신규 제품 표현 불가.
- **일시**: 2026-06-04

## [GRAPH] complement(보완재) 엣지 v1 포함 여부
- **결정**: v1 제외
- **내용**: hin/ 엣지 4종으로 baseline 확립. `complement_lift_pairs.csv`(968쌍)는 ablation 단계에서 추가.
- **이유**: 변수 통제. complement는 product↔product라 키워드 순회엔 간접 기여. 세븐 POS에만 존재(CU/GS25 product 없음)해 채널 비대칭 발생 → 먼저 4종 효과 검증 후 도입. norm_id 정합성 검증도 선행 필요.
- **일시**: 2026-06-04

## [MODEL] 모델 용량 프리셋
- **결정**: 표준 — hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3
- **이유**: 메타패스 keyword→product→keyword(2홉) 추천에 num_layers=2면 충분. 노드 규모(5,143/3,540/288)·엣지 33k 대비 과적합 위험 낮음. 3홉(고용량)은 472 product-ip 등 희소 엣지에서 oversmoothing 위험.
- **일시**: 2026-06-04

## [LABEL/LOSS] 클래스 불균형 처리
- **결정**: weighted BCE — `pos_weight ≈ 3.24`
- **내용**: `BCEWithLogitsLoss(pos_weight=3.24)` (실패 3,929 / 성공 1,214, 양성 23.6%). 평가지표 AUC-ROC + PR-AUC 병행.
- **이유**: 단순·안정적이며 임계값 조정으로 Precision/Recall 트레이드오프 제어 용이. 1:3.2 불균형은 focal까지 갈 강도는 아님.
- **일시**: 2026-06-04

## [SPLIT] train/val/test 분할
- **결정**: 계층화 랜덤 70/15/15 (`편의점명 × 성공여부` stratify, seed=42), transductive product 노드 마스크
- **이유**: CU/GS25(인스타 라벨)와 세븐 POS 라벨이 섞여도 분포 보존. 첫 검증에 적합. 시간 split 은 CU/GS25 날짜 기준 혼재로 후속 검토.
- **일시**: 2026-06-04

## [OPTIM] 최적화 (이중 최적화 분리)
- **결정**: 표준 Adam — W: lr=0.005/wd=5e-4, α(DiffMG): lr=0.005/wd=1e-3, epochs=200, early stopping patience=30 (val PR-AUC)
- **내용**: `src/train/.claude-rules.md` 준수 — W 는 train_step(train 데이터), α 는 val_step(val 데이터)로 분리 업데이트. 두 optimizer/backward 격리.
- **일시**: 2026-06-04

## [EVAL] 평가지표
- **결정**: PR-AUC 주지표 + AUC-ROC 병기, F1 은 best-threshold 보고
- **이유**: 양성 23.6% 불균형에 PR-AUC 가 민감. early stopping 기준 = val PR-AUC.
- **일시**: 2026-06-04

## [RECOMMEND] 순회 추천 점수 함수
- **결정**: 하이브리드 α×성공 — 메타패스 `keyword→product→keyword`, 점수 = 경로 엣지 어텐션(α) × 경유 product 예측 성공확률
- **이유**: "조합이 좋은(=성공) 키워드" 의도와 직결. 학습된 엣지 중요도 + 성공 신호 동시 반영.
- **일시**: 2026-06-04

---

## 미결 게이트 (후속)
- `[COLD_START]` 가상 신상품 노드 초기화·삽입 인터페이스 (현재 content aggregation 함수는 존재, 그래프 주입 API 미구현)
- `[GRAPH]` complement 엣지 ablation 도입 시점
