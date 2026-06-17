# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=1, num_heads=4, dropout=0.4, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8163 | 0.9457 | 0.7707 | 0.6005 |
| val | 0.5667 | 0.8021 | 0.5803 | 0.6224 |
| test | 0.5681 | 0.8227 | 0.6047 | 0.5723 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__rev_sim_ip__product`: 0.0625
- `keyword__rev_has_kw__product`: 0.0625
- `product__sim_ip__product`: 0.0625
- `product__sim_kw__product`: 0.0625
- `product__has_kw__keyword`: 0.0625
- `ip__has_kw__keyword`: 0.0625
- `keyword__trend_to__keyword`: 0.0625
- `product__has_ip__ip`: 0.0625
- `keyword__rev_has_kw__ip`: 0.0625
- `keyword__rev_trend_to__keyword`: 0.0625
- `product__co_quick__product`: 0.0625
- `product__rev_sim_kw__product`: 0.0625
- `ip__rev_has_ip__product`: 0.0625
- `product__rev_co_quick__product`: 0.0625
- `product__rev_co_offline__product`: 0.0624
- `product__co_offline__product`: 0.0624

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.03651), 향라(0.03499), 탄탄(0.03311), 박은영(0.02187), 샹궈(0.01908), 중독성(0.01693), 짜파게티(0.01164), 적음(0.01104), 피(0.01043), 마라탕(0.00997)
- **로제** → 떡볶퀸(0.01931), 늘어남(0.01863), 중독성(0.01623), 마카로니(0.01242), 당면(0.00574), 파마산(0.00378), 고단백(0.00343), 떡볶이(0.00310), 누들(0.00264), 납작(0.00233)
- **흑임자** → 공룡(0.01820), 시루떡(0.01359), 알(0.00780), 비비빅(0.00680), 마카롱(0.00291), 묵직함(0.00176), 찰떡(0.00143), 컵(0.00080), 바나나(0.00080), KBO(0.00074)
- **단백질** → 베노프(0.00808), 엽떡(0.00798), 밸런스밀(0.00768), 동물(0.00748), 헬스(0.00745), 이지프로틴(0.00711), 아르기닌(0.00711), 플러스(0.00679), 편함(0.00636), 테이크핏(0.00478)
- **위스키** → 블랙서클(0.00639), 예술(0.00586), 산토리(0.00506), 티처스위스키(0.00506), 컬렉션(0.00382), 키싱(0.00377), 메이커스마크(0.00358), 트레이스(0.00342), 클레이모어(0.00326), 캐리비안(0.00287)
- **딸기** → 분홍(0.00443), 돌직구(0.00425), 고칸(0.00423), 쿠냥이(0.00423), 몽쉘(0.00404), 톡핑(0.00371), 4D(0.00326), 픽업(0.00322), 포장(0.00322), 헬로키티(0.00245)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
