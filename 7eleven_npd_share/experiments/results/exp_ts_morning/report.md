# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7302 | 0.9028 | 0.6896 | 0.5921 |
| val | 0.6435 | 0.8227 | 0.5768 | 0.6819 |
| test | 0.6295 | 0.8258 | 0.6026 | 0.4659 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__rev_co_quick__product`: 0.3205
- `product__co_quick__product`: 0.2065
- `product__has_kw__keyword`: 0.0629
- `keyword__rev_has_kw__product`: 0.0604
- `product__has_ip__ip`: 0.0600
- `ip__rev_has_ip__product`: 0.0581
- `ip__has_kw__keyword`: 0.0579
- `keyword__rev_has_kw__ip`: 0.0579
- `keyword__rev_trend_to__keyword`: 0.0579
- `keyword__trend_to__keyword`: 0.0579

**layer 1**
- `ip__rev_has_ip__product`: 0.1643
- `product__rev_co_quick__product`: 0.0994
- `keyword__rev_has_kw__product`: 0.0984
- `product__co_quick__product`: 0.0958
- `product__has_kw__keyword`: 0.0903
- `ip__has_kw__keyword`: 0.0903
- `keyword__trend_to__keyword`: 0.0903
- `product__has_ip__ip`: 0.0903
- `keyword__rev_has_kw__ip`: 0.0903
- `keyword__rev_trend_to__keyword`: 0.0903

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04153), 향라(0.02912), 탄탄(0.02729), 박은영(0.02002), 중독성(0.01702), 짜파게티(0.01389), 적음(0.00908), 마라탕(0.00748), 자극적(0.00733), 중식(0.00637)
- **로제** → 중독성(0.01637), 늘어남(0.01313), 떡볶퀸(0.01008), 마카로니(0.00877), 당면(0.00378), 히밥(0.00325), 파마산(0.00309), 볶이(0.00292), 고단백(0.00289), 하트(0.00261)
- **흑임자** → 공룡(0.01887), 롱롱이(0.00828), 알(0.00811), 파운드케이크(0.00276), 마카롱(0.00212), 묵직함(0.00182), 시루떡(0.00160), 비비빅(0.00096), 컵(0.00084), KBO(0.00076)
- **단백질** → 헬스(0.00971), 베노프(0.00925), 동물(0.00697), 플러스(0.00690), 밸런스밀(0.00681), 이지프로틴(0.00641), 아르기닌(0.00641), 엽떡(0.00575), 청키(0.00331), 프로틴(0.00324)
- **위스키** → 블랙서클(0.00638), 산토리(0.00299), 티처스위스키(0.00299), 예술(0.00268), 컬렉션(0.00160), 블론드(0.00147), 맥캘란(0.00080), 스카치(0.00073), 글렌피딕(0.00069), 스트레이트(0.00059)
- **딸기** → 분홍(0.00369), 고칸(0.00355), 돌직구(0.00351), 4D(0.00333), 쿠냥이(0.00330), 픽업(0.00320), 포장(0.00320), 헬로키티(0.00316), 통크(0.00315), 치토스(0.00313)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,051행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
