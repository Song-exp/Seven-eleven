# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7958 | 0.9393 | 0.7593 | 0.5324 |
| val | 0.6409 | 0.8193 | 0.5971 | 0.7051 |
| test | 0.6524 | 0.8334 | 0.6103 | 0.6553 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__rev_co_quick__product`: 0.2923
- `product__co_quick__product`: 0.2177
- `ip__has_kw__keyword`: 0.0615
- `keyword__trend_to__keyword`: 0.0614
- `keyword__rev_has_kw__ip`: 0.0614
- `keyword__rev_trend_to__keyword`: 0.0614
- `product__has_kw__keyword`: 0.0613
- `product__has_ip__ip`: 0.0612
- `keyword__rev_has_kw__product`: 0.0610
- `ip__rev_has_ip__product`: 0.0608

**layer 1**
- `product__co_quick__product`: 0.1122
- `product__rev_co_quick__product`: 0.1118
- `ip__rev_has_ip__product`: 0.1105
- `product__has_kw__keyword`: 0.0971
- `ip__has_kw__keyword`: 0.0971
- `keyword__trend_to__keyword`: 0.0971
- `product__has_ip__ip`: 0.0971
- `keyword__rev_has_kw__ip`: 0.0971
- `keyword__rev_trend_to__keyword`: 0.0971
- `keyword__rev_has_kw__product`: 0.0826

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04113), 탄탄(0.01927), 중독성(0.01712), 박은영(0.01677), 짜파게티(0.01357), 향라(0.01259), 마라탕(0.00772), 적음(0.00642), 샹궈(0.00419), 중식(0.00398)
- **로제** → 중독성(0.01645), 늘어남(0.01449), 마카로니(0.00966), 고단백(0.00314), 하트(0.00251), 누들(0.00241), 파마산(0.00176), 히밥(0.00175), 볶이(0.00158), 떡볶이(0.00151)
- **흑임자** → 공룡(0.01900), 알(0.00809), 마카롱(0.00220), 묵직함(0.00184), 시루떡(0.00128), 파운드케이크(0.00101), 컵(0.00084), KBO(0.00077), 롱롱이(0.00076), 비비빅(0.00067)
- **단백질** → 베노프(0.00951), 헬스(0.00897), 이지프로틴(0.00718), 아르기닌(0.00718), 동물(0.00686), 밸런스밀(0.00566), 엽떡(0.00541), 테이크핏(0.00413), 닥터유(0.00354), 플러스(0.00338)
- **위스키** → 블랙서클(0.00644), 예술(0.00462), 산토리(0.00424), 티처스위스키(0.00424), 컬렉션(0.00338), 메이커스마크(0.00331), 맥캘란(0.00308), 티처스하이랜드(0.00270), 스트레이트(0.00244), 캐리비안(0.00216)
- **딸기** → 분홍(0.00401), 돌직구(0.00368), 고칸(0.00365), 쿠냥이(0.00351), 픽업(0.00341), 포장(0.00341), 몽쉘(0.00339), 4D(0.00332), 헬로키티(0.00326), 통크(0.00303)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,051행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
