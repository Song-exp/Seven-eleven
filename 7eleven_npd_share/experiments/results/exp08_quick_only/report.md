# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8532 | 0.9622 | 0.8371 | 0.5359 |
| val | 0.6332 | 0.8078 | 0.5888 | 0.6313 |
| test | 0.6324 | 0.8130 | 0.5986 | 0.4610 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_quick__product`: 0.1025
- `keyword__rev_trend_to__keyword`: 0.1020
- `ip__has_kw__keyword`: 0.1019
- `product__rev_co_quick__product`: 0.1019
- `keyword__rev_has_kw__ip`: 0.1018
- `keyword__trend_to__keyword`: 0.1017
- `ip__rev_has_ip__product`: 0.1009
- `keyword__rev_has_kw__product`: 0.1008
- `product__has_ip__ip`: 0.0991
- `product__has_kw__keyword`: 0.0874

**layer 1**
- `product__rev_co_quick__product`: 0.1158
- `product__co_quick__product`: 0.1153
- `product__has_kw__keyword`: 0.1085
- `ip__has_kw__keyword`: 0.1085
- `keyword__trend_to__keyword`: 0.1085
- `product__has_ip__ip`: 0.1085
- `keyword__rev_has_kw__ip`: 0.1085
- `keyword__rev_trend_to__keyword`: 0.1085
- `ip__rev_has_ip__product`: 0.0695
- `keyword__rev_has_kw__product`: 0.0486

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04142), 중독성(0.02049), 짜파게티(0.01375), 박은영(0.01221), 마라탕(0.00852), 마파(0.00355), 정통(0.00341), 짜장(0.00229), 로제(0.00171), 두부(0.00168)
- **로제** → 중독성(0.01965), 늘어남(0.01219), 마카로니(0.00812), 고단백(0.00386), 파마산(0.00379), 누들(0.00297), 당면(0.00277), 하트(0.00267), 떡볶이(0.00256), 납작(0.00233)
- **흑임자** → 공룡(0.01890), 알(0.00810), 마카롱(0.00232), 묵직함(0.00183), 컵(0.00083), KBO(0.00077), 바나나(0.00064), 케이크(0.00044), 바닐라(0.00038), 콜라(0.00031)
- **단백질** → 베노프(0.00980), 동물(0.00951), 이지프로틴(0.00879), 아르기닌(0.00879), 헬스(0.00860), 엽떡(0.00834), 밸런스밀(0.00512), 테이크핏(0.00413), 닥터유(0.00366), 청키(0.00329)
- **위스키** → 블랙서클(0.00638), 예술(0.00604), 캐리비안(0.00306), 스프레드(0.00298), 스트레이트(0.00286), 맥캘란(0.00226), 콜드브루(0.00198), 쉐리(0.00192), 캐스크(0.00167), 한국(0.00101)
- **딸기** → 분홍(0.00427), 고칸(0.00415), 쿠냥이(0.00409), 돌직구(0.00404), 4D(0.00404), 몽쉘(0.00401), 픽업(0.00394), 포장(0.00394), 톡핑(0.00336), 헬로키티(0.00335)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,051행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
