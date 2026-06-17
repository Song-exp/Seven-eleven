# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.005), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8054 | 0.9486 | 0.8109 | 0.4502 |
| val | 0.5623 | 0.7874 | 0.5798 | 0.0614 |
| test | 0.5821 | 0.7960 | 0.5714 | 0.6660 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__has_kw__keyword`: 0.1330
- `ip__has_kw__keyword`: 0.1241
- `keyword__trend_to__keyword`: 0.1240
- `keyword__rev_has_kw__ip`: 0.1239
- `keyword__rev_trend_to__keyword`: 0.1239
- `ip__rev_has_ip__product`: 0.1239
- `keyword__rev_has_kw__product`: 0.1238
- `product__has_ip__ip`: 0.1234

**layer 1**
- `product__has_kw__keyword`: 0.1272
- `ip__has_kw__keyword`: 0.1272
- `keyword__trend_to__keyword`: 0.1272
- `product__has_ip__ip`: 0.1272
- `keyword__rev_has_kw__ip`: 0.1272
- `keyword__rev_trend_to__keyword`: 0.1272
- `ip__rev_has_ip__product`: 0.1238
- `keyword__rev_has_kw__product`: 0.1129

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.03709), 박은영(0.02013), 중독성(0.01921), 짜파게티(0.01239), 마라탕(0.00908), 마파(0.00499), 정통(0.00358), 중식(0.00261), 두부(0.00242), 짜장(0.00188)
- **로제** → 중독성(0.01983), 늘어남(0.01737), 마카로니(0.01192), 고단백(0.00398), 누들(0.00301), 하트(0.00221), 떡볶이(0.00216), 저녁(0.00187), 마라(0.00159), 스파클링(0.00145)
- **흑임자** → 공룡(0.01761), 알(0.00730), 마카롱(0.00236), 묵직함(0.00172), 컵(0.00076), 파운드케이크(0.00074), KBO(0.00071), 바나나(0.00066), 케이크(0.00047), 바닐라(0.00040)
- **단백질** → 베노프(0.00949), 동물(0.00903), 이지프로틴(0.00902), 아르기닌(0.00902), 엽떡(0.00874), 밸런스밀(0.00696), 테이크핏(0.00430), 닥터유(0.00409), 프로틴(0.00328), 청키(0.00325)
- **위스키** → 블랙서클(0.00586), 예술(0.00378), 캐리비안(0.00277), 스프레드(0.00271), 맥캘란(0.00248), 스트레이트(0.00182), 쉐리(0.00178), 콜드브루(0.00178), 캐스크(0.00133), 한국(0.00062)
- **딸기** → 분홍(0.00424), 돌직구(0.00414), 고칸(0.00411), 쿠냥이(0.00395), 4D(0.00392), 몽쉘(0.00391), 픽업(0.00383), 포장(0.00383), 톡핑(0.00350), 헬로키티(0.00331)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,051행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
