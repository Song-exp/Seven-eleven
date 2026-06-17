# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8581 | 0.9661 | 0.8503 | 0.4406 |
| val | 0.6317 | 0.7979 | 0.5869 | 0.2189 |
| test | 0.6229 | 0.7980 | 0.5823 | 0.3447 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__rev_co_quick__product`: 0.1013
- `keyword__rev_has_kw__ip`: 0.1012
- `keyword__rev_trend_to__keyword`: 0.1010
- `ip__rev_has_ip__product`: 0.1010
- `keyword__rev_has_kw__product`: 0.1009
- `ip__has_kw__keyword`: 0.1008
- `keyword__trend_to__keyword`: 0.1008
- `product__co_quick__product`: 0.1005
- `product__has_ip__ip`: 0.1000
- `product__has_kw__keyword`: 0.0925

**layer 1**
- `product__rev_co_quick__product`: 0.1535
- `product__co_quick__product`: 0.1452
- `product__has_kw__keyword`: 0.1031
- `ip__has_kw__keyword`: 0.1031
- `keyword__trend_to__keyword`: 0.1031
- `product__has_ip__ip`: 0.1031
- `keyword__rev_has_kw__ip`: 0.1031
- `keyword__rev_trend_to__keyword`: 0.1031
- `ip__rev_has_ip__product`: 0.0548
- `keyword__rev_has_kw__product`: 0.0276

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04187), 중독성(0.02086), 짜파게티(0.01380), 마라탕(0.00982), 박은영(0.00832), 정통(0.00393), 짜장(0.00230), 중식(0.00224), 로제(0.00174), 국물(0.00096)
- **로제** → 중독성(0.01998), 늘어남(0.01902), 마카로니(0.01267), 떡볶퀸(0.00576), 고단백(0.00392), 누들(0.00301), 떡볶이(0.00268), 하트(0.00267), 저녁(0.00211), 마라(0.00174)
- **흑임자** → 공룡(0.01903), 알(0.00816), 묵직함(0.00184), 컵(0.00084), KBO(0.00077), 케이크(0.00044), 촉촉(0.00021), 고소(0.00010), 디저트(0.00008), 달콤(0.00004)
- **단백질** → 베노프(0.00992), 동물(0.00971), 엽떡(0.00951), 이지프로틴(0.00775), 아르기닌(0.00775), 테이크핏(0.00489), 헬스(0.00429), 청키(0.00332), 안심(0.00250), 쁘띠(0.00249)
- **위스키** → 블랙서클(0.00641), 예술(0.00568), 스프레드(0.00314), 캐리비안(0.00310), 스트레이트(0.00282), 맥캘란(0.00225), 콜드브루(0.00209), 캐스크(0.00153), 쉐리(0.00135), 한국(0.00095)
- **딸기** → 분홍(0.00431), 고칸(0.00421), 4D(0.00419), 쿠냥이(0.00418), 픽업(0.00413), 포장(0.00413), 몽쉘(0.00412), 톡핑(0.00409), 헬로키티(0.00339), 데이(0.00274)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,051행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
