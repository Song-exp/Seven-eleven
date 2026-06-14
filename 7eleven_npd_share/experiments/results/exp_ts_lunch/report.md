# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8661 | 0.9664 | 0.8447 | 0.6802 |
| val | 0.6260 | 0.7997 | 0.6032 | 0.6763 |
| test | 0.6265 | 0.8056 | 0.5953 | 0.7071 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__has_kw__keyword`: 0.1083
- `product__has_ip__ip`: 0.1050
- `product__rev_co_quick__product`: 0.0989
- `keyword__rev_trend_to__keyword`: 0.0988
- `product__co_quick__product`: 0.0986
- `keyword__rev_has_kw__ip`: 0.0985
- `keyword__rev_has_kw__product`: 0.0983
- `ip__rev_has_ip__product`: 0.0983
- `ip__has_kw__keyword`: 0.0979
- `keyword__trend_to__keyword`: 0.0974

**layer 1**
- `product__rev_co_quick__product`: 0.1368
- `product__co_quick__product`: 0.1347
- `product__has_kw__keyword`: 0.1003
- `ip__has_kw__keyword`: 0.1003
- `keyword__trend_to__keyword`: 0.1003
- `product__has_ip__ip`: 0.1003
- `keyword__rev_has_kw__ip`: 0.1003
- `keyword__rev_trend_to__keyword`: 0.1003
- `ip__rev_has_ip__product`: 0.0807
- `keyword__rev_has_kw__product`: 0.0460

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04168), 중독성(0.02055), 짜파게티(0.01367), 박은영(0.01180), 마라탕(0.00928), 정통(0.00371), 중식(0.00339), 짜장(0.00228), 로제(0.00171), 국물(0.00104)
- **로제** → 중독성(0.01973), 늘어남(0.01544), 마카로니(0.01030), 고단백(0.00388), 누들(0.00298), 하트(0.00264), 떡볶이(0.00247), 저녁(0.00210), 마라(0.00171), 분식(0.00169)
- **흑임자** → 공룡(0.01888), 알(0.00810), 마카롱(0.00192), 묵직함(0.00183), 컵(0.00083), KBO(0.00077), 바나나(0.00052), 케이크(0.00043), 바닐라(0.00032), 콜라(0.00025)
- **단백질** → 베노프(0.00984), 동물(0.00958), 엽떡(0.00905), 이지프로틴(0.00576), 아르기닌(0.00576), 헬스(0.00548), 테이크핏(0.00462), 닥터유(0.00403), 청키(0.00332), 피쉬(0.00309)
- **위스키** → 블랙서클(0.00643), 예술(0.00574), 스프레드(0.00311), 캐리비안(0.00303), 메이커스마크(0.00287), 스트레이트(0.00265), 맥캘란(0.00228), 콜드브루(0.00207), 쉐리(0.00194), 캐스크(0.00162)
- **딸기** → 분홍(0.00426), 고칸(0.00415), 쿠냥이(0.00412), 픽업(0.00412), 포장(0.00412), 4D(0.00409), 몽쉘(0.00398), 톡핑(0.00369), 헬로키티(0.00338), 데이(0.00272)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,051행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
