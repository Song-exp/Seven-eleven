# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8377 | 0.9587 | 0.8296 | 0.3577 |
| val | 0.6335 | 0.8092 | 0.5771 | 0.0664 |
| test | 0.6488 | 0.8179 | 0.6136 | 0.2962 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__has_ip__ip`: 0.1046
- `keyword__rev_has_kw__product`: 0.1004
- `product__has_kw__keyword`: 0.1002
- `ip__rev_has_ip__product`: 0.0995
- `keyword__rev_has_kw__ip`: 0.0994
- `keyword__trend_to__keyword`: 0.0993
- `product__co_quick__product`: 0.0993
- `keyword__rev_trend_to__keyword`: 0.0993
- `ip__has_kw__keyword`: 0.0991
- `product__rev_co_quick__product`: 0.0991

**layer 1**
- `product__rev_co_quick__product`: 0.1526
- `product__co_quick__product`: 0.1492
- `product__has_kw__keyword`: 0.1065
- `ip__has_kw__keyword`: 0.1065
- `keyword__trend_to__keyword`: 0.1065
- `product__has_ip__ip`: 0.1065
- `keyword__rev_has_kw__ip`: 0.1065
- `keyword__rev_trend_to__keyword`: 0.1065
- `ip__rev_has_ip__product`: 0.0366
- `keyword__rev_has_kw__product`: 0.0227

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04161), 중독성(0.01988), 짜파게티(0.01379), 마라탕(0.00927), 박은영(0.00406), 정통(0.00370), 짜장(0.00230), 로제(0.00166), 중식(0.00099), 라면(0.00092)
- **로제** → 중독성(0.01910), 늘어남(0.01515), 마카로니(0.01011), 고단백(0.00361), 파마산(0.00283), 누들(0.00277), 하트(0.00264), 떡볶이(0.00226), 당면(0.00195), 납작(0.00174)
- **흑임자** → 공룡(0.01898), 알(0.00813), 마카롱(0.00268), 묵직함(0.00184), 파운드케이크(0.00160), 컵(0.00084), KBO(0.00077), 바나나(0.00074), 케이크(0.00057), 바닐라(0.00044)
- **단백질** → 헬스(0.00954), 베노프(0.00947), 동물(0.00876), 이지프로틴(0.00740), 아르기닌(0.00740), 밸런스밀(0.00501), 엽떡(0.00468), 테이크핏(0.00445), 닥터유(0.00393), 널담(0.00344)
- **위스키** → 블랙서클(0.00637), 예술(0.00533), 맥캘란(0.00343), 메이커스마크(0.00315), 캐리비안(0.00296), 스프레드(0.00254), 스트레이트(0.00237), 쉐리(0.00186), 콜드브루(0.00169), 캐스크(0.00140)
- **딸기** → 분홍(0.00423), 쿠냥이(0.00416), 돌직구(0.00406), 고칸(0.00396), 몽쉘(0.00395), 톡핑(0.00349), 4D(0.00338), 헬로키티(0.00308), 쌍둥이(0.00217), 배트(0.00217)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,051행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
