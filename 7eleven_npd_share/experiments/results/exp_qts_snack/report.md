# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8482 | 0.9605 | 0.8286 | 0.6916 |
| val | 0.6469 | 0.8113 | 0.5968 | 0.7703 |
| test | 0.6500 | 0.8216 | 0.6192 | 0.8381 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_quick__product`: 0.1014
- `product__rev_co_quick__product`: 0.1014
- `keyword__rev_trend_to__keyword`: 0.1007
- `keyword__rev_has_kw__ip`: 0.1006
- `keyword__rev_has_kw__product`: 0.1005
- `ip__has_kw__keyword`: 0.1001
- `ip__rev_has_ip__product`: 0.0999
- `keyword__trend_to__keyword`: 0.0994
- `product__has_kw__keyword`: 0.0992
- `product__has_ip__ip`: 0.0969

**layer 1**
- `product__rev_co_quick__product`: 0.1529
- `product__co_quick__product`: 0.1494
- `product__has_kw__keyword`: 0.1010
- `ip__has_kw__keyword`: 0.1010
- `keyword__trend_to__keyword`: 0.1010
- `product__has_ip__ip`: 0.1010
- `keyword__rev_has_kw__ip`: 0.1010
- `keyword__rev_trend_to__keyword`: 0.1010
- `ip__rev_has_ip__product`: 0.0629
- `keyword__rev_has_kw__product`: 0.0289

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04212), 중독성(0.02034), 박은영(0.01954), 짜파게티(0.01391), 마라탕(0.00964), 중식(0.00387), 정통(0.00384), 마파(0.00303), 짜장(0.00231), 로제(0.00169)
- **로제** → 중독성(0.01947), 늘어남(0.01539), 마카로니(0.01026), 고단백(0.00373), 누들(0.00287), 하트(0.00266), 떡볶이(0.00248), 저녁(0.00204), 파마산(0.00193), 분식(0.00182)
- **흑임자** → 공룡(0.01922), 알(0.00823), 빵또아(0.00370), 베이커리(0.00324), 파운드케이크(0.00315), 마카롱(0.00274), 묵직함(0.00186), 인절미(0.00096), 컵(0.00085), KBO(0.00078)
- **단백질** → 베노프(0.00983), 헬스(0.00962), 동물(0.00937), 엽떡(0.00866), 이지프로틴(0.00797), 아르기닌(0.00797), 널담(0.00707), 밸런스밀(0.00580), 테이크핏(0.00450), 닥터유(0.00377)
- **위스키** → 블랙서클(0.00646), 예술(0.00596), 메이커스마크(0.00496), 캐리비안(0.00308), 맥캘란(0.00307), 스프레드(0.00305), 스트레이트(0.00292), 콜드브루(0.00203), 쉐리(0.00195), 캐스크(0.00165)
- **딸기** → 분홍(0.00428), 쿠냥이(0.00414), 돌직구(0.00412), 픽업(0.00406), 포장(0.00406), 고칸(0.00405), 몽쉘(0.00404), 톡핑(0.00384), 4D(0.00377), 헬로키티(0.00338)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,051행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
