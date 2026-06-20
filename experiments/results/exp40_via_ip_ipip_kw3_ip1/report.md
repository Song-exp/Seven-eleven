# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=64, num_layers=1, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8003 | 0.9413 | 0.7713 | 0.6404 |
| val | 0.7366 | 0.8633 | 0.6609 | 0.8060 |
| test | 0.6722 | 0.8623 | 0.6482 | 0.8111 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.2170
- `product__rev_sim_kw__product`: 0.1678
- `product__sim_kw__product`: 0.0990
- `product__rev_co_offline__product`: 0.0746
- `product__rev_sim_ip__product`: 0.0655
- `product__rev_co_quick__product`: 0.0628
- `product__sim_ip__product`: 0.0609
- `product__co_quick__product`: 0.0326
- `ip__rev_has_ip__product`: 0.0162
- `keyword__rev_has_kw__product`: 0.0160
- `keyword__rev_has_kw_via_ip__product`: 0.0158
- `product__has_kw__keyword`: 0.0156
- `ip__has_kw__keyword`: 0.0156
- `keyword__trend_to__keyword`: 0.0156
- `product__has_ip__ip`: 0.0156
- `ip__has_ip__ip`: 0.0156
- `product__has_kw_via_ip__keyword`: 0.0156
- `product__has_kw_ipip__keyword`: 0.0156
- `keyword__rev_has_kw__ip`: 0.0156
- `keyword__rev_trend_to__keyword`: 0.0156
- `ip__rev_has_ip__ip`: 0.0156
- `keyword__rev_has_kw_ipip__product`: 0.0156

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04113), 향라(0.04077), 탄탄(0.03992), 샹궈(0.02153), 중독성(0.01455), 피(0.01368), 적음(0.01331), 짜파게티(0.01292), 곤약(0.01048), 자극적(0.00984)
- **로제** → 늘어남(0.01787), 중독성(0.01455), 마카로니(0.01192), 고단백(0.00566), 당면(0.00522), 파마산(0.00338), 떡볶이(0.00331), 누들(0.00305), 분식(0.00289), 하트(0.00275)
- **흑임자** → 공룡(0.02353), 알(0.01008), 마카롱(0.00339), 파운드케이크(0.00307), 묵직함(0.00228), 작은별(0.00164), 컵(0.00104), 바나나(0.00103), 할매니얼(0.00086), 케이크(0.00084)
- **단백질** → 헬스(0.01085), 엽떡(0.01082), 동물(0.01069), 베노프(0.00969), 소이조이(0.00759), 밸런스밀(0.00688), 이지프로틴(0.00669), 아르기닌(0.00669), 테이크핏(0.00565), 닥터유(0.00398)
- **위스키** → 블랙서클(0.00628), 예술(0.00590), 산토리(0.00503), 맥캘란(0.00454), 메이커스마크(0.00343), 트레이스(0.00334), 아이리쉬(0.00330), 클레이모어(0.00318), 블론드(0.00316), 캐리비안(0.00296)
- **딸기** → 분홍(0.00449), 몽쉘(0.00423), 고칸(0.00399), 픽업(0.00330), 포장(0.00330), 톡핑(0.00320), 통크(0.00308), 쌍둥이(0.00227), 스키틀즈(0.00216), 트위스트(0.00214)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
