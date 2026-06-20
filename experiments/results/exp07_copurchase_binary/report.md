# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7665 | 0.9125 | 0.7306 | 0.4774 |
| val | 0.7301 | 0.8503 | 0.6548 | 0.8297 |
| test | 0.6354 | 0.8437 | 0.6387 | 0.6731 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.1768
- `product__rev_co_quick__product`: 0.1577
- `product__rev_co_offline__product`: 0.1040
- `product__co_quick__product`: 0.0703
- `product__has_ip__ip`: 0.0515
- `ip__rev_has_ip__product`: 0.0495
- `ip__has_kw__keyword`: 0.0490
- `keyword__rev_has_kw__ip`: 0.0490
- `ip__has_ip__ip`: 0.0490
- `ip__rev_has_ip__ip`: 0.0490
- `keyword__rev_trend_to__keyword`: 0.0490
- `keyword__trend_to__keyword`: 0.0490
- `keyword__rev_has_kw__product`: 0.0486
- `product__has_kw__keyword`: 0.0475

**layer 1**
- `product__co_offline__product`: 0.1661
- `product__co_quick__product`: 0.1102
- `product__rev_co_offline__product`: 0.1061
- `product__rev_co_quick__product`: 0.1050
- `ip__rev_has_ip__product`: 0.0677
- `keyword__rev_has_kw__product`: 0.0562
- `product__has_kw__keyword`: 0.0486
- `ip__has_kw__keyword`: 0.0486
- `keyword__trend_to__keyword`: 0.0486
- `product__has_ip__ip`: 0.0486
- `ip__has_ip__ip`: 0.0486
- `keyword__rev_has_kw__ip`: 0.0486
- `keyword__rev_trend_to__keyword`: 0.0486
- `ip__rev_has_ip__ip`: 0.0486

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04028), 향라(0.03987), 탄탄(0.03902), 샹궈(0.01538), 피(0.01345), 짜파게티(0.01328), 적음(0.01297), 자극적(0.00757), 곤약(0.00757), 누들(0.00609)
- **로제** → 늘어남(0.01219), 마카로니(0.00812), 당면(0.00725), 고단백(0.00546), 중독성(0.00513), 파마산(0.00395), 납작(0.00372), 누들(0.00294), 하트(0.00245), 떡볶이(0.00235)
- **흑임자** → 공룡(0.02295), 알(0.00989), 마카롱(0.00303), 묵직함(0.00220), 파운드케이크(0.00176), 작은별(0.00168), 시루떡(0.00135), 컵(0.00102), 바나나(0.00092), 적음(0.00081)
- **단백질** → 헬스(0.01081), 엽떡(0.01060), 동물(0.01031), 베노프(0.00746), 소이조이(0.00540), 테이크핏(0.00519), 피쉬(0.00369), 청키(0.00364), 닥터유(0.00304), 비건(0.00294)
- **위스키** → 블랙서클(0.00638), 산토리(0.00451), 예술(0.00177), 스카치(0.00081), 블론드(0.00046), 버번(0.00046), 글렌피딕(0.00044), 맥캘란(0.00041), 캐스크(0.00041), 클레이모어(0.00039)
- **딸기** → 분홍(0.00439), 고칸(0.00404), 통크(0.00319), 치토스(0.00309), 몽쉘(0.00308), 톡핑(0.00271), 쌍둥이(0.00221), 트위스트(0.00215), 아모스(0.00210), 후루츄(0.00206)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
