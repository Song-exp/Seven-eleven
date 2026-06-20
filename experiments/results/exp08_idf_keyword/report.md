# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7957 | 0.9361 | 0.7876 | 0.7787 |
| val | 0.7166 | 0.8514 | 0.6686 | 0.8855 |
| test | 0.6557 | 0.8514 | 0.6341 | 0.7713 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__rev_co_quick__product`: 0.1652
- `product__co_offline__product`: 0.1614
- `product__rev_co_offline__product`: 0.1103
- `product__co_quick__product`: 0.0739
- `product__has_kw__keyword`: 0.0510
- `product__has_ip__ip`: 0.0490
- `ip__rev_has_ip__product`: 0.0489
- `keyword__rev_trend_to__keyword`: 0.0486
- `ip__has_ip__ip`: 0.0486
- `keyword__trend_to__keyword`: 0.0486
- `ip__has_kw__keyword`: 0.0486
- `ip__rev_has_ip__ip`: 0.0486
- `keyword__rev_has_kw__ip`: 0.0486
- `keyword__rev_has_kw__product`: 0.0486

**layer 1**
- `product__co_offline__product`: 0.1488
- `product__rev_co_quick__product`: 0.1226
- `product__rev_co_offline__product`: 0.1117
- `product__co_quick__product`: 0.1081
- `ip__rev_has_ip__product`: 0.0603
- `product__has_kw__keyword`: 0.0499
- `ip__has_kw__keyword`: 0.0499
- `keyword__trend_to__keyword`: 0.0499
- `product__has_ip__ip`: 0.0499
- `ip__has_ip__ip`: 0.0499
- `keyword__rev_has_kw__ip`: 0.0499
- `keyword__rev_trend_to__keyword`: 0.0499
- `ip__rev_has_ip__ip`: 0.0499
- `keyword__rev_has_kw__product`: 0.0490

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04098), 향라(0.04007), 탄탄(0.03974), 샹궈(0.01399), 피(0.01358), 짜파게티(0.01346), 적음(0.01330), 곤약(0.00769), 자극적(0.00692), 누들(0.00618)
- **로제** → 늘어남(0.01762), 마카로니(0.01173), 고단백(0.00565), 당면(0.00562), 누들(0.00304), 파마산(0.00302), 하트(0.00277), 떡볶이(0.00252), 토핑(0.00225), 납작(0.00209)
- **흑임자** → 공룡(0.02265), 알(0.00976), 파운드케이크(0.00370), 마카롱(0.00313), 묵직함(0.00221), 컵(0.00100), 바나나(0.00095), 케이크(0.00088), 바닐라(0.00070), 아몬드(0.00057)
- **단백질** → 헬스(0.01081), 엽떡(0.01067), 동물(0.01060), 베노프(0.00745), 소이조이(0.00540), 테이크핏(0.00529), 청키(0.00361), 피쉬(0.00360), 비건(0.00305), 닥터유(0.00287)
- **위스키** → 블랙서클(0.00630), 산토리(0.00596), 예술(0.00583), 맥캘란(0.00461), 캐리비안(0.00273), 건조(0.00172), 쉐리(0.00162), 캐스크(0.00137), 스카치(0.00109), 한국(0.00097)
- **딸기** → 분홍(0.00432), 고칸(0.00406), 몽쉘(0.00405), 톡핑(0.00374), 쌍둥이(0.00219), 트위스트(0.00214), 아모스(0.00211), 후루츄(0.00210), 스키틀즈(0.00208), 저지방(0.00208)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
