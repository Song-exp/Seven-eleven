# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=3, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7993 | 0.9321 | 0.7588 | 0.5751 |
| val | 0.7292 | 0.8523 | 0.6628 | 0.8224 |
| test | 0.6487 | 0.8463 | 0.6276 | 0.6542 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.1890
- `product__rev_co_quick__product`: 0.1780
- `product__rev_co_offline__product`: 0.0944
- `product__co_quick__product`: 0.0785
- `product__has_ip__ip`: 0.0467
- `ip__rev_has_ip__product`: 0.0461
- `keyword__trend_to__keyword`: 0.0460
- `ip__has_kw__keyword`: 0.0460
- `ip__has_ip__ip`: 0.0460
- `keyword__rev_trend_to__keyword`: 0.0460
- `ip__rev_has_ip__ip`: 0.0460
- `keyword__rev_has_kw__ip`: 0.0460
- `product__has_kw__keyword`: 0.0458
- `keyword__rev_has_kw__product`: 0.0457

**layer 1**
- `product__co_offline__product`: 0.1825
- `product__rev_co_quick__product`: 0.1231
- `product__co_quick__product`: 0.1043
- `product__rev_co_offline__product`: 0.0947
- `keyword__rev_has_kw__product`: 0.0627
- `ip__rev_has_ip__product`: 0.0588
- `ip__has_kw__keyword`: 0.0467
- `product__has_ip__ip`: 0.0467
- `keyword__rev_trend_to__keyword`: 0.0467
- `ip__has_ip__ip`: 0.0467
- `ip__rev_has_ip__ip`: 0.0467
- `keyword__trend_to__keyword`: 0.0467
- `product__has_kw__keyword`: 0.0467
- `keyword__rev_has_kw__ip`: 0.0467

**layer 2**
- `product__co_offline__product`: 0.1771
- `product__rev_co_quick__product`: 0.1040
- `product__co_quick__product`: 0.0928
- `product__rev_co_offline__product`: 0.0918
- `keyword__rev_has_kw__product`: 0.0573
- `product__has_kw__keyword`: 0.0531
- `ip__has_kw__keyword`: 0.0531
- `keyword__trend_to__keyword`: 0.0531
- `product__has_ip__ip`: 0.0531
- `ip__has_ip__ip`: 0.0531
- `keyword__rev_has_kw__ip`: 0.0531
- `keyword__rev_trend_to__keyword`: 0.0531
- `ip__rev_has_ip__ip`: 0.0531
- `ip__rev_has_ip__product`: 0.0523

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04133), 향라(0.03999), 탄탄(0.03873), 샹궈(0.01478), 피(0.01367), 짜파게티(0.01363), 적음(0.01302), 자극적(0.00783), 곤약(0.00762), 누들(0.00608)
- **로제** → 늘어남(0.01461), 마카로니(0.00969), 중독성(0.00592), 고단백(0.00547), 당면(0.00404), 누들(0.00290), 하트(0.00280), 떡볶이(0.00249), 토핑(0.00216), 밀(0.00191)
- **흑임자** → 공룡(0.02298), 알(0.00991), 마카롱(0.00310), 파운드케이크(0.00238), 묵직함(0.00223), 컵(0.00102), 바나나(0.00093), 작은별(0.00093), 시루떡(0.00083), 케이크(0.00077)
- **단백질** → 헬스(0.01097), 엽떡(0.01054), 동물(0.01027), 베노프(0.00750), 테이크핏(0.00527), 소이조이(0.00488), 청키(0.00365), 피쉬(0.00363), 이지프로틴(0.00341), 아르기닌(0.00341)
- **위스키** → 블랙서클(0.00645), 예술(0.00520), 산토리(0.00448), 맥캘란(0.00274), 캐리비안(0.00191), 쉐리(0.00128), 스카치(0.00119), 캐스크(0.00103), 한국(0.00086), 버번(0.00071)
- **딸기** → 분홍(0.00435), 고칸(0.00416), 몽쉘(0.00376), 톡핑(0.00310), 쌍둥이(0.00219), 트위스트(0.00217), 아모스(0.00211), 저지방(0.00210), 후루츄(0.00208), 스키틀즈(0.00206)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
