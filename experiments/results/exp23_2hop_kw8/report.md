# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=1, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8051 | 0.9436 | 0.7738 | 0.7820 |
| val | 0.7461 | 0.8690 | 0.6631 | 0.7932 |
| test | 0.6755 | 0.8605 | 0.6525 | 0.8072 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.3937
- `product__sim_kw__product`: 0.1010
- `product__rev_sim_kw__product`: 0.0740
- `product__rev_co_quick__product`: 0.0661
- `keyword__rev_has_kw__product`: 0.0582
- `product__rev_co_offline__product`: 0.0304
- `product__co_quick__product`: 0.0286
- `product__sim_ip__product`: 0.0274
- `product__rev_sim_ip__product`: 0.0272
- `ip__rev_has_ip__product`: 0.0239
- `product__has_kw__keyword`: 0.0212
- `ip__has_kw__keyword`: 0.0212
- `keyword__trend_to__keyword`: 0.0212
- `product__has_ip__ip`: 0.0212
- `ip__has_ip__ip`: 0.0212
- `keyword__rev_has_kw__ip`: 0.0212
- `keyword__rev_trend_to__keyword`: 0.0212
- `ip__rev_has_ip__ip`: 0.0212

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04090), 향라(0.04088), 탄탄(0.04013), 샹궈(0.01949), 중독성(0.01682), 피(0.01350), 적음(0.01338), 짜파게티(0.01323), 자극적(0.01214), 곤약(0.01019)
- **로제** → 늘어남(0.01839), 중독성(0.01682), 마카로니(0.01226), 고단백(0.00568), 당면(0.00473), 넘버원(0.00455), 떡볶이(0.00357), 분식(0.00344), 파마산(0.00341), 반반(0.00334)
- **흑임자** → 공룡(0.02341), 알(0.01003), 파운드케이크(0.00338), 마카롱(0.00328), 묵직함(0.00227), 컵(0.00103), 바나나(0.00099), 케이크(0.00086), 아몬드(0.00085), 바닐라(0.00074)
- **단백질** → 엽떡(0.01093), 동물(0.01072), 헬스(0.01070), 베노프(0.00966), 밸런스밀(0.00804), 소이조이(0.00718), 이지프로틴(0.00705), 아르기닌(0.00705), 테이크핏(0.00513), 닥터유(0.00501)
- **위스키** → 블랙서클(0.00629), 예술(0.00593), 산토리(0.00497), 맥캘란(0.00455), 메이커스마크(0.00340), 트레이스(0.00316), 캐리비안(0.00304), 클레이모어(0.00292), 아이리쉬(0.00279), 블론드(0.00264)
- **딸기** → 분홍(0.00446), 몽쉘(0.00425), 고칸(0.00406), 톡핑(0.00382), 통크(0.00364), 쌍둥이(0.00226), 픽업(0.00225), 포장(0.00225), 스키틀즈(0.00214), 트위스트(0.00211)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
