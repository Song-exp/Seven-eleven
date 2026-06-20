# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8064 | 0.9435 | 0.7822 | 0.8422 |
| val | 0.7381 | 0.8633 | 0.6720 | 0.8469 |
| test | 0.6684 | 0.8560 | 0.6545 | 0.8665 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.3646
- `product__rev_co_quick__product`: 0.1195
- `keyword__rev_has_kw__product`: 0.1192
- `product__rev_co_offline__product`: 0.0538
- `product__co_quick__product`: 0.0364
- `ip__rev_has_ip__product`: 0.0364
- `product__has_ip__ip`: 0.0338
- `ip__has_ip__ip`: 0.0338
- `keyword__rev_has_kw__ip`: 0.0338
- `ip__rev_has_ip__ip`: 0.0338
- `keyword__trend_to__keyword`: 0.0338
- `keyword__rev_trend_to__keyword`: 0.0338
- `ip__has_kw__keyword`: 0.0338
- `product__has_kw__keyword`: 0.0338

**layer 1**
- `product__has_kw__keyword`: 0.0718
- `ip__has_kw__keyword`: 0.0718
- `keyword__trend_to__keyword`: 0.0718
- `product__has_ip__ip`: 0.0718
- `ip__has_ip__ip`: 0.0718
- `keyword__rev_has_kw__ip`: 0.0718
- `keyword__rev_trend_to__keyword`: 0.0718
- `ip__rev_has_ip__ip`: 0.0718
- `product__rev_co_quick__product`: 0.0714
- `product__co_quick__product`: 0.0714
- `ip__rev_has_ip__product`: 0.0713
- `product__co_offline__product`: 0.0708
- `keyword__rev_has_kw__product`: 0.0707
- `product__rev_co_offline__product`: 0.0700

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04179), 향라(0.04103), 탄탄(0.04050), 샹궈(0.01979), 중독성(0.01545), 피(0.01374), 짜파게티(0.01371), 적음(0.01350), 곤약(0.01073), 자극적(0.01009)
- **로제** → 늘어남(0.01885), 중독성(0.01544), 마카로니(0.01256), 고단백(0.00573), 당면(0.00547), 넘버원(0.00447), 떡볶이(0.00351), 파마산(0.00344), 분식(0.00313), 누들(0.00308)
- **흑임자** → 공룡(0.02318), 알(0.00992), 마카롱(0.00332), 묵직함(0.00224), 파운드케이크(0.00164), 컵(0.00102), 바나나(0.00101), 바닐라(0.00074), 케이크(0.00070), 담백(0.00044)
- **단백질** → 헬스(0.01104), 엽떡(0.01087), 동물(0.01080), 베노프(0.00969), 소이조이(0.00904), 밸런스밀(0.00761), 이지프로틴(0.00670), 아르기닌(0.00670), 테이크핏(0.00534), 닥터유(0.00494)
- **위스키** → 블랙서클(0.00643), 예술(0.00597), 산토리(0.00504), 맥캘란(0.00475), 후드티(0.00340), 캐리비안(0.00301), 메이커스마크(0.00286), 건조(0.00278), 트레이스(0.00257), 클레이모어(0.00229)
- **딸기** → 분홍(0.00443), 몽쉘(0.00425), 고칸(0.00404), 톡핑(0.00396), 통크(0.00375), 픽업(0.00283), 포장(0.00283), 쌍둥이(0.00224), 트위스트(0.00218), 스키틀즈(0.00217)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
