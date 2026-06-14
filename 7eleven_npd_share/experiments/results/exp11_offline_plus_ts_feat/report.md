# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7530 | 0.9220 | 0.7365 | 0.7350 |
| val | 0.7249 | 0.8642 | 0.6570 | 0.8654 |
| test | 0.6966 | 0.8613 | 0.6541 | 0.7600 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.2776
- `product__rev_co_offline__product`: 0.2599
- `product__has_ip__ip`: 0.0680
- `product__has_kw__keyword`: 0.0657
- `keyword__rev_has_kw__product`: 0.0581
- `keyword__trend_to__keyword`: 0.0554
- `ip__has_kw__keyword`: 0.0554
- `keyword__rev_trend_to__keyword`: 0.0536
- `keyword__rev_has_kw__ip`: 0.0533
- `ip__rev_has_ip__product`: 0.0530

**layer 1**
- `product__co_offline__product`: 0.2442
- `product__rev_co_offline__product`: 0.2304
- `ip__rev_has_ip__product`: 0.1015
- `product__has_kw__keyword`: 0.0622
- `ip__has_kw__keyword`: 0.0622
- `keyword__trend_to__keyword`: 0.0622
- `product__has_ip__ip`: 0.0622
- `keyword__rev_has_kw__ip`: 0.0622
- `keyword__rev_trend_to__keyword`: 0.0622
- `keyword__rev_has_kw__product`: 0.0505

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 향라(0.04050), 도쿠시마(0.04037), 탄탄(0.03960), 샹궈(0.02856), 박은영(0.02506), 중독성(0.01943), 피(0.01367), 적음(0.01328), 짜파게티(0.01235), 자극적(0.01200)
- **로제** → 바리스타룰스(0.01909), 원데이(0.01898), 중독성(0.01862), 떡볶퀸(0.01728), 늘어남(0.01562), 마카로니(0.01037), 당면(0.00599), 앙리(0.00437), 하트(0.00433), 파마산(0.00423)
- **흑임자** → 공룡(0.01863), 롱롱이(0.01530), 시루떡(0.01099), 알(0.00808), 비비빅(0.00591), 파운드케이크(0.00390), 마카롱(0.00248), 빵또아(0.00198), 묵직함(0.00180), 베이커리(0.00174)
- **단백질** → 엽떡(0.00962), 동물(0.00960), 베노프(0.00954), 헬스(0.00937), 이지프로틴(0.00830), 아르기닌(0.00830), 소이조이(0.00733), 밸런스밀(0.00711), 백반(0.00523), 편함(0.00512)
- **위스키** → 블랙서클(0.00628), 산토리(0.00541), 티처스위스키(0.00541), 예술(0.00491), 컬렉션(0.00427), 메이커스마크(0.00352), 블론드(0.00263), 스트레이트(0.00255), 스프레드(0.00249), 맥캘란(0.00216)
- **딸기** → 분홍(0.00421), 4D(0.00407), 돌직구(0.00398), 고칸(0.00396), 쿠냥이(0.00393), 픽업(0.00387), 포장(0.00387), 몽쉘(0.00381), 통크(0.00346), 치토스(0.00341)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,051행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
