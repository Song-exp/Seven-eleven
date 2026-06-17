# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7399 | 0.9124 | 0.7028 | 0.5719 |
| val | 0.6475 | 0.8273 | 0.5874 | 0.6114 |
| test | 0.6465 | 0.8421 | 0.6294 | 0.6164 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__rev_co_quick__product`: 0.3251
- `product__co_quick__product`: 0.2530
- `product__has_kw__keyword`: 0.0566
- `keyword__rev_has_kw__product`: 0.0552
- `product__has_ip__ip`: 0.0539
- `ip__rev_has_ip__product`: 0.0515
- `keyword__trend_to__keyword`: 0.0512
- `keyword__rev_trend_to__keyword`: 0.0512
- `ip__has_kw__keyword`: 0.0512
- `keyword__rev_has_kw__ip`: 0.0512

**layer 1**
- `ip__rev_has_ip__product`: 0.1462
- `product__rev_co_quick__product`: 0.0976
- `product__co_quick__product`: 0.0970
- `product__has_kw__keyword`: 0.0943
- `ip__has_kw__keyword`: 0.0943
- `keyword__trend_to__keyword`: 0.0943
- `product__has_ip__ip`: 0.0943
- `keyword__rev_has_kw__ip`: 0.0943
- `keyword__rev_trend_to__keyword`: 0.0943
- `keyword__rev_has_kw__product`: 0.0932

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04217), 향라(0.03024), 탄탄(0.02967), 박은영(0.02015), 중독성(0.01732), 샹궈(0.01628), 짜파게티(0.01417), 적음(0.00991), 피(0.00901), 자극적(0.00835)
- **로제** → 중독성(0.01655), 늘어남(0.01459), 떡볶퀸(0.01394), 마카로니(0.00971), 당면(0.00460), 파마산(0.00340), 히밥(0.00328), 고단백(0.00301), 볶이(0.00297), 떡볶이(0.00282)
- **흑임자** → 공룡(0.01925), 시루떡(0.00996), 롱롱이(0.00993), 알(0.00838), 비비빅(0.00504), 파운드케이크(0.00322), 마카롱(0.00221), 묵직함(0.00186), 찰떡(0.00104), 컵(0.00086)
- **단백질** → 헬스(0.00973), 베노프(0.00933), 동물(0.00720), 플러스(0.00717), 이지프로틴(0.00712), 아르기닌(0.00712), 엽떡(0.00687), 밸런스밀(0.00683), 편함(0.00539), 소이조이(0.00365)
- **위스키** → 블랙서클(0.00649), 예술(0.00460), 산토리(0.00451), 티처스위스키(0.00451), 컬렉션(0.00419), 블론드(0.00263), 키싱(0.00245), 메이커스마크(0.00231), 스트레이트(0.00216), 스프레드(0.00210)
- **딸기** → 분홍(0.00390), 고칸(0.00370), 돌직구(0.00367), 쿠냥이(0.00347), 4D(0.00347), 픽업(0.00339), 포장(0.00339), 통크(0.00335), 치토스(0.00333), 톡핑(0.00329)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,051행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
