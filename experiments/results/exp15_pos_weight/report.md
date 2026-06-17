# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=4.5), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8595 | 0.9648 | 0.8393 | 0.8595 |
| val | 0.7332 | 0.8567 | 0.6630 | 0.9187 |
| test | 0.6676 | 0.8547 | 0.6425 | 0.9305 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.2590
- `product__rev_co_quick__product`: 0.1557
- `product__rev_co_offline__product`: 0.1098
- `product__co_quick__product`: 0.0767
- `keyword__rev_has_kw__product`: 0.0687
- `keyword__rev_trend_to__keyword`: 0.0501
- `product__has_ip__ip`: 0.0500
- `keyword__rev_has_kw__ip`: 0.0500
- `ip__has_kw__keyword`: 0.0499
- `keyword__trend_to__keyword`: 0.0499
- `ip__rev_has_ip__product`: 0.0458
- `product__has_kw__keyword`: 0.0343

**layer 1**
- `product__co_offline__product`: 0.1364
- `product__rev_co_offline__product`: 0.1079
- `product__has_kw__keyword`: 0.0878
- `ip__has_kw__keyword`: 0.0878
- `keyword__trend_to__keyword`: 0.0878
- `product__has_ip__ip`: 0.0878
- `keyword__rev_has_kw__ip`: 0.0878
- `keyword__rev_trend_to__keyword`: 0.0878
- `product__co_quick__product`: 0.0805
- `product__rev_co_quick__product`: 0.0803
- `keyword__rev_has_kw__product`: 0.0422
- `ip__rev_has_ip__product`: 0.0262

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04297), 향라(0.04229), 탄탄(0.04057), 박은영(0.02301), 중독성(0.01916), 짜파게티(0.01406), 적음(0.01352), 샹궈(0.01308), 피(0.01295), 마라탕(0.01013)
- **로제** → 늘어남(0.01941), 중독성(0.01839), 원데이(0.01641), 바리스타룰스(0.01640), 마카로니(0.01294), 고단백(0.00406), 누들(0.00312), 하트(0.00280), 떡볶이(0.00255), 저녁(0.00215)
- **흑임자** → 공룡(0.01938), 알(0.00830), 마카롱(0.00259), 묵직함(0.00188), 컵(0.00086), KBO(0.00079), 바나나(0.00071), 케이크(0.00045), 바닐라(0.00043), 시루떡(0.00041)
- **단백질** → 동물(0.01009), 엽떡(0.01001), 편함(0.00948), 이지프로틴(0.00924), 아르기닌(0.00924), 헬스(0.00873), 베노프(0.00694), 테이크핏(0.00500), 닥터유(0.00470), 소이조이(0.00468)
- **위스키** → 블랙서클(0.00655), 산토리(0.00629), 티처스위스키(0.00629), 예술(0.00616), 후드티(0.00612), 메이커스마크(0.00465), 맥캘란(0.00382), 캐리비안(0.00312), 스트레이트(0.00306), 스프레드(0.00305)
- **딸기** → 분홍(0.00445), 몽쉘(0.00432), 고칸(0.00428), 쿠냥이(0.00422), 4D(0.00422), 톡핑(0.00417), 픽업(0.00375), 포장(0.00375), 돌직구(0.00362), 헬로키티(0.00306)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
