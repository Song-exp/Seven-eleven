# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8096 | 0.9480 | 0.7912 | 0.6449 |
| val | 0.7428 | 0.8654 | 0.6800 | 0.8317 |
| test | 0.6777 | 0.8621 | 0.6685 | 0.8436 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.2752
- `product__rev_co_quick__product`: 0.1887
- `product__rev_co_offline__product`: 0.0668
- `keyword__rev_has_kw__product`: 0.0626
- `product__has_kw__keyword`: 0.0535
- `keyword__trend_to__keyword`: 0.0535
- `keyword__rev_trend_to__keyword`: 0.0535
- `ip__has_kw__keyword`: 0.0535
- `keyword__rev_has_kw__ip`: 0.0535
- `product__has_ip__ip`: 0.0535
- `product__co_quick__product`: 0.0510
- `ip__rev_has_ip__product`: 0.0346

**layer 1**
- `keyword__rev_has_kw__product`: 0.0994
- `product__has_kw__keyword`: 0.0826
- `ip__has_kw__keyword`: 0.0826
- `keyword__trend_to__keyword`: 0.0826
- `product__has_ip__ip`: 0.0826
- `keyword__rev_has_kw__ip`: 0.0826
- `keyword__rev_trend_to__keyword`: 0.0826
- `ip__rev_has_ip__product`: 0.0822
- `product__rev_co_quick__product`: 0.0819
- `product__co_quick__product`: 0.0815
- `product__rev_co_offline__product`: 0.0807
- `product__co_offline__product`: 0.0787

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04160), 향라(0.04025), 탄탄(0.03939), 박은영(0.01902), 샹궈(0.01646), 중독성(0.01536), 피(0.01364), 짜파게티(0.01348), 적음(0.01313), 마라탕(0.00930)
- **로제** → 바리스타룰스(0.01897), 원데이(0.01897), 늘어남(0.01707), 중독성(0.01471), 마카로니(0.01138), 고단백(0.00376), 파마산(0.00304), 누들(0.00289), 하트(0.00267), 클래식(0.00237)
- **흑임자** → 공룡(0.01914), 알(0.00821), 시루떡(0.00400), 마카롱(0.00256), 비비빅(0.00200), 묵직함(0.00185), 파운드케이크(0.00155), 컵(0.00084), KBO(0.00078), 바나나(0.00070)
- **단백질** → 헬스(0.00976), 엽떡(0.00947), 동물(0.00927), 베노프(0.00880), 밸런스밀(0.00694), 이지프로틴(0.00603), 아르기닌(0.00603), 편함(0.00589), 소이조이(0.00486), 테이크핏(0.00459)
- **위스키** → 블랙서클(0.00635), 예술(0.00581), 맥캘란(0.00496), 산토리(0.00488), 티처스위스키(0.00488), 컬렉션(0.00409), 아이리쉬(0.00337), 메이커스마크(0.00301), 캐리비안(0.00290), 건조(0.00275)
- **딸기** → 분홍(0.00433), 몽쉘(0.00412), 돌직구(0.00400), 고칸(0.00393), 4D(0.00388), 쿠냥이(0.00377), 톡핑(0.00362), 픽업(0.00290), 포장(0.00290), 데이(0.00281)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
