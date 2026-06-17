# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=1, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7916 | 0.9397 | 0.7679 | 0.6641 |
| val | 0.7424 | 0.8678 | 0.6685 | 0.7552 |
| test | 0.6828 | 0.8646 | 0.6597 | 0.7663 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.1699
- `product__rev_co_quick__product`: 0.1158
- `product__rev_co_offline__product`: 0.0878
- `product__rev_sim_kw__product`: 0.0870
- `product__co_quick__product`: 0.0817
- `product__sim_kw__product`: 0.0759
- `keyword__rev_has_kw__product`: 0.0560
- `product__rev_sim_ip__product`: 0.0398
- `product__sim_ip__product`: 0.0381
- `product__has_kw__keyword`: 0.0356
- `ip__has_kw__keyword`: 0.0356
- `keyword__trend_to__keyword`: 0.0356
- `product__has_ip__ip`: 0.0356
- `keyword__rev_has_kw__ip`: 0.0356
- `keyword__rev_trend_to__keyword`: 0.0356
- `ip__rev_has_ip__product`: 0.0340

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04143), 향라(0.03914), 탄탄(0.03837), 샹궈(0.01839), 중독성(0.01639), 피(0.01359), 짜파게티(0.01316), 적음(0.01279), 마라탕(0.00899), 자극적(0.00878)
- **로제** → 원데이(0.01855), 늘어남(0.01638), 중독성(0.01570), 마카로니(0.01092), 고단백(0.00365), 파마산(0.00314), 누들(0.00281), 하트(0.00259), 당면(0.00254), 떡볶이(0.00240)
- **흑임자** → 공룡(0.01917), 알(0.00822), 시루떡(0.00654), 파운드케이크(0.00334), 비비빅(0.00328), 마카롱(0.00254), 묵직함(0.00186), 롱롱이(0.00121), 컵(0.00085), 케이크(0.00072)
- **단백질** → 헬스(0.00961), 엽떡(0.00919), 동물(0.00891), 베노프(0.00887), 밸런스밀(0.00685), 이지프로틴(0.00632), 아르기닌(0.00632), 소이조이(0.00588), 플러스(0.00522), 테이크핏(0.00461)
- **위스키** → 블랙서클(0.00630), 예술(0.00564), 산토리(0.00487), 티처스위스키(0.00487), 컬렉션(0.00458), 맥캘란(0.00409), 키싱(0.00380), 메이커스마크(0.00343), 아이리쉬(0.00311), 캐리비안(0.00286)
- **딸기** → 분홍(0.00432), 몽쉘(0.00405), 돌직구(0.00396), 4D(0.00389), 고칸(0.00383), 픽업(0.00335), 포장(0.00335), 톡핑(0.00274), 데이(0.00273), 통크(0.00252)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (39,266행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
