# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=64, num_layers=1, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7528 | 0.9056 | 0.6897 | 0.5996 |
| val | 0.7288 | 0.8625 | 0.6469 | 0.7535 |
| test | 0.6959 | 0.8734 | 0.6528 | 0.5552 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.1314
- `product__rev_co_offline__product`: 0.0998
- `product__rev_co_quick__product`: 0.0929
- `product__co_quick__product`: 0.0787
- `product__sim_ip__product`: 0.0690
- `product__rev_sim_ip__product`: 0.0686
- `product__sim_kw__product`: 0.0492
- `product__rev_sim_kw__product`: 0.0400
- `ip__rev_has_ip__product`: 0.0233
- `keyword__rev_has_kw__product`: 0.0233
- `keyword__rev_has_kw_trend__product`: 0.0232
- `keyword__rev_has_kw_via_ip__product`: 0.0231
- `keyword__rev_has_kw_ipip__product`: 0.0231
- `product__has_kw__keyword`: 0.0231
- `ip__has_kw__keyword`: 0.0231
- `keyword__trend_to__keyword`: 0.0231
- `product__has_ip__ip`: 0.0231
- `ip__has_ip__ip`: 0.0231
- `product__has_kw_via_ip__keyword`: 0.0231
- `product__has_kw_ipip__keyword`: 0.0231
- `product__has_kw_trend__keyword`: 0.0231
- `keyword__rev_has_kw__ip`: 0.0231
- `keyword__rev_trend_to__keyword`: 0.0231
- `ip__rev_has_ip__ip`: 0.0231

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.03978), 향라(0.03747), 탄탄(0.03652), 샹궈(0.01995), 중독성(0.01432), 피(0.01285), 짜파게티(0.01275), 적음(0.01217), 자극적(0.00916), 곤약(0.00808)
- **로제** → 중독성(0.01432), 늘어남(0.00966), 마카로니(0.00644), 고단백(0.00509), 당면(0.00397), 파마산(0.00319), 누들(0.00274), 하트(0.00271), 떡볶이(0.00257), 분식(0.00214)
- **흑임자** → 공룡(0.02298), 알(0.00985), 작은별(0.00564), 파운드케이크(0.00319), 마카롱(0.00260), 할매니얼(0.00258), 묵직함(0.00222), 드레싱(0.00186), 시루떡(0.00130), 적음(0.00128)
- **단백질** → 헬스(0.01040), 엽떡(0.00994), 동물(0.00980), 베노프(0.00941), 밸런스밀(0.00674), 소이조이(0.00672), 이지프로틴(0.00638), 아르기닌(0.00638), 테이크핏(0.00543), 피쉬(0.00383)
- **위스키** → 블랙서클(0.00597), 산토리(0.00442), 블론드(0.00251), 트레이스(0.00240), 예술(0.00226), 클레이모어(0.00217), 메이커스마크(0.00192), 신년(0.00167), 글렌피딕(0.00146), 임페리얼(0.00136)
- **딸기** → 분홍(0.00433), 고칸(0.00378), 몽쉘(0.00310), 통크(0.00293), 치토스(0.00275), 톡핑(0.00227), 쌍둥이(0.00223), 트위스트(0.00207), 잼(0.00199), 스키틀즈(0.00195)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
