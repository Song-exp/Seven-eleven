# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=64, num_layers=1, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7980 | 0.9390 | 0.7629 | 0.7632 |
| val | 0.7437 | 0.8696 | 0.6667 | 0.7585 |
| test | 0.6659 | 0.8640 | 0.6537 | 0.8368 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.1938
- `product__sim_kw__product`: 0.1230
- `product__rev_sim_kw__product`: 0.0893
- `product__rev_co_quick__product`: 0.0863
- `product__rev_co_offline__product`: 0.0831
- `product__co_quick__product`: 0.0385
- `product__rev_sim_ip__product`: 0.0294
- `product__sim_ip__product`: 0.0274
- `keyword__rev_has_kw__product`: 0.0209
- `ip__rev_has_ip__product`: 0.0209
- `keyword__rev_has_kw_trend__product`: 0.0207
- `product__has_kw__keyword`: 0.0206
- `ip__has_kw__keyword`: 0.0206
- `keyword__trend_to__keyword`: 0.0206
- `product__has_ip__ip`: 0.0206
- `ip__has_ip__ip`: 0.0206
- `product__has_kw_via_ip__keyword`: 0.0206
- `product__has_kw_ipip__keyword`: 0.0206
- `product__has_kw_trend__keyword`: 0.0206
- `keyword__rev_has_kw__ip`: 0.0206
- `keyword__rev_trend_to__keyword`: 0.0206
- `ip__rev_has_ip__ip`: 0.0206
- `keyword__rev_has_kw_via_ip__product`: 0.0205
- `keyword__rev_has_kw_ipip__product`: 0.0201

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04175), 향라(0.04059), 탄탄(0.03999), 샹궈(0.02121), 중독성(0.01669), 피(0.01375), 적음(0.01333), 짜파게티(0.01328), 자극적(0.01061), 곤약(0.00938)
- **로제** → 늘어남(0.01829), 중독성(0.01669), 마카로니(0.01220), 고단백(0.00564), 당면(0.00554), 파마산(0.00356), 떡볶이(0.00343), 누들(0.00304), 분식(0.00299), 하트(0.00280)
- **흑임자** → 공룡(0.02371), 알(0.01017), 파운드케이크(0.00512), 작은별(0.00429), 마카롱(0.00334), 할매니얼(0.00258), 묵직함(0.00230), 아몬드(0.00107), 컵(0.00105), 케이크(0.00103)
- **단백질** → 헬스(0.01096), 엽떡(0.01081), 동물(0.01056), 베노프(0.00977), 소이조이(0.00840), 백반(0.00743), 밸런스밀(0.00739), 이지프로틴(0.00656), 아르기닌(0.00656), 테이크핏(0.00574)
- **위스키** → 블랙서클(0.00639), 예술(0.00609), 산토리(0.00538), 맥캘란(0.00457), 애스턴마틴(0.00385), 아이리쉬(0.00316), 트레이스(0.00300), 캐리비안(0.00296), 블론드(0.00295), 캐스크(0.00284)
- **딸기** → 분홍(0.00454), 몽쉘(0.00424), 고칸(0.00418), 통크(0.00326), 톡핑(0.00324), 픽업(0.00289), 포장(0.00289), 치토스(0.00237), 쌍둥이(0.00229), 트위스트(0.00214)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
