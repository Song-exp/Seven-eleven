# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=64, num_layers=1, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7784 | 0.9331 | 0.7447 | 0.5909 |
| val | 0.6989 | 0.8594 | 0.6541 | 0.5204 |
| test | 0.6741 | 0.8599 | 0.6391 | 0.7935 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.1607
- `product__rev_sim_kw__product`: 0.1381
- `product__sim_kw__product`: 0.1368
- `product__rev_co_offline__product`: 0.1136
- `product__rev_sim_ip__product`: 0.0523
- `product__sim_ip__product`: 0.0522
- `product__rev_co_quick__product`: 0.0458
- `product__co_quick__product`: 0.0354
- `ip__rev_has_ip__product`: 0.0171
- `keyword__rev_has_kw_ipip__product`: 0.0166
- `product__has_kw__keyword`: 0.0165
- `ip__has_kw__keyword`: 0.0165
- `keyword__trend_to__keyword`: 0.0165
- `product__has_ip__ip`: 0.0165
- `ip__has_ip__ip`: 0.0165
- `product__has_kw_via_ip__keyword`: 0.0165
- `product__has_kw_ipip__keyword`: 0.0165
- `product__has_kw_trend__keyword`: 0.0165
- `keyword__rev_has_kw__ip`: 0.0165
- `keyword__rev_trend_to__keyword`: 0.0165
- `ip__rev_has_ip__ip`: 0.0165
- `keyword__rev_has_kw__product`: 0.0165
- `keyword__rev_has_kw_via_ip__product`: 0.0165
- `keyword__rev_has_kw_trend__product`: 0.0163

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 향라(0.03987), 도쿠시마(0.03808), 탄탄(0.03688), 샹궈(0.01844), 중독성(0.01468), 피(0.01262), 적음(0.01229), 짜파게티(0.01186), 곤약(0.00895), 자극적(0.00878)
- **로제** → 늘어남(0.01532), 중독성(0.01468), 마카로니(0.01021), 고단백(0.00524), 당면(0.00352), 파마산(0.00335), 하트(0.00322), 누들(0.00282), 떡볶이(0.00265), 분식(0.00231)
- **흑임자** → 공룡(0.02330), 알(0.00998), 마카롱(0.00316), 묵직함(0.00226), 파운드케이크(0.00180), 작은별(0.00127), 컵(0.00103), 바나나(0.00096), 케이크(0.00072), 바닐라(0.00071)
- **단백질** → 엽떡(0.01031), 동물(0.00989), 베노프(0.00985), 헬스(0.00983), 밸런스밀(0.00737), 이지프로틴(0.00702), 아르기닌(0.00702), 소이조이(0.00651), 테이크핏(0.00567), 닥터유(0.00412)
- **위스키** → 블랙서클(0.00623), 예술(0.00559), 산토리(0.00531), 맥캘란(0.00263), 캐리비안(0.00255), 트레이스(0.00253), 메이커스마크(0.00240), 블론드(0.00232), 클레이모어(0.00232), 글렌피딕(0.00186)
- **딸기** → 분홍(0.00446), 고칸(0.00393), 몽쉘(0.00392), 통크(0.00267), 쌍둥이(0.00227), 치토스(0.00225), 트위스트(0.00206), 아모스(0.00204), 후루츄(0.00199), 톡핑(0.00197)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
