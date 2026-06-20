# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=64, num_layers=1, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7426 | 0.9103 | 0.7040 | 0.5444 |
| val | 0.7346 | 0.8631 | 0.6667 | 0.7160 |
| test | 0.6762 | 0.8675 | 0.6489 | 0.6645 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.1363
- `product__sim_kw__product`: 0.1267
- `product__rev_sim_ip__product`: 0.1030
- `product__rev_co_offline__product`: 0.0954
- `product__rev_sim_kw__product`: 0.0787
- `product__sim_ip__product`: 0.0647
- `product__rev_co_quick__product`: 0.0517
- `product__co_quick__product`: 0.0328
- `keyword__rev_has_kw__product`: 0.0199
- `product__has_kw__keyword`: 0.0195
- `ip__has_kw__keyword`: 0.0195
- `keyword__trend_to__keyword`: 0.0195
- `product__has_ip__ip`: 0.0195
- `ip__has_ip__ip`: 0.0195
- `product__has_kw_via_ip__keyword`: 0.0195
- `product__has_kw_ipip__keyword`: 0.0195
- `product__has_kw_trend__keyword`: 0.0195
- `keyword__rev_has_kw__ip`: 0.0195
- `keyword__rev_trend_to__keyword`: 0.0195
- `ip__rev_has_ip__ip`: 0.0195
- `keyword__rev_has_kw_via_ip__product`: 0.0194
- `keyword__rev_has_kw_ipip__product`: 0.0194
- `keyword__rev_has_kw_trend__product`: 0.0190
- `ip__rev_has_ip__product`: 0.0188

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04121), 향라(0.03967), 탄탄(0.03819), 샹궈(0.02087), 피(0.01377), 중독성(0.01321), 적음(0.01273), 짜파게티(0.01214), 자극적(0.00974), 곤약(0.00901)
- **로제** → 중독성(0.01321), 늘어남(0.01108), 마카로니(0.00739), 고단백(0.00532), 당면(0.00294), 누들(0.00287), 하트(0.00266), 떡볶이(0.00241), 파마산(0.00231), 분식(0.00222)
- **흑임자** → 공룡(0.02356), 알(0.01010), 작은별(0.00419), 파운드케이크(0.00365), 마카롱(0.00304), 묵직함(0.00228), 할매니얼(0.00200), 드레싱(0.00136), 컵(0.00104), 바나나(0.00092)
- **단백질** → 헬스(0.01078), 엽떡(0.01048), 동물(0.01035), 베노프(0.00963), 소이조이(0.00690), 밸런스밀(0.00575), 이지프로틴(0.00573), 아르기닌(0.00573), 테이크핏(0.00555), 피쉬(0.00368)
- **위스키** → 블랙서클(0.00619), 산토리(0.00380), 예술(0.00360), 블론드(0.00266), 트레이스(0.00254), 클레이모어(0.00247), 메이커스마크(0.00244), 신년(0.00185), 임페리얼(0.00151), 글렌피딕(0.00140)
- **딸기** → 분홍(0.00450), 고칸(0.00388), 몽쉘(0.00355), 픽업(0.00341), 포장(0.00341), 통크(0.00300), 치토스(0.00293), 쌍둥이(0.00227), 트위스트(0.00214), 스키틀즈(0.00212)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
