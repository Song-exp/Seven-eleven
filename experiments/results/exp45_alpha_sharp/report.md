# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=64, num_layers=1, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.05), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7287 | 0.9051 | 0.6935 | 0.6780 |
| val | 0.7088 | 0.8637 | 0.6726 | 0.7841 |
| test | 0.6610 | 0.8674 | 0.6521 | 0.7501 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.8973
- `product__rev_co_quick__product`: 0.0402
- `product__sim_ip__product`: 0.0168
- `product__rev_co_offline__product`: 0.0125
- `product__rev_sim_kw__product`: 0.0029
- `product__sim_kw__product`: 0.0025
- `keyword__rev_has_kw_via_ip__product`: 0.0017
- `keyword__rev_has_kw__product`: 0.0017
- `product__has_kw__keyword`: 0.0016
- `ip__has_kw__keyword`: 0.0016
- `keyword__trend_to__keyword`: 0.0016
- `product__has_ip__ip`: 0.0016
- `ip__has_ip__ip`: 0.0016
- `product__has_kw_via_ip__keyword`: 0.0016
- `product__has_kw_ipip__keyword`: 0.0016
- `product__has_kw_trend__keyword`: 0.0016
- `keyword__rev_has_kw__ip`: 0.0016
- `keyword__rev_trend_to__keyword`: 0.0016
- `ip__rev_has_ip__ip`: 0.0016
- `keyword__rev_has_kw_ipip__product`: 0.0016
- `ip__rev_has_ip__product`: 0.0016
- `product__co_quick__product`: 0.0013
- `product__rev_sim_ip__product`: 0.0009
- `keyword__rev_has_kw_trend__product`: 0.0008

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 향라(0.04101), 탄탄(0.04097), 도쿠시마(0.03881), 샹궈(0.02159), 중독성(0.01588), 적음(0.01366), 피(0.01363), 짜파게티(0.01283), 자극적(0.01039), 중식(0.00923)
- **로제** → 중독성(0.01588), 늘어남(0.01134), 마카로니(0.00756), 고단백(0.00585), 당면(0.00502), 파마산(0.00361), 누들(0.00315), 떡볶이(0.00303), 하트(0.00278), 분식(0.00248)
- **흑임자** → 공룡(0.02257), 알(0.00967), 작은별(0.00482), 파운드케이크(0.00401), 마카롱(0.00301), 할매니얼(0.00252), 묵직함(0.00218), 드레싱(0.00133), 컵(0.00100), 케이크(0.00092)
- **단백질** → 엽떡(0.01096), 동물(0.01096), 헬스(0.01041), 베노프(0.01008), 소이조이(0.00747), 밸런스밀(0.00702), 이지프로틴(0.00686), 아르기닌(0.00686), 테이크핏(0.00439), 프로틴(0.00380)
- **위스키** → 블랙서클(0.00602), 산토리(0.00491), 블론드(0.00294), 트레이스(0.00276), 클레이모어(0.00262), 예술(0.00256), 메이커스마크(0.00240), 신년(0.00198), 조니워커(0.00149), 임페리얼(0.00148)
- **딸기** → 분홍(0.00434), 고칸(0.00408), 몽쉘(0.00348), 픽업(0.00347), 포장(0.00347), 통크(0.00328), 치토스(0.00312), 톡핑(0.00255), 쌍둥이(0.00218), 스키틀즈(0.00216)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
