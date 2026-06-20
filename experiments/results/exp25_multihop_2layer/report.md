# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7855 | 0.9341 | 0.7467 | 0.7920 |
| val | 0.7441 | 0.8690 | 0.6612 | 0.8130 |
| test | 0.6736 | 0.8652 | 0.6648 | 0.8608 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.2171
- `product__rev_sim_kw__product`: 0.1800
- `product__rev_co_quick__product`: 0.0958
- `product__sim_kw__product`: 0.0601
- `product__rev_co_offline__product`: 0.0592
- `keyword__rev_has_kw__product`: 0.0487
- `product__co_quick__product`: 0.0433
- `product__sim_ip__product`: 0.0294
- `product__rev_sim_ip__product`: 0.0290
- `product__has_kw__keyword`: 0.0264
- `ip__has_ip__ip`: 0.0264
- `ip__rev_has_ip__ip`: 0.0264
- `keyword__rev_trend_to__keyword`: 0.0264
- `keyword__rev_has_kw__ip`: 0.0264
- `keyword__trend_to__keyword`: 0.0264
- `ip__has_kw__keyword`: 0.0264
- `product__has_ip__ip`: 0.0264
- `ip__rev_has_ip__product`: 0.0259

**layer 1**
- `product__rev_sim_kw__product`: 0.0560
- `keyword__rev_has_kw__product`: 0.0560
- `product__co_quick__product`: 0.0559
- `product__has_kw__keyword`: 0.0556
- `ip__has_kw__keyword`: 0.0556
- `keyword__trend_to__keyword`: 0.0556
- `product__has_ip__ip`: 0.0556
- `ip__has_ip__ip`: 0.0556
- `keyword__rev_has_kw__ip`: 0.0556
- `keyword__rev_trend_to__keyword`: 0.0556
- `ip__rev_has_ip__ip`: 0.0556
- `product__rev_co_quick__product`: 0.0555
- `product__rev_co_offline__product`: 0.0555
- `product__sim_kw__product`: 0.0554
- `product__co_offline__product`: 0.0553
- `ip__rev_has_ip__product`: 0.0553
- `product__rev_sim_ip__product`: 0.0553
- `product__sim_ip__product`: 0.0553

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04226), 향라(0.04009), 탄탄(0.03954), 샹궈(0.02232), 중독성(0.01685), 짜파게티(0.01372), 피(0.01365), 적음(0.01315), 자극적(0.01220), 곤약(0.00958)
- **로제** → 늘어남(0.01827), 중독성(0.01686), 마카로니(0.01218), 고단백(0.00559), 당면(0.00462), 파마산(0.00371), 떡볶이(0.00339), 분식(0.00325), 반반(0.00304), 누들(0.00302)
- **흑임자** → 공룡(0.02355), 알(0.01007), 파운드케이크(0.00536), 마카롱(0.00319), 할매니얼(0.00309), 묵직함(0.00228), 작은별(0.00132), 컵(0.00104), 케이크(0.00104), 바나나(0.00097)
- **단백질** → 헬스(0.01098), 엽떡(0.01063), 동물(0.01019), 베노프(0.00992), 소이조이(0.00935), 밸런스밀(0.00823), 이지프로틴(0.00748), 아르기닌(0.00748), 테이크핏(0.00521), 백반(0.00497)
- **위스키** → 블랙서클(0.00647), 예술(0.00583), 산토리(0.00522), 아이리쉬(0.00458), 맥캘란(0.00432), 트레이스(0.00402), 메이커스마크(0.00386), 블론드(0.00385), 클레이모어(0.00383), 시바스리갈(0.00315)
- **딸기** → 분홍(0.00451), 몽쉘(0.00419), 고칸(0.00413), 통크(0.00375), 톡핑(0.00363), 치토스(0.00354), 픽업(0.00264), 포장(0.00264), 쌍둥이(0.00229), 트위스트(0.00218)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
