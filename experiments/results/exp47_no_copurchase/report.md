# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=64, num_layers=1, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7935 | 0.9410 | 0.7852 | 0.4890 |
| val | 0.5781 | 0.8096 | 0.5959 | 0.2246 |
| test | 0.5699 | 0.8175 | 0.5867 | 0.5006 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__rev_sim_kw__product`: 0.1742
- `product__rev_sim_ip__product`: 0.1539
- `product__sim_kw__product`: 0.1404
- `product__sim_ip__product`: 0.1305
- `keyword__rev_has_kw__product`: 0.0253
- `keyword__rev_has_kw_trend__product`: 0.0253
- `keyword__rev_has_kw_via_ip__product`: 0.0252
- `ip__rev_has_ip__product`: 0.0250
- `product__has_kw__keyword`: 0.0250
- `ip__has_kw__keyword`: 0.0250
- `keyword__trend_to__keyword`: 0.0250
- `product__has_ip__ip`: 0.0250
- `ip__has_ip__ip`: 0.0250
- `product__has_kw_via_ip__keyword`: 0.0250
- `product__has_kw_ipip__keyword`: 0.0250
- `product__has_kw_trend__keyword`: 0.0250
- `keyword__rev_has_kw__ip`: 0.0250
- `keyword__rev_trend_to__keyword`: 0.0250
- `ip__rev_has_ip__ip`: 0.0250
- `keyword__rev_has_kw_ipip__product`: 0.0250

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.03676), 향라(0.03597), 탄탄(0.03566), 중독성(0.01261), 짜파게티(0.01192), 적음(0.01189), 샹궈(0.01028), 중식(0.00784), 피(0.00774), 자극적(0.00755)
- **로제** → 늘어남(0.01791), 중독성(0.01261), 마카로니(0.01194), 당면(0.00494), 고단백(0.00488), 파마산(0.00415), 떡볶이(0.00274), 누들(0.00263), 납작(0.00238), 하트(0.00210)
- **흑임자** → 공룡(0.02191), 알(0.00939), 마카롱(0.00351), 묵직함(0.00212), 바나나(0.00106), 컵(0.00097), 바닐라(0.00079), 케이크(0.00054), 콜라(0.00047), 딸기(0.00032)
- **단백질** → 엽떡(0.00948), 베노프(0.00917), 밸런스밀(0.00916), 이지프로틴(0.00882), 아르기닌(0.00882), 동물(0.00869), 헬스(0.00728), 테이크핏(0.00515), 프로틴(0.00451), 소이조이(0.00398)
- **위스키** → 블랙서클(0.00610), 예술(0.00602), 산토리(0.00544), 캐리비안(0.00310), 맥캘란(0.00301), 야마자키(0.00184), 쉐리(0.00174), 임페리얼(0.00173), 캐스크(0.00173), 트레이스(0.00138)
- **딸기** → 분홍(0.00444), 고칸(0.00421), 몽쉘(0.00403), 톡핑(0.00266), 통크(0.00266), 쌍둥이(0.00220), 필링(0.00196), 탕종(0.00186), 저지방(0.00184), 아모스(0.00180)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
