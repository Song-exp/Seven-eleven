# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=64, num_layers=1, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7912 | 0.9344 | 0.7501 | 0.6654 |
| val | 0.7383 | 0.8704 | 0.6700 | 0.6776 |
| test | 0.6881 | 0.8690 | 0.6649 | 0.7590 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.1560
- `product__rev_co_quick__product`: 0.1031
- `product__rev_sim_kw__product`: 0.0951
- `product__rev_co_offline__product`: 0.0816
- `product__sim_kw__product`: 0.0787
- `product__co_quick__product`: 0.0574
- `product__rev_sim_ip__product`: 0.0286
- `product__sim_ip__product`: 0.0250
- `ip__rev_has_ip__product`: 0.0240
- `keyword__rev_has_kw_via_ip__product`: 0.0237
- `product__has_kw__keyword`: 0.0234
- `ip__has_kw__keyword`: 0.0234
- `keyword__trend_to__keyword`: 0.0234
- `product__has_ip__ip`: 0.0234
- `ip__has_ip__ip`: 0.0234
- `product__has_kw_via_ip__keyword`: 0.0234
- `product__has_kw_ipip__keyword`: 0.0234
- `product__has_kw_trend__keyword`: 0.0234
- `keyword__rev_has_kw__ip`: 0.0234
- `keyword__rev_trend_to__keyword`: 0.0234
- `ip__rev_has_ip__ip`: 0.0234
- `keyword__rev_has_kw__product`: 0.0234
- `keyword__rev_has_kw_ipip__product`: 0.0233
- `keyword__rev_has_kw_trend__product`: 0.0231

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04083), 향라(0.03880), 탄탄(0.03825), 샹궈(0.02101), 중독성(0.01522), 피(0.01319), 짜파게티(0.01311), 적음(0.01275), 자극적(0.01059), 곤약(0.00919)
- **로제** → 늘어남(0.01622), 중독성(0.01522), 마카로니(0.01082), 고단백(0.00534), 당면(0.00465), 파마산(0.00332), 떡볶이(0.00299), 누들(0.00287), 넘버원(0.00272), 하트(0.00266)
- **흑임자** → 공룡(0.02339), 알(0.01003), 작은별(0.00495), 파운드케이크(0.00452), 마카롱(0.00304), 할매니얼(0.00226), 묵직함(0.00226), 드레싱(0.00126), 컵(0.00103), 아몬드(0.00102)
- **단백질** → 헬스(0.01074), 엽떡(0.01031), 동물(0.01002), 베노프(0.00944), 소이조이(0.00780), 밸런스밀(0.00737), 이지프로틴(0.00687), 아르기닌(0.00687), 테이크핏(0.00534), 닥터유(0.00398)
- **위스키** → 블랙서클(0.00622), 예술(0.00548), 산토리(0.00505), 맥캘란(0.00325), 애스턴마틴(0.00282), 블론드(0.00254), 캐리비안(0.00253), 트레이스(0.00250), 메이커스마크(0.00241), 글렌피딕(0.00234)
- **딸기** → 분홍(0.00442), 고칸(0.00403), 몽쉘(0.00388), 통크(0.00332), 치토스(0.00301), 톡핑(0.00283), 쌍둥이(0.00226), 트위스트(0.00212), 스키틀즈(0.00205), 저지방(0.00197)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
