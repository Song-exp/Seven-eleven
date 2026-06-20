# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=1, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8005 | 0.9396 | 0.7642 | 0.5774 |
| val | 0.7371 | 0.8665 | 0.6685 | 0.6552 |
| test | 0.6842 | 0.8650 | 0.6588 | 0.8070 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.1590
- `product__sim_kw__product`: 0.1452
- `product__rev_sim_kw__product`: 0.1427
- `product__rev_co_offline__product`: 0.0708
- `product__rev_sim_ip__product`: 0.0654
- `product__rev_co_quick__product`: 0.0596
- `product__sim_ip__product`: 0.0535
- `product__co_quick__product`: 0.0337
- `keyword__rev_has_kw__product`: 0.0170
- `keyword__rev_has_kw_via_ip__product`: 0.0169
- `ip__rev_has_ip__product`: 0.0169
- `product__has_kw__keyword`: 0.0169
- `ip__has_kw__keyword`: 0.0169
- `keyword__trend_to__keyword`: 0.0169
- `product__has_ip__ip`: 0.0169
- `ip__has_ip__ip`: 0.0169
- `product__has_kw_via_ip__keyword`: 0.0169
- `product__has_kw_ipip__keyword`: 0.0169
- `product__has_kw_trend__keyword`: 0.0169
- `keyword__rev_has_kw__ip`: 0.0169
- `keyword__rev_trend_to__keyword`: 0.0169
- `ip__rev_has_ip__ip`: 0.0169
- `keyword__rev_has_kw_ipip__product`: 0.0169
- `keyword__rev_has_kw_trend__product`: 0.0167

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04133), 향라(0.03947), 탄탄(0.03843), 샹궈(0.02124), 피(0.01362), 짜파게티(0.01286), 적음(0.01281), 중독성(0.01172), 곤약(0.00998), 자극적(0.00903)
- **로제** → 늘어남(0.01627), 중독성(0.01172), 마카로니(0.01085), 고단백(0.00543), 파마산(0.00303), 누들(0.00292), 하트(0.00267), 당면(0.00234), 분식(0.00223), 떡볶이(0.00214)
- **흑임자** → 공룡(0.02350), 알(0.01007), 마카롱(0.00297), 파운드케이크(0.00246), 묵직함(0.00227), 작은별(0.00106), 컵(0.00104), 바나나(0.00090), 케이크(0.00078), 바닐라(0.00067)
- **단백질** → 헬스(0.01077), 엽떡(0.01048), 동물(0.01036), 베노프(0.00909), 소이조이(0.00684), 밸런스밀(0.00657), 이지프로틴(0.00619), 아르기닌(0.00619), 테이크핏(0.00524), 청키(0.00362)
- **위스키** → 블랙서클(0.00608), 예술(0.00536), 산토리(0.00397), 트레이스(0.00359), 메이커스마크(0.00355), 블론드(0.00349), 클레이모어(0.00346), 캐리비안(0.00282), 신년(0.00263), 맥캘란(0.00254)
- **딸기** → 분홍(0.00445), 몽쉘(0.00408), 고칸(0.00368), 톡핑(0.00279), 통크(0.00246), 쌍둥이(0.00227), 픽업(0.00214), 포장(0.00214), 트위스트(0.00213), 스키틀즈(0.00211)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
