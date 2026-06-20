# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=1, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8171 | 0.9476 | 0.7820 | 0.7302 |
| val | 0.7378 | 0.8692 | 0.6667 | 0.8153 |
| test | 0.6681 | 0.8596 | 0.6421 | 0.8262 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.3682
- `product__rev_sim_kw__product`: 0.1505
- `product__sim_kw__product`: 0.1331
- `product__rev_co_offline__product`: 0.0490
- `product__rev_co_quick__product`: 0.0479
- `product__co_quick__product`: 0.0337
- `keyword__rev_has_kw__product`: 0.0272
- `product__sim_ip__product`: 0.0251
- `product__rev_sim_ip__product`: 0.0213
- `product__has_kw__keyword`: 0.0161
- `ip__has_kw__keyword`: 0.0161
- `keyword__trend_to__keyword`: 0.0161
- `product__has_ip__ip`: 0.0161
- `ip__has_ip__ip`: 0.0161
- `keyword__rev_has_kw__ip`: 0.0161
- `keyword__rev_trend_to__keyword`: 0.0161
- `ip__rev_has_ip__ip`: 0.0161
- `ip__rev_has_ip__product`: 0.0155

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 향라(0.04059), 도쿠시마(0.04050), 탄탄(0.04024), 샹궈(0.01731), 중독성(0.01402), 피(0.01358), 적음(0.01341), 짜파게티(0.01292), 자극적(0.01167), 곤약(0.00872)
- **로제** → 늘어남(0.01892), 중독성(0.01402), 마카로니(0.01261), 고단백(0.00570), 당면(0.00493), 파마산(0.00357), 떡볶이(0.00313), 누들(0.00307), 하트(0.00274), 분식(0.00263)
- **흑임자** → 공룡(0.02338), 알(0.01002), 파운드케이크(0.00520), 마카롱(0.00332), 묵직함(0.00226), 할매니얼(0.00141), 컵(0.00103), 케이크(0.00102), 바나나(0.00101), 작은별(0.00088)
- **단백질** → 엽떡(0.01084), 동물(0.01063), 헬스(0.01059), 베노프(0.00946), 소이조이(0.00944), 밸런스밀(0.00842), 이지프로틴(0.00712), 아르기닌(0.00712), 테이크핏(0.00550), 백반(0.00520)
- **위스키** → 블랙서클(0.00629), 예술(0.00572), 산토리(0.00528), 트레이스(0.00302), 캐리비안(0.00298), 메이커스마크(0.00295), 맥캘란(0.00264), 클레이모어(0.00256), 블론드(0.00235), 신년(0.00198)
- **딸기** → 분홍(0.00451), 고칸(0.00418), 몽쉘(0.00416), 톡핑(0.00388), 통크(0.00374), 치토스(0.00295), 픽업(0.00252), 포장(0.00252), 쌍둥이(0.00227), 스키틀즈(0.00212)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
