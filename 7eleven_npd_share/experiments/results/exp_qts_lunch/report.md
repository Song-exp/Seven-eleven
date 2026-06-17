# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8350 | 0.9553 | 0.8063 | 0.5665 |
| val | 0.6420 | 0.8177 | 0.6000 | 0.6081 |
| test | 0.6297 | 0.8180 | 0.5920 | 0.7630 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `keyword__rev_trend_to__keyword`: 0.1015
- `keyword__rev_has_kw__ip`: 0.1015
- `product__has_ip__ip`: 0.1015
- `ip__has_kw__keyword`: 0.1015
- `ip__rev_has_ip__product`: 0.1014
- `keyword__trend_to__keyword`: 0.1014
- `product__co_quick__product`: 0.1013
- `keyword__rev_has_kw__product`: 0.1005
- `product__rev_co_quick__product`: 0.0999
- `product__has_kw__keyword`: 0.0895

**layer 1**
- `product__rev_co_quick__product`: 0.1075
- `product__co_quick__product`: 0.1073
- `product__has_kw__keyword`: 0.1071
- `ip__has_kw__keyword`: 0.1071
- `keyword__trend_to__keyword`: 0.1071
- `product__has_ip__ip`: 0.1071
- `keyword__rev_has_kw__ip`: 0.1071
- `keyword__rev_trend_to__keyword`: 0.1071
- `keyword__rev_has_kw__product`: 0.0718
- `ip__rev_has_ip__product`: 0.0705

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04104), 중독성(0.01996), 박은영(0.01831), 짜파게티(0.01362), 탄탄(0.00941), 마라탕(0.00930), 정통(0.00372), 마파(0.00337), 중식(0.00326), 적음(0.00314)
- **로제** → 중독성(0.01917), 늘어남(0.01771), 마카로니(0.01182), 떡볶퀸(0.00813), 당면(0.00372), 고단백(0.00365), 파마산(0.00354), 누들(0.00281), 떡볶이(0.00269), 하트(0.00263)
- **흑임자** → 공룡(0.01865), 알(0.00799), 마카롱(0.00235), 묵직함(0.00180), 컵(0.00082), KBO(0.00076), 바나나(0.00064), 파운드케이크(0.00047), 케이크(0.00047), 바닐라(0.00039)
- **단백질** → 베노프(0.00972), 헬스(0.00897), 동물(0.00850), 엽떡(0.00702), 이지프로틴(0.00685), 아르기닌(0.00685), 밸런스밀(0.00550), 테이크핏(0.00471), 닥터유(0.00433), 포카치아(0.00349)
- **위스키** → 블랙서클(0.00634), 예술(0.00556), 캐리비안(0.00278), 스프레드(0.00237), 맥캘란(0.00224), 스트레이트(0.00211), 쉐리(0.00180), 콜드브루(0.00158), 캐스크(0.00149), 한국(0.00093)
- **딸기** → 분홍(0.00418), 돌직구(0.00401), 쿠냥이(0.00398), 고칸(0.00397), 몽쉘(0.00393), 픽업(0.00385), 포장(0.00385), 4D(0.00380), 톡핑(0.00353), 헬로키티(0.00332)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,051행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
