# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7695 | 0.9321 | 0.7547 | 0.3657 |
| val | 0.5908 | 0.8048 | 0.5721 | 0.4237 |
| test | 0.5872 | 0.8199 | 0.5966 | 0.1429 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `keyword__rev_has_kw__product`: 0.1260
- `ip__rev_has_ip__product`: 0.1256
- `keyword__rev_has_kw__ip`: 0.1254
- `keyword__rev_trend_to__keyword`: 0.1254
- `product__has_ip__ip`: 0.1253
- `keyword__trend_to__keyword`: 0.1253
- `ip__has_kw__keyword`: 0.1253
- `product__has_kw__keyword`: 0.1217

**layer 1**
- `keyword__rev_has_kw__product`: 0.1791
- `product__has_kw__keyword`: 0.1174
- `ip__has_kw__keyword`: 0.1174
- `keyword__trend_to__keyword`: 0.1174
- `product__has_ip__ip`: 0.1174
- `keyword__rev_has_kw__ip`: 0.1174
- `keyword__rev_trend_to__keyword`: 0.1174
- `ip__rev_has_ip__product`: 0.1162

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.03548), 향라(0.03450), 탄탄(0.02336), 박은영(0.02256), 중독성(0.01762), 짜파게티(0.01168), 마라탕(0.00886), 적음(0.00782), 자극적(0.00763), 중식(0.00713)
- **로제** → 중독성(0.01663), 늘어남(0.01660), 마카로니(0.01092), 떡볶퀸(0.00914), 고단백(0.00336), 당면(0.00319), 떡볶이(0.00259), 누들(0.00257), 파마산(0.00253), 하트(0.00191)
- **흑임자** → 공룡(0.01632), 알(0.00685), 시루떡(0.00313), 마카롱(0.00231), 묵직함(0.00160), 비비빅(0.00158), 컵(0.00071), KBO(0.00066), 빵또아(0.00064), 바나나(0.00064)
- **단백질** → 베노프(0.00850), 엽떡(0.00840), 이지프로틴(0.00828), 아르기닌(0.00828), 플러스(0.00799), 동물(0.00794), 편함(0.00783), 밸런스밀(0.00749), 테이크핏(0.00430), 프로틴(0.00350)
- **위스키** → 블랙서클(0.00560), 산토리(0.00544), 티처스위스키(0.00544), 예술(0.00524), 맥캘란(0.00401), 컬렉션(0.00329), 스프레드(0.00258), 스트레이트(0.00249), 캐리비안(0.00213), 건조(0.00207)
- **딸기** → 돌직구(0.00406), 분홍(0.00397), 고칸(0.00387), 몽쉘(0.00358), 4D(0.00350), 쿠냥이(0.00350), 통크(0.00311), 톡핑(0.00296), 치토스(0.00282), 헬로키티(0.00261)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
