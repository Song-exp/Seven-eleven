# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7869 | 0.9345 | 0.7504 | 0.6966 |
| val | 0.7337 | 0.8661 | 0.6530 | 0.6036 |
| test | 0.6594 | 0.8626 | 0.6413 | 0.8123 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.2938
- `product__sim_kw__product`: 0.1224
- `product__rev_sim_kw__product`: 0.1201
- `product__rev_co_quick__product`: 0.0888
- `product__rev_co_offline__product`: 0.0574
- `product__co_quick__product`: 0.0424
- `keyword__rev_has_kw__product`: 0.0369
- `ip__rev_has_ip__product`: 0.0273
- `product__rev_sim_ip__product`: 0.0271
- `product__has_ip__ip`: 0.0264
- `keyword__rev_has_kw__ip`: 0.0264
- `keyword__rev_trend_to__keyword`: 0.0264
- `ip__has_kw__keyword`: 0.0264
- `keyword__trend_to__keyword`: 0.0264
- `product__has_kw__keyword`: 0.0264
- `product__sim_ip__product`: 0.0257

**layer 1**
- `product__co_quick__product`: 0.0632
- `ip__rev_has_ip__product`: 0.0631
- `product__sim_ip__product`: 0.0629
- `product__has_kw__keyword`: 0.0629
- `ip__has_kw__keyword`: 0.0629
- `keyword__trend_to__keyword`: 0.0629
- `product__has_ip__ip`: 0.0629
- `keyword__rev_has_kw__ip`: 0.0629
- `keyword__rev_trend_to__keyword`: 0.0629
- `product__rev_co_quick__product`: 0.0629
- `product__rev_sim_ip__product`: 0.0628
- `product__sim_kw__product`: 0.0623
- `product__rev_sim_kw__product`: 0.0618
- `product__rev_co_offline__product`: 0.0614
- `product__co_offline__product`: 0.0612
- `keyword__rev_has_kw__product`: 0.0611

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04109), 향라(0.04021), 탄탄(0.03951), 샹궈(0.02026), 박은영(0.01750), 중독성(0.01560), 피(0.01352), 짜파게티(0.01323), 적음(0.01317), 자극적(0.01116)
- **로제** → 늘어남(0.01803), 중독성(0.01496), 원데이(0.01249), 바리스타룰스(0.01249), 마카로니(0.01202), 고단백(0.00372), 파마산(0.00358), 당면(0.00290), 누들(0.00286), 하트(0.00258)
- **흑임자** → 공룡(0.01948), 시루떡(0.01120), 롱롱이(0.01064), 알(0.00834), 비비빅(0.00565), 파운드케이크(0.00460), 작은별(0.00294), 마카롱(0.00210), 묵직함(0.00188), 할매니얼(0.00183)
- **단백질** → 엽떡(0.00958), 헬스(0.00953), 동물(0.00934), 베노프(0.00793), 소이조이(0.00783), 백반(0.00704), 밸런스밀(0.00665), 플러스(0.00582), 편함(0.00532), 이지프로틴(0.00526)
- **위스키** → 블랙서클(0.00626), 예술(0.00595), 산토리(0.00453), 티처스위스키(0.00453), 키싱(0.00431), 컬렉션(0.00413), 아이리쉬(0.00347), 12년(0.00345), 블론드(0.00337), 트레이스(0.00326)
- **딸기** → 분홍(0.00439), 고칸(0.00423), 몽쉘(0.00398), 4D(0.00379), 돌직구(0.00374), 톡핑(0.00329), 쿠냥이(0.00304), 통크(0.00304), 데이(0.00276), 치토스(0.00236)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
