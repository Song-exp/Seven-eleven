# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=1, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8490 | 0.9607 | 0.8261 | 0.7658 |
| val | 0.7463 | 0.8606 | 0.6631 | 0.8525 |
| test | 0.6646 | 0.8498 | 0.6382 | 0.8497 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.3903
- `product__rev_sim_kw__product`: 0.1000
- `product__rev_co_quick__product`: 0.0780
- `product__sim_kw__product`: 0.0573
- `product__rev_co_offline__product`: 0.0489
- `keyword__rev_has_kw__product`: 0.0472
- `product__co_quick__product`: 0.0448
- `product__sim_ip__product`: 0.0371
- `product__rev_sim_ip__product`: 0.0326
- `product__has_kw__keyword`: 0.0236
- `ip__has_kw__keyword`: 0.0236
- `keyword__trend_to__keyword`: 0.0236
- `product__has_ip__ip`: 0.0236
- `keyword__rev_has_kw__ip`: 0.0236
- `keyword__rev_trend_to__keyword`: 0.0236
- `ip__rev_has_ip__product`: 0.0222

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04149), 향라(0.04113), 탄탄(0.04057), 박은영(0.02052), 중독성(0.01672), 샹궈(0.01433), 피(0.01370), 적음(0.01352), 짜파게티(0.01326), 자극적(0.01130)
- **로제** → 늘어남(0.01886), 원데이(0.01840), 바리스타룰스(0.01840), 떡볶퀸(0.01824), 중독성(0.01603), 마카로니(0.01257), 당면(0.00599), 파마산(0.00414), 고단백(0.00386), 떡볶이(0.00331)
- **흑임자** → 공룡(0.01939), 알(0.00831), 롱롱이(0.00504), 시루떡(0.00439), 파운드케이크(0.00302), 마카롱(0.00284), 빵또아(0.00276), 베이커리(0.00242), 비비빅(0.00220), 묵직함(0.00188)
- **단백질** → 엽떡(0.00973), 동물(0.00962), 헬스(0.00955), 베노프(0.00884), 편함(0.00820), 밸런스밀(0.00806), 소이조이(0.00644), 이지프로틴(0.00626), 아르기닌(0.00626), 테이크핏(0.00496)
- **위스키** → 블랙서클(0.00638), 예술(0.00589), 산토리(0.00529), 티처스위스키(0.00529), 메이커스마크(0.00360), 후드티(0.00318), 키싱(0.00312), 캐리비안(0.00311), 맥캘란(0.00289), 스프레드(0.00265)
- **딸기** → 분홍(0.00441), 몽쉘(0.00423), 고칸(0.00414), 돌직구(0.00402), 톡핑(0.00402), 쿠냥이(0.00391), 4D(0.00386), 데이(0.00286), 헬로키티(0.00264), 쌍둥이(0.00222)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
