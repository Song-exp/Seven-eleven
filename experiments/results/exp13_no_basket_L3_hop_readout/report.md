# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=3, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7478 | 0.9184 | 0.7198 | 0.7180 |
| val | 0.5918 | 0.8159 | 0.5904 | 0.7031 |
| test | 0.6136 | 0.8275 | 0.5967 | 0.6706 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `ip__has_kw__keyword`: 0.1262
- `keyword__rev_trend_to__keyword`: 0.1261
- `keyword__trend_to__keyword`: 0.1259
- `keyword__rev_has_kw__ip`: 0.1257
- `keyword__rev_has_kw__product`: 0.1253
- `ip__rev_has_ip__product`: 0.1253
- `product__has_ip__ip`: 0.1245
- `product__has_kw__keyword`: 0.1210

**layer 1**
- `ip__rev_has_ip__product`: 0.1892
- `keyword__rev_has_kw__product`: 0.1554
- `product__has_kw__keyword`: 0.1093
- `ip__has_kw__keyword`: 0.1092
- `keyword__rev_trend_to__keyword`: 0.1092
- `product__has_ip__ip`: 0.1092
- `keyword__rev_has_kw__ip`: 0.1092
- `keyword__trend_to__keyword`: 0.1092

**layer 2**
- `product__has_kw__keyword`: 0.1255
- `ip__has_kw__keyword`: 0.1255
- `keyword__trend_to__keyword`: 0.1255
- `product__has_ip__ip`: 0.1255
- `keyword__rev_has_kw__ip`: 0.1255
- `keyword__rev_trend_to__keyword`: 0.1255
- `ip__rev_has_ip__product`: 0.1248
- `keyword__rev_has_kw__product`: 0.1223

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.03741), 향라(0.03592), 탄탄(0.03094), 박은영(0.02336), 중독성(0.01829), 샹궈(0.01379), 짜파게티(0.01217), 적음(0.01011), 곤약(0.00897), 피(0.00874)
- **로제** → 중독성(0.01779), 늘어남(0.01556), 떡볶퀸(0.01552), 마카로니(0.01047), 당면(0.00531), 원데이(0.00409), 바리스타룰스(0.00405), 파마산(0.00399), 히밥(0.00368), 고단백(0.00348)
- **흑임자** → 시루떡(0.01718), 공룡(0.01626), 비비빅(0.00857), 알(0.00702), 파운드케이크(0.00366), 마카롱(0.00226), 찰떡(0.00181), 묵직함(0.00156), 빵또아(0.00134), 베이커리(0.00117)
- **단백질** → 베노프(0.00846), 이지프로틴(0.00824), 아르기닌(0.00824), 엽떡(0.00816), 밸런스밀(0.00806), 동물(0.00802), 편함(0.00792), 플러스(0.00761), 테이크핏(0.00500), 닥터유(0.00388)
- **위스키** → 블랙서클(0.00586), 산토리(0.00578), 티처스위스키(0.00578), 예술(0.00523), 컬렉션(0.00511), 맥캘란(0.00460), 메이커스마크(0.00277), 스프레드(0.00267), 스트레이트(0.00259), 건조(0.00228)
- **딸기** → 분홍(0.00409), 고칸(0.00384), 돌직구(0.00380), 4D(0.00370), 치토스(0.00359), 통크(0.00359), 톡핑(0.00352), 몽쉘(0.00351), 쿠냥이(0.00348), 죠스바(0.00337)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
