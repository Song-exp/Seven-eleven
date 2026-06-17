# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8119 | 0.9471 | 0.7935 | 0.5949 |
| val | 0.6465 | 0.8155 | 0.5880 | 0.5445 |
| test | 0.6507 | 0.8282 | 0.5990 | 0.7067 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `keyword__rev_has_kw__product`: 0.1014
- `product__co_quick__product`: 0.1011
- `product__rev_co_quick__product`: 0.1008
- `ip__rev_has_ip__product`: 0.1002
- `product__has_kw__keyword`: 0.1002
- `keyword__trend_to__keyword`: 0.0999
- `keyword__rev_has_kw__ip`: 0.0999
- `keyword__rev_trend_to__keyword`: 0.0998
- `ip__has_kw__keyword`: 0.0998
- `product__has_ip__ip`: 0.0968

**layer 1**
- `product__rev_co_quick__product`: 0.1424
- `product__co_quick__product`: 0.1346
- `product__has_kw__keyword`: 0.0997
- `ip__has_kw__keyword`: 0.0997
- `keyword__trend_to__keyword`: 0.0997
- `product__has_ip__ip`: 0.0997
- `keyword__rev_has_kw__ip`: 0.0997
- `keyword__rev_trend_to__keyword`: 0.0997
- `ip__rev_has_ip__product`: 0.0789
- `keyword__rev_has_kw__product`: 0.0457

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04031), 탄탄(0.01953), 중독성(0.01898), 박은영(0.01449), 짜파게티(0.01332), 마라탕(0.00871), 향라(0.00778), 적음(0.00649), 정통(0.00347), 중식(0.00305)
- **로제** → 중독성(0.01821), 늘어남(0.01555), 마카로니(0.01037), 하트(0.00367), 고단백(0.00344), 파마산(0.00293), 앙리(0.00290), 누들(0.00264), 떡볶이(0.00227), 당면(0.00200)
- **흑임자** → 공룡(0.01842), 알(0.00788), 마카롱(0.00251), 묵직함(0.00178), 파운드케이크(0.00118), 컵(0.00081), KBO(0.00075), 바나나(0.00069), 빵또아(0.00055), 시루떡(0.00053)
- **단백질** → 베노프(0.00947), 헬스(0.00925), 동물(0.00799), 이지프로틴(0.00780), 아르기닌(0.00780), 엽떡(0.00747), 밸런스밀(0.00685), 널담(0.00621), 닥터유(0.00446), 테이크핏(0.00425)
- **위스키** → 블랙서클(0.00624), 예술(0.00555), 티처스하이랜드(0.00447), 산토리(0.00442), 티처스위스키(0.00442), 맥캘란(0.00376), 메이커스마크(0.00298), 캐리비안(0.00279), 스트레이트(0.00275), 스프레드(0.00257)
- **딸기** → 분홍(0.00405), 돌직구(0.00395), 쿠냥이(0.00390), 픽업(0.00377), 포장(0.00377), 몽쉘(0.00371), 고칸(0.00369), 4D(0.00366), 헬로키티(0.00322), 톡핑(0.00316)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,051행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
