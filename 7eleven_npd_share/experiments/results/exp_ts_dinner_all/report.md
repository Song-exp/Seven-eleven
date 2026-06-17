# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.9069 | 0.9838 | 0.9375 | 0.7228 |
| val | 0.6367 | 0.7955 | 0.5842 | 0.5822 |
| test | 0.6174 | 0.8028 | 0.5676 | 0.0625 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__has_ip__ip`: 0.1057
- `keyword__trend_to__keyword`: 0.1054
- `ip__has_kw__keyword`: 0.1052
- `product__co_quick__product`: 0.1052
- `keyword__rev_trend_to__keyword`: 0.1051
- `keyword__rev_has_kw__ip`: 0.1051
- `ip__rev_has_ip__product`: 0.1051
- `product__rev_co_quick__product`: 0.1050
- `keyword__rev_has_kw__product`: 0.1047
- `product__has_kw__keyword`: 0.0535

**layer 1**
- `product__has_kw__keyword`: 0.1219
- `ip__has_kw__keyword`: 0.1219
- `keyword__trend_to__keyword`: 0.1219
- `product__has_ip__ip`: 0.1219
- `keyword__rev_has_kw__ip`: 0.1219
- `keyword__rev_trend_to__keyword`: 0.1219
- `product__rev_co_quick__product`: 0.1170
- `product__co_quick__product`: 0.1054
- `ip__rev_has_ip__product`: 0.0458
- `keyword__rev_has_kw__product`: 0.0002

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04330), 중독성(0.01888), 짜파게티(0.01443), 짜장(0.00242), 로제(0.00157), 간장(0.00084), 떡볶이(0.00082), 깔끔(0.00075), 마라탕(0.00069), 라면(0.00068)
- **로제** → 늘어남(0.02044), 중독성(0.01798), 마카로니(0.01361), 고단백(0.00400), 누들(0.00309), 하트(0.00276), 저녁(0.00213), 떡볶이(0.00203), 스파클링(0.00178), 마라(0.00157)
- **흑임자** → 공룡(0.01962), 알(0.00842), 마카롱(0.00287), 묵직함(0.00190), 컵(0.00087), KBO(0.00080), 바나나(0.00078), 바닐라(0.00047), 케이크(0.00045), 콜라(0.00038)
- **단백질** → 헬스(0.01015), 엽떡(0.01007), 동물(0.01005), 베노프(0.00950), 코어리지(0.00838), 백반(0.00643), 테이크핏(0.00504), 오트(0.00502), 랩노쉬(0.00415), 청키(0.00342)
- **위스키** → 블랙서클(0.00661), 티처스하이랜드(0.00647), 예술(0.00628), 캐리비안(0.00318), 스프레드(0.00316), 스트레이트(0.00308), 콜드브루(0.00210), 맥캘란(0.00142), 캐스크(0.00139), 한국(0.00105)
- **딸기** → 고칸(0.00448), 분홍(0.00443), 톡핑(0.00434), 4D(0.00432), 몽쉘(0.00420), 쿠냥이(0.00399), 데이(0.00286), 헬로키티(0.00255), 액티비아(0.00240), 배트(0.00223)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,051행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
