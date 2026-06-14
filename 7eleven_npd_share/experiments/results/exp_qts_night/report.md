# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8686 | 0.9690 | 0.8491 | 0.7576 |
| val | 0.6413 | 0.8008 | 0.6090 | 0.9240 |
| test | 0.6451 | 0.8125 | 0.6010 | 0.7224 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_quick__product`: 0.2957
- `product__rev_co_quick__product`: 0.2948
- `product__has_kw__keyword`: 0.1252
- `product__has_ip__ip`: 0.0468
- `keyword__rev_has_kw__product`: 0.0403
- `ip__rev_has_ip__product`: 0.0398
- `keyword__trend_to__keyword`: 0.0394
- `keyword__rev_trend_to__keyword`: 0.0394
- `keyword__rev_has_kw__ip`: 0.0393
- `ip__has_kw__keyword`: 0.0393

**layer 1**
- `product__co_quick__product`: 0.1216
- `product__rev_co_quick__product`: 0.1214
- `product__has_kw__keyword`: 0.1136
- `ip__has_kw__keyword`: 0.1136
- `keyword__trend_to__keyword`: 0.1136
- `product__has_ip__ip`: 0.1136
- `keyword__rev_has_kw__ip`: 0.1136
- `keyword__rev_trend_to__keyword`: 0.1136
- `ip__rev_has_ip__product`: 0.0622
- `keyword__rev_has_kw__product`: 0.0130

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04314), 중독성(0.02130), 짜파게티(0.01437), 박은영(0.01384), 중식(0.00403), 짜장(0.00240), 로제(0.00178), 우동(0.00118), 떡볶이(0.00093), 간장(0.00083)
- **로제** → 중독성(0.02040), 늘어남(0.01812), 마카로니(0.01208), 고단백(0.00404), 히밥(0.00401), 볶이(0.00361), 하트(0.00329), 누들(0.00311), 떡볶이(0.00242), 저녁(0.00217)
- **흑임자** → 공룡(0.01944), 알(0.00835), 롱롱이(0.00343), 마카롱(0.00273), 묵직함(0.00188), 컵(0.00086), KBO(0.00079), 바나나(0.00075), 케이크(0.00045), 바닐라(0.00045)
- **단백질** → 동물(0.01002), 엽떡(0.00995), 이지프로틴(0.00954), 아르기닌(0.00954), 테이크핏(0.00949), 베노프(0.00821), 감동란(0.00756), 헬스(0.00495), 쁘띠(0.00466), 랩노쉬(0.00384)
- **위스키** → 블랙서클(0.00657), 예술(0.00616), 메이커스마크(0.00395), 맥캘란(0.00353), 스프레드(0.00319), 캐리비안(0.00311), 스트레이트(0.00287), 콜드브루(0.00212), 쉐리(0.00149), 캐스크(0.00131)
- **딸기** → 분홍(0.00441), 고칸(0.00434), 4D(0.00432), 쿠냥이(0.00431), 톡핑(0.00423), 픽업(0.00419), 포장(0.00419), 몽쉘(0.00411), 헬로키티(0.00334), 데이(0.00287)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,051행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
