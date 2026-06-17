# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=3, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8315 | 0.9520 | 0.8047 | 0.4989 |
| val | 0.7192 | 0.8533 | 0.6686 | 0.8466 |
| test | 0.6665 | 0.8584 | 0.6525 | 0.8197 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__rev_co_quick__product`: 0.2459
- `product__co_offline__product`: 0.1859
- `product__rev_co_offline__product`: 0.0856
- `product__co_quick__product`: 0.0636
- `keyword__rev_has_kw__product`: 0.0537
- `ip__rev_has_ip__product`: 0.0534
- `ip__has_kw__keyword`: 0.0532
- `keyword__rev_trend_to__keyword`: 0.0532
- `keyword__trend_to__keyword`: 0.0532
- `keyword__rev_has_kw__ip`: 0.0532
- `product__has_kw__keyword`: 0.0518
- `product__has_ip__ip`: 0.0472

**layer 1**
- `product__co_offline__product`: 0.2209
- `product__rev_co_quick__product`: 0.1212
- `product__rev_co_offline__product`: 0.1087
- `product__co_quick__product`: 0.1033
- `keyword__rev_trend_to__keyword`: 0.0588
- `product__has_ip__ip`: 0.0588
- `product__has_kw__keyword`: 0.0588
- `keyword__trend_to__keyword`: 0.0588
- `ip__has_kw__keyword`: 0.0588
- `keyword__rev_has_kw__ip`: 0.0588
- `keyword__rev_has_kw__product`: 0.0467
- `ip__rev_has_ip__product`: 0.0465

**layer 2**
- `product__co_offline__product`: 0.1940
- `product__rev_co_quick__product`: 0.1266
- `product__rev_co_offline__product`: 0.1203
- `product__co_quick__product`: 0.0963
- `product__has_kw__keyword`: 0.0614
- `ip__has_kw__keyword`: 0.0614
- `keyword__trend_to__keyword`: 0.0614
- `product__has_ip__ip`: 0.0614
- `keyword__rev_has_kw__ip`: 0.0614
- `keyword__rev_trend_to__keyword`: 0.0614
- `keyword__rev_has_kw__product`: 0.0472
- `ip__rev_has_ip__product`: 0.0471

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04234), 향라(0.04025), 탄탄(0.03941), 박은영(0.01880), 중독성(0.01479), 짜파게티(0.01393), 샹궈(0.01355), 피(0.01341), 적음(0.01305), 마라탕(0.00895)
- **로제** → 떡볶퀸(0.01833), 늘어남(0.01786), 원데이(0.01549), 바리스타룰스(0.01545), 중독성(0.01427), 마카로니(0.01197), 고단백(0.00377), 당면(0.00355), 하트(0.00305), 누들(0.00293)
- **흑임자** → 공룡(0.01884), 알(0.00800), 마카롱(0.00232), 묵직함(0.00182), 파운드케이크(0.00136), 컵(0.00082), KBO(0.00076), 바나나(0.00064), 케이크(0.00055), 바닐라(0.00038)
- **단백질** → 헬스(0.00965), 동물(0.00932), 엽떡(0.00930), 편함(0.00840), 베노프(0.00773), 테이크핏(0.00454), 소이조이(0.00431), 청키(0.00324), 피쉬(0.00319), 저지방(0.00280)
- **위스키** → 블랙서클(0.00651), 산토리(0.00592), 티처스위스키(0.00592), 예술(0.00590), 맥캘란(0.00437), 스프레드(0.00279), 캐리비안(0.00271), 건조(0.00268), 스트레이트(0.00265), 쉐리(0.00193)
- **딸기** → 분홍(0.00437), 4D(0.00411), 몽쉘(0.00409), 고칸(0.00398), 돌직구(0.00398), 쿠냥이(0.00394), 톡핑(0.00362), 데이(0.00276), 헬로키티(0.00251), 쌍둥이(0.00219)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
