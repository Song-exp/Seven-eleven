# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=3, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.9285 | 0.9821 | 0.8881 | 0.8222 |
| val | 0.5994 | 0.8166 | 0.6051 | 0.2620 |
| test | 0.5484 | 0.8093 | 0.5856 | 0.7544 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__has_kw__keyword`: 0.1000
- `keyword__rev_has_kw__product`: 0.1000
- `ip__rev_has_ip__ip`: 0.1000
- `keyword__trend_to__keyword`: 0.1000
- `ip__rev_has_ip__product`: 0.1000
- `ip__has_ip__ip`: 0.1000
- `ip__has_kw__keyword`: 0.1000
- `product__has_ip__ip`: 0.1000
- `keyword__rev_trend_to__keyword`: 0.1000
- `keyword__rev_has_kw__ip`: 0.1000

**layer 1**
- `product__has_ip__ip`: 0.1210
- `keyword__trend_to__keyword`: 0.1210
- `keyword__rev_trend_to__keyword`: 0.1210
- `ip__has_ip__ip`: 0.1210
- `ip__has_kw__keyword`: 0.1210
- `ip__rev_has_ip__ip`: 0.1210
- `keyword__rev_has_kw__ip`: 0.1210
- `product__has_kw__keyword`: 0.1210
- `keyword__rev_has_kw__product`: 0.0207
- `ip__rev_has_ip__product`: 0.0109

**layer 2**
- `ip__rev_has_ip__product`: 0.1000
- `keyword__rev_has_kw__product`: 0.1000
- `product__has_kw__keyword`: 0.1000
- `ip__has_kw__keyword`: 0.1000
- `keyword__trend_to__keyword`: 0.1000
- `product__has_ip__ip`: 0.1000
- `ip__has_ip__ip`: 0.1000
- `keyword__rev_has_kw__ip`: 0.1000
- `keyword__rev_trend_to__keyword`: 0.1000
- `ip__rev_has_ip__ip`: 0.1000

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04111), 향라(0.03939), 중독성(0.02026), 탄탄(0.01923), 짜파게티(0.01278), 중식(0.00762), 자극적(0.00750), 곤약(0.00724), 적음(0.00658), 마파(0.00580)
- **로제** → 중독성(0.01999), 늘어남(0.01956), 마카로니(0.01290), 당면(0.00478), 고단백(0.00465), 파마산(0.00388), 떡볶이(0.00327), 하트(0.00271), 누들(0.00243), 저녁(0.00221)
- **흑임자** → 공룡(0.02338), 알(0.01045), 마카롱(0.00365), 묵직함(0.00221), 바나나(0.00107), 컵(0.00105), 바닐라(0.00078), 케이크(0.00056), 콜라(0.00047), 딸기(0.00033)
- **단백질** → 동물(0.01096), 엽떡(0.01072), 베노프(0.00985), 이지프로틴(0.00937), 아르기닌(0.00937), 테이크핏(0.00557), 포카치아(0.00553), 닥터유(0.00519), 프로틴(0.00419), 청키(0.00347)
- **위스키** → 블랙서클(0.00683), 예술(0.00663), 산토리(0.00632), 캐리비안(0.00339), 캐스크(0.00156), 맥캘란(0.00144), 스카치(0.00120), 한국(0.00111), 쉐리(0.00100), 리저브(0.00092)
- **딸기** → 고칸(0.00458), 분홍(0.00437), 몽쉘(0.00425), 톡핑(0.00416), 쌍둥이(0.00220), 저지방(0.00214), 필링(0.00208), 후루츄(0.00206), 아모스(0.00204), 탕종(0.00203)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
