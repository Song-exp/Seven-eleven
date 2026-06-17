# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8319 | 0.9508 | 0.7994 | 0.3780 |
| val | 0.7117 | 0.8497 | 0.6429 | 0.8313 |
| test | 0.6572 | 0.8518 | 0.6508 | 0.5181 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.2337
- `product__rev_co_quick__product`: 0.1567
- `product__rev_co_offline__product`: 0.1093
- `product__co_quick__product`: 0.0739
- `product__has_kw__keyword`: 0.0554
- `ip__rev_has_ip__product`: 0.0541
- `keyword__rev_trend_to__keyword`: 0.0532
- `ip__has_kw__keyword`: 0.0532
- `keyword__rev_has_kw__ip`: 0.0532
- `keyword__trend_to__keyword`: 0.0532
- `keyword__rev_has_kw__product`: 0.0528
- `product__has_ip__ip`: 0.0515

**layer 1**
- `product__co_offline__product`: 0.1840
- `product__rev_co_offline__product`: 0.1241
- `product__co_quick__product`: 0.1127
- `product__rev_co_quick__product`: 0.1093
- `product__has_kw__keyword`: 0.0611
- `ip__has_kw__keyword`: 0.0611
- `keyword__trend_to__keyword`: 0.0611
- `product__has_ip__ip`: 0.0611
- `keyword__rev_has_kw__ip`: 0.0611
- `keyword__rev_trend_to__keyword`: 0.0611
- `ip__rev_has_ip__product`: 0.0609
- `keyword__rev_has_kw__product`: 0.0422

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04147), 향라(0.03911), 탄탄(0.03787), 짜파게티(0.01325), 적음(0.01260), 샹궈(0.01189), 피(0.01162), 마라탕(0.00767), 곤약(0.00724), 자극적(0.00662)
- **로제** → 떡볶퀸(0.01756), 늘어남(0.01648), 마카로니(0.01097), 하트(0.00424), 앙리(0.00421), 고단백(0.00352), 당면(0.00311), 중독성(0.00295), 누들(0.00271), 토핑(0.00206)
- **흑임자** → 공룡(0.01890), 알(0.00808), 마카롱(0.00254), 묵직함(0.00183), 컵(0.00083), KBO(0.00077), 바나나(0.00070), 시루떡(0.00057), 케이크(0.00045), 롱롱이(0.00044)
- **단백질** → 헬스(0.00925), 동물(0.00906), 엽떡(0.00896), 편함(0.00851), 이지프로틴(0.00684), 아르기닌(0.00684), 베노프(0.00641), 테이크핏(0.00436), 소이조이(0.00399), 피쉬(0.00319)
- **위스키** → 블랙서클(0.00643), 산토리(0.00601), 티처스위스키(0.00601), 예술(0.00587), 맥캘란(0.00526), 메이커스마크(0.00439), 후드티(0.00413), 스트레이트(0.00287), 스프레드(0.00284), 캐리비안(0.00272)
- **딸기** → 분홍(0.00434), 몽쉘(0.00413), 4D(0.00400), 돌직구(0.00397), 쿠냥이(0.00394), 고칸(0.00386), 데이(0.00270), 톡핑(0.00241), 쌍둥이(0.00218), 배트(0.00213)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
