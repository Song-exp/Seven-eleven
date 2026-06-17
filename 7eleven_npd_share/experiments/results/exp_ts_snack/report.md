# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8328 | 0.9566 | 0.8240 | 0.7567 |
| val | 0.6371 | 0.8083 | 0.5794 | 0.8740 |
| test | 0.6288 | 0.8114 | 0.6082 | 0.8637 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `ip__has_kw__keyword`: 0.1031
- `keyword__rev_has_kw__ip`: 0.1031
- `keyword__rev_trend_to__keyword`: 0.1031
- `keyword__rev_has_kw__product`: 0.1029
- `product__co_quick__product`: 0.1029
- `ip__rev_has_ip__product`: 0.1028
- `keyword__trend_to__keyword`: 0.1025
- `product__rev_co_quick__product`: 0.1019
- `product__has_ip__ip`: 0.0937
- `product__has_kw__keyword`: 0.0842

**layer 1**
- `product__rev_co_quick__product`: 0.1287
- `product__co_quick__product`: 0.1218
- `product__has_kw__keyword`: 0.1083
- `ip__has_kw__keyword`: 0.1083
- `keyword__trend_to__keyword`: 0.1083
- `product__has_ip__ip`: 0.1083
- `keyword__rev_has_kw__ip`: 0.1083
- `keyword__rev_trend_to__keyword`: 0.1083
- `ip__rev_has_ip__product`: 0.0672
- `keyword__rev_has_kw__product`: 0.0326

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04184), 중독성(0.02069), 박은영(0.01709), 짜파게티(0.01390), 마라탕(0.00995), 탄탄(0.00671), 정통(0.00397), 마파(0.00345), 중식(0.00285), 깊은맛(0.00242)
- **로제** → 중독성(0.01981), 늘어남(0.01918), 마카로니(0.01278), 파마산(0.00432), 고단백(0.00388), 누들(0.00299), 당면(0.00292), 하트(0.00271), 납작(0.00266), 떡볶이(0.00259)
- **흑임자** → 공룡(0.01900), 알(0.00815), 마카롱(0.00267), 묵직함(0.00184), 컵(0.00084), KBO(0.00077), 바나나(0.00073), 빵또아(0.00046), 케이크(0.00045), 바닐라(0.00044)
- **단백질** → 베노프(0.00991), 헬스(0.00958), 동물(0.00955), 이지프로틴(0.00887), 아르기닌(0.00887), 엽떡(0.00872), 밸런스밀(0.00791), 널담(0.00711), 닥터유(0.00602), 테이크핏(0.00493)
- **위스키** → 블랙서클(0.00641), 예술(0.00581), 캐리비안(0.00309), 스트레이트(0.00297), 스프레드(0.00287), 맥캘란(0.00250), 콜드브루(0.00191), 쉐리(0.00191), 캐스크(0.00161), 메이커스마크(0.00159)
- **딸기** → 분홍(0.00430), 쿠냥이(0.00426), 돌직구(0.00425), 고칸(0.00417), 몽쉘(0.00416), 픽업(0.00416), 포장(0.00416), 4D(0.00403), 톡핑(0.00399), 헬로키티(0.00340)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,051행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
