# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.9012 | 0.9776 | 0.8792 | 0.7192 |
| val | 0.6379 | 0.7899 | 0.5806 | 0.8385 |
| test | 0.6372 | 0.8092 | 0.6121 | 0.4033 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `ip__rev_has_ip__product`: 0.1039
- `product__co_quick__product`: 0.1038
- `product__has_ip__ip`: 0.1036
- `keyword__trend_to__keyword`: 0.1030
- `keyword__rev_trend_to__keyword`: 0.1030
- `ip__has_kw__keyword`: 0.1030
- `keyword__rev_has_kw__ip`: 0.1029
- `keyword__rev_has_kw__product`: 0.1015
- `product__rev_co_quick__product`: 0.0999
- `product__has_kw__keyword`: 0.0753

**layer 1**
- `product__co_quick__product`: 0.1476
- `product__rev_co_quick__product`: 0.1443
- `product__has_kw__keyword`: 0.1140
- `ip__has_kw__keyword`: 0.1140
- `keyword__trend_to__keyword`: 0.1140
- `product__has_ip__ip`: 0.1140
- `keyword__rev_has_kw__ip`: 0.1140
- `keyword__rev_trend_to__keyword`: 0.1140
- `ip__rev_has_ip__product`: 0.0166
- `keyword__rev_has_kw__product`: 0.0077

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04223), 중독성(0.02072), 짜파게티(0.01396), 박은영(0.01358), 마라탕(0.01008), 정통(0.00403), 중식(0.00376), 짜장(0.00233), 로제(0.00173), 국물(0.00111)
- **로제** → 늘어남(0.01985), 중독성(0.01985), 마카로니(0.01324), 파마산(0.00444), 고단백(0.00391), 당면(0.00317), 누들(0.00301), 납작(0.00274), 하트(0.00271), 떡볶이(0.00260)
- **흑임자** → 공룡(0.01907), 알(0.00817), 파운드케이크(0.00494), 마카롱(0.00268), 묵직함(0.00185), 케이크(0.00085), 컵(0.00084), KBO(0.00077), 바나나(0.00073), 건강(0.00049)
- **단백질** → 동물(0.00980), 엽떡(0.00957), 베노프(0.00879), 이지프로틴(0.00842), 아르기닌(0.00842), 테이크핏(0.00518), 닥터유(0.00400), 청키(0.00330), 피쉬(0.00288), 프로틴(0.00261)
- **위스키** → 블랙서클(0.00646), 예술(0.00613), 캐리비안(0.00294), 스트레이트(0.00290), 스프레드(0.00283), 콜드브루(0.00189), 캐스크(0.00164), 맥캘란(0.00123), 쉐리(0.00110), 한국(0.00102)
- **딸기** → 고칸(0.00433), 분홍(0.00432), 톡핑(0.00422), 쿠냥이(0.00419), 몽쉘(0.00414), 4D(0.00411), 헬로키티(0.00320), 데이(0.00266), 액티비아(0.00224), 배트(0.00217)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,051행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
