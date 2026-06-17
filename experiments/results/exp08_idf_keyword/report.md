# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7939 | 0.9448 | 0.7891 | 0.6028 |
| val | 0.7222 | 0.8647 | 0.6740 | 0.7936 |
| test | 0.6718 | 0.8677 | 0.6512 | 0.8929 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__rev_co_quick__product`: 0.1890
- `product__co_offline__product`: 0.1518
- `product__rev_co_offline__product`: 0.1223
- `product__co_quick__product`: 0.0764
- `product__has_kw__keyword`: 0.0693
- `product__has_ip__ip`: 0.0570
- `keyword__rev_has_kw__product`: 0.0564
- `ip__rev_has_ip__product`: 0.0560
- `keyword__rev_trend_to__keyword`: 0.0554
- `keyword__trend_to__keyword`: 0.0554
- `ip__has_kw__keyword`: 0.0554
- `keyword__rev_has_kw__ip`: 0.0554

**layer 1**
- `product__co_offline__product`: 0.1907
- `product__rev_co_quick__product`: 0.1467
- `product__rev_co_offline__product`: 0.1423
- `product__co_quick__product`: 0.0756
- `ip__rev_has_ip__product`: 0.0610
- `product__has_kw__keyword`: 0.0558
- `ip__has_kw__keyword`: 0.0558
- `keyword__trend_to__keyword`: 0.0558
- `product__has_ip__ip`: 0.0558
- `keyword__rev_has_kw__ip`: 0.0558
- `keyword__rev_trend_to__keyword`: 0.0558
- `keyword__rev_has_kw__product`: 0.0488

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04159), 향라(0.04106), 탄탄(0.04098), 박은영(0.01881), 샹궈(0.01435), 짜파게티(0.01376), 피(0.01375), 적음(0.01363), 중독성(0.01315), 마라탕(0.00892)
- **로제** → 원데이(0.01914), 바리스타룰스(0.01914), 떡볶퀸(0.01795), 늘어남(0.01599), 중독성(0.01269), 마카로니(0.01068), 당면(0.00485), 고단백(0.00395), 히밥(0.00347), 볶이(0.00314)
- **흑임자** → 공룡(0.01879), 알(0.00798), 마카롱(0.00252), 파운드케이크(0.00211), 묵직함(0.00181), 컵(0.00082), KBO(0.00076), 바나나(0.00069), 케이크(0.00061), 바닐라(0.00042)
- **단백질** → 헬스(0.00976), 엽떡(0.00967), 동물(0.00966), 베노프(0.00915), 편함(0.00864), 이지프로틴(0.00671), 아르기닌(0.00671), 소이조이(0.00481), 테이크핏(0.00479), 청키(0.00325)
- **위스키** → 블랙서클(0.00633), 산토리(0.00577), 티처스위스키(0.00577), 예술(0.00571), 맥캘란(0.00410), 캐리비안(0.00260), 스프레드(0.00254), 스트레이트(0.00253), 건조(0.00236), 쉐리(0.00191)
- **딸기** → 분홍(0.00432), 4D(0.00415), 몽쉘(0.00404), 돌직구(0.00402), 고칸(0.00401), 쿠냥이(0.00396), 픽업(0.00368), 포장(0.00368), 톡핑(0.00365), 헬로키티(0.00315)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
