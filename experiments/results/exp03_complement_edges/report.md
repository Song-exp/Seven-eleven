# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7510 | 0.9203 | 0.7247 | 0.6482 |
| val | 0.5138 | 0.7663 | 0.5308 | 0.5710 |
| test | 0.5464 | 0.7883 | 0.5529 | 0.7097 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__has_kw__keyword`: 0.1035
- `keyword__trend_to__keyword`: 0.0998
- `product__has_ip__ip`: 0.0998
- `keyword__rev_trend_to__keyword`: 0.0998
- `product__complement__product`: 0.0997
- `product__rev_complement__product`: 0.0997
- `keyword__rev_has_kw__ip`: 0.0997
- `ip__has_kw__keyword`: 0.0997
- `ip__rev_has_ip__product`: 0.0994
- `keyword__rev_has_kw__product`: 0.0988

**layer 1**
- `product__has_kw__keyword`: 0.1090
- `ip__has_kw__keyword`: 0.1090
- `keyword__trend_to__keyword`: 0.1090
- `product__has_ip__ip`: 0.1090
- `product__complement__product`: 0.1090
- `keyword__rev_has_kw__ip`: 0.1090
- `keyword__rev_trend_to__keyword`: 0.1090
- `product__rev_complement__product`: 0.1090
- `keyword__rev_has_kw__product`: 0.0680
- `ip__rev_has_ip__product`: 0.0598

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시(0.03698), 향라(0.03537), 탄탄(0.03279), 박은영(0.02119), 샹궈(0.01905), 중독성(0.01893), 짜파게티(0.01268), 누들핏(0.01093), 적음(0.01089), 피(0.00996)
- **로제** → 중독성(0.01817), 로제와인(0.01708), 로트캡션(0.01512), 늘어남(0.01508), 대왕(0.01064), 마카로니(0.01008), 납작(0.00647), 앙리(0.00488), 파마산(0.00463), 럭히밥(0.00456)
- **흑임자** → 롱롱이(0.02021), 시루떡(0.01905), 공룡(0.01640), 컵케익(0.01232), 비비빅(0.00952), 알(0.00814), 파운드(0.00302), 마카롱(0.00210), 찰떡(0.00204), 드레싱(0.00183)
- **단백질** → 이지프로틴(0.00945), 아르기닌(0.00945), 베노프(0.00923), 밸런스밀(0.00899), 동물(0.00894), 엽떡(0.00892), 헬스(0.00829), 편함(0.00825), 플러스(0.00737), 프로틴바(0.00555)
- **위스키** → 빔산토리(0.00584), 산토리(0.00584), 티처스위스키(0.00584), 블랙서클(0.00583), 컬렉션(0.00565), 더글렌드로낙(0.00542), 예술(0.00536), 노마드리저브(0.00450), 트레이스(0.00444), 클레이모어(0.00421)
- **딸기** → 분홍(0.00434), 돌직구(0.00421), 트롤(0.00419), 키티(0.00415), 고칸(0.00405), 니타(0.00404), 픽업(0.00377), 포장(0.00377), 빅파이(0.00366), 4D(0.00365)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (41,335행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,161행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
