# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7743 | 0.9319 | 0.7539 | 0.6633 |
| val | 0.6396 | 0.8123 | 0.6154 | 0.7508 |
| test | 0.6598 | 0.8294 | 0.6059 | 0.7804 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__has_ip__ip`: 0.0960
- `product__rev_co_quick__product`: 0.0841
- `ip__rev_has_ip__product`: 0.0841
- `ip__has_kw__keyword`: 0.0838
- `keyword__trend_to__keyword`: 0.0837
- `keyword__rev_has_kw__ip`: 0.0836
- `keyword__rev_trend_to__keyword`: 0.0835
- `product__co_quick__product`: 0.0834
- `keyword__rev_has_kw__product`: 0.0830
- `product__rev_co_offline__product`: 0.0829
- `product__co_offline__product`: 0.0796
- `product__has_kw__keyword`: 0.0725

**layer 1**
- `product__rev_co_offline__product`: 0.1768
- `product__co_offline__product`: 0.1420
- `ip__rev_has_ip__product`: 0.0860
- `product__rev_co_quick__product`: 0.0834
- `product__co_quick__product`: 0.0787
- `keyword__rev_has_kw__product`: 0.0621
- `product__has_kw__keyword`: 0.0618
- `ip__has_kw__keyword`: 0.0618
- `keyword__trend_to__keyword`: 0.0618
- `product__has_ip__ip`: 0.0618
- `keyword__rev_has_kw__ip`: 0.0618
- `keyword__rev_trend_to__keyword`: 0.0618

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시(0.03914), 향라(0.03902), 탄탄(0.03454), 중독성(0.01641), 샹궈(0.01486), 피(0.01321), 짜파게티(0.01272), 누들핏(0.01151), 적음(0.01149), 마라탕(0.00756)
- **로제** → 로제와인(0.01927), 원데이(0.01605), 바리스타룰스(0.01605), 중독성(0.01577), 늘어남(0.01370), 앙리(0.01340), 로트캡션(0.01072), 마카로니(0.00914), 앙리마티스(0.00893), 대왕(0.00880)
- **흑임자** → 공룡(0.01807), 롱롱이(0.01687), 시루떡(0.01385), 컵케익(0.01352), 알(0.00902), 비비빅(0.00700), 마카롱(0.00216), 묵직함(0.00175), 찰떡(0.00144), 할매니얼(0.00081)
- **단백질** → 헬스(0.00956), 베노프(0.00950), 엽떡(0.00934), 동물(0.00913), 이지프로틴(0.00792), 아르기닌(0.00792), 밸런스밀(0.00675), 편함(0.00557), 소이조이(0.00481), 프로틴바(0.00471)
- **위스키** → 블랙서클(0.00622), 빔산토리(0.00555), 산토리(0.00555), 티처스위스키(0.00555), 컬렉션(0.00479), 예술(0.00462), 더글렌드로낙(0.00429), 메이커스마크(0.00321), 12년(0.00242), 스트레이트(0.00233)
- **딸기** → 분홍(0.00431), 트롤(0.00417), 키티(0.00397), 돌직구(0.00385), 니타(0.00378), 4D(0.00377), 고칸(0.00377), 몽쉘(0.00361), 쿠냥이(0.00344), 빅파이(0.00335)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (41,335행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,161행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
