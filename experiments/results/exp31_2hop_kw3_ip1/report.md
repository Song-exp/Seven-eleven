# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=1, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7992 | 0.9415 | 0.7592 | 0.7385 |
| val | 0.7281 | 0.8611 | 0.6531 | 0.7139 |
| test | 0.6670 | 0.8633 | 0.6533 | 0.8565 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.1973
- `product__rev_sim_kw__product`: 0.0816
- `product__sim_kw__product`: 0.0780
- `product__rev_co_offline__product`: 0.0776
- `product__rev_sim_ip__product`: 0.0766
- `product__rev_co_quick__product`: 0.0758
- `product__sim_ip__product`: 0.0749
- `product__co_quick__product`: 0.0604
- `keyword__rev_has_kw__product`: 0.0344
- `product__has_kw__keyword`: 0.0271
- `ip__has_kw__keyword`: 0.0271
- `keyword__trend_to__keyword`: 0.0271
- `product__has_ip__ip`: 0.0271
- `ip__has_ip__ip`: 0.0271
- `keyword__rev_has_kw__ip`: 0.0271
- `keyword__rev_trend_to__keyword`: 0.0271
- `ip__rev_has_ip__ip`: 0.0271
- `ip__rev_has_ip__product`: 0.0266

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04116), 향라(0.04011), 탄탄(0.03952), 샹궈(0.01879), 피(0.01353), 짜파게티(0.01319), 적음(0.01317), 중독성(0.01259), 곤약(0.01014), 자극적(0.01011)
- **로제** → 늘어남(0.01847), 중독성(0.01259), 마카로니(0.01231), 고단백(0.00553), 파마산(0.00317), 누들(0.00298), 분식(0.00274), 하트(0.00265), 떡볶이(0.00245), 넘버원(0.00226)
- **흑임자** → 공룡(0.02360), 알(0.01011), 파운드케이크(0.00475), 마카롱(0.00319), 묵직함(0.00228), 작은별(0.00165), 컵(0.00104), 케이크(0.00098), 바나나(0.00097), 아몬드(0.00094)
- **단백질** → 헬스(0.01084), 엽떡(0.01064), 동물(0.01033), 베노프(0.00914), 소이조이(0.00827), 밸런스밀(0.00757), 이지프로틴(0.00666), 아르기닌(0.00666), 테이크핏(0.00538), 글루텐프리(0.00419)
- **위스키** → 블랙서클(0.00634), 예술(0.00601), 산토리(0.00478), 맥캘란(0.00419), 캐리비안(0.00309), 메이커스마크(0.00305), 아이리쉬(0.00301), 트레이스(0.00277), 블론드(0.00263), 시바스리갈(0.00262)
- **딸기** → 분홍(0.00448), 몽쉘(0.00422), 고칸(0.00398), 톡핑(0.00343), 통크(0.00333), 쌍둥이(0.00228), 치토스(0.00222), 트위스트(0.00212), 스키틀즈(0.00209), 아모스(0.00202)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
