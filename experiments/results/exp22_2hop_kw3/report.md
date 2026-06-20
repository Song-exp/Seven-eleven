# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=1, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8375 | 0.9551 | 0.8075 | 0.7951 |
| val | 0.7482 | 0.8684 | 0.6688 | 0.8787 |
| test | 0.6744 | 0.8649 | 0.6650 | 0.7307 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__sim_kw__product`: 0.2240
- `product__co_offline__product`: 0.1883
- `product__rev_sim_kw__product`: 0.1847
- `keyword__rev_has_kw__product`: 0.0719
- `product__rev_co_quick__product`: 0.0579
- `product__rev_co_offline__product`: 0.0539
- `product__co_quick__product`: 0.0346
- `product__rev_sim_ip__product`: 0.0177
- `product__sim_ip__product`: 0.0172
- `ip__rev_has_ip__product`: 0.0169
- `product__has_kw__keyword`: 0.0166
- `ip__has_kw__keyword`: 0.0166
- `keyword__trend_to__keyword`: 0.0166
- `product__has_ip__ip`: 0.0166
- `ip__has_ip__ip`: 0.0166
- `keyword__rev_has_kw__ip`: 0.0166
- `keyword__rev_trend_to__keyword`: 0.0166
- `ip__rev_has_ip__ip`: 0.0166

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04194), 향라(0.04098), 탄탄(0.03975), 샹궈(0.02002), 중독성(0.01676), 피(0.01359), 짜파게티(0.01356), 적음(0.01325), 곤약(0.01050), 중식(0.00881)
- **로제** → 늘어남(0.01881), 중독성(0.01676), 마카로니(0.01254), 고단백(0.00562), 당면(0.00538), 파마산(0.00348), 떡볶이(0.00322), 누들(0.00303), 하트(0.00280), 분식(0.00246)
- **흑임자** → 공룡(0.02346), 알(0.01006), 마카롱(0.00321), 묵직함(0.00227), 파운드케이크(0.00113), 컵(0.00103), 바나나(0.00097), 바닐라(0.00072), 케이크(0.00066), 담백(0.00043)
- **단백질** → 헬스(0.01106), 엽떡(0.01082), 동물(0.01060), 베노프(0.00955), 백반(0.00725), 밸런스밀(0.00717), 소이조이(0.00657), 이지프로틴(0.00653), 아르기닌(0.00653), 테이크핏(0.00543)
- **위스키** → 블랙서클(0.00643), 예술(0.00588), 산토리(0.00463), 트레이스(0.00323), 메이커스마크(0.00321), 맥캘란(0.00315), 클레이모어(0.00309), 캐리비안(0.00300), 블론드(0.00284), 신년(0.00233)
- **딸기** → 분홍(0.00451), 몽쉘(0.00419), 고칸(0.00410), 톡핑(0.00389), 픽업(0.00276), 포장(0.00276), 쌍둥이(0.00227), 트위스트(0.00218), 스키틀즈(0.00202), 저지방(0.00199)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
