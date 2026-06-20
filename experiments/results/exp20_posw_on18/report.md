# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=1, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=4.5), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7720 | 0.9277 | 0.7353 | 0.7673 |
| val | 0.7435 | 0.8659 | 0.6569 | 0.8428 |
| test | 0.6648 | 0.8614 | 0.6560 | 0.8252 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.2182
- `product__sim_kw__product`: 0.1638
- `product__rev_sim_kw__product`: 0.1145
- `product__rev_co_quick__product`: 0.0727
- `keyword__rev_has_kw__product`: 0.0678
- `product__rev_co_offline__product`: 0.0475
- `product__co_quick__product`: 0.0359
- `product__sim_ip__product`: 0.0321
- `product__rev_sim_ip__product`: 0.0314
- `ip__rev_has_ip__product`: 0.0313
- `product__has_kw__keyword`: 0.0231
- `ip__has_kw__keyword`: 0.0231
- `keyword__trend_to__keyword`: 0.0231
- `product__has_ip__ip`: 0.0231
- `ip__has_ip__ip`: 0.0231
- `keyword__rev_has_kw__ip`: 0.0231
- `keyword__rev_trend_to__keyword`: 0.0231
- `ip__rev_has_ip__ip`: 0.0231

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04255), 향라(0.04172), 탄탄(0.04088), 샹궈(0.02307), 중독성(0.01749), 피(0.01398), 짜파게티(0.01385), 적음(0.01363), 자극적(0.01242), 곤약(0.01058)
- **로제** → 늘어남(0.01759), 중독성(0.01749), 마카로니(0.01172), 고단백(0.00579), 파마산(0.00373), 당면(0.00336), 분식(0.00320), 떡볶이(0.00313), 누들(0.00312), 반반(0.00303)
- **흑임자** → 공룡(0.02371), 알(0.01016), 파운드케이크(0.00508), 마카롱(0.00311), 묵직함(0.00229), 컵(0.00105), 케이크(0.00102), 바나나(0.00094), 바닐라(0.00070), 건강(0.00052)
- **단백질** → 헬스(0.01113), 엽떡(0.01107), 동물(0.01076), 베노프(0.01046), 소이조이(0.00977), 밸런스밀(0.00827), 이지프로틴(0.00807), 아르기닌(0.00807), 테이크핏(0.00516), 글루텐프리(0.00483)
- **위스키** → 블랙서클(0.00650), 예술(0.00583), 산토리(0.00522), 아이리쉬(0.00487), 맥캘란(0.00472), 트레이스(0.00445), 클레이모어(0.00437), 메이커스마크(0.00437), 블론드(0.00434), 시바스리갈(0.00383)
- **딸기** → 분홍(0.00454), 몽쉘(0.00426), 고칸(0.00410), 통크(0.00377), 치토스(0.00364), 톡핑(0.00359), 픽업(0.00352), 포장(0.00352), 죠스바(0.00258), 쌍둥이(0.00229)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
