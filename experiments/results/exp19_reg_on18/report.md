# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=1, num_heads=4, dropout=0.4, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7842 | 0.9301 | 0.7284 | 0.4716 |
| val | 0.5755 | 0.8024 | 0.5744 | 0.4518 |
| test | 0.5991 | 0.8268 | 0.6176 | 0.4494 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_quick__product`: 0.0556
- `product__co_offline__product`: 0.0556
- `ip__rev_has_ip__product`: 0.0556
- `product__rev_co_offline__product`: 0.0556
- `product__sim_kw__product`: 0.0556
- `product__rev_sim_kw__product`: 0.0556
- `product__has_kw__keyword`: 0.0556
- `ip__has_kw__keyword`: 0.0556
- `keyword__trend_to__keyword`: 0.0556
- `product__has_ip__ip`: 0.0556
- `ip__has_ip__ip`: 0.0556
- `keyword__rev_has_kw__ip`: 0.0556
- `keyword__rev_trend_to__keyword`: 0.0556
- `ip__rev_has_ip__ip`: 0.0556
- `product__rev_co_quick__product`: 0.0556
- `product__sim_ip__product`: 0.0556
- `keyword__rev_has_kw__product`: 0.0556
- `product__rev_sim_ip__product`: 0.0556

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.02720), 향라(0.02457), 탄탄(0.02260), 샹궈(0.01313), 중독성(0.00971), 짜파게티(0.00780), 적음(0.00753), 피(0.00661), 중식(0.00653), 자극적(0.00440)
- **로제** → 중독성(0.00971), 늘어남(0.00736), 마카로니(0.00491), 당면(0.00477), 고단백(0.00311), 파마산(0.00255), 떡볶이(0.00231), 하트(0.00223), 토핑(0.00216), 밀(0.00194)
- **흑임자** → 공룡(0.01330), 알(0.00570), 마카롱(0.00281), 묵직함(0.00129), 바나나(0.00085), 바닐라(0.00063), 컵(0.00059), 콜라(0.00037), 케이크(0.00034), 파운드케이크(0.00026)
- **단백질** → 베노프(0.00643), 엽떡(0.00626), 동물(0.00624), 밸런스밀(0.00597), 이지프로틴(0.00574), 아르기닌(0.00574), 헬스(0.00561), 프로틴(0.00347), 소이조이(0.00318), 테이크핏(0.00252)
- **위스키** → 블랙서클(0.00478), 산토리(0.00370), 트레이스(0.00279), 메이커스마크(0.00275), 클레이모어(0.00219), 시바스리갈(0.00151), 임페리얼(0.00148), 립서비스(0.00142), 신년(0.00141), 버팔로(0.00140)
- **딸기** → 분홍(0.00420), 고칸(0.00359), 몽쉘(0.00272), 톡핑(0.00224), 쌍둥이(0.00145), 블라썸(0.00143), 탕종(0.00128), 저지방(0.00126), 필링(0.00122), 아모스(0.00120)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
