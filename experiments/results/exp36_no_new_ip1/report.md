# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=64, num_layers=1, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7451 | 0.9094 | 0.7062 | 0.5746 |
| val | 0.7375 | 0.8638 | 0.6599 | 0.6143 |
| test | 0.6730 | 0.8660 | 0.6551 | 0.7714 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.1480
- `product__sim_kw__product`: 0.1304
- `product__rev_co_offline__product`: 0.1164
- `product__rev_sim_kw__product`: 0.1078
- `product__rev_sim_ip__product`: 0.0941
- `product__sim_ip__product`: 0.0811
- `product__rev_co_quick__product`: 0.0430
- `product__co_quick__product`: 0.0280
- `ip__rev_has_ip__product`: 0.0252
- `product__has_kw__keyword`: 0.0251
- `ip__has_kw__keyword`: 0.0251
- `keyword__trend_to__keyword`: 0.0251
- `product__has_ip__ip`: 0.0251
- `ip__has_ip__ip`: 0.0251
- `keyword__rev_has_kw__ip`: 0.0251
- `keyword__rev_trend_to__keyword`: 0.0251
- `ip__rev_has_ip__ip`: 0.0251
- `keyword__rev_has_kw__product`: 0.0249

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04180), 향라(0.04142), 탄탄(0.04087), 샹궈(0.02186), 중독성(0.01440), 피(0.01408), 적음(0.01362), 짜파게티(0.01257), 자극적(0.01077), 곤약(0.00930)
- **로제** → 중독성(0.01440), 늘어남(0.01280), 마카로니(0.00853), 고단백(0.00576), 당면(0.00313), 누들(0.00310), 하트(0.00274), 파마산(0.00269), 떡볶이(0.00256), 분식(0.00226)
- **흑임자** → 공룡(0.02360), 알(0.01011), 작은별(0.00464), 파운드케이크(0.00412), 마카롱(0.00281), 할매니얼(0.00259), 묵직함(0.00228), 드레싱(0.00163), 컵(0.00104), 케이크(0.00096)
- **단백질** → 헬스(0.01106), 엽떡(0.01101), 동물(0.01089), 베노프(0.01011), 소이조이(0.00803), 밸런스밀(0.00700), 이지프로틴(0.00652), 아르기닌(0.00652), 테이크핏(0.00619), 피쉬(0.00414)
- **위스키** → 블랙서클(0.00642), 산토리(0.00398), 블론드(0.00285), 예술(0.00278), 트레이스(0.00278), 클레이모어(0.00264), 메이커스마크(0.00238), 신년(0.00199), 임페리얼(0.00154), 글렌피딕(0.00149)
- **딸기** → 분홍(0.00454), 고칸(0.00396), 몽쉘(0.00356), 통크(0.00324), 치토스(0.00319), 픽업(0.00313), 포장(0.00313), 톡핑(0.00258), 쌍둥이(0.00228), 트위스트(0.00218)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
