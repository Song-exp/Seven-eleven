# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=64, num_layers=1, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7801 | 0.9309 | 0.7465 | 0.6275 |
| val | 0.7366 | 0.8717 | 0.6650 | 0.6192 |
| test | 0.6867 | 0.8694 | 0.6510 | 0.8176 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.2014
- `product__rev_sim_kw__product`: 0.1451
- `product__sim_kw__product`: 0.1234
- `product__rev_co_offline__product`: 0.0829
- `product__rev_co_quick__product`: 0.0723
- `product__co_quick__product`: 0.0534
- `product__rev_sim_ip__product`: 0.0323
- `product__sim_ip__product`: 0.0319
- `keyword__rev_has_kw__product`: 0.0267
- `ip__rev_has_ip__product`: 0.0266
- `product__has_kw__keyword`: 0.0255
- `ip__has_kw__keyword`: 0.0255
- `keyword__trend_to__keyword`: 0.0255
- `product__has_ip__ip`: 0.0255
- `ip__has_ip__ip`: 0.0255
- `keyword__rev_has_kw__ip`: 0.0255
- `keyword__rev_trend_to__keyword`: 0.0255
- `ip__rev_has_ip__ip`: 0.0255

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04065), 향라(0.03979), 탄탄(0.03943), 샹궈(0.01938), 중독성(0.01630), 피(0.01349), 적음(0.01314), 짜파게티(0.01306), 자극적(0.01017), 곤약(0.00831)
- **로제** → 중독성(0.01630), 늘어남(0.01630), 마카로니(0.01086), 고단백(0.00555), 당면(0.00413), 파마산(0.00325), 누들(0.00299), 떡볶이(0.00289), 하트(0.00269), 분식(0.00251)
- **흑임자** → 공룡(0.02344), 알(0.01004), 파운드케이크(0.00466), 마카롱(0.00298), 작은별(0.00272), 할매니얼(0.00240), 묵직함(0.00227), 컵(0.00103), 케이크(0.00098), 바나나(0.00090)
- **단백질** → 헬스(0.01072), 엽떡(0.01061), 동물(0.01034), 베노프(0.00962), 소이조이(0.00783), 밸런스밀(0.00727), 이지프로틴(0.00625), 아르기닌(0.00625), 테이크핏(0.00539), 백반(0.00400)
- **위스키** → 블랙서클(0.00621), 예술(0.00547), 산토리(0.00504), 블론드(0.00260), 트레이스(0.00259), 캐리비안(0.00255), 클레이모어(0.00243), 맥캘란(0.00234), 메이커스마크(0.00232), 신년(0.00185)
- **딸기** → 분홍(0.00441), 고칸(0.00415), 몽쉘(0.00391), 통크(0.00292), 톡핑(0.00253), 치토스(0.00250), 쌍둥이(0.00227), 스키틀즈(0.00213), 트위스트(0.00210), 픽업(0.00208)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
