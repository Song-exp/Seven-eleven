# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=64, num_layers=1, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.05), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8426 | 0.9550 | 0.7958 | 0.6987 |
| val | 0.6600 | 0.8431 | 0.6516 | 0.6895 |
| test | 0.6375 | 0.8407 | 0.6230 | 0.6814 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__rev_co_offline__product`: 0.4228
- `product__co_offline__product`: 0.3916
- `product__co_quick__product`: 0.1058
- `keyword__rev_has_kw_trend__product`: 0.0053
- `product__has_kw__keyword`: 0.0042
- `ip__has_kw__keyword`: 0.0042
- `keyword__trend_to__keyword`: 0.0042
- `product__has_ip__ip`: 0.0042
- `ip__has_ip__ip`: 0.0042
- `product__has_kw_via_ip__keyword`: 0.0042
- `product__has_kw_ipip__keyword`: 0.0042
- `product__has_kw_trend__keyword`: 0.0042
- `keyword__rev_has_kw__ip`: 0.0042
- `keyword__rev_trend_to__keyword`: 0.0042
- `ip__rev_has_ip__ip`: 0.0042
- `keyword__rev_has_kw_ipip__product`: 0.0040
- `product__rev_sim_ip__product`: 0.0037
- `ip__rev_has_ip__product`: 0.0035
- `product__sim_ip__product`: 0.0034
- `keyword__rev_has_kw_via_ip__product`: 0.0034
- `keyword__rev_has_kw__product`: 0.0030
- `product__rev_co_quick__product`: 0.0027
- `product__sim_kw__product`: 0.0024
- `product__rev_sim_kw__product`: 0.0019

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 향라(0.03915), 도쿠시마(0.03860), 탄탄(0.03768), 샹궈(0.01400), 적음(0.01256), 중독성(0.01241), 피(0.01235), 짜파게티(0.01183), 자극적(0.00767), 곤약(0.00765)
- **로제** → 늘어남(0.01961), 마카로니(0.01307), 중독성(0.01241), 고단백(0.00500), 파마산(0.00350), 하트(0.00301), 당면(0.00270), 누들(0.00269), 떡볶이(0.00220), 납작(0.00204)
- **흑임자** → 공룡(0.02345), 알(0.01005), 마카롱(0.00354), 묵직함(0.00227), 바나나(0.00107), 컵(0.00103), 바닐라(0.00080), 케이크(0.00058), 작은별(0.00053), 콜라(0.00047)
- **단백질** → 엽떡(0.01016), 동물(0.00988), 헬스(0.00961), 베노프(0.00912), 밸런스밀(0.00819), 테이크핏(0.00698), 이지프로틴(0.00663), 아르기닌(0.00663), 닥터유(0.00562), 소이조이(0.00554)
- **위스키** → 블랙서클(0.00664), 예술(0.00654), 산토리(0.00556), 맥캘란(0.00504), 캐리비안(0.00330), 야마자키(0.00263), 캐스크(0.00258), 쉐리(0.00249), 건조(0.00223), 싱글몰트(0.00173)
- **딸기** → 분홍(0.00455), 몽쉘(0.00425), 고칸(0.00304), 톡핑(0.00278), 통크(0.00256), 쌍둥이(0.00228), 아모스(0.00202), 트위스트(0.00193), 후루츄(0.00193), 저지방(0.00178)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
