# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=64, num_layers=1, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7951 | 0.9364 | 0.7539 | 0.6026 |
| val | 0.7373 | 0.8703 | 0.6745 | 0.8077 |
| test | 0.6943 | 0.8689 | 0.6593 | 0.8052 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.1661
- `product__sim_kw__product`: 0.1170
- `product__rev_co_offline__product`: 0.1046
- `product__rev_sim_kw__product`: 0.0780
- `product__co_quick__product`: 0.0729
- `product__rev_co_quick__product`: 0.0679
- `product__rev_sim_ip__product`: 0.0372
- `product__sim_ip__product`: 0.0357
- `keyword__rev_has_kw_via_ip__product`: 0.0235
- `keyword__rev_has_kw__product`: 0.0235
- `ip__rev_has_ip__product`: 0.0230
- `keyword__rev_has_kw_ipip__product`: 0.0228
- `product__has_kw__keyword`: 0.0228
- `ip__has_kw__keyword`: 0.0228
- `keyword__trend_to__keyword`: 0.0228
- `product__has_ip__ip`: 0.0228
- `ip__has_ip__ip`: 0.0228
- `product__has_kw_via_ip__keyword`: 0.0228
- `product__has_kw_ipip__keyword`: 0.0228
- `keyword__rev_has_kw__ip`: 0.0228
- `keyword__rev_trend_to__keyword`: 0.0228
- `ip__rev_has_ip__ip`: 0.0228

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.03985), 향라(0.03868), 탄탄(0.03807), 샹궈(0.02081), 중독성(0.01411), 피(0.01318), 짜파게티(0.01272), 적음(0.01269), 자극적(0.00977), 곤약(0.00952)
- **로제** → 늘어남(0.01668), 중독성(0.01411), 마카로니(0.01112), 고단백(0.00535), 당면(0.00504), 파마산(0.00334), 떡볶이(0.00301), 누들(0.00288), 하트(0.00263), 분식(0.00250)
- **흑임자** → 공룡(0.02309), 알(0.00990), 파운드케이크(0.00382), 마카롱(0.00306), 묵직함(0.00224), 작은별(0.00158), 컵(0.00102), 바나나(0.00093), 할매니얼(0.00090), 케이크(0.00089)
- **단백질** → 헬스(0.01053), 엽떡(0.01031), 동물(0.01014), 베노프(0.00927), 소이조이(0.00693), 밸런스밀(0.00668), 이지프로틴(0.00644), 아르기닌(0.00644), 테이크핏(0.00531), 백반(0.00354)
- **위스키** → 블랙서클(0.00611), 예술(0.00571), 산토리(0.00463), 맥캘란(0.00347), 메이커스마크(0.00298), 트레이스(0.00296), 블론드(0.00296), 애스턴마틴(0.00283), 클레이모어(0.00282), 캐리비안(0.00276)
- **딸기** → 분홍(0.00434), 몽쉘(0.00401), 고칸(0.00392), 통크(0.00315), 픽업(0.00284), 포장(0.00284), 톡핑(0.00281), 치토스(0.00231), 쌍둥이(0.00224), 스키틀즈(0.00207)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
