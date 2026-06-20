# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=64, num_layers=1, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7542 | 0.9143 | 0.7105 | 0.5021 |
| val | 0.7317 | 0.8651 | 0.6588 | 0.7605 |
| test | 0.6858 | 0.8694 | 0.6558 | 0.7152 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.1564
- `product__rev_co_offline__product`: 0.1225
- `product__rev_co_quick__product`: 0.0882
- `product__sim_ip__product`: 0.0723
- `product__rev_sim_ip__product`: 0.0686
- `product__co_quick__product`: 0.0536
- `product__rev_sim_kw__product`: 0.0398
- `product__sim_kw__product`: 0.0365
- `keyword__rev_has_kw__product`: 0.0228
- `ip__rev_has_ip__product`: 0.0228
- `keyword__rev_has_kw_ipip__product`: 0.0227
- `product__has_kw__keyword`: 0.0226
- `ip__has_kw__keyword`: 0.0226
- `keyword__trend_to__keyword`: 0.0226
- `product__has_ip__ip`: 0.0226
- `ip__has_ip__ip`: 0.0226
- `product__has_kw_via_ip__keyword`: 0.0226
- `product__has_kw_ipip__keyword`: 0.0226
- `product__has_kw_trend__keyword`: 0.0226
- `keyword__rev_has_kw__ip`: 0.0226
- `keyword__rev_trend_to__keyword`: 0.0226
- `ip__rev_has_ip__ip`: 0.0226
- `keyword__rev_has_kw_via_ip__product`: 0.0225
- `keyword__rev_has_kw_trend__product`: 0.0224

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04093), 향라(0.03881), 탄탄(0.03757), 샹궈(0.02043), 피(0.01350), 중독성(0.01255), 적음(0.01252), 짜파게티(0.01248), 자극적(0.01050), 곤약(0.00904)
- **로제** → 늘어남(0.01364), 중독성(0.01254), 마카로니(0.00909), 고단백(0.00527), 당면(0.00291), 누들(0.00284), 하트(0.00262), 떡볶이(0.00241), 분식(0.00230), 파마산(0.00230)
- **흑임자** → 공룡(0.02341), 알(0.01004), 파운드케이크(0.00387), 작은별(0.00312), 마카롱(0.00268), 묵직함(0.00227), 할매니얼(0.00172), 컵(0.00103), 케이크(0.00092), 드레싱(0.00086)
- **단백질** → 헬스(0.01069), 엽떡(0.01029), 동물(0.01015), 베노프(0.00937), 소이조이(0.00704), 밸런스밀(0.00577), 테이크핏(0.00571), 이지프로틴(0.00551), 아르기닌(0.00551), 피쉬(0.00366)
- **위스키** → 블랙서클(0.00613), 예술(0.00367), 산토리(0.00361), 블론드(0.00246), 트레이스(0.00231), 클레이모어(0.00221), 메이커스마크(0.00202), 신년(0.00166), 조니워커(0.00122), 임페리얼(0.00120)
- **딸기** → 분홍(0.00446), 고칸(0.00392), 몽쉘(0.00363), 통크(0.00319), 치토스(0.00308), 픽업(0.00289), 포장(0.00289), 톡핑(0.00264), 쌍둥이(0.00226), 트위스트(0.00212)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
