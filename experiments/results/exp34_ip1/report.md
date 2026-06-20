# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=64, num_layers=1, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8060 | 0.9420 | 0.7709 | 0.5843 |
| val | 0.7310 | 0.8629 | 0.6550 | 0.6201 |
| test | 0.6719 | 0.8647 | 0.6562 | 0.7311 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__rev_sim_kw__product`: 0.1762
- `product__co_offline__product`: 0.1511
- `product__sim_kw__product`: 0.1002
- `product__rev_co_quick__product`: 0.0768
- `product__rev_co_offline__product`: 0.0723
- `product__sim_ip__product`: 0.0715
- `product__rev_sim_ip__product`: 0.0619
- `product__co_quick__product`: 0.0444
- `keyword__rev_has_kw__product`: 0.0182
- `product__has_kw__keyword`: 0.0175
- `ip__has_kw__keyword`: 0.0175
- `keyword__trend_to__keyword`: 0.0175
- `product__has_ip__ip`: 0.0175
- `ip__has_ip__ip`: 0.0175
- `product__has_kw_via_ip__keyword`: 0.0175
- `product__has_kw_ipip__keyword`: 0.0175
- `keyword__rev_has_kw__ip`: 0.0175
- `keyword__rev_trend_to__keyword`: 0.0175
- `ip__rev_has_ip__ip`: 0.0175
- `keyword__rev_has_kw_ipip__product`: 0.0175
- `keyword__rev_has_kw_via_ip__product`: 0.0175
- `ip__rev_has_ip__product`: 0.0173

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04061), 향라(0.03944), 탄탄(0.03921), 샹궈(0.01677), 피(0.01328), 적음(0.01308), 짜파게티(0.01304), 중독성(0.01277), 자극적(0.00904), 곤약(0.00830)
- **로제** → 늘어남(0.01504), 중독성(0.01277), 마카로니(0.01003), 고단백(0.00547), 넘버원(0.00309), 누들(0.00295), 하트(0.00255), 당면(0.00244), 떡볶이(0.00244), 파마산(0.00241)
- **흑임자** → 공룡(0.02360), 알(0.01012), 파운드케이크(0.00412), 마카롱(0.00341), 묵직함(0.00228), 작은별(0.00204), 할매니얼(0.00128), 컵(0.00104), 바나나(0.00103), 케이크(0.00094)
- **단백질** → 헬스(0.01067), 엽떡(0.01047), 동물(0.00997), 베노프(0.00934), 밸런스밀(0.00718), 소이조이(0.00699), 이지프로틴(0.00604), 아르기닌(0.00604), 테이크핏(0.00519), 청키(0.00359)
- **위스키** → 블랙서클(0.00632), 예술(0.00576), 산토리(0.00507), 맥캘란(0.00324), 캐리비안(0.00290), 트레이스(0.00210), 캐스크(0.00207), 메이커스마크(0.00200), 블론드(0.00194), 아이리쉬(0.00191)
- **딸기** → 분홍(0.00449), 몽쉘(0.00402), 고칸(0.00359), 톡핑(0.00239), 통크(0.00236), 쌍둥이(0.00228), 트위스트(0.00210), 스키틀즈(0.00207), 저지방(0.00197), 아모스(0.00196)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
