# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=1, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8102 | 0.9465 | 0.7856 | 0.5959 |
| val | 0.7306 | 0.8637 | 0.6723 | 0.7584 |
| test | 0.6753 | 0.8636 | 0.6667 | 0.7705 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.2241
- `product__rev_sim_kw__product`: 0.1030
- `product__rev_sim_ip__product`: 0.0928
- `product__sim_kw__product`: 0.0926
- `product__sim_ip__product`: 0.0893
- `product__rev_co_offline__product`: 0.0809
- `product__rev_co_quick__product`: 0.0685
- `product__co_quick__product`: 0.0370
- `product__has_kw__keyword`: 0.0265
- `ip__has_kw__keyword`: 0.0265
- `keyword__trend_to__keyword`: 0.0265
- `product__has_ip__ip`: 0.0265
- `keyword__rev_has_kw__ip`: 0.0265
- `keyword__rev_trend_to__keyword`: 0.0265
- `ip__rev_has_ip__product`: 0.0264
- `keyword__rev_has_kw__product`: 0.0263

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04136), 향라(0.04012), 탄탄(0.03912), 샹궈(0.01866), 박은영(0.01436), 피(0.01367), 적음(0.01304), 짜파게티(0.01269), 중독성(0.01076), 마라탕(0.00923)
- **로제** → 늘어남(0.01704), 마카로니(0.01136), 중독성(0.01031), 고단백(0.00370), 파마산(0.00311), 누들(0.00285), 하트(0.00253), 당면(0.00229), 분식(0.00227), 바리스타룰스(0.00210)
- **흑임자** → 공룡(0.01945), 알(0.00834), 시루떡(0.00725), 비비빅(0.00363), 롱롱이(0.00346), 파운드케이크(0.00299), 마카롱(0.00270), 묵직함(0.00188), 컵(0.00086), 작은별(0.00083)
- **단백질** → 헬스(0.00951), 엽떡(0.00941), 동물(0.00916), 베노프(0.00774), 소이조이(0.00609), 밸런스밀(0.00588), 이지프로틴(0.00487), 아르기닌(0.00487), 플러스(0.00480), 테이크핏(0.00473)
- **위스키** → 블랙서클(0.00635), 예술(0.00594), 산토리(0.00427), 티처스위스키(0.00427), 컬렉션(0.00364), 키싱(0.00351), 맥캘란(0.00345), 캐리비안(0.00301), 메이커스마크(0.00285), 12년(0.00269)
- **딸기** → 분홍(0.00441), 몽쉘(0.00417), 돌직구(0.00381), 4D(0.00375), 고칸(0.00367), 쿠냥이(0.00318), 데이(0.00279), 톡핑(0.00265), 쌍둥이(0.00223), 통크(0.00222)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
