# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=64, num_layers=1, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7493 | 0.9132 | 0.7116 | 0.5372 |
| val | 0.7333 | 0.8669 | 0.6585 | 0.5641 |
| test | 0.6800 | 0.8711 | 0.6462 | 0.7678 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.1598
- `product__rev_co_offline__product`: 0.1366
- `product__sim_kw__product`: 0.1137
- `product__rev_co_quick__product`: 0.0950
- `product__co_quick__product`: 0.0837
- `product__rev_sim_kw__product`: 0.0782
- `ip__rev_has_ip__product`: 0.0353
- `keyword__rev_has_kw__product`: 0.0344
- `product__has_kw__keyword`: 0.0338
- `ip__has_kw__keyword`: 0.0338
- `keyword__trend_to__keyword`: 0.0338
- `product__has_ip__ip`: 0.0338
- `keyword__rev_has_kw__ip`: 0.0338
- `keyword__rev_trend_to__keyword`: 0.0338
- `product__sim_ip__product`: 0.0305
- `product__rev_sim_ip__product`: 0.0299

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.03998), 향라(0.03834), 탄탄(0.03789), 샹궈(0.01888), 중독성(0.01591), 박은영(0.01488), 피(0.01314), 짜파게티(0.01273), 적음(0.01263), 자극적(0.00934)
- **로제** → 원데이(0.01823), 바리스타룰스(0.01823), 중독성(0.01525), 늘어남(0.01230), 마카로니(0.00820), 떡볶퀸(0.00519), 고단백(0.00359), 당면(0.00306), 파마산(0.00303), 누들(0.00276)
- **흑임자** → 공룡(0.01894), 시루떡(0.01049), 롱롱이(0.00904), 알(0.00812), 비비빅(0.00545), 파운드케이크(0.00331), 작은별(0.00249), 마카롱(0.00233), 묵직함(0.00183), 할매니얼(0.00169)
- **단백질** → 헬스(0.00926), 엽떡(0.00906), 동물(0.00894), 베노프(0.00809), 소이조이(0.00606), 밸런스밀(0.00572), 이지프로틴(0.00509), 아르기닌(0.00509), 테이크핏(0.00504), 플러스(0.00491)
- **위스키** → 블랙서클(0.00600), 산토리(0.00412), 티처스위스키(0.00412), 컬렉션(0.00336), 키싱(0.00282), 예술(0.00251), 블론드(0.00249), 트레이스(0.00226), 클레이모어(0.00221), 메이커스마크(0.00204)
- **딸기** → 분홍(0.00424), 돌직구(0.00407), 고칸(0.00394), 4D(0.00388), 몽쉘(0.00350), 통크(0.00292), 쿠냥이(0.00287), 치토스(0.00270), 데이(0.00267), 톡핑(0.00257)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
