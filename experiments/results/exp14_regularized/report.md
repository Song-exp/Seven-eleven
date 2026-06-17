# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.4, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7952 | 0.9373 | 0.7453 | 0.7151 |
| val | 0.7453 | 0.8611 | 0.6706 | 0.8320 |
| test | 0.6473 | 0.8601 | 0.6313 | 0.8360 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.4713
- `product__rev_co_offline__product`: 0.1065
- `product__rev_co_quick__product`: 0.1002
- `product__co_quick__product`: 0.0826
- `keyword__rev_has_kw__product`: 0.0303
- `product__has_ip__ip`: 0.0299
- `keyword__rev_has_kw__ip`: 0.0299
- `keyword__trend_to__keyword`: 0.0299
- `keyword__rev_trend_to__keyword`: 0.0299
- `ip__has_kw__keyword`: 0.0299
- `product__has_kw__keyword`: 0.0299
- `ip__rev_has_ip__product`: 0.0295

**layer 1**
- `product__rev_co_offline__product`: 0.0834
- `product__co_offline__product`: 0.0834
- `product__rev_co_quick__product`: 0.0834
- `product__has_kw__keyword`: 0.0833
- `ip__has_kw__keyword`: 0.0833
- `keyword__trend_to__keyword`: 0.0833
- `product__has_ip__ip`: 0.0833
- `keyword__rev_has_kw__ip`: 0.0833
- `keyword__rev_trend_to__keyword`: 0.0833
- `product__co_quick__product`: 0.0833
- `keyword__rev_has_kw__product`: 0.0833
- `ip__rev_has_ip__product`: 0.0832

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04051), 향라(0.03857), 탄탄(0.03749), 샹궈(0.02182), 박은영(0.02034), 중독성(0.01649), 짜파게티(0.01301), 피(0.01270), 적음(0.01250), 자극적(0.01180)
- **로제** → 원데이(0.01804), 바리스타룰스(0.01804), 떡볶퀸(0.01750), 늘어남(0.01750), 중독성(0.01581), 마카로니(0.01167), 당면(0.00504), 넘버원(0.00401), 고단백(0.00356), 떡볶이(0.00343)
- **흑임자** → 공룡(0.01910), 시루떡(0.01627), 롱롱이(0.01078), 알(0.00818), 비비빅(0.00816), 파운드케이크(0.00384), 빵또아(0.00275), 마카롱(0.00268), 베이커리(0.00240), 묵직함(0.00185)
- **단백질** → 헬스(0.00919), 엽떡(0.00908), 동물(0.00900), 베노프(0.00885), 소이조이(0.00861), 밸런스밀(0.00754), 플러스(0.00733), 이지프로틴(0.00706), 아르기닌(0.00706), 편함(0.00654)
- **위스키** → 블랙서클(0.00616), 예술(0.00551), 티처스하이랜드(0.00547), 산토리(0.00492), 티처스위스키(0.00492), 컬렉션(0.00423), 키싱(0.00407), 메이커스마크(0.00394), 트레이스(0.00369), 클레이모어(0.00368)
- **딸기** → 분홍(0.00433), 몽쉘(0.00401), 고칸(0.00394), 돌직구(0.00390), 쿠냥이(0.00385), 톡핑(0.00374), 4D(0.00373), 통크(0.00369), 픽업(0.00362), 포장(0.00362)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
