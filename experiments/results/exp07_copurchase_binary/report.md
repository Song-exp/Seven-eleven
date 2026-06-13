# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7395 | 0.9054 | 0.6946 | 0.4527 |
| val | 0.6693 | 0.8156 | 0.6237 | 0.5652 |
| test | 0.6877 | 0.8424 | 0.6205 | 0.6166 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__rev_co_offline__product`: 0.1738
- `product__co_offline__product`: 0.1366
- `product__rev_co_quick__product`: 0.1009
- `product__co_quick__product`: 0.0980
- `ip__rev_has_ip__product`: 0.0619
- `keyword__rev_has_kw__product`: 0.0618
- `ip__has_kw__keyword`: 0.0614
- `keyword__rev_trend_to__keyword`: 0.0614
- `keyword__rev_has_kw__ip`: 0.0614
- `keyword__trend_to__keyword`: 0.0614
- `product__has_ip__ip`: 0.0610
- `product__has_kw__keyword`: 0.0606

**layer 1**
- `product__rev_co_offline__product`: 0.1618
- `product__co_offline__product`: 0.1358
- `product__rev_co_quick__product`: 0.1011
- `product__co_quick__product`: 0.0998
- `ip__rev_has_ip__product`: 0.0758
- `keyword__rev_has_kw__product`: 0.0731
- `product__has_kw__keyword`: 0.0588
- `ip__has_kw__keyword`: 0.0588
- `keyword__trend_to__keyword`: 0.0588
- `product__has_ip__ip`: 0.0588
- `keyword__rev_has_kw__ip`: 0.0588
- `keyword__rev_trend_to__keyword`: 0.0588

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시(0.04058), 향라(0.03939), 탄탄(0.03850), 샹궈(0.01885), 중독성(0.01394), 피(0.01360), 짜파게티(0.01318), 적음(0.01288), 누들핏(0.01281), 곤약(0.00805)
- **로제** → 로제와인(0.02187), 바리스타룰스(0.01897), 원데이(0.01889), 중독성(0.01333), 납작(0.00810), 늘어남(0.00744), 미정당(0.00568), 로트캡션(0.00552), 뽀므리(0.00548), 당면(0.00534)
- **흑임자** → 공룡(0.01849), 컵케익(0.01383), 시루떡(0.01146), 롱롱이(0.01083), 알(0.00921), 비비빅(0.00763), 작은별(0.00727), 할매니얼(0.00311), 프라임(0.00293), 빵또아(0.00234)
- **단백질** → 헬스(0.00984), 엽떡(0.00964), 동물(0.00949), 베노프(0.00941), 소이조이(0.00607), 테이크핏(0.00574), 밸런스밀(0.00473), 이지프로틴(0.00430), 아르기닌(0.00430), 프로틴바(0.00418)
- **위스키** → 블랙서클(0.00618), 빔산토리(0.00518), 산토리(0.00518), 티처스위스키(0.00518), 티처스하이랜드(0.00461), 컬렉션(0.00301), 블론드(0.00199), 스카치(0.00188), 예술(0.00187), 키싱(0.00180)
- **딸기** → 분홍(0.00423), 트롤(0.00422), 키티(0.00399), 4D(0.00396), 돌직구(0.00384), 니타(0.00379), 고칸(0.00375), 픽업(0.00350), 포장(0.00350), 몽쉘(0.00324)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (41,335행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,161행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
