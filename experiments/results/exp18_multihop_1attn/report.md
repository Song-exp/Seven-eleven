# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=1, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8236 | 0.9488 | 0.7918 | 0.7288 |
| val | 0.7394 | 0.8643 | 0.6667 | 0.8300 |
| test | 0.6815 | 0.8664 | 0.6724 | 0.8559 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.3471
- `product__rev_sim_kw__product`: 0.1763
- `product__sim_kw__product`: 0.0843
- `product__rev_co_quick__product`: 0.0682
- `product__rev_co_offline__product`: 0.0511
- `keyword__rev_has_kw__product`: 0.0498
- `product__co_quick__product`: 0.0318
- `product__rev_sim_ip__product`: 0.0238
- `product__sim_ip__product`: 0.0238
- `ip__rev_has_ip__product`: 0.0208
- `product__has_kw__keyword`: 0.0205
- `ip__has_kw__keyword`: 0.0205
- `keyword__trend_to__keyword`: 0.0205
- `product__has_ip__ip`: 0.0205
- `keyword__rev_has_kw__ip`: 0.0205
- `keyword__rev_trend_to__keyword`: 0.0205

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04152), 향라(0.03998), 탄탄(0.03908), 샹궈(0.01965), 박은영(0.01852), 중독성(0.01483), 피(0.01360), 짜파게티(0.01316), 적음(0.01303), 마라탕(0.00931)
- **로제** → 늘어남(0.01781), 중독성(0.01421), 마카로니(0.01187), 원데이(0.01156), 바리스타룰스(0.01156), 떡볶퀸(0.00657), 파마산(0.00379), 고단백(0.00371), 당면(0.00367), 누들(0.00285)
- **흑임자** → 공룡(0.01949), 시루떡(0.01133), 알(0.00835), 비비빅(0.00566), 마카롱(0.00276), 롱롱이(0.00189), 묵직함(0.00189), 파운드케이크(0.00187), 찰떡(0.00119), 컵(0.00086)
- **단백질** → 헬스(0.00960), 엽떡(0.00942), 동물(0.00915), 베노프(0.00835), 밸런스밀(0.00691), 소이조이(0.00569), 이지프로틴(0.00549), 아르기닌(0.00549), 플러스(0.00530), 편함(0.00508)
- **위스키** → 블랙서클(0.00630), 예술(0.00570), 산토리(0.00476), 티처스위스키(0.00476), 컬렉션(0.00371), 키싱(0.00358), 메이커스마크(0.00329), 트레이스(0.00311), 클레이모어(0.00303), 캐리비안(0.00293)
- **딸기** → 분홍(0.00441), 몽쉘(0.00413), 돌직구(0.00411), 고칸(0.00394), 4D(0.00383), 쿠냥이(0.00380), 톡핑(0.00368), 데이(0.00280), 헬로키티(0.00248), 픽업(0.00245)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
