# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=1, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=4.5), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8047 | 0.9450 | 0.7796 | 0.8159 |
| val | 0.7346 | 0.8614 | 0.6685 | 0.8279 |
| test | 0.6611 | 0.8595 | 0.6559 | 0.8719 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.3171
- `product__rev_sim_kw__product`: 0.1879
- `product__sim_kw__product`: 0.1004
- `product__rev_co_quick__product`: 0.0671
- `product__rev_co_offline__product`: 0.0495
- `keyword__rev_has_kw__product`: 0.0444
- `product__co_quick__product`: 0.0327
- `product__sim_ip__product`: 0.0282
- `product__rev_sim_ip__product`: 0.0270
- `product__has_kw__keyword`: 0.0214
- `ip__has_kw__keyword`: 0.0214
- `keyword__trend_to__keyword`: 0.0214
- `product__has_ip__ip`: 0.0214
- `keyword__rev_has_kw__ip`: 0.0214
- `keyword__rev_trend_to__keyword`: 0.0214
- `ip__rev_has_ip__product`: 0.0172

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04228), 향라(0.04116), 탄탄(0.04056), 박은영(0.02127), 중독성(0.01815), 샹궈(0.01756), 피(0.01377), 짜파게티(0.01374), 적음(0.01352), 자극적(0.01124)
- **로제** → 늘어남(0.01838), 중독성(0.01739), 원데이(0.01677), 바리스타룰스(0.01677), 마카로니(0.01225), 고단백(0.00385), 파마산(0.00375), 누들(0.00297), 당면(0.00296), 하트(0.00294)
- **흑임자** → 공룡(0.01955), 알(0.00838), 시루떡(0.00485), 롱롱이(0.00450), 파운드케이크(0.00439), 마카롱(0.00266), 비비빅(0.00243), 묵직함(0.00189), 컵(0.00086), 케이크(0.00082)
- **단백질** → 헬스(0.00973), 엽떡(0.00970), 동물(0.00935), 베노프(0.00910), 소이조이(0.00777), 밸런스밀(0.00771), 이지프로틴(0.00675), 아르기닌(0.00675), 플러스(0.00616), 편함(0.00561)
- **위스키** → 블랙서클(0.00646), 예술(0.00606), 산토리(0.00539), 티처스위스키(0.00539), 컬렉션(0.00477), 맥캘란(0.00426), 키싱(0.00403), 아이리쉬(0.00357), 메이커스마크(0.00353), 12년(0.00308)
- **딸기** → 분홍(0.00445), 몽쉘(0.00426), 돌직구(0.00425), 고칸(0.00416), 4D(0.00404), 쿠냥이(0.00388), 톡핑(0.00367), 통크(0.00357), 픽업(0.00298), 포장(0.00298)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
