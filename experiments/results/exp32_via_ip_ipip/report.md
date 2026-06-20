# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=64, num_layers=1, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8015 | 0.9382 | 0.7619 | 0.6083 |
| val | 0.7333 | 0.8681 | 0.6549 | 0.7918 |
| test | 0.6717 | 0.8648 | 0.6510 | 0.6916 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.2066
- `product__sim_kw__product`: 0.1379
- `product__rev_sim_kw__product`: 0.0964
- `product__rev_co_offline__product`: 0.0826
- `product__rev_co_quick__product`: 0.0816
- `product__co_quick__product`: 0.0430
- `product__sim_ip__product`: 0.0346
- `product__rev_sim_ip__product`: 0.0298
- `keyword__rev_has_kw_via_ip__product`: 0.0213
- `ip__rev_has_ip__product`: 0.0209
- `keyword__rev_has_kw__product`: 0.0207
- `keyword__rev_has_kw_ipip__product`: 0.0204
- `product__has_kw__keyword`: 0.0204
- `ip__has_kw__keyword`: 0.0204
- `keyword__trend_to__keyword`: 0.0204
- `product__has_ip__ip`: 0.0204
- `ip__has_ip__ip`: 0.0204
- `product__has_kw_via_ip__keyword`: 0.0204
- `product__has_kw_ipip__keyword`: 0.0204
- `keyword__rev_has_kw__ip`: 0.0204
- `keyword__rev_trend_to__keyword`: 0.0204
- `ip__rev_has_ip__ip`: 0.0204

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04161), 향라(0.04056), 탄탄(0.03998), 샹궈(0.01792), 중독성(0.01593), 피(0.01362), 짜파게티(0.01341), 적음(0.01333), 자극적(0.00893), 곤약(0.00811)
- **로제** → 늘어남(0.01643), 중독성(0.01593), 마카로니(0.01095), 고단백(0.00559), 당면(0.00433), 떡볶이(0.00311), 누들(0.00301), 하트(0.00293), 파마산(0.00280), 넘버원(0.00275)
- **흑임자** → 공룡(0.02365), 알(0.01013), 파운드케이크(0.00417), 마카롱(0.00340), 묵직함(0.00229), 작은별(0.00119), 컵(0.00104), 바나나(0.00103), 케이크(0.00094), 할매니얼(0.00090)
- **단백질** → 헬스(0.01093), 엽떡(0.01076), 동물(0.01041), 베노프(0.00986), 소이조이(0.00722), 밸런스밀(0.00671), 이지프로틴(0.00601), 아르기닌(0.00601), 테이크핏(0.00533), 청키(0.00366)
- **위스키** → 블랙서클(0.00645), 예술(0.00596), 산토리(0.00543), 맥캘란(0.00331), 애스턴마틴(0.00275), 캐리비안(0.00275), 글렌피딕(0.00220), 캐스크(0.00213), 트레이스(0.00208), 메이커스마크(0.00204)
- **딸기** → 분홍(0.00453), 몽쉘(0.00415), 고칸(0.00400), 통크(0.00264), 픽업(0.00243), 포장(0.00243), 톡핑(0.00240), 쌍둥이(0.00228), 트위스트(0.00214), 스키틀즈(0.00214)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
