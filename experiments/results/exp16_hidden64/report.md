# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=64, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8603 | 0.9648 | 0.8465 | 0.6313 |
| val | 0.7289 | 0.8643 | 0.6747 | 0.8762 |
| test | 0.6703 | 0.8589 | 0.6541 | 0.8436 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.2247
- `product__rev_co_quick__product`: 0.1528
- `product__rev_co_offline__product`: 0.1507
- `ip__rev_has_ip__product`: 0.0648
- `product__co_quick__product`: 0.0640
- `keyword__rev_has_kw__product`: 0.0574
- `product__has_kw__keyword`: 0.0536
- `product__has_ip__ip`: 0.0464
- `ip__has_kw__keyword`: 0.0464
- `keyword__rev_has_kw__ip`: 0.0464
- `keyword__rev_trend_to__keyword`: 0.0464
- `keyword__trend_to__keyword`: 0.0464

**layer 1**
- `product__co_offline__product`: 0.1381
- `product__rev_co_offline__product`: 0.1122
- `product__has_kw__keyword`: 0.0838
- `ip__has_kw__keyword`: 0.0838
- `keyword__trend_to__keyword`: 0.0838
- `product__has_ip__ip`: 0.0838
- `keyword__rev_has_kw__ip`: 0.0838
- `keyword__rev_trend_to__keyword`: 0.0838
- `product__co_quick__product`: 0.0779
- `product__rev_co_quick__product`: 0.0766
- `keyword__rev_has_kw__product`: 0.0470
- `ip__rev_has_ip__product`: 0.0455

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04136), 향라(0.03950), 탄탄(0.03772), 짜파게티(0.01314), 박은영(0.01279), 샹궈(0.01273), 피(0.01258), 적음(0.01253), 마라탕(0.00886), 곤약(0.00729)
- **로제** → 바리스타룰스(0.01899), 원데이(0.01899), 떡볶퀸(0.01799), 늘어남(0.01788), 마카로니(0.01196), 당면(0.00573), 파마산(0.00404), 고단백(0.00369), 중독성(0.00343), 누들(0.00286)
- **흑임자** → 공룡(0.01900), 알(0.00815), 마카롱(0.00266), 묵직함(0.00185), 컵(0.00084), KBO(0.00077), 바나나(0.00073), 케이크(0.00045), 바닐라(0.00044), 콜라(0.00035)
- **단백질** → 엽떡(0.00929), 헬스(0.00927), 동물(0.00925), 편함(0.00820), 베노프(0.00759), 밸런스밀(0.00681), 이지프로틴(0.00592), 아르기닌(0.00592), 테이크핏(0.00476), 소이조이(0.00422)
- **위스키** → 블랙서클(0.00636), 예술(0.00594), 산토리(0.00544), 티처스위스키(0.00544), 맥캘란(0.00349), 캐리비안(0.00299), 스프레드(0.00262), 건조(0.00249), 스트레이트(0.00237), 콜드브루(0.00175)
- **딸기** → 분홍(0.00434), 몽쉘(0.00411), 고칸(0.00404), 돌직구(0.00401), 4D(0.00398), 쿠냥이(0.00387), 톡핑(0.00385), 데이(0.00278), 쌍둥이(0.00218), 배트(0.00214)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
