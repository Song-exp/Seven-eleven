# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.005), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.5953 | 0.8169 | 0.5711 | 0.6149 |
| val | 0.5043 | 0.7456 | 0.5000 | 0.3260 |
| test | 0.5265 | 0.7725 | 0.5333 | 0.3889 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__has_kw__keyword`: 0.1253
- `keyword__trend_to__keyword`: 0.1251
- `keyword__rev_trend_to__keyword`: 0.1251
- `keyword__rev_has_kw__ip`: 0.1251
- `ip__has_kw__keyword`: 0.1251
- `product__has_ip__ip`: 0.1249
- `keyword__rev_has_kw__product`: 0.1248
- `ip__rev_has_ip__product`: 0.1245

**layer 1**
- `ip__rev_has_ip__product`: 0.1362
- `product__has_kw__keyword`: 0.1235
- `ip__has_kw__keyword`: 0.1235
- `keyword__trend_to__keyword`: 0.1235
- `product__has_ip__ip`: 0.1235
- `keyword__rev_has_kw__ip`: 0.1235
- `keyword__rev_trend_to__keyword`: 0.1235
- `keyword__rev_has_kw__product`: 0.1229

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시(0.03313), 향라(0.03272), 탄탄(0.03270), 샹궈(0.02601), 박은영(0.02182), 중독성(0.01677), 떠먹음(0.01497), 파티팩(0.01409), 얼얼함(0.01406), 특식(0.01334)
- **로제** → 로제와인(0.02156), 중독성(0.01604), 로트캡션(0.01497), 뽀므리(0.01138), 대왕(0.00954), 납작(0.00798), 늘어남(0.00758), 앙리(0.00688), 원데이(0.00659), 바리스타룰스(0.00659)
- **흑임자** → 시루떡(0.02984), 비비빅(0.01920), 공룡(0.01366), 롱롱이(0.01114), 작은별(0.01052), 컵케익(0.01031), 알(0.00687), 프라임(0.00683), 적음(0.00471), 빵또아(0.00375)
- **단백질** → 베노프(0.00804), 동물(0.00786), 밸런스밀(0.00785), 엽떡(0.00785), 이지프로틴(0.00784), 아르기닌(0.00784), 플러스(0.00783), 헬스(0.00781), 편함(0.00779), 동의(0.00731)
- **위스키** → 빔산토리(0.00524), 산토리(0.00524), 티처스위스키(0.00524), 블랙서클(0.00509), 컬렉션(0.00496), 키노(0.00490), 블론드(0.00489), 노마드리저브(0.00488), 키싱(0.00487), 더글렌드로낙(0.00485)
- **딸기** → 분홍(0.00370), 니타(0.00370), 키티(0.00368), 돌직구(0.00322), 트롤(0.00318), 4D(0.00316), 고칸(0.00312), 픽업(0.00284), 포장(0.00284), 헬로키티(0.00248)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (41,335행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,161행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
