# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.5728 | 0.8030 | 0.5646 | 0.4013 |
| val | 0.4962 | 0.7400 | 0.5116 | 0.5772 |
| test | 0.5275 | 0.7699 | 0.5316 | 0.4001 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `keyword__rev_has_kw__ip`: 0.1257
- `keyword__trend_to__keyword`: 0.1257
- `ip__has_kw__keyword`: 0.1256
- `keyword__rev_trend_to__keyword`: 0.1256
- `keyword__rev_has_kw__product`: 0.1249
- `product__has_ip__ip`: 0.1247
- `product__has_kw__keyword`: 0.1246
- `ip__rev_has_ip__product`: 0.1232

**layer 1**
- `ip__rev_has_ip__product`: 0.2166
- `product__has_kw__keyword`: 0.1126
- `ip__has_kw__keyword`: 0.1126
- `keyword__trend_to__keyword`: 0.1126
- `product__has_ip__ip`: 0.1126
- `keyword__rev_has_kw__ip`: 0.1126
- `keyword__rev_trend_to__keyword`: 0.1126
- `keyword__rev_has_kw__product`: 0.1079

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시(0.03205), 탄탄(0.03128), 향라(0.03128), 샹궈(0.02600), 박은영(0.02114), 떠먹음(0.01675), 파티팩(0.01639), 얼얼함(0.01636), 특식(0.01604), 중독성(0.01565)
- **로제** → 로제와인(0.02227), 중독성(0.01504), 로트캡션(0.01443), 뽀므리(0.01402), 대왕(0.00971), 바리스타룰스(0.00877), 원데이(0.00877), 앙리(0.00859), 노제(0.00843), 늘어남(0.00828)
- **흑임자** → 시루떡(0.03073), 비비빅(0.02038), 공룡(0.01195), 롱롱이(0.01152), 작은별(0.01127), 컵케익(0.00888), 프라임(0.00820), 적음(0.00615), 알(0.00596), 소금빵(0.00453)
- **단백질** → 베노프(0.00792), 동물(0.00783), 엽떡(0.00782), 밸런스밀(0.00782), 이지프로틴(0.00782), 아르기닌(0.00782), 플러스(0.00781), 편함(0.00779), 헬스(0.00779), 동의(0.00771)
- **위스키** → 빔산토리(0.00490), 산토리(0.00490), 티처스위스키(0.00490), 블랙서클(0.00488), 컬렉션(0.00473), 키노(0.00471), 블론드(0.00471), 키싱(0.00470), 노마드리저브(0.00469), 더글렌드로낙(0.00468)
- **딸기** → 분홍(0.00357), 니타(0.00355), 키티(0.00353), 4D(0.00297), 고칸(0.00287), 돌직구(0.00283), 트롤(0.00282), 픽업(0.00251), 포장(0.00251), 헬로키티(0.00234)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (41,335행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,161행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
