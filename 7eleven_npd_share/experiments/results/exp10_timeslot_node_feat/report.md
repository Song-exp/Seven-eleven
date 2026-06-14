# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.005), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7216 | 0.9029 | 0.6928 | 0.6025 |
| val | 0.5638 | 0.8005 | 0.5673 | 0.5504 |
| test | 0.6157 | 0.8183 | 0.6150 | 0.6224 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__has_kw__keyword`: 0.1269
- `keyword__rev_has_kw__product`: 0.1251
- `product__has_ip__ip`: 0.1251
- `ip__has_kw__keyword`: 0.1247
- `keyword__trend_to__keyword`: 0.1247
- `keyword__rev_has_kw__ip`: 0.1246
- `keyword__rev_trend_to__keyword`: 0.1245
- `ip__rev_has_ip__product`: 0.1244

**layer 1**
- `ip__rev_has_ip__product`: 0.1322
- `keyword__rev_has_kw__product`: 0.1245
- `product__has_kw__keyword`: 0.1239
- `ip__has_kw__keyword`: 0.1239
- `keyword__trend_to__keyword`: 0.1239
- `product__has_ip__ip`: 0.1239
- `keyword__rev_has_kw__ip`: 0.1239
- `keyword__rev_trend_to__keyword`: 0.1239

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.03535), 향라(0.03297), 탄탄(0.03288), 박은영(0.02276), 중독성(0.01795), 샹궈(0.01460), 짜파게티(0.01179), 적음(0.01077), 피(0.00968), 자극적(0.00917)
- **로제** → 중독성(0.01786), 늘어남(0.01511), 떡볶퀸(0.01238), 마카로니(0.01023), 당면(0.00485), 파마산(0.00406), 앙리(0.00384), 하트(0.00379), 히밥(0.00366), 고단백(0.00346)
- **흑임자** → 공룡(0.01580), 롱롱이(0.00907), 시루떡(0.00881), 알(0.00674), 비비빅(0.00453), 파운드케이크(0.00293), 마카롱(0.00230), 묵직함(0.00152), 찰떡(0.00089), 컵(0.00069)
- **단백질** → 베노프(0.00852), 이지프로틴(0.00844), 아르기닌(0.00844), 밸런스밀(0.00839), 동물(0.00826), 엽떡(0.00807), 플러스(0.00792), 헬스(0.00754), 편함(0.00709), 소이조이(0.00460)
- **위스키** → 블랙서클(0.00527), 산토리(0.00479), 티처스위스키(0.00479), 컬렉션(0.00472), 예술(0.00427), 메이커스마크(0.00396), 블론드(0.00309), 스트레이트(0.00245), 스프레드(0.00244), 클레이모어(0.00212)
- **딸기** → 분홍(0.00395), 고칸(0.00375), 돌직구(0.00368), 4D(0.00367), 쿠냥이(0.00357), 픽업(0.00347), 포장(0.00347), 통크(0.00342), 몽쉘(0.00340), 치토스(0.00339)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,051행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
