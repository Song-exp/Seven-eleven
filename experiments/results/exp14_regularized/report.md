# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.4, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.5850 | 0.8429 | 0.6099 | 0.6418 |
| val | 0.5429 | 0.7844 | 0.5374 | 0.6681 |
| test | 0.5586 | 0.8079 | 0.5875 | 0.6679 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.0719
- `product__co_quick__product`: 0.0716
- `ip__rev_has_ip__product`: 0.0716
- `product__has_kw__keyword`: 0.0716
- `product__has_ip__ip`: 0.0715
- `keyword__rev_has_kw__ip`: 0.0715
- `keyword__trend_to__keyword`: 0.0715
- `ip__has_ip__ip`: 0.0715
- `ip__rev_has_ip__ip`: 0.0715
- `keyword__rev_trend_to__keyword`: 0.0715
- `ip__has_kw__keyword`: 0.0715
- `keyword__rev_has_kw__product`: 0.0711
- `product__rev_co_offline__product`: 0.0709
- `product__rev_co_quick__product`: 0.0707

**layer 1**
- `product__rev_co_offline__product`: 0.0721
- `keyword__rev_has_kw__product`: 0.0718
- `product__co_offline__product`: 0.0717
- `ip__rev_has_ip__product`: 0.0715
- `product__has_kw__keyword`: 0.0713
- `ip__has_kw__keyword`: 0.0713
- `keyword__trend_to__keyword`: 0.0713
- `product__has_ip__ip`: 0.0713
- `ip__has_ip__ip`: 0.0713
- `keyword__rev_has_kw__ip`: 0.0713
- `keyword__rev_trend_to__keyword`: 0.0713
- `ip__rev_has_ip__ip`: 0.0713
- `product__co_quick__product`: 0.0712
- `product__rev_co_quick__product`: 0.0710

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 향라(0.03137), 탄탄(0.03130), 도쿠시마(0.03113), 샹궈(0.02511), 중독성(0.01582), 짜파게티(0.01046), 적음(0.01043), 피(0.01032), 자극적(0.01015), 중식(0.00934)
- **로제** → 중독성(0.01583), 늘어남(0.01461), 마카로니(0.00974), 고단백(0.00446), 당면(0.00431), 파마산(0.00387), 떡볶이(0.00311), 볶이(0.00306), 반반(0.00291), 분식(0.00284)
- **흑임자** → 공룡(0.01779), 알(0.00762), 작은별(0.00596), 파운드케이크(0.00434), 드레싱(0.00388), 할매니얼(0.00311), 마카롱(0.00258), 콩(0.00203), 묵직함(0.00172), 참깨(0.00129)
- **단백질** → 베노프(0.00838), 엽떡(0.00836), 밸런스밀(0.00835), 동물(0.00833), 이지프로틴(0.00833), 아르기닌(0.00833), 헬스(0.00831), 소이조이(0.00809), 메추리알(0.00646), 백반(0.00640)
- **위스키** → 산토리(0.00483), 블론드(0.00479), 블랙서클(0.00477), 트레이스(0.00477), 클레이모어(0.00477), 메이커스마크(0.00476), 예술(0.00452), 신년(0.00357), 아이리쉬(0.00337), 임페리얼(0.00325)
- **딸기** → 분홍(0.00336), 고칸(0.00335), 픽업(0.00323), 포장(0.00323), 치토스(0.00320), 통크(0.00320), 몽쉘(0.00315), 죠스바(0.00313), 톡핑(0.00305), 잼(0.00203)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
