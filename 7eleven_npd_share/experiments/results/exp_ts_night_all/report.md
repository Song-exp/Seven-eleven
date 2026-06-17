# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8301 | 0.9552 | 0.8210 | 0.5817 |
| val | 0.6366 | 0.8129 | 0.5969 | 0.6211 |
| test | 0.6441 | 0.8236 | 0.6093 | 0.4555 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__has_kw__keyword`: 0.1057
- `product__has_ip__ip`: 0.1037
- `keyword__rev_has_kw__product`: 0.1023
- `ip__rev_has_ip__product`: 0.1006
- `keyword__trend_to__keyword`: 0.0998
- `keyword__rev_trend_to__keyword`: 0.0998
- `keyword__rev_has_kw__ip`: 0.0998
- `ip__has_kw__keyword`: 0.0997
- `product__co_quick__product`: 0.0963
- `product__rev_co_quick__product`: 0.0922

**layer 1**
- `product__co_quick__product`: 0.1365
- `product__rev_co_quick__product`: 0.1341
- `product__has_kw__keyword`: 0.1060
- `ip__has_kw__keyword`: 0.1060
- `keyword__trend_to__keyword`: 0.1060
- `product__has_ip__ip`: 0.1060
- `keyword__rev_has_kw__ip`: 0.1060
- `keyword__rev_trend_to__keyword`: 0.1060
- `ip__rev_has_ip__product`: 0.0618
- `keyword__rev_has_kw__product`: 0.0316

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04119), 중독성(0.01990), 짜파게티(0.01368), 마라탕(0.00883), 박은영(0.00608), 정통(0.00353), 탄탄(0.00282), 짜장(0.00228), 로제(0.00166), 마파(0.00133)
- **로제** → 중독성(0.01905), 늘어남(0.01647), 마카로니(0.01097), 고단백(0.00373), 파마산(0.00346), 누들(0.00287), 하트(0.00262), 당면(0.00235), 떡볶이(0.00232), 납작(0.00213)
- **흑임자** → 공룡(0.01865), 알(0.00800), 파운드케이크(0.00302), 마카롱(0.00258), 묵직함(0.00180), 컵(0.00082), KBO(0.00076), 바나나(0.00071), 케이크(0.00068), 바닐라(0.00043)
- **단백질** → 베노프(0.00971), 헬스(0.00951), 동물(0.00902), 이지프로틴(0.00855), 아르기닌(0.00855), 엽떡(0.00817), 밸런스밀(0.00670), 테이크핏(0.00457), 닥터유(0.00439), 프로틴(0.00339)
- **위스키** → 블랙서클(0.00631), 예술(0.00477), 캐리비안(0.00286), 스트레이트(0.00273), 스프레드(0.00267), 맥캘란(0.00225), 메이커스마크(0.00209), 쉐리(0.00187), 콜드브루(0.00178), 캐스크(0.00140)
- **딸기** → 분홍(0.00420), 쿠냥이(0.00414), 돌직구(0.00407), 몽쉘(0.00402), 고칸(0.00402), 4D(0.00386), 픽업(0.00367), 포장(0.00367), 헬로키티(0.00330), 톡핑(0.00287)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,051행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
