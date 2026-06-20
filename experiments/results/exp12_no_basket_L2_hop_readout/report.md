# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8065 | 0.9389 | 0.7501 | 0.6255 |
| val | 0.5712 | 0.8049 | 0.5956 | 0.5646 |
| test | 0.5680 | 0.8237 | 0.6169 | 0.6370 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__has_kw__keyword`: 0.1001
- `keyword__rev_trend_to__keyword`: 0.1001
- `ip__rev_has_ip__ip`: 0.1001
- `ip__has_ip__ip`: 0.1001
- `keyword__rev_has_kw__ip`: 0.1001
- `ip__has_kw__keyword`: 0.1000
- `ip__rev_has_ip__product`: 0.1000
- `keyword__trend_to__keyword`: 0.1000
- `product__has_ip__ip`: 0.1000
- `keyword__rev_has_kw__product`: 0.0995

**layer 1**
- `product__has_kw__keyword`: 0.1002
- `ip__has_kw__keyword`: 0.1002
- `keyword__trend_to__keyword`: 0.1002
- `product__has_ip__ip`: 0.1002
- `ip__has_ip__ip`: 0.1002
- `keyword__rev_has_kw__ip`: 0.1002
- `keyword__rev_trend_to__keyword`: 0.1002
- `ip__rev_has_ip__ip`: 0.1002
- `ip__rev_has_ip__product`: 0.1001
- `keyword__rev_has_kw__product`: 0.0984

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 향라(0.03138), 탄탄(0.03131), 도쿠시마(0.02907), 샹궈(0.01868), 중독성(0.01604), 짜파게티(0.01052), 적음(0.01018), 피(0.00942), 중식(0.00911), 곤약(0.00669)
- **로제** → 늘어남(0.01755), 중독성(0.01679), 마카로니(0.01193), 당면(0.00602), 고단백(0.00434), 파마산(0.00432), 떡볶이(0.00295), 납작(0.00257), 누들(0.00245), 하트(0.00231)
- **흑임자** → 공룡(0.01971), 알(0.00828), 마카롱(0.00288), 묵직함(0.00196), 바나나(0.00087), 컵(0.00084), 바닐라(0.00067), 케이크(0.00049), 콜라(0.00039), 딸기(0.00026)
- **단백질** → 엽떡(0.00855), 헬스(0.00841), 동물(0.00840), 밸런스밀(0.00824), 베노프(0.00771), 이지프로틴(0.00713), 아르기닌(0.00713), 남양(0.00465), 소이조이(0.00424), 닥터유(0.00401)
- **위스키** → 예술(0.00579), 블랙서클(0.00539), 산토리(0.00477), 블론드(0.00421), 트레이스(0.00409), 클레이모어(0.00401), 메이커스마크(0.00395), 신년(0.00298), 캐리비안(0.00287), 립서비스(0.00204)
- **딸기** → 고칸(0.00422), 분홍(0.00393), 톡핑(0.00347), 몽쉘(0.00335), 쌍둥이(0.00204), 통크(0.00194), 저지방(0.00161), 필링(0.00157), 아모스(0.00147), 탕종(0.00143)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
