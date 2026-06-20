# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=1, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.7757 | 0.9315 | 0.7463 | 0.6994 |
| val | 0.7299 | 0.8654 | 0.6628 | 0.8096 |
| test | 0.6802 | 0.8651 | 0.6540 | 0.7875 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.1775
- `product__rev_co_quick__product`: 0.1473
- `product__rev_co_offline__product`: 0.0941
- `product__co_quick__product`: 0.0744
- `keyword__rev_has_kw__product`: 0.0714
- `product__has_kw__keyword`: 0.0488
- `ip__has_kw__keyword`: 0.0488
- `keyword__trend_to__keyword`: 0.0488
- `product__has_ip__ip`: 0.0488
- `ip__has_ip__ip`: 0.0488
- `keyword__rev_has_kw__ip`: 0.0488
- `keyword__rev_trend_to__keyword`: 0.0488
- `ip__rev_has_ip__ip`: 0.0488
- `ip__rev_has_ip__product`: 0.0454

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04129), 향라(0.03993), 탄탄(0.03929), 샹궈(0.02260), 중독성(0.01615), 피(0.01366), 짜파게티(0.01333), 적음(0.01310), 자극적(0.01147), 곤약(0.01014)
- **로제** → 늘어남(0.01684), 중독성(0.01615), 마카로니(0.01122), 고단백(0.00557), 당면(0.00360), 파마산(0.00341), 떡볶이(0.00306), 누들(0.00300), 분식(0.00297), 하트(0.00271)
- **흑임자** → 공룡(0.02308), 알(0.00989), 파운드케이크(0.00493), 마카롱(0.00294), 묵직함(0.00223), 할매니얼(0.00137), 작은별(0.00129), 컵(0.00102), 케이크(0.00099), 바나나(0.00089)
- **단백질** → 헬스(0.01096), 엽떡(0.01063), 동물(0.01050), 베노프(0.01000), 소이조이(0.00801), 밸런스밀(0.00772), 이지프로틴(0.00769), 아르기닌(0.00769), 테이크핏(0.00525), 글루텐프리(0.00402)
- **위스키** → 블랙서클(0.00637), 예술(0.00563), 산토리(0.00511), 애스턴마틴(0.00388), 맥캘란(0.00371), 트레이스(0.00331), 메이커스마크(0.00331), 블론드(0.00329), 클레이모어(0.00318), 아이리쉬(0.00303)
- **딸기** → 분홍(0.00438), 몽쉘(0.00406), 고칸(0.00396), 통크(0.00351), 픽업(0.00334), 포장(0.00334), 톡핑(0.00311), 치토스(0.00306), 쌍둥이(0.00223), 트위스트(0.00217)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
