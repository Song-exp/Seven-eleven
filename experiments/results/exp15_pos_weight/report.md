# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=4.5), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8221 | 0.9503 | 0.7936 | 0.6727 |
| val | 0.7419 | 0.8666 | 0.6739 | 0.8453 |
| test | 0.6679 | 0.8558 | 0.6385 | 0.7560 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.2516
- `keyword__rev_has_kw__product`: 0.2008
- `product__rev_co_quick__product`: 0.1140
- `product__rev_co_offline__product`: 0.0662
- `ip__rev_has_ip__product`: 0.0434
- `product__co_quick__product`: 0.0406
- `product__has_kw__keyword`: 0.0354
- `product__has_ip__ip`: 0.0354
- `ip__has_kw__keyword`: 0.0354
- `ip__has_ip__ip`: 0.0354
- `keyword__trend_to__keyword`: 0.0354
- `ip__rev_has_ip__ip`: 0.0354
- `keyword__rev_has_kw__ip`: 0.0354
- `keyword__rev_trend_to__keyword`: 0.0354

**layer 1**
- `product__rev_co_offline__product`: 0.0750
- `product__rev_co_quick__product`: 0.0722
- `product__co_quick__product`: 0.0719
- `product__has_kw__keyword`: 0.0716
- `ip__has_kw__keyword`: 0.0716
- `keyword__trend_to__keyword`: 0.0716
- `product__has_ip__ip`: 0.0716
- `ip__has_ip__ip`: 0.0716
- `keyword__rev_has_kw__ip`: 0.0716
- `keyword__rev_trend_to__keyword`: 0.0716
- `ip__rev_has_ip__ip`: 0.0716
- `ip__rev_has_ip__product`: 0.0715
- `keyword__rev_has_kw__product`: 0.0702
- `product__co_offline__product`: 0.0663

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04160), 향라(0.04119), 탄탄(0.04070), 샹궈(0.02072), 중독성(0.01663), 피(0.01379), 적음(0.01360), 짜파게티(0.01358), 곤약(0.01094), 중식(0.00925)
- **로제** → 늘어남(0.02001), 중독성(0.01661), 마카로니(0.01333), 고단백(0.00577), 당면(0.00555), 파마산(0.00355), 떡볶이(0.00324), 누들(0.00310), 하트(0.00275), 토핑(0.00224)
- **흑임자** → 공룡(0.02323), 알(0.00998), 마카롱(0.00332), 묵직함(0.00224), 컵(0.00103), 바나나(0.00101), 바닐라(0.00075), 케이크(0.00056), 담백(0.00044), 콜라(0.00044)
- **단백질** → 헬스(0.01108), 엽떡(0.01096), 동물(0.01087), 베노프(0.00986), 밸런스밀(0.00778), 이지프로틴(0.00735), 아르기닌(0.00735), 소이조이(0.00587), 테이크핏(0.00548), 포카치아(0.00481)
- **위스키** → 블랙서클(0.00640), 예술(0.00620), 산토리(0.00530), 맥캘란(0.00373), 메이커스마크(0.00326), 캐리비안(0.00313), 트레이스(0.00299), 클레이모어(0.00270), 건조(0.00263), 블론드(0.00237)
- **딸기** → 분홍(0.00443), 몽쉘(0.00432), 고칸(0.00419), 톡핑(0.00411), 픽업(0.00347), 포장(0.00347), 쌍둥이(0.00224), 스키틀즈(0.00218), 트위스트(0.00218), 저지방(0.00210)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
