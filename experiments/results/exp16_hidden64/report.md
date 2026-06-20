# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=64, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8082 | 0.9496 | 0.8016 | 0.5862 |
| val | 0.7281 | 0.8594 | 0.6686 | 0.8099 |
| test | 0.6625 | 0.8542 | 0.6419 | 0.7835 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__co_offline__product`: 0.2111
- `product__rev_co_quick__product`: 0.1591
- `product__rev_co_offline__product`: 0.1052
- `product__co_quick__product`: 0.0628
- `ip__rev_has_ip__product`: 0.0579
- `keyword__rev_has_kw__product`: 0.0494
- `ip__rev_has_ip__ip`: 0.0444
- `keyword__rev_trend_to__keyword`: 0.0444
- `keyword__trend_to__keyword`: 0.0444
- `ip__has_ip__ip`: 0.0444
- `ip__has_kw__keyword`: 0.0444
- `keyword__rev_has_kw__ip`: 0.0444
- `product__has_ip__ip`: 0.0443
- `product__has_kw__keyword`: 0.0439

**layer 1**
- `product__co_offline__product`: 0.1904
- `product__rev_co_offline__product`: 0.1411
- `product__rev_co_quick__product`: 0.0645
- `product__co_quick__product`: 0.0618
- `product__has_kw__keyword`: 0.0564
- `ip__has_kw__keyword`: 0.0564
- `keyword__trend_to__keyword`: 0.0564
- `product__has_ip__ip`: 0.0564
- `ip__has_ip__ip`: 0.0564
- `keyword__rev_has_kw__ip`: 0.0564
- `keyword__rev_trend_to__keyword`: 0.0564
- `ip__rev_has_ip__ip`: 0.0564
- `keyword__rev_has_kw__product`: 0.0518
- `ip__rev_has_ip__product`: 0.0394

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 도쿠시마(0.04156), 향라(0.04026), 탄탄(0.03938), 샹궈(0.01464), 피(0.01346), 짜파게티(0.01338), 적음(0.01318), 곤약(0.01018), 중독성(0.00769), 중식(0.00740)
- **로제** → 늘어남(0.01681), 마카로니(0.01119), 중독성(0.00766), 고단백(0.00557), 당면(0.00373), 누들(0.00298), 하트(0.00270), 떡볶이(0.00248), 토핑(0.00188), 분식(0.00180)
- **흑임자** → 공룡(0.02320), 알(0.00997), 마카롱(0.00317), 묵직함(0.00224), 컵(0.00103), 바나나(0.00096), 바닐라(0.00071), 케이크(0.00059), 담백(0.00043), 콜라(0.00042)
- **단백질** → 헬스(0.01069), 엽떡(0.01050), 동물(0.01036), 베노프(0.00932), 이지프로틴(0.00709), 아르기닌(0.00709), 소이조이(0.00524), 테이크핏(0.00513), 밸런스밀(0.00441), 청키(0.00358)
- **위스키** → 블랙서클(0.00649), 예술(0.00575), 산토리(0.00561), 맥캘란(0.00522), 후드티(0.00396), 캐리비안(0.00282), 건조(0.00263), 캐스크(0.00184), 야마자키(0.00170), 쉐리(0.00150)
- **딸기** → 분홍(0.00443), 몽쉘(0.00414), 고칸(0.00390), 톡핑(0.00341), 쌍둥이(0.00224), 트위스트(0.00217), 스키틀즈(0.00213), 저지방(0.00207), 아모스(0.00205), 후루츄(0.00204)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (37,333행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,033행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
