# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.02), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.8036 | 0.9519 | 0.8260 | 0.3698 |
| val | 0.7340 | 0.8529 | 0.6804 | 0.8838 |
| test | 0.6646 | 0.8347 | 0.6304 | 0.7549 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__has_kw__keyword`: 0.1648
- `product__co_offline__product`: 0.1643
- `product__rev_co_offline__product`: 0.1210
- `ip__rev_has_ip__product`: 0.0794
- `keyword__rev_has_kw__ip`: 0.0785
- `ip__has_kw__keyword`: 0.0785
- `keyword__rev_trend_to__keyword`: 0.0785
- `product__has_ip__ip`: 0.0784
- `keyword__rev_has_kw__product`: 0.0783
- `keyword__trend_to__keyword`: 0.0782

**layer 1**
- `product__co_offline__product`: 0.2777
- `product__rev_co_offline__product`: 0.2705
- `product__has_kw__keyword`: 0.0648
- `ip__has_kw__keyword`: 0.0648
- `keyword__trend_to__keyword`: 0.0648
- `product__has_ip__ip`: 0.0648
- `keyword__rev_has_kw__ip`: 0.0648
- `keyword__rev_trend_to__keyword`: 0.0648
- `ip__rev_has_ip__product`: 0.0450
- `keyword__rev_has_kw__product`: 0.0182

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 향라(0.04175), 도쿠시마(0.04173), 탄탄(0.04149), 중독성(0.02003), 샹궈(0.01398), 피(0.01393), 적음(0.01385), 박은영(0.01364), 짜파게티(0.00901), 마라탕(0.00849)
- **로제** → 바리스타룰스(0.01987), 원데이(0.01987), 중독성(0.01918), 늘어남(0.01674), 마카로니(0.01116), 고단백(0.00400), 누들(0.00307), 하트(0.00267), 클래식(0.00248), 떡볶이(0.00234)
- **흑임자** → 공룡(0.01881), 알(0.00806), 마카롱(0.00239), 묵직함(0.00182), 컵(0.00083), KBO(0.00076), 바나나(0.00065), 케이크(0.00045), 바닐라(0.00040), 담백(0.00037)
- **단백질** → 헬스(0.00990), 동물(0.00990), 엽떡(0.00990), 베노프(0.00934), 소이조이(0.00495), 테이크핏(0.00492), 피쉬(0.00331), 청키(0.00331), 비건(0.00284), 글루텐프리(0.00248)
- **위스키** → 블랙서클(0.00636), 예술(0.00552), 캐리비안(0.00303), 스프레드(0.00253), 스트레이트(0.00234), 맥캘란(0.00220), 콜드브루(0.00169), 캐스크(0.00156), 쉐리(0.00139), 메이커스마크(0.00138)
- **딸기** → 분홍(0.00430), 몽쉘(0.00414), 4D(0.00413), 돌직구(0.00411), 쿠냥이(0.00406), 고칸(0.00398), 픽업(0.00393), 포장(0.00393), 헬로키티(0.00294), 데이(0.00287)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (40,168행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,051행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
