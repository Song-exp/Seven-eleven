# HIN-GNN 학습 결과 리포트

> `src/eval/export_results.py` 자동 생성. 모델: `checkpoints/hin_gnn_best.pt`
> 재현: `python -m src.eval.export_results`

## 1. 설정 (표준 프리셋)

- hidden_dim=128, num_layers=2, num_heads=4, dropout=0.3, diffmg=True
- loss=weighted BCE(pos_weight=3.24), split=[0.7, 0.15, 0.15] (계층화), optim=Adam(W lr=0.005, α lr=0.005), early stop=val pr_auc

## 2. 성능 지표

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | 0.6014 | 0.8824 | 0.6793 | 0.7352 |
| val | 0.4949 | 0.7681 | 0.5426 | 0.6951 |
| test | 0.4844 | 0.7789 | 0.5574 | 0.7944 |

> 양성 23.6% → 랜덤 PR-AUC=0.236 기준. test PR-AUC 가 그 약 2배면 유의미한 신호.

## 3. DiffMG 관계 중요도 α_r (XAI)

**layer 0**
- `product__has_ip__ip`: 0.1253
- `keyword__trend_to__keyword`: 0.1252
- `ip__has_kw__keyword`: 0.1251
- `keyword__rev_trend_to__keyword`: 0.1251
- `keyword__rev_has_kw__ip`: 0.1251
- `keyword__rev_has_kw__product`: 0.1250
- `ip__rev_has_ip__product`: 0.1250
- `product__has_kw__keyword`: 0.1242

**layer 1**
- `keyword__rev_has_kw__product`: 0.1258
- `product__has_kw__keyword`: 0.1250
- `ip__has_kw__keyword`: 0.1250
- `keyword__trend_to__keyword`: 0.1250
- `product__has_ip__ip`: 0.1250
- `keyword__rev_has_kw__ip`: 0.1250
- `keyword__rev_trend_to__keyword`: 0.1250
- `ip__rev_has_ip__product`: 0.1241

> α_r 가 거의 균등(≈1/R)하면 관계 게이트 미분화 상태 → lr_alpha↑·temperature↓ 재학습 권장.

## 4. 순회 추천 샘플 (시드 → 조합 키워드 top10)

- **마라** → 유산슬(0.02402), 박은영(0.02396), 중독성(0.02278), 미식(0.01605), 온더고(0.01139), 샹궈(0.00929), 팟타이(0.00827), 완탕면(0.00827), 3+1(0.00801), 매일(0.00603)
- **로제** → 중독성(0.03356), 덕개(0.02989), 라더(0.02989), 각별(0.02989), 수현(0.02979), 공룡(0.02979), 이마트(0.02740), 떡볶퀸(0.02451), 파우치(0.02133), 아이(0.01780)
- **흑임자** → 수키도키(0.01662), 파운드(0.01181), 쑥(0.00947), 툇마루(0.00659), 봄(0.00355), 마카롱(0.00276), 제철(0.00230), 칩(0.00225), 퍽퍽(0.00210), 팝콘(0.00207)
- **단백질** → 베노프(0.01454), 쭈쭈바(0.01304), 에그(0.01068), 동물(0.00726), 한도초과(0.00714), 비건(0.00559), 명태(0.00481), 기사식당(0.00476), 파스퇴르(0.00468), 총동원(0.00448)
- **위스키** → 스트레이트(0.01380), 지평(0.01148), 말띠(0.00980), 세븐일레븐(0.00742), 짐빔(0.00426), 버번(0.00303), 오크통(0.00276), 대만(0.00222), 블렌디드(0.00221), 원데이클래스(0.00167)
- **딸기** → 트윈스(0.00353), 저당(0.00351), 스스스(0.00347), 쿠숭이(0.00344), 쿠냥이(0.00344), 카타르(0.00337), 캐릭터상품(0.00321), 진공동결건조(0.00320), 헬로키티(0.00315), 레인보우(0.00310)

## 5. 영속화 산출물

- `data/processed/hin/weighted_product_keyword_edges.parquet` — 가중 네트워크 (33,761행)
- `data/processed/hin/learned_product_scores.parquet` — product 예측확률+임베딩norm (5,143행)
- `data/processed/hin/relation_importance.json` — 층별 α_r
- `checkpoints/hin_gnn_best.pt` — 학습된 모델 가중치
