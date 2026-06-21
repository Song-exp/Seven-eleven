# 7-Eleven NPD 프레임워크 — 방법론 전체 문서

> 신제품(NPD) 성공 예측 HIN-GNN의 **구축 → 초기화 → 모델(3논문 융합) → 학습 → 추천 → XAI** 전 과정을  
> 선행논문 수식과 실제 코드·데이터 예시로 1:1 매핑하여 정리한 통합 문서.
>
> - 선행논문: `방법론_논문/` — **KGAT**(SIGIR'19), **HGT**(WWW'20), **DiffMG**(KDD'21)
> - 소스: `src/data_builder/`, `src/models/`, `src/train/`, `src/eval/`
> - <span style="color:#2f9e44">**최종 모델: `v2_sweepA`** (test PR-AUC **0.6083**, AUC-ROC 0.8314, F1 0.6121 · `HINGNNv2` 멀티태스크 · **leak-free**)</span> — <span style="color:#1971c2">이전 확정본 `exp41_trend_kw3_ip1`(0.6959)</span>이 동반구매 엣지의 **target leakage**로 판명되어, 누수를 제거·재배치한 모델로 전환 (그 전 exp22 0.6852 / exp06_3 0.6729)
> - 보완 문서: 데이터·엣지 파일 상세는 `hin_network_construction.md`, 누수 전환 설계는 `docs/final_model_leakfree_switch_plan.md` 참조

> 🔵 **변경 표시 규칙(3색)**: <span style="color:#2f9e44">이 색(초록) 텍스트</span> = **이번 갱신에서 exp41 → 최종 모델 v2_sweepA로 바뀐 내용**(현재 정답). <span style="color:#1971c2">이 색(파랑) 텍스트</span> = 이전 갱신(exp22 → exp41) 이력. <span style="color:#c92a2a">이 색(crimson) 텍스트</span> = 그 이전(exp06_3 → exp22) 이력. 색 없는 본문 = 모든 모델 공통으로 유효. ⚠ 파랑·crimson은 *이력 보존용*이며, **v2에서 달라진 지점은 초록이 최종**이다(특히 동반구매 관련 파랑/crimson 서술은 v2에서 무효 — 초록 주석을 따른다).
>
> <span style="color:#c92a2a">**exp06_3 → exp22 핵심 변경 요약**: ① 2층 A^L 스택 → **1층 + 사전 구성 2홉 유사도 엣지**(`sim_kw`/`sim_ip`) ② forward 엣지 6종 → **8종** ③ product 부가피처 9차원(앱행동 7차원 포함) → **2차원**(`has_promo`,`insta_m30`) ④ 동반구매 Lift 가중치 **OFF** ⑤ test PR-AUC 0.6729 → **0.6852**. DiffMG·HGT·KGAT 한 층 내부 수식과 bi-level 학습은 **불변**.</span>
>
> <span style="color:#1971c2">**exp22 → exp41 핵심 변경 요약**: ① forward 엣지 8종 → **12종** — 신규 4종(`ip→has_ip→ip`, `has_kw_via_ip`=P-I-K, `has_kw_ipip`=P-I-I-K, `has_kw_trend`=P-K-K)은 모두 **멀티홉 경로를 빌드 시점에 명시적 1홉 엣지로 압축**한 것(§5.1 sim 철학을 키워드 도달 경로까지 확장 = 이름의 `trend`) ② 관계 수 R 16 → **24** ③ `sim_ip` 임계 공유 IP ≥2 → **≥1**(이름의 `ip1`) ④ hidden_dim 128 → **64** ⑤ test PR-AUC 0.6852 → **0.6959**. α_r은 동반구매(`co_offline`/`co_quick`)·유사상품(`sim_ip`/`sim_kw`) 관계만 분화하고 키워드·IP 경로 관계는 균등 수렴(§8). **불변**: 1층(`num_layers=1`)·2차원 부가피처·Lift OFF·bi-level·DiffMG/HGT/KGAT 한 층 내부.</span>
>
> <span style="color:#2f9e44">**exp41 → v2_sweepA 핵심 변경 요약(최종 · leak-free)**: 동기 = exp41이 가장 의존하던 `co_offline`(α=0.131)이 **출시-후 매출 기반 target leakage**(성공 제품 동반구매 보유율 46% vs 실패 5%; degree 단독 PR-AUC 0.518)임이 판명. cold-start 신제품 예측엔 사용 불가 → ① 동반구매 엣지(`co_offline`/`co_quick`)를 **입력에서 제거** ② 동반구매 도메인 가치를 **누수 없이 재배치**: (a) `keyword__basket_comp__keyword`(동반구매 제품쌍의 키워드 교차, support≥2 — 키워드 단위라 특정 제품 성공이 직접 인코딩 안 됨) (b) `HINGNNv2`(`HINGNN` 상속 + 보조 링크예측 헤드 `aux_proj`)로 동반구매를 **입력이 아닌 보조 예측 타깃(멀티태스크)**으로 학습 ③ forward 엣지 12 → **11종**(co_* 제거 −2, `basket_comp` +1), R 24 → **22** ④ 과적합 억제: hidden_dim 64 → **32** · dropout↑ · weight_decay↑ · **DropEdge** ⑤ test PR-AUC 0.6959 → **0.6083**(랭킹 꼬리 손실분은 *증발한 누수*이며, 생존율 동기화 운영점 P=R=F1은 거의 동일). **α_r 최상위가 동반구매 → 유사키워드 `sim_kw`(0.61)로 이동**(co_* 소멸; §8). **불변**: `HINGNNv2`의 **주 forward(성공 로짓)는 HINGNN과 동일** → §4(DiffMG/HGT/KGAT 한 층 내부)·bi-level·content_aggregation·1층·2차원 부가피처 모두 유효. 달라진 건 입력 엣지 구성·보조 헤드·정규화 하이퍼파라미터뿐.</span>

---

## 0. 한눈에 보는 전체 파이프라인

```
[Phase 1] 데이터 → 그래프            build_hetero_data.py
  parquet 7종 ──(id 정규화·매핑)──▶ HeteroData(product/keyword/ip, [exp41] 12+12 엣지 ★sim_kw/sim_ip + 확장 멀티홉 4종)
                                         │
[Phase 2] 노드 초기 표현              hin_gnn.py::_init_product
  keyword/ip = 학습 임베딩,  product = 이웃 임베딩 평균(content aggregation)
                                         │
[Phase 3] 한 층 = 3논문 융합          hin_gnn.py::forward 루프
  (1) DiffMG 관계 게이트 α_r   ──▶ "어떤 엣지 타입이 중요한가" (구조 학습)
  (2) HGT 타입격리 어텐션      ──▶ "어떤 이웃을 볼까" (타입별 메시지)
  (3) KGAT Bi-Interaction      ──▶ "메시지를 어떻게 융합할까" (고차 결합)
                                         │
[Phase 4] [exp41] 1층 + 사전구성 멀티홉 엣지 = 유사제품·IP/트렌드 경로 흡수 (Cold Start 극복; 이전 exp06_3은 2층 A^L)
                                         │
[Phase 5] Bi-level 학습               trainer.py
  Step1: W를 train으로 갱신  /  Step2: α를 val로 갱신  →  weighted BCE
                                         │
[Phase 6] 산출 ① 성공확률 분류        success_predictor.py
          산출 ② 키워드 조합 추천      recommend.py (메타패스 순회)
          산출 ③ 관계중요도 α_r (XAI)  export_results.py
```

> <span style="color:#1971c2">위 다이어그램의 `[exp41]` 표기 = exp41에서 달라진 단계: 엣지 **12+12종**(2홉 sim + 확장 멀티홉 4종 포함), Phase 4가 "L층 스택"이 아니라 "1층 + 사전 구성 멀티홉 엣지"다(상세 §2.1·§5·§9).</span>
>
> <span style="color:#2f9e44">**최종 모델 v2_sweepA 기준 보정**: Phase 1 엣지는 **11+11종**(동반구매 `co_*` 제거 + `keyword__basket_comp__keyword` 추가). Phase 5는 단일 손실이 아니라 **멀티태스크**(주=성공 BCE + 보조=동반구매 link-pred)이며, 동반구매는 *입력이 아니라 보조 예측 타깃*으로만 쓰여 **leak-free**다(상세 §2.1·§2.4·§4 머리·§9).</span>

핵심 한 줄 요약:

> **DiffMG가 "쓸 관계"를 고르고 → HGT가 "관계별로" 이웃 메시지를 만들고 → KGAT가 그 메시지를 "고차로 융합"하며,** <span style="color:#c92a2a">이 한 층(exp22는 1층)을 거쳐 신상품이 — 사전 구성한 2홉 유사도 엣지를 통해 — 히트 상품·트렌드 맥락까지 흡수한다.</span> <span style="color:#1971c2">exp41은 여기에 IP·트렌드 경유 키워드 도달 엣지(`has_kw_via_ip`/`has_kw_ipip`/`has_kw_trend`)까지 더해 한 층에서 흡수 범위를 넓혔다.</span>

---

## 1. 선행논문 3편 — 역할 분담과 차용 포인트

| 논문 | 원 논문이 푸는 문제 | 본 프레임워크에서 차용한 메커니즘 | 구현 파일 |
|---|---|---|---|
| **KGAT** | 고차 연결성(high-order connectivity)을 메타패스 수동 탐색 없이 end-to-end로 | **Bi-Interaction 집계** + **A^L 재귀 전파** | `kgat_layer.py` |
| **HGT** | 이기종 그래프의 타입 차이를 무시한 1/N 균등 혼합(over-smoothing) | **메타관계별 W 행렬 격리** + **타입별 동적 어텐션** | `hgt_layer.py` |
| **DiffMG** | 메타그래프 수동 설계의 인간 편견·블랙박스 | **아키텍처 파라미터 λ의 미분 가능 softmax** + **bi-level 최적화** | `diffmg_pruner.py`, `trainer.py` |

세 논문은 GNN의 서로 다른 층위를 담당하므로 **충돌 없이 한 층에 직렬 결합**된다.
- DiffMG = *관계 선택*(어떤 엣지 타입을 살릴까)
- HGT = *메시지 생성*(그 엣지 타입의 메시지를 어떻게 만들까)
- KGAT = *노드 갱신*(만들어진 메시지로 노드 상태를 어떻게 바꿀까)

---

## 2. Phase 1 — 데이터에서 그래프로 (`build_hetero_data.py`)

### 2.1 노드·엣지 정의

**노드 (3종) — 개수 · 피처 · 모델 입력 표현**

| 노드 타입 | 개수 | 노드 피처 (원천 컬럼) | 모델 입력 표현 | 소스 parquet |
|---|---|---|---|---|
| `product` | 5,033 | `has_promo`(프로모션 18종 one-hot 집계 → 0/1), `insta_mention_30d`(출시 30일내 인스타 언급 수), `성공여부`(→ 타겟 `y`) <span style="color:#c92a2a">〔exp22: 부가피처는 이 2개뿐. 이전 exp06_3의 `extra_feat`(앱 행동 7차원)은 **현 모델 코드에서 제거됨**〕</span> | content aggregation(이웃 keyword/ip 임베딩 평균) + 부가 피처 투영 | `product_nodes.parquet` |
| `keyword` | <span style="color:#c92a2a">2,063</span> | `is_trend_keyword`(트렌드 플래그 0/1, <span style="color:#c92a2a">트렌드 342</span>), `인스타_첫_등장일`  〔`추출_속성`은 전 행 빈 값 `[]` → 미사용〕 | `nn.Embedding(`<span style="color:#c92a2a">2063</span>`, d)` Xavier (학습, **수치 피처 없음**) | `keyword_nodes.parquet` |
| `ip` | <span style="color:#c92a2a">335</span> | `키워드_final`(연관 키워드 배열 — ip→keyword 엣지의 source) | `nn.Embedding(`<span style="color:#c92a2a">335</span>`, d)` Xavier (학습, **수치 피처 없음**) | `ip_nodes.parquet` |

> **노드 피처 사용 원칙**: 모델에 들어가는 **수치 피처는 `product`에만 존재**한다(`has_promo`, `insta_mention_30d`, `extra_feat`). `keyword`·`ip`는 수치 피처 없이 **학습 임베딩**만으로 표현되며, 그들의 parquet 컬럼은 식별·엣지생성·해석용 메타데이터다(예: ip의 `키워드_final`은 `(ip, has_kw, keyword)` 엣지를 만드는 원천). `성공여부`는 노드 피처가 아니라 오직 타겟 `y`로만 쓴다(§2.3 Data Leakage 차단).
>
> 주의 두 가지: ① <span style="color:#c92a2a">product의 부가 수치 피처는 **exp22에서 `has_promo`·`insta_mention_30d` 2개뿐**이다(`hin_gnn.py`의 `product_feat_lin = nn.Linear(2, hidden)`). 이전 exp06_3이 쓰던 `extra_feat`(앱 행동 7차원, 650개만 실측)은 현 모델 코드의 `_init_product`에서 제거되어 더는 들어가지 않는다.</span> ② keyword의 `추출_속성`은 컬럼은 있으나 전 행이 빈 리스트 `[]`라 실질 피처가 아니다 — keyword의 정체성은 `keyword` 문자열(노드 id)이며 모델은 이를 임베딩 인덱스로만 참조한다. (초기 표현 식 상세는 §3)

**노드별 실제 예시** (`data/processed/hin/*_final.parquet`에서 발췌)

```
[product]  ITEM_CD=72369  "CJ)맥스봉구운풍미마늘후랑크80g"  (세븐일레븐 · 성공 · 첫등장 2025-09-17)
  ├ has_promo          = 0          (프로모션 18종 모두 0)
  ├ insta_mention_30d  = 0
  ├ 키워드_final(14개)  = [간식, 고기, 풍미, 반찬, 육향, 마늘, 소시지, 야식, 안주, 짭조름함, 고소, 맥스봉, 구움, 후랑크]
  │                       → (product,has_kw,keyword) 엣지 14개 생성
  └ [exp22 미사용] extra_feat(앱 7차원) = [view 2.12, cart 1.20, purchase 0, wishlist 0.51, search 0.29, inventory 0, cvr1 0.28]
                          (feat_view/cart/purchase/wishlist/search/inventory/cvr1 = 앱 조회·장바구니·구매·찜·검색·재고·전환율)
                          ← parquet엔 존재하나 exp22는 입력하지 않음 (이전 exp06_3 전용 피처)

[keyword]  keyword="간식"
  ├ is_trend_keyword = 1            (트렌드 키워드 342개 중 하나)
  ├ 인스타_첫_등장일  = 2025-01-02
  ├ 추출_속성        = []           (전 행 빈 값 → 미사용)
  └ 모델 입력        = 임베딩 인덱스로만 참조. 위 "맥스봉…" 등 다수 상품과 has_kw로 연결

[ip]       ip_name="KBO"
  ├ 키워드_final = [스포츠, 투수, 내야수]   → (ip,has_kw,keyword) 엣지 3개 생성
  └ 모델 입력    = 학습 임베딩. [최종데이터] 335개 IP 중 하나 (이 키워드들을 매개로 상품과 2-hop 연결)
```

**엣지 (<span style="color:#1971c2">exp41: 기본 6종 + 2홉 sim 2종 + 확장 멀티홉 4종 = forward 12종</span> · <span style="color:#2f9e44">v2_sweepA: 동반구매 `co_*` 2종 **제거(누수)** + `basket_comp` 1종 추가 = **forward 11종**</span>, 역방향 자동 생성)**

| 엣지 타입 | 개수 | 소스 파일 |
|---|---|---|
| `(product, has_kw, keyword)` | 37,333 | `product_keyword_edges.parquet` |
| `(ip, has_kw, keyword)` | <span style="color:#c92a2a">2,019</span> | `ip_keyword_edges.parquet` |
| `(keyword, trend_to, keyword)` | 2,019 | `trend_keyword_edges.parquet` |
| `(product, has_ip, ip)` | 1,213 | `product_ip_edges.parquet` |
| `(product, co_offline, product)` 〔선택〕 <span style="color:#2f9e44">〔**v2 제거: target leakage**〕</span> | 968 | `offline_commerce_edge_lift_pair_out.csv` |
| `(product, co_quick, product)` 〔선택〕 <span style="color:#2f9e44">〔**v2 제거: target leakage**〕</span> | 761 | `quick_commerce_edge_lift_pair_out.csv` |
| <span style="color:#2f9e44">`(keyword, basket_comp, keyword)` 〔v2 신규·누수 희석〕</span> | <span style="color:#2f9e44">13,366</span> | <span style="color:#2f9e44">동반구매 제품쌍의 키워드 교차 집계(support≥2) — `src/data_builder/build_basket_comp_edges.py` → `keyword_basket_comp_edges.parquet`</span> |
| <span style="color:#c92a2a">`(product, sim_kw, product)` 〔exp22 신규·2홉〕</span> | <span style="color:#c92a2a">≈1,048,106</span> | <span style="color:#c92a2a">`A@Aᵀ`(공유 키워드 수 ≥ 3) — 파일 아닌 빌드 시 계산(방향 포함)</span> |
| <span style="color:#c92a2a">`(product, sim_ip, product)` 〔2홉〕</span> | <span style="color:#1971c2">19,776</span> | <span style="color:#1971c2">`A@Aᵀ`(공유 IP 수 ≥ 1 — exp41은 임계 1) — 빌드 시 계산(방향 포함)</span> |
| <span style="color:#1971c2">`(ip, has_ip, ip)` 〔exp41 신규〕</span> | <span style="color:#1971c2">66</span> | <span style="color:#1971c2">`ip_ip_edges_final.parquet` (IP↔IP)</span> |
| <span style="color:#1971c2">`(product, has_kw_via_ip, keyword)` 〔exp41 신규·2홉 P-I-K〕</span> | <span style="color:#1971c2">5,732</span> | <span style="color:#1971c2">`A_PI@A_IK` — IP 보유 키워드를 제품에 직결(빌드 시 계산)</span> |
| <span style="color:#1971c2">`(product, has_kw_ipip, keyword)` 〔exp41 신규·3홉 P-I-I-K〕</span> | <span style="color:#1971c2">4,706</span> | <span style="color:#1971c2">`A_PI@A_II@A_IK` — IP-IP 경로 경유(빌드 시 계산)</span> |
| <span style="color:#1971c2">`(product, has_kw_trend, keyword)` 〔exp41 신규·2홉 P-K-K〕</span> | <span style="color:#1971c2">66,663</span> | <span style="color:#1971c2">`A_PK@A_KK` — 트렌드 속성 키워드를 제품에 직결(빌드 시 계산)</span> |

> <span style="color:#c92a2a">**`sim_kw`/`sim_ip`(exp22 신규)** — "2홉 유사도 엣지". 두 상품이 **공유하는 키워드(또는 IP) 수가 임계 이상**이면 product↔product 직접 엣지를 만든다. 키워드 공유 인접행렬 `A`(product×keyword)에 대해 `S = A@Aᵀ`(상품쌍별 공유 키워드 수)를 구하고, `S ≥ 임계`(<span style="color:#c92a2a">kw≥3</span>, <span style="color:#1971c2">exp41은 ip≥1</span>)이며 자기자신이 아닌 쌍만 엣지로 채택한다(`build_hetero_data.py:_hop2`). 이렇게 2홉(상품→키워드→상품)을 **미리 1홉 엣지로 압축**해 두면, **1층 모델**이 어텐션 한 번으로 "비슷한 기존 상품"에 도달한다(상세·이유 §5).</span>

> <span style="color:#1971c2">**확장 멀티홉 엣지 4종(exp41 신규)** — sim 철학을 "키워드 도달 경로"로 일반화. sim_kw/sim_ip가 *product↔product* 2홉을 1홉으로 압축했다면, exp41은 *product→keyword* 도달 경로 중 의미 있는 멀티홉을 똑같이 미리 1홉으로 깐다(`build_hetero_data.py:195-241`). ① `has_kw_via_ip`(P-I-K, `A_PI@A_IK`): 제품의 협찬 IP가 가진 키워드를 제품에 직결(5,732). ② `has_kw_ipip`(P-I-I-K, `A_PI@A_II@A_IK`): IP-IP 관계까지 한 단계 더 타고 도달(4,706). ③ `has_kw_trend`(P-K-K, `A_PK@A_KK`): 보유 키워드가 `trend_to`로 이어지는 트렌드 속성 키워드를 제품에 직결(66,663 — 이름의 `trend`). ④ `ip→has_ip→ip`(66): IP 간 직접 관계 자체도 관계 타입으로 추가. 모두 **product가 1층 어텐션 한 번으로 IP·트렌드 경유 키워드 맥락까지 흡수**하게 한다(상세 §5.1).</span>

> <span style="color:#2f9e44">**`basket_comp`(keyword↔keyword, v2_sweepA 신규)** — 동반구매(장바구니) 도메인을 **누수 없이** 그래프에 되살리는 동종 엣지. exp41은 동반구매를 *제품↔제품* 입력 엣지(`co_offline`/`co_quick`)로 썼으나 이는 출시-후 행동이라 누수였다(§2.4·요약 박스). v2는 대신 **동반구매된 제품쌍 (A,B)의 보유 키워드를 교차**하여 `(키워드a, 키워드b)` 쌍으로 집계한다(`build_basket_comp_edges.py`): support(그 키워드쌍을 동반구매한 제품쌍 수) ≥ 2인 쌍만 채택(13,366개). **키워드 단위 집계라 특정 제품의 성공이 직접 인코딩되지 않아 누수가 희석**되고("궁합 좋은 *속성* 조합"만 남음), 누수 안전을 위해 train 제품쌍만 쓰는 옵션(`restrict_ids`)도 있다. α_r에서는 거의 균등(≈0.007) 수준으로, 주 신호는 `sim_kw`/`sim_ip`다(§8).</span>

**`(keyword, trend_to, keyword)` 란? — 트렌드 키워드 → 연관 속성 키워드**

같은 `keyword` 타입끼리 잇는 유일한 **동종(homogeneous) 엣지**다(`trend_keyword_edges_final.parquet`, `src_keyword → tgt_keyword`, 2,019개).

- **어떻게 뽑았나(생성 출처)**: 트렌드 도메인 `trend_keywords.parquet`은 〔`트렌드_키워드`(key) → `추출_속성`(value = 그 트렌드에서 추출된 속성 키워드 목록)〕 구조다. 이 `추출_속성_final` 리스트를 **explode**하여 `(트렌드_키워드 → 각 속성 키워드)`를 한 줄씩 엣지로 만든다(`docs/data_schema.md`). 즉 **src = 트렌드 키워드, tgt = 그 트렌드와 연관된 속성 키워드**이며, 단순 "동시출현"이 아니라 "이 트렌드는 이런 속성들과 묶인다"는 의미다.

- **실제 예시** (★=트렌드 키워드 / ○=비트렌드, 모두 실제 데이터):
  ```
  갈비★ → 감칠맛★, 달콤★, 안주○, 짭조름함○, 양념○, 쫄깃함○
  감귤★ → 간식★, 달콤★
  밀크○ → 당○, 디저트○, 음료○            ← 양 끝 모두 비트렌드 (아래 참고)
  ```

- **"키워드-키워드 엣지면 한쪽은 트렌드 키워드여야 하지 않나?" → 거의 맞지만 보장은 안 됨.**
  2,019개 엣지의 **src는 98%(1,979개)가 트렌드 키워드**다(설계상 src=`트렌드_키워드`라 당연). tgt는 트렌드 1,208 / 비트렌드 811로 **tgt가 비트렌드인 경우가 40%**(일반 속성 키워드도 tgt가 됨). 그런데 **양 끝이 모두 비트렌드인 엣지가 19개 존재**한다(`밀크→당`, `티→음료`, `밀크→디저트` 등). 이는 원본에서 src가 트렌드 키워드였으나 키워드 노드 정규화 과정(`트렌드 518→453개`)에서 `is_trend_keyword=1` 플래그를 못 받은 **정규화 불일치** 때문이다. → 결론: 가설은 **전체의 99%에서 성립하지만(한쪽 이상이 트렌드), 엄밀히 보장되지는 않는다**.

이 엣지 덕분에 신상품이 보유 키워드를 넘어 **연관 속성/트렌드 키워드까지 2-hop으로 흡수**한다(§5 Cold Start의 `trend_to` 전이). 예) 신상품이 '갈비' 키워드를 가지면 `trend_to`를 타고 '감칠맛·짭조름함·안주' 맥락이 전파된다.

**역방향(reverse) 엣지 — 무엇이 · 언제 · 왜 생기나**

- **무엇이**: 위 표의 **모든 순방향 엣지**(<span style="color:#1971c2">exp41 기준 12종</span>)에 대해 `(s, r, t) → (t, rev_r, s)` 로 1:1 역방향이 생긴다.

  | 순방향 | 자동 생성 역방향 |
  |---|---|
  | `(product, has_kw, keyword)` | `(keyword, rev_has_kw, product)` |
  | `(ip, has_kw, keyword)` | `(keyword, rev_has_kw, ip)` |
  | `(keyword, trend_to, keyword)` | `(keyword, rev_trend_to, keyword)` |
  | `(product, has_ip, ip)` | `(ip, rev_has_ip, product)` |
  | `(product, co_offline, product)` 〔선택〕 | `(product, rev_co_offline, product)` |
  | `(product, co_quick, product)` 〔선택〕 | `(product, rev_co_quick, product)` |
  | <span style="color:#c92a2a">`(product, sim_kw, product)`</span> | <span style="color:#c92a2a">`(product, rev_sim_kw, product)`</span> |
  | <span style="color:#c92a2a">`(product, sim_ip, product)`</span> | <span style="color:#c92a2a">`(product, rev_sim_ip, product)`</span> |
  | <span style="color:#1971c2">`(ip, has_ip, ip)` 〔exp41〕</span> | <span style="color:#1971c2">`(ip, rev_has_ip, ip)`</span> |
  | <span style="color:#1971c2">`(product, has_kw_via_ip, keyword)` 〔exp41〕</span> | <span style="color:#1971c2">`(keyword, rev_has_kw_via_ip, product)`</span> |
  | <span style="color:#1971c2">`(product, has_kw_ipip, keyword)` 〔exp41〕</span> | <span style="color:#1971c2">`(keyword, rev_has_kw_ipip, product)`</span> |
  | <span style="color:#1971c2">`(product, has_kw_trend, keyword)` 〔exp41〕</span> | <span style="color:#1971c2">`(keyword, rev_has_kw_trend, product)`</span> |

- **언제**: 데이터 적재 시점이 아니라 **매 모델 forward마다** `build_reverse_edges()`가 무조건 실행되어(`HINGNN.forward`), 그 시점에 그래프에 존재하는 모든 순방향 엣지의 인덱스를 뒤집어(`edge_index.flip(0)`) 추가한다. `edge_attr`(Lift 가중치)도 방향 무관하게 그대로 복사된다(`build_reverse_edge_attrs`). → 선택 엣지(`co_offline`/`co_quick`)를 끄면 그 역방향도 당연히 생기지 않는다.
- **왜**: HGT 메시지는 `source → target` **단방향**이라, 역방향이 없으면 정보가 한쪽으로만 흐른다. 예) `(product, has_kw, keyword)`만 있으면 keyword가 product로부터 메시지를 받을 뿐 **product는 자기 키워드로부터 메시지를 못 받는다**. 역방향 `rev_has_kw`가 있어야 product ← keyword 전파가 가능해져 양방향 메시지패싱이 성립한다.
- **방향 보존**: `trend_to`와 `rev_trend_to`는 같은 keyword↔keyword 사이라도 **서로 다른 관계 키**로 취급되어 각자 별도의 HGT 가중치(`w_att`/`w_msg`)와 DiffMG `α_r`를 학습한다. 즉 "전이 방향"의 비대칭성이 보존된다(`라떼→디저트`와 `디저트→라떼`를 다르게 학습).

### 2.2 ID 정규화 — "데이터가 없다"와 "연결이 안 됐다"를 가르는 지점

```python
def norm_id(x) -> str:
    try:    return str(int(float(x)))   # 숫자 코드: leading-zero 제거  "008100"→"8100"
    except (ValueError, TypeError):
        return str(x)                   # CU_xxx 등 합성 ID는 그대로
```

매핑 실패(한쪽 끝 노드가 노드 테이블에 없음) 엣지는 자동 제거되고 `edges_dropped`에 카운트된다. → 커버리지 판단 시 "원래 없는 것"과 "정규화/매핑 누락"을 구분해야 함.

### 2.3 product 레이블·부가 피처·분할

```python
# 타겟: 성공여부 == "성공" → 1
y = (pnodes["성공여부"] == "성공").astype(int)   # 성공 1,197 / 실패 3,836  (양성 23.8%)

# 부가 피처
has_promo = (pnodes[promo_*].sum(axis=1) > 0)    # 프로모션 18종 중 하나라도 → 1.0
insta_mention_30d                                # 출시 30일내 인스타 언급 횟수

# 계층화 분할 (편의점명 × 성공여부)
strata = 편의점명 + "|" + 성공여부               # 예: "CU|성공"
train / val / test = 0.70 / 0.15 / 0.15  (seed=42)
```

> **Data Leakage 차단(규칙)**: 성공여부(KPI)는 오직 product의 타겟 `y`로만 쓰고, keyword/ip 노드 피처에는 절대 주입하지 않는다.

### 2.4 동반구매 엣지의 Lift 가중치

> <span style="color:#2f9e44">**⚠ v2_sweepA에서 본 절 전체 비적용**: 최종 모델 v2는 동반구매(`co_offline`/`co_quick`)를 **입력 엣지에서 완전히 제거**했다(`include_offline/quick_copurchase=false`) — 이 엣지가 출시-후 매출 기반 **target leakage**(성공 보유율 46% vs 실패 5%)이기 때문(요약 박스·`docs/final_model_leakfree_switch_plan.md`). 따라서 아래 Lift 정규화는 v2에 존재하지 않는 엣지에 대한 설명이며, **이력 참고용**이다. v2는 동반구매 도메인을 누수 없이 ① `basket_comp`(키워드 단위 입력, §2.1) ② `HINGNNv2`의 **보조 예측 타깃**(멀티태스크, §4 머리)으로 재배치한다.</span>
>
> <span style="color:#c92a2a">**exp22 주의**: exp22는 `use_lift_weights=false`다. 아래 Lift 정규화 로직은 코드에 그대로 있으나 **exp22는 끈 상태**라 `co_offline`/`co_quick`에 `edge_attr`가 부착되지 않는다 → HGT 메시지에 Lift 곱셈이 적용되지 않고, 동반구매는 "엣지 존재(연결)" 자체로만 쓰인다. (이전 exp06_3은 log1p Lift 가중 ON.)</span>

`co_offline / co_quick` 엣지는 동반구매 향상도(Lift)를 정규화하여 `edge_attr`로 부착한다<span style="color:#c92a2a">(아래는 가중치를 켰을 때의 동작; exp22는 OFF)</span>.

$$
\text{lift\_norm}(e) = \frac{\log(1 + \text{lift}_e)}{\log(1 + \text{lift}_{\max}) + \epsilon}
$$

여기서 $\text{lift}_e$ = 엣지 $e$의 동반구매 향상도(Lift), $\text{lift}_{\max}$ = 해당 채널의 최대 Lift, $\epsilon = 10^{-8}$(0으로 나눔 방지). 결과는 $[0,1]$ 범위로 압축된다.

> 예) 오프라인 Lift 범위 3.0~5,614. log1p 정규화로 극단값(5,614)이 1.0 부근에 압축되어, 한 엣지가 메시지를 과도하게 지배하는 것을 막는다. 이 가중치는 Phase 3 HGT 메시지에 곱해진다.

---

## 3. Phase 2 — 노드 초기 표현 (`_init_product`)

모든 노드는 **공통 hidden 차원 <span style="color:#1971c2">`d = 64`(exp41; exp22까지는 128)</span> <span style="color:#2f9e44">(v2_sweepA: 과적합 억제로 **32**)</span>** 로 표현된다. 노드 타입별로 그 <span style="color:#1971c2">64</span>차원을 얻는 방식이 다르다. <span style="color:#1971c2">(이하 §3 본문·수식·코드의 "128"은 모두 exp41에서 **64**로 읽는다. Xavier 범위 `a`도 `d`가 줄어 미세히 커졌다.)</span> <span style="color:#2f9e44">(v2는 같은 식을 `d=32`로 읽는다. 초기화·집계 로직은 동일.)</span>

| 노드 | 출력 텐서 | 초기화 방식 | "128차원"이 되는 경로 |
|---|---|---|---|
| `keyword` | (<span style="color:#c92a2a">2063</span>, <span style="color:#1971c2">64</span>) | `nn.Embedding(`<span style="color:#c92a2a">2063</span>`, `<span style="color:#1971c2">64</span>`)` — Xavier 균등 `U(−`<span style="color:#1971c2">0.0531</span>`, `<span style="color:#1971c2">0.0531</span>`)` | 학습 파라미터 테이블에서 노드 id로 1행(<span style="color:#1971c2">64</span>) 조회 |
| `ip` | (<span style="color:#c92a2a">335</span>, <span style="color:#1971c2">64</span>) | `nn.Embedding(`<span style="color:#c92a2a">335</span>`, `<span style="color:#1971c2">64</span>`)` — Xavier 균등 `U(−`<span style="color:#1971c2">0.1226</span>`, `<span style="color:#1971c2">0.1226</span>`)` | 학습 파라미터 테이블에서 노드 id로 1행(<span style="color:#1971c2">64</span>) 조회 |
| `product` | (5033, <span style="color:#1971c2">64</span>) | **content aggregation** (학습 임베딩 없음) | 이웃 keyword/ip 임베딩(각 <span style="color:#1971c2">64</span>) 평균 + 부가피처(<span style="color:#1971c2">**2→64** Linear</span>) |

- **keyword / ip — 학습 임베딩 테이블**: 각각 크기 `(N, `<span style="color:#1971c2">64</span>`)`인 학습 파라미터 행렬이며, 노드 id로 한 행(<span style="color:#1971c2">64</span>차원)을 조회한다. 초기값은 **Xavier 균등분포** `U(−a, a)`, `a = √(6 / (N + `<span style="color:#1971c2">64</span>`))` — <span style="color:#1971c2">keyword(N=2063)→`a≈0.0531`, ip(N=335)→`a≈0.1226`</span>(노드 수가 적은 ip의 초기 분산이 더 큼). 이후 역전파로 갱신되는 자유 파라미터다(`nn.init.xavier_uniform_`).

- **product — 저장 임베딩 없이 매 forward 재계산**: product는 임베딩 테이블이 없고, 아래 세 항을 더해 128차원을 만든다.

$$
\mathbf{h}^{(0)}_p \;=\; \underbrace{\operatorname{mean}_{k \in \mathcal{N}_{kw}(p)} \mathbf{e}_k}_{(64,)\ \text{보유 키워드 평균}} \;+\; \underbrace{\operatorname{mean}_{i \in \mathcal{N}_{ip}(p)} \mathbf{e}_i}_{(64,)\ \text{연결 IP 평균}} \;+\; \underbrace{\mathbf{W}_{\text{feat}}\,[\,\text{has\_promo},\ \text{insta\_m30}\,]}_{(2)\to(64,)\ \text{Linear}}
$$

> <span style="color:#c92a2a">**exp22 정정**: 부가 피처 항은 **2차원** `[has_promo, insta_m30]`이며 투영은 `nn.Linear(2,`<span style="color:#1971c2">64</span>`)`이다(이전 exp06_3은 `extra` 7차원을 더해 9차원, `Linear(9,128)`였음).</span> <span style="color:#1971c2">(exp41: 출력 차원 128→64.)</span>

여기서 $\mathbf h^{(0)}_p$ = product $p$의 0층 표현(<span style="color:#1971c2">64</span>차원), $\mathbf e_k / \mathbf e_i$ = 연결된 keyword/ip 임베딩(각 <span style="color:#1971c2">64</span>), $\mathcal N_{kw}(p) / \mathcal N_{ip}(p)$ = $p$에 연결된 키워드/IP 집합, $\mathbf W_{\text{feat}}$ = 부가 피처 투영 행렬(<span style="color:#1971c2">`nn.Linear(2,64)`</span>).

  - **1·2항**: 연결된 keyword/ip 임베딩(각 <span style="color:#1971c2">64</span>차원)을 `scatter(reduce="mean")`로 **평균** → <span style="color:#1971c2">64</span>차원 유지. 연결이 없으면 0 기여(예: IP 미연결 상품은 2항=0).
  - **3항**: <span style="color:#c92a2a">부가 피처 `[has_promo(1), insta_mention_30d(1)]` = **2차원** 을 `product_feat_lin = nn.Linear(2, `<span style="color:#1971c2">64</span>`)` 으로 <span style="color:#1971c2">64</span>차원 승격. (이전 exp06_3은 `extra_feat(7)`까지 9차원이었으나 exp22 코드에서 제거됨.)</span>
  - 세 항 모두 `(`<span style="color:#1971c2">64</span>`,)` 이므로 합도 `(`<span style="color:#1971c2">64</span>`,)` — 이것이 product의 0층 표현이며, 학습 임베딩이 없으므로 **연결 구조가 바뀌면 표현도 즉시 바뀐다**(신상품 즉시 반영).

> **Cold Start 직관**: 출시 이력이 없는 신상품도 "어떤 키워드/IP에 붙어 있는가"만으로 초기 벡터를 얻는다. 예) 신상품 *"말차라떼"* 가 키워드 {말차, 디저트, 달콤}에 연결되면, 그 3개 임베딩 평균이 곧 출발 표현이 된다.

> <span style="color:#c92a2a">**[exp22에서 제거된 설명]** ~~`extra_feat`는 앱 예약 상품 650개(전체의 12.9%)에 한해 앱 행동 7차원(`feat_view`/`cart`/`purchase`/`wishlist`/`search`/`inventory`/`cvr1`)을 추가로 투영한다(`product_node_features_v2.npy`). 나머지 상품은 0벡터.~~ 이 앱 행동 7차원 피처는 **이전 exp06_3 전용**이며, exp22의 `_init_product`는 이를 입력하지 않는다(부가 피처 = `has_promo`·`insta_mention_30d` 2개뿐).</span>

**실제 예시 — product `72369` "CJ)맥스봉구운풍미마늘후랑크80g" 의 초기 표현 $\mathbf h^{(0)}$**

```
입력 재료
  보유 키워드 14개 : [간식, 고기, 풍미, 반찬, 육향, 마늘, 소시지, 야식, 안주, 짭조름함, 고소, 맥스봉, 구움, 후랑크]
  연결 IP          : 없음
  부가 피처(2차원) : has_promo=0,  insta_mention_30d=0          ← [exp22] 앱행동 7차원 제거

h⁰(72369) =  (1) keyword 항 :  mean( e(간식), e(고기), …, e(후랑크) )   ← [exp41] 64차원 임베딩 14개 평균 → (64,)
          +  (2) ip 항      :  연결 IP 없음                            → 0 벡터              → (64,)
          +  (3) feat 항    :  Linear₂→₆₄([has_promo=0, insta_m30=0])                        → (64,)
          ─────────────────────────────────────────────────────────────────────────────────
          =  product 초기 벡터  ∈ ℝ⁶⁴   ([exp41] exp22까지는 ℝ¹²⁸)
```

> 즉 이 상품은 출시 이력(POS) 없이도 "간식·소시지·마늘·짭조름함" 등 14개 키워드 임베딩의 평균 <span style="color:#c92a2a">+ 프로모션·인스타 언급(2차원)</span>만으로 <span style="color:#1971c2">64</span>차원 출발 표현을 얻는다. 이후 Phase 3의 <span style="color:#c92a2a">전파(exp22는 1층)</span>에서 이 키워드들을 공유하는 기존 히트 상품·트렌드 맥락이 <span style="color:#c92a2a">— `sim_kw`/`sim_ip` 2홉 엣지</span><span style="color:#1971c2"> 및 exp41의 `has_kw_via_ip`/`has_kw_trend` 등 확장 멀티홉 엣지</span> <span style="color:#c92a2a">를 통해 —</span> 합쳐진다(Cold Start 극복).

---

## 4. Phase 3 — 한 층의 내부: 3논문 융합

> <span style="color:#2f9e44">**v2_sweepA 적용 메모**: 최종 모델 `HINGNNv2`는 `HINGNN`을 **상속**하며 **주 forward(성공 로짓)는 그대로**다 — 즉 본 §4의 DiffMG·HGT·KGAT 한 층 내부 수식은 v2에서도 **그대로 유효**하다(차원만 `d=32`). v2가 추가한 것은 ① 동반구매 link-prediction **보조 헤드**(`aux_proj`: `score(p,q)=proj(e_p)·e_q`, 동반구매를 입력이 아닌 *예측 타깃*으로 학습 → 멀티태스크) ② 학습 시 **DropEdge**(엣지 무작위 드롭 정규화, 추론 시 p=0)뿐이다(`src/models/hin_gnn_v2.py`). 보조 헤드는 학습에만 쓰이고 성공확률 추론에는 관여하지 않는다.</span>

한 층은 **세 질문에 순서대로 답하는 파이프라인**이다. 한 노드가 이웃으로부터 표현을 갱신할 때:

| 순서 | 질문 | 담당 | 산출 |
|---|---|---|---|
| (1) | 어떤 **관계(엣지 타입)** 를 신뢰할까 | DiffMG | 관계별 스칼라 가중치 $\alpha_r$ |
| (2) | 그 관계 안에서 어떤 **이웃**을, 어떤 메시지로 받을까 | HGT | 이웃집계 메시지 $\mathbf m$ |
| (3) | 받은 메시지를 자기 표현과 어떻게 **융합**할까 | KGAT | 갱신된 노드 표현 $\mathbf h^{(l)}$ |

`hin_gnn.py::forward`의 층 루프:

```python
for layer_i in range(num_layers):
    rel_alpha = self.gates[layer_i]()                       # (1) DiffMG: 관계별 α_r
    agg_dict  = self.hgt[layer_i](x_dict, full_edges,       # (2) HGT: 타입격리 메시지
                                  rel_alpha, full_attrs)
    x_dict    = self.kgat[layer_i](x_dict, agg_dict)        # (3) KGAT: Bi-Interaction 갱신
```

> **이 절의 관통 예시(마라 생태계)**: 아래 세 단계를 하나의 시나리오로 따라간다.
> - keyword `마라` 가 두 상품과 `has_kw` 로 연결: **A=마라라면**(예측 성공확률 $\hat p_A$=0.8), **B=마라젤리**($\hat p_B$=0.2).
> - 관계 후보: <span style="color:#1971c2">exp41 기준 **R=24**(순방향 12 + 역방향 12)</span><span style="color:#2f9e44"> · 최종 v2_sweepA 기준 **R=22**(`co_*` 제거, `basket_comp` 추가)</span>. 그 중 `product→has_kw→keyword`(상품의 키워드 보유)·`co_offline`(동반구매)·<span style="color:#c92a2a">`sim_kw`(유사 상품 2홉)</span>·<span style="color:#1971c2">`has_kw_trend`(트렌드 속성 경유)</span>·`keyword→rev_has_kw→product`(키워드→상품 역전파) 등.
> - 목표: 이 한 층에서 keyword `마라` 와 product `마라라면` 의 표현이 어떻게 갱신되는지.

---

### 4.1 (1) DiffMG 관계 게이트 — "어떤 관계를 신뢰할까"

**목적.** HIN에는 의미가 전혀 다른 관계가 섞여 있다(키워드 보유·IP 협찬·트렌드 전이·동반구매…). 이 중 **성공 예측에 실제로 기여하는 관계는 증폭하고, 노이즈 관계는 0에 수렴**시키는 것이 목적이다. 전통적으로는 사람이 "어떤 메타패스를 쓸지" 손으로 골랐다(편견·블랙박스). DiffMG는 이 선택 자체를 **학습 파라미터**로 바꾼다.

> **R은 순방향 + 역방향을 모두 센다.** 게이트는 `HINGNN`에서 `all_edge_types = 순방향 + [rev_*]` 를 받으므로 **R = 2 × (순방향 엣지 타입 수)** 이고, 순방향과 역방향이 **각각 독립적인 $\alpha_r$** 을 학습한다(예: `trend_to` 와 `rev_trend_to` 는 별개 관계). → exp01(순방향 4종)은 R=**8**, exp06_3(순방향 6종)은 R=**12**, <span style="color:#c92a2a">exp22(순방향 8종 = 기본 6 + `sim_kw`·`sim_ip`)는 R=**16**</span>, <span style="color:#1971c2">exp41(순방향 12종 = 기본 6 + sim 2 + 확장 멀티홉 4)는 R=**24**</span><span style="color:#2f9e44">, 최종 모델 v2_sweepA(순방향 11종 = exp41에서 `co_*` 2종 제거 + `basket_comp` 1종)는 R=**22**</span>. exp01 `report.md`의 α_r 목록에 `keyword__rev_has_kw__product` 등 역방향 항목이 그대로 등장하는 것이 그 증거다.

**개념.** 관계별 중요도를 *이산적 선택*(쓴다/안 쓴다)이 아니라 **연속 변수 $\lambda_r$** 로 두면 미분이 가능해져 역전파로 학습된다. 학습이 끝나면 $\lambda$를 softmax로 정규화한 $\alpha_r$ 이 "관계 신뢰도"가 되고, 이는 그대로 **XAI 근거**(MD가 읽는 관계 중요도 맵)가 된다.

**수식 (논문 → 구현).** 논문(DiffMG §4.2)은 DAG의 각 링크마다 후보 엣지 타입을 섞는다:

$$
\alpha^m_{k,i} = \frac{\exp(\lambda^m_{k,i})}{\sum_{m'} \exp(\lambda^{m'}_{k,i})}, \qquad
\bar f_{k,i}(\mathbf H^{(i)}) = \sum_m \alpha^m_{k,i}\, f(\mathbf H^{(i)}; \mathbf A^m_{k,i})
$$

여기서 $\lambda^m_{k,i}$ = DAG 링크 $(k,i)$에서 후보 엣지타입 $m$의 아키텍처 로짓, $\alpha^m_{k,i}$ = 그 softmax 가중치, $\mathbf A^m_{k,i}$ = 엣지타입 $m$의 인접행렬, $f(\cdot)$ = 메시지패싱 1스텝, $\mathbf H^{(i)}$ = 중간 상태 $i$의 노드 표현. (본 구현은 이 링크별 혼합을 아래 단일 게이트로 단순화)

**두 번째 수식 $\bar f$ 상세 — "링크 하나 위에서 어떤 엣지 타입으로 한 홉 전파할지를 soft하게 섞기".** 메타그래프 DAG의 링크 $(i\to k)$마다, 후보 연산을 $\alpha$로 가중 평균한 결과가 그 링크의 출력 $\bar f_{k,i}$다.

- **인접행렬 $\mathbf A^m_{k,i}$ — "후보 한 개"**: 논문 §2 정의대로 *엣지 타입 $r$로 만들어진 $N\times N$ 행렬 $\mathbf A_r$* ($\mathbf A_r[u,v]=1$ ⇔ 타입 $r$ 엣지 $u\to v$ 존재; 방향이 다르면 다른 행렬 — 예: $\mathbf A_{PA}$=저자→논문, $\mathbf A_{AP}$=논문→저자). 각 링크의 **후보 집합**은 $\mathbf A_{k,i}=\mathbf A\cup\{\mathbf I\}\cup\{\mathbf O\}$ = (실제 엣지 타입 인접행렬 전부) ∪ {항등행렬 $\mathbf I$ = "전파 없이 표현 그대로 통과"} ∪ {영행렬 $\mathbf O$ = "메시지 안 보냄 = 링크 끊기"}. 즉 $m$은 이 집합을 도는 인덱스, $\mathbf A^m_{k,i}$는 **그중 딱 하나**(특정 엣지 타입 / $\mathbf I$ / $\mathbf O$).
- **$f(\cdot)$ — "그 한 개로 한 홉 메시지패싱"**: 선택된 인접행렬을 따라 표현을 1회 전파하는 **GCN식 한 스텝**. 논문 식(1) $\mathbf Z=\sigma(\hat{\mathbf A}\mathbf X\boldsymbol\Theta)$가 원형이며 $f(\mathbf H^{(i)};\mathbf A^m)=\sigma(\hat{\mathbf A}^m\mathbf H^{(i)}\boldsymbol\Theta)$ — 정규화 $\hat{\mathbf A}^m$을 곱해 그 엣지 타입 이웃만 모으고 $\boldsymbol\Theta$로 변환+비선형.
- **합 $\sum_m\alpha^m f$ — "연속 완화"**: 링크마다 엣지 타입을 하나로 딱 고르면 이산이라 미분 불가 → 후보 연산을 $\alpha^m$으로 **볼록결합**해 미분 가능하게 만든다. 탐색 종료 후 $\arg\max_m\alpha^m$으로 승자 엣지 타입 하나만 남겨 **이산 메타그래프**를 확정.

> **우리 구현과 대응**: 본 구현은 중간상태 DAG 없이 층마다 단일 게이트 $\alpha_r$로 단순화했지만 역할은 1:1 대응한다 — $\mathbf A^m$ ↔ 엣지 타입별 `edge_index`(COO 인접), $f(\mathbf H;\mathbf A^m)$ ↔ HGT 메시지+집계(`msg=W_msg·h` 어텐션 가중합, §4.2), $\sum_m\alpha^m f$ ↔ `msg * rel_alpha[et]`의 관계별 합산(§4.4). $\mathbf I,\mathbf O$ 후보는 단일 게이트엔 명시되지 않으나 $\alpha_r\to0$이 $\mathbf O$(관계 도태) 역할을 한다.

본 구현(`diffmg_pruner.py`)은 이를 **전체 그래프의 관계 R개에 대한 단일 softmax 게이트**로 단순화한다(층마다 1개):

$$
\alpha_r = \frac{\exp(\lambda_r / \tau)}{\sum_{r'=1}^{R} \exp(\lambda_{r'} / \tau)}, \qquad \sum_{r=1}^{R}\alpha_r = 1
$$

- $\lambda_r$ : 관계 $r$의 학습 로짓(`self.logits`, 초기값 0 → 처음엔 모든 관계 균등 $1/R$).
- $\tau$ : temperature. **작을수록 분포가 뾰족**해져 소수 관계에 가중치가 몰린다(선택 강화). 클수록 평평(=모든 관계 비슷하게 사용).
- 출력 $\alpha_r$ : 합이 1인 관계별 스칼라. HGT 메시지에 곱해져 게이팅.

```python
self.logits = nn.Parameter(torch.zeros(R))          # = λ, 연속 변수 (초기 균등)
weights = softmax(self.logits / temperature, dim=0) # τ↓ → 분포 날카로워짐(선택 강화)
# forward 시: msg = msg * rel_alpha[et]   (관계별 스칼라 게이팅)
```

> **왜 $\tau$를 낮췄나**: exp01($\tau$=1.0)은 학습 후에도 $\alpha_r$이 거의 균등(≈1/8=0.125, §8 참고)이라 게이트가 "선택"을 안 했다. <span style="color:#c92a2a">exp22는(exp06_3와 동일) $\tau$=0.5 + `lr_alpha` 0.005→0.02 로 올려 관계 선택을 활성화했다.</span>

**예시 (설명 단순화를 위해 R=8 가정 = exp01 관계 수, $\tau$=0.5).** 학습 결과 로짓이 `λ = [0.8, 0.1, 0.1, 0, 0, 0, 0, 0]` 이라 하자(`has_kw`가 가장 큼). $\tau$로 나누면 `λ/τ = [1.6, 0.2, 0.2, 0, 0, 0, 0, 0]`.

```
exp(1.6)=4.953,  exp(0.2)=1.221 (×2),  exp(0)=1.0 (×5)
합 = 4.953 + 2(1.221) + 5(1.0) = 12.40
α(has_kw)      = 4.953 / 12.40 ≈ 0.40   ← 가장 신뢰하는 관계
α(0.2 관계 2개) = 1.221 / 12.40 ≈ 0.099  (각)
α(나머지 5개)   = 1.0   / 12.40 ≈ 0.081  (각)
```

→ "상품-키워드 보유 관계가 성공 신호의 40%를 나른다"를 모델이 **스스로** 판단한 상태. 마라 예시에서 `has_kw` 경로의 메시지는 0.40배로 살아남고, 약한 관계의 메시지는 0.08배로 눌린다.

---

### 4.2 (2) HGT 타입격리 어텐션 — "그 관계 안에서 어떤 이웃을, 어떤 메시지로"

**목적.** 같은 관계라도 이웃마다 중요도가 다르다(마라 → 마라라면 vs 마라젤리). 동시에 **관계 타입마다 정보의 성질이 다르다**(키워드 텍스트 vs 동반구매 통계). 일반 GNN(GCN/GraphSAGE)은 이웃을 $1/N$로 균등 평균해 둘을 뭉개버린다(**over-smoothing**: 모든 노드가 비슷해짐). HGT의 목적은 ① 이웃별 차등 어텐션 + ② **관계 타입별 독립 가중치**로 이 혼합을 막는 것.

**개념.** Transformer의 Q·K·V 어텐션을 이기종 그래프로 확장한다. 타겟 노드가 **Query**, 이웃(소스)이 **Key/Value**. 단, 노드 타입별로 Q/K/V 투영을 분리하고, **엣지 타입별로 $W^{ATT}, W^{MSG}$ 행렬을 따로** 둔다(메타관계 $\langle\tau(s),\phi(e),\tau(t)\rangle$ 격리). 그래서 "키워드 보유" 메시지와 "동반구매" 메시지가 절대 같은 행렬을 거치지 않는다.

**수식 (논문 → 구현).** 논문(HGT §2): 엣지 $e=(s,t)$에 대해

$$
\text{ATT-head}_i(s,e,t) = \Big( K_i(s)\, W^{ATT}_{\phi(e)}\, Q_i(t)^\top \Big)\cdot \frac{\mu_{\langle\tau(s),\phi(e),\tau(t)\rangle}}{\sqrt{d}}, \qquad
\alpha = \underset{s\in N(t)}{\text{Softmax}}\big(\text{ATT-head}\big)
$$
$$
\text{Message}_i(s,e,t) = M\text{-Linear}_{\tau(s)}\big(H^{(l-1)}[s]\big)\, W^{MSG}_{\phi(e)}, \qquad
\tilde H^{(l)}[t] = \sum_{s\in N(t)} \alpha(s,e,t)\cdot \text{Message}(s,e,t)
$$

항별 의미:
- $Q_i(t)=$ 타겟 노드 Query, $K_i(s)=$ 소스 노드 Key (노드 타입별 Linear로 투영, head $i$).
- $W^{ATT}_{\phi(e)}$ : **엣지 타입별** 어텐션 변환 행렬 — 관계 의미를 주입.
- $\mu$ : 관계별 prior 스칼라(이 관계 자체가 평균적으로 얼마나 중요한가).
- $\text{Softmax}_{s\in N(t)}$ : **타겟 노드가 받는 incoming 엣지들끼리** 정규화(합 1). → 이웃 차등.
- $W^{MSG}_{\phi(e)}$ : 엣지 타입별 메시지 변환. 어텐션 가중으로 합산.

**Q·K·V 어텐션이 실제로 어떻게 돌아가나 (위 수식과 1:1).** 표준 Transformer 어텐션은 $\text{softmax}(QK^\top/\sqrt d)\,V$ — "Query가 Key들과 얼마나 맞는지로 가중치를 만들어 Value를 가중합"이다. HGT는 이 3요소를 그래프·이기종용으로 확장한다. 비유: **타겟 노드가 면접관(Query: '나는 어떤 이웃 정보가 필요한가'), 이웃 소스가 지원자(Key: '나는 이런 특징을 가졌다' / Value: '내가 실제로 넘겨줄 내용')**.

1. **Q/K/V 생성 — 노드 타입별 투영.** 표준은 $Q=XW_Q,\,K=XW_K,\,V=XW_V$로 한 벌의 행렬을 쓴다. HGT는 **노드 타입마다 다른 투영**을 쓴다 — 타겟은 `q_lin[t_type]`로 $Q_i(t)$, 소스는 `k_lin[s_type]`로 $K_i(s)$, 메시지는 `M-Linear_{τ(s)}`로 Value를 만든다(식 328의 $M\text{-Linear}_{\tau(s)}$). product의 Query 공간과 keyword의 Key 공간이 분리돼 타입 의미가 섞이지 않는다.
2. **관계 주입 — $K$에 $W^{ATT}_{\phi(e)}$.** 표준은 $Q\cdot K$를 바로 내적하지만, HGT는 Key를 **엣지 타입별 행렬 $W^{ATT}_{\phi(e)}$로 한 번 더 회전**시킨다(`k_e = K·w_att[et]`). 그래야 같은 소스라도 `has_kw` 관계로 볼 때와 `co_offline` 관계로 볼 때 다른 Key가 된다. → 식 324의 $K_i(s)\,W^{ATT}_{\phi(e)}\,Q_i(t)^\top$가 바로 "관계 렌즈를 낀 Q·K 유사도".
3. **스코어 = Q·K_e, 그리고 스케일·prior.** $\text{score}=Q_i(t)\cdot(K_i(s)W^{ATT})$ 가 "타겟의 질의와 소스의 관계별 키가 얼마나 부합하나". 여기에 $1/\sqrt d$(gradient 안정)와 **관계 prior $\mu_{\langle\tau(s),\phi(e),\tau(t)\rangle}$**(이 메타관계가 평균적으로 얼마나 중요한가)를 곱한다(식 324 우변).
4. **Softmax — 이웃끼리 경쟁.** 표준 어텐션은 전체 토큰에 softmax하지만, 그래프에선 **타겟 $t$로 들어오는 엣지 $s\in N(t)$끼리만** 정규화한다(식 325, `softmax(score, dst)`). 그래서 $\alpha(s,e,t)$는 "타겟이 받는 이웃들 사이의 상대 중요도"(합 1)가 된다.
5. **Value 가중합 = 이웃집계.** 마지막으로 $\alpha$로 Value를 가중합한다 — $\tilde H^{(l)}[t]=\sum_{s}\alpha(s,e,t)\cdot\text{Message}(s,e,t)$ (식 329·330). Value도 **엣지 타입별 $W^{MSG}_{\phi(e)}$**를 거치므로, "무엇을 얼마나 가져올지"(α)와 "어떤 형태로 가져올지"($W^{MSG}$)가 모두 관계별로 분리된다.

> 한 줄 요약: **Q(타겟의 질의) · K(관계 렌즈 낀 소스 키) → softmax(이웃 경쟁) → V(관계별 메시지) 가중합.** 표준 QKV 대비 (a) Q/K/V 투영이 노드 타입별, (b) K·V가 엣지 타입별 $W^{ATT}/W^{MSG}$로 한 번 더 변환, (c) softmax가 타겟의 이웃으로 한정 — 이 세 확장이 이기종 그래프의 타입 격리를 만든다.

**본 구현을 수식으로 옮기면 (논문과 달라진 부분만 반영).** 어텐션 스코어·softmax는 논문과 구조 동일하되, 메시지에 **Lift·DiffMG $\alpha_r$** 가 곱해지고, 출력은 **A-Linear까지만**(활성 $\sigma$·잔차 $+H^{(l-1)}[t]$ 는 §4.3 KGAT로 이관):

$$
\text{score}_i(s,e,t) = \Big(Q_i(t)\cdot\big(W^{ATT}_{\phi(e)}K_i(s)\big)\Big)\cdot\frac{\mu_{\langle\tau(s),\phi(e),\tau(t)\rangle}}{\sqrt{d_k}},\qquad
\alpha_i(s,e,t) = \text{Dropout}\Big(\underset{s\in N_r(t)}{\text{Softmax}}\;\text{score}_i\Big)
$$
$$
\tilde H^{(l)}[t] = \text{A-Linear}_{\tau(t)}\Bigg(\;\big\Vert_{i=1}^{H}\;\sum_{r\in\mathcal R}\sum_{s\in N_r(t)} \underbrace{\alpha^{\text{DiffMG}}_r}_{\text{(1) §4.1}}\cdot \underbrace{\text{lift}_{s\to t}}_{\text{§2.4}}\cdot\, \alpha_i(s,e,t)\cdot\big(W^{MSG}_{\phi(e)} V_i(s)\big)\Bigg)
$$

여기서 $i$ = head 번호($H$=4), $\Vert$ = head concat, $V_i(s)=\text{V-Linear}_{\tau(s)}(\mathbf h_s)$ = 소스 메시지 투영(논문 $\text{M-Linear}$ 역할), $\mathcal R$ = 전체 관계(순+역), $\alpha^{\text{DiffMG}}_r$·$\text{lift}_{s\to t}$ = **논문 HGT엔 없는 삽입항**(관계 게이트·동반구매 Lift), $\tilde H^{(l)}[t]$ = HGT가 내보내는 이웃집계(=KGAT의 입력 $\mathbf m$). 논문 대비 (a) 메시지에 $\alpha^{\text{DiffMG}}_r,\text{lift}$ 곱, (b) 출력에서 $\sigma$ 와 잔차 $+H^{(l-1)}[t]$ 를 제거(KGAT로 이관) — 이 두 가지가 차이다.

본 구현(`hgt_layer.py`) — $N_s/N_t$=src/dst 노드수, $E$=엣지수, $H$=heads, $d_k=d/H$=<span style="color:#1971c2">16</span> (<span style="color:#1971c2">exp41: d=64/H=4</span>):

```python
k = k_lin[s_type](x[s_type]).view(-1, H, d_k)   # 소스 타입별 Key  (N_s,H,d_k)
q = q_lin[t_type](x[t_type]).view(-1, H, d_k)   # 타겟 타입별 Query (N_t,H,d_k)
k_e = einsum("ehk,hkd->ehd", k[src], w_att[et]) # 엣지 타입별 W_att 적용
score = (q[dst] * k_e).sum(-1) * mu[et] / sqrt(d_k)   # (E, H)  q·k 유사도
alpha = softmax(score, dst)                     # 타겟 노드별 incoming 엣지 정규화
msg   = einsum("ehk,hkd->ehd", v[src], w_msg[et]) * alpha   # 엣지 타입별 W_msg × α
msg   = msg * lift_w * rel_alpha[et]            # ← Lift 가중치 & DiffMG α_r(4.1) 결합
agg[t_type] += scatter(msg, dst, reduce="sum")  # 타겟으로 합산
```

핵심: **엣지 타입마다 `w_att`, `w_msg`, `mu`가 따로 존재**(ParameterDict). `(product,has_kw,keyword)`의 "키워드 보유" 신호와 `(product,co_offline,product)`의 "동반구매" 신호가 서로 다른 행렬로 처리되어 섞이지 않는다 → **over-smoothing 차단**. 또 4.1의 $\alpha_r$과 2.4의 Lift가 여기서 메시지에 곱해진다(세 메커니즘의 결합 지점).

**예시 (마라, H=1, $d_k$=2 로 단순화).** keyword `마라`(=타겟 Query)가 두 상품(=소스)으로부터 `rev_has_kw` 로 메시지를 받는다. 투영·$W^{ATT}$ 적용 후 q·k 유사도가:

```
score(마라라면 → 마라) = 2.0     # 마라가 라면 맥락에서 강하게 정의됨
score(마라젤리 → 마라) = 0.6     # 약한 결합

타겟(마라) 기준 softmax:
  exp(2.0)=7.39,  exp(0.6)=1.82,  합=9.21
  α(마라라면)=7.39/9.21 ≈ 0.80
  α(마라젤리)=1.82/9.21 ≈ 0.20

마라의 이웃집계 m(마라) = 0.80·msg(마라라면) + 0.20·msg(마라젤리)
```

→ 단순 평균(각 0.5)이라면 잃었을 "**마라는 젤리보다 라면 맥락에서 더 강하다**"를 보존. 만약 `co_offline` 관계로도 마라가 무언가와 연결돼 있었다면, 그 메시지는 **다른 $W^{MSG}$** 를 거쳐 별도 계산된 뒤 4.1의 $\alpha_{co\_offline}$ 배율로 합쳐진다.

---

### 4.3 (3) KGAT Bi-Interaction — "받은 메시지를 자기 표현과 어떻게 융합할까"

**목적.** HGT가 만든 이웃집계 $\mathbf m$ 을 노드의 기존 표현 $\mathbf h$ 에 합칠 때, 단순 덧셈만 하면 "**둘 다 강한 특징의 시너지**"를 못 잡는다. 목적은 *덧셈(정보 보존)* 과 *원소곱(상호작용 증폭)* 을 **둘 다** 써서 고차 결합까지 포착하는 것.

**개념.** 두 갈래로 융합한다. ① $\mathbf h+\mathbf m$ : 자기 정보와 이웃 정보를 **합쳐 보존**. ② $\mathbf h\odot\mathbf m$(원소곱): 같은 차원에서 **둘 다 큰 값**일 때만 커진다 → "내 속성과 이웃 맥락이 동시에 강한 축"을 증폭(예: 나도 '매운맛'↑ + 이웃도 '매운맛'↑ → 곱 폭증). 마지막에 잔차 + LayerNorm으로 안정화.

**수식 (논문 → 구현).** 논문(KGAT §3.2, Eq.8):

$$
f_{\text{Bi}} = \underbrace{\text{LeakyReLU}\big(W_1 (\mathbf h + \mathbf m)\big)}_{\text{덧셈: 정보 보존}} + \underbrace{\text{LeakyReLU}\big(W_2 (\mathbf h \odot \mathbf m)\big)}_{\text{원소곱: 특징 상호작용}}
$$

여기서 $\mathbf h\,(=e_h)$ = 노드 자기표현, $\mathbf m\,(=e_{\mathcal N_h})$ = HGT가 만든 이웃집계, $W_1, W_2$ = 각각 덧셈·원소곱 항의 학습 변환행렬, $\odot$ = 원소별 곱, LeakyReLU = 음수 기울기 0.2의 활성함수.

**본 구현을 수식으로 옮기면 (노드 타입 $\tau$에 대해):**

$$
\mathbf h^{(l)}_{\tau} = \text{LayerNorm}_{\tau}\Big( \underbrace{\text{Dropout}\big[\, \text{LeakyReLU}(W^{add}_{\tau}(\mathbf h+\mathbf m)) + \text{LeakyReLU}(W^{mul}_{\tau}(\mathbf h\odot\mathbf m)) \,\big]}_{f_{\text{Bi}}\text{(논문식 그대로)}} \;+\; \underbrace{\mathbf h}_{\text{잔차}} \Big)
$$

여기서 $\tau\in\{\text{product, keyword, ip}\}$ = 노드 타입, $W^{add}_{\tau}/W^{mul}_{\tau}$ = **타입별로 분리된** $W_1/W_2$(논문은 전 노드 공유, 본 구현은 `w_add[nt]`/`w_mul[nt]`로 격리), $\mathbf m$ = **HGT 어텐션+DiffMG $\alpha_r$+Lift로 만든 이웃집계**(논문의 TransR 어텐션 $\pi$가 아님, §4.1·§4.2), $\text{Dropout}$ = 학습 시 0.3, $\text{LayerNorm}_{\tau}+\mathbf h$ = **논문에 없는 잔차+정규화**(Transformer식 안정화).

> **논문식과의 차이 요약**: 안쪽 $f_{\text{Bi}}$(덧셈 항+원소곱 항)는 논문 그대로지만, ① $W$를 타입별로 분리, ② 이웃집계 $\mathbf m$을 KGAT 어텐션이 아닌 HGT+DiffMG로 생성, ③ Dropout, ④ 잔차$+$LayerNorm 으로 감싼 점이 다르다(§4.3 말미 참조).

본 구현(`kgat_layer.py`) — `h`=자기표현, `agg`(=$\mathbf m$)=HGT 이웃집계:

```python
add_term = act(w_add[nt](h + agg))      # 덧셈 항  LeakyReLU(W1(h+m))
mul_term = act(w_mul[nt](h * agg))      # 원소곱 항 LeakyReLU(W2(h⊙m))
z   = dropout(add_term + mul_term)      # Bi-Interaction
out = norm[nt](z + h)                   # 잔차 연결 + LayerNorm (h 소실/발산 방지)
```

**예시 (마라라면 product, 2차원으로 단순화).** 차원0='매운맛', 차원1='디저트성'.

```
h(마라라면)   = [1.0, 0.2]    # 자신: 매운맛 강, 디저트 약
m(이웃집계)   = [0.8, 0.1]    # 이웃(마라 키워드 맥락)도 매운맛 강

덧셈 항 입력 :  h + m   = [1.8, 0.3]     → 정보를 합쳐 보존
원소곱 항 입력:  h ⊙ m   = [0.80, 0.02]   → 매운맛(0.8) 시너지 폭증 / 디저트(0.02) 거의 소거
```

→ 원소곱 항이 "마라라면은 **매운맛 축에서 이웃과 강하게 공명**한다"를 포착해 그 축을 키운다. 디저트 축은 양쪽 다 약하므로(0.2×0.1) 자동으로 죽는다. 덧셈만 썼다면 [1.8, 0.3] 으로 디저트 축도 살아남아 신호가 흐려졌을 것. 두 항을 $W_1,W_2$로 변환·합산 후, 잔차로 원래 $\mathbf h$를 더해 LayerNorm → 갱신된 `마라라면` 표현.

---

### 4.4 한 층 종합 — 세 단계가 합쳐진 갱신식

product 노드 $p$의 한 층 갱신(역방향 포함 전체 관계 $\mathcal R$):

$$
\mathbf m_p = \sum_{r \in \mathcal R} \underbrace{\alpha_r}_{\text{(1)DiffMG}} \!\!\sum_{s \in N_r(p)} \text{lift}_{s\to p}\cdot \underbrace{\text{Att}_r(s,p)}_{\text{(2)HGT}}\, \big(W^{MSG}_r \mathbf h_s\big)
$$
$$
\mathbf h_p^{(l)} = \underbrace{\text{LayerNorm}\Big( \text{LeakyReLU}(W_1(\mathbf h_p^{(l-1)} + \mathbf m_p)) + \text{LeakyReLU}(W_2(\mathbf h_p^{(l-1)} \odot \mathbf m_p)) + \mathbf h_p^{(l-1)} \Big)}_{\text{(3)KGAT Bi-Interaction + 잔차}}
$$

여기서 $\mathcal R$ = 전체 관계(순방향+역방향), $\alpha_r$ = (1)관계 게이트, $N_r(p)$ = 관계 $r$로 $p$에 연결된 이웃, $\text{lift}_{s\to p}$ = Lift 가중치(co_* 외 관계는 1), $\text{Att}_r(s,p)$ = (2)HGT 어텐션, $W^{MSG}_r$ = 관계별 메시지 행렬, $\mathbf m_p$ = 이웃집계, $\mathbf h_p^{(l)}$ = $p$의 $l$층 표현, $W_1/W_2$ = (3)KGAT 변환.

**한 줄 해석**: 관계마다 (1)DiffMG 신뢰도 $\alpha_r$ 로 가중하고, 그 안에서 (2)HGT 어텐션 $\text{Att}_r$ 와 Lift로 이웃 메시지를 모은 뒤($\mathbf m_p$), (3)KGAT가 자기표현과 덧셈·원소곱으로 융합해 $\mathbf h_p^{(l)}$ 를 만든다.

> **마라 시나리오 종합**: keyword `마라`는 (2)에서 마라라면(0.8)·마라젤리(0.2)를 차등 집계하고, 그 경로 전체는 (1)에서 `has_kw` 신뢰도 0.40배로 스케일된다. product `마라라면`은 (3)에서 "매운맛 축 시너지"를 증폭한 새 표현을 얻는다. <span style="color:#c92a2a">~~이 한 층을 2번 반복(§5)하면~~</span> <span style="color:#1971c2">exp41은 이 한 층(1층)만 거치면 — 사전 구성한 `sim_kw` 2홉 엣지를 통해 —</span> 마라라면은 *마라 키워드를 공유하는 다른 히트 상품* 의 성공 맥락까지 2-hop으로 흡수한다.

---

## 5. Phase 4 — L층 스택 = $A^L$ 다중홉 (KGAT 고차 전파)

**논문(KGAT §3.2 Eq.9)**: 한 층을 $L$회 재귀하면 $L$-hop 이웃까지 정보가 수렴.

$$
\mathbf e_h^{(l)} = f\big(\mathbf e_h^{(l-1)},\ \mathbf e_{\mathcal N_h}^{(l-1)}\big)
$$

여기서 $\mathbf e_h^{(l)}$ = 노드 $h$의 $l$층 표현, $f(\cdot)$ = 한 층 갱신 함수(§4.4 전체), $\mathbf e_{\mathcal N_h}^{(l-1)}$ = 직전 층의 이웃집계, $l$ = 층 인덱스. $l$을 키울수록 인접행렬의 거듭제곱 $A^l$ 효과로 더 먼 홉까지 정보가 수렴한다.

> <span style="color:#c92a2a">**[exp22의 핵심 설계 전환]** 위 KGAT 수식(층을 쌓아 $A^L$로 다중홉)은 이전 exp06_3(2층)의 방식이다. **exp22는 `num_layers=1`** 이다. 대신 2홉 정보를 **층 스택이 아니라 그래프에 미리 깔아 둔 2홉 유사도 엣지(`sim_kw`/`sim_ip`)** 로 가져온다(아래 §5.1). 즉 "$A^2$를 층 재귀로 만들지 않고, 의미 있는 $A^2$ 부분만 명시적 1홉 엣지로 바꿔 1층에서 흡수"한다.</span> <span style="color:#1971c2">exp41도 동일하게 `num_layers=1`이며, 이 "의미 있는 멀티홉만 명시적 1홉 엣지로" 철학을 product↔product 2홉(sim)을 넘어 **product→keyword 도달 경로**(IP 경유 `has_kw_via_ip`/`has_kw_ipip`, 트렌드 경유 `has_kw_trend`)까지 확장했다(§5.2).</span>

<span style="color:#1971c2">exp41 신상품 $p$의 정보 도달 범위(1층 + 사전구성 멀티홉 엣지):</span>

```
0-hop  product 자신 (보유 키워드/IP 평균)
1-hop  직접 연결 keyword·ip·동반구매 상품  +  [exp22] sim_kw/sim_ip로 직결된 "유사 상품"
       +  [exp41] has_kw_via_ip/has_kw_ipip(IP 경유 키워드)·has_kw_trend(트렌드 속성 키워드)로 직결
       (즉 1층 어텐션 한 번에 "유사 상품 + IP/트렌드 경유 키워드 맥락"까지 도달)
```

> <span style="color:#2f9e44">**v2_sweepA 보정**: v2의 1-hop에는 **동반구매 상품이 없다**(`co_offline`/`co_quick` 제거). 대신 신규 `basket_comp`(keyword↔keyword)가 더해져, 제품의 보유 키워드가 1층에서 **동반구매로 자주 함께 등장한 보완 키워드**까지 닿는다(제품→키워드→보완키워드). 즉 동반구매 정보는 *제품 직결*이 아니라 *키워드 보완 경로*로 흡수되어 누수 없이 작동한다.</span>

> **Cold Start 극복 시나리오**: 신상품 *"흑임자 라떼"*
> - 1-hop: {흑임자, 라떼, 고소함} 키워드 + 연결 IP
> - <span style="color:#c92a2a">[exp22] **`sim_kw`로 직결**: '흑임자·라떼·고소함'을 ≥3개 공유하는 **기존 히트 상품**(예: 흑임자라떼·흑임자아이스크림)에 1홉으로 바로 닿아 그 성공 맥락을 흡수</span>
>
> 출시 이력이 0이어도 <span style="color:#c92a2a">유사도 엣지로 "흑임자 카테고리의 성공 패턴"을 1층에서 흡수</span>한다. 이것이 KGAT(고차 연결)를 차용하되 exp22가 택한 방식이다.

<span style="color:#c92a2a">층을 깊게 쌓지 않는 이유: 깊은 스택은 over-smoothing·파라미터 증가 위험이 있고, 무엇보다 "어떤 2홉을 볼지"가 불투명하다. exp22는 **의미 있는 2홉(공유 키워드/IP ≥ 임계)만 골라 명시적 엣지로 만든 뒤 1층**을 쓴다 → 노이즈 2홉을 차단하고 해석이 명확해진다. (실험: exp18→exp22 계열에서 이 방식의 2홉 후보가 test PR-AUC 최고.)</span>

### 5.1 <span style="color:#c92a2a">2홉 유사도 엣지 `sim_kw`/`sim_ip` (exp22 신규)</span>

<span style="color:#c92a2a">**무엇**: 두 상품이 공유하는 키워드(또는 IP) 수가 임계 이상이면 잇는 **product↔product 엣지**. "상품→키워드→상품"이라는 2홉 메타패스를, 빌드 시점에 **1홉 엣지로 미리 압축**한 것이다.</span>

<span style="color:#c92a2a">**어떻게(생성식, `build_hetero_data.py:_hop2`)**:</span>

$$
\mathbf S_{kw} = \mathbf A_{pk}\,\mathbf A_{pk}^{\top}, \qquad
E_{\text{sim\_kw}} = \{\,(i,j)\ :\ \mathbf S_{kw}[i,j] \ge \theta_{kw},\ i \ne j\,\}
$$

<span style="color:#c92a2a">**$\mathbf A$와 $\mathbf S$는 둘 다 행렬이다.** $\mathbf A_{pk}\in\{0,1\}^{P\times K}$ = product×keyword **이진 인접행렬**(`has_kw`; 행=상품 $P$=5,033, 열=키워드 $K$=2,063, 상품 $i$가 키워드 $k$를 가지면 1). 그 곱 $\mathbf S_{kw}=\mathbf A_{pk}\mathbf A_{pk}^{\top}\in\mathbb Z^{P\times P}$ = **상품×상품 행렬**이고, 원소 $\mathbf S_{kw}[i,j]$ = 상품 $i,j$가 **공유하는 키워드 수**($i$행·$j$열 내적 = 두 상품의 키워드 벡터가 동시에 1인 개수). $\theta_{kw}$ = 임계(**exp22: kw≥3**). `sim_ip`도 동일하게 $\mathbf A_{pi}\in\{0,1\}^{P\times I}$의 곱 $\mathbf A_{pi}\mathbf A_{pi}^{\top}\in\mathbb Z^{P\times P}$로 만들며 임계 $\theta_{ip}$**=2**(<span style="color:#1971c2">exp41은 **=1** — 공유 IP 1개만 있어도 엣지</span>). 자기자신($i=j$)은 제외. 실제로는 밀집 $P\times P$를 만들지 않고 **희소행렬**(`scipy.sparse`)로 계산한다.</span>

<span style="color:#c92a2a">**왜 (L=2 층 스택 대비 장점)**:</span>
- <span style="color:#c92a2a">**① 명시적**: "공유 키워드 3개 이상"이라는 해석 가능한 기준으로 어떤 2홉을 쓸지 정한다(층 스택의 $A^2$는 모든 2홉을 뭉뚱그림).</span>
- <span style="color:#c92a2a">**② 노이즈 차단**: 임계 미만(우연히 1~2개만 겹치는) 쌍은 엣지가 안 생겨 약한 2홉이 끼어들지 않는다.</span>
- <span style="color:#c92a2a">**③ 파라미터·깊이 절약**: 1층만으로 2홉에 도달 → over-smoothing 위험↓, 학습 안정.</span>
- <span style="color:#c92a2a">**④ DiffMG 게이트가 가중 학습**: `sim_kw`/`sim_ip`도 하나의 관계 타입이라 $\alpha_r$가 "유사도 2홉을 얼마나 신뢰할지"를 데이터로 정한다(§8).</span>

> <span style="color:#c92a2a">**임계의 트레이드오프**(실험 근거): 키워드 임계를 낮추면(kw≥2) 엣지가 폭증해 OOM·노이즈, 높이면(kw≥8) 너무 희소해 커버리지↓. exp22는 **kw≥3**(조밀하되 의미 있는 수준)이 test PR-AUC 최고였다. IP는 1개만 공유해도 강신호라 임계가 낮다(<span style="color:#c92a2a">exp22 **ip≥2**</span><span style="color:#1971c2">→ exp41 **ip≥1**</span>). 최종 데이터 기준 kw≥3에서 `sim_kw`는 **약 105만 엣지(1,048,106, 방향 포함)로 단연 최대 관계**이고, `sim_ip`는 <span style="color:#1971c2">**ip≥1에서 19,776개**(exp22 ip≥2일 때 904개)</span> — DiffMG 게이트 $\alpha_r$ 해석 시 "엣지 수×$\alpha_r$"의 영향 총량 차이(§8 함정 ③)를 반드시 함께 본다.</span>

### 5.2 <span style="color:#1971c2">확장 멀티홉 키워드 도달 엣지 (exp41 신규)</span>

<span style="color:#1971c2">**무엇**: §5.1 sim 엣지가 *product↔product* 2홉을 1홉으로 압축했다면, exp41은 *product→keyword* 도달 경로 중 의미 있는 멀티홉을 똑같이 빌드 시점에 1홉 엣지로 압축한다. 신상품이 "내가 직접 가진 키워드"를 넘어 **협찬 IP가 가진 키워드·트렌드 속성 키워드**까지 1층 어텐션 한 번에 흡수하게 하는 것이 목적이다(이름의 `trend`).</span>

<span style="color:#1971c2">**어떻게(생성식, `build_hetero_data.py:195-241`)** — 모두 인접행렬 곱의 비영(non-zero) 위치를 product→keyword 엣지로 채택한다($\mathbf A_{PI}$=product×ip, $\mathbf A_{IK}$=ip×keyword, $\mathbf A_{II}$=ip×ip, $\mathbf A_{PK}$=product×keyword, $\mathbf A_{KK}$=keyword×keyword(`trend_to`) 이진 인접행렬):</span>

$$
\mathbf A_{\text{via\_ip}} = \mathbf A_{PI}\,\mathbf A_{IK}, \qquad
\mathbf A_{\text{ipip}} = \mathbf A_{PI}\,\mathbf A_{II}\,\mathbf A_{IK}, \qquad
\mathbf A_{\text{trend}} = \mathbf A_{PK}\,\mathbf A_{KK}
$$

| <span style="color:#1971c2">엣지 타입</span> | <span style="color:#1971c2">경로</span> | <span style="color:#1971c2">생성식</span> | <span style="color:#1971c2">개수</span> |
|---|---|---|---|
| <span style="color:#1971c2">`has_kw_via_ip`</span> | <span style="color:#1971c2">P→I→K (2홉)</span> | <span style="color:#1971c2">$\mathbf A_{PI}\mathbf A_{IK}$</span> | <span style="color:#1971c2">5,732</span> |
| <span style="color:#1971c2">`has_kw_ipip`</span> | <span style="color:#1971c2">P→I→I→K (3홉)</span> | <span style="color:#1971c2">$\mathbf A_{PI}\mathbf A_{II}\mathbf A_{IK}$</span> | <span style="color:#1971c2">4,706</span> |
| <span style="color:#1971c2">`has_kw_trend`</span> | <span style="color:#1971c2">P→K→K (2홉)</span> | <span style="color:#1971c2">$\mathbf A_{PK}\mathbf A_{KK}$</span> | <span style="color:#1971c2">66,663</span> |
| <span style="color:#1971c2">`ip→has_ip→ip`</span> | <span style="color:#1971c2">I→I (1홉)</span> | <span style="color:#1971c2">`ip_ip_edges_final`</span> | <span style="color:#1971c2">66</span> |

> <span style="color:#1971c2">**왜 (sim 엣지와 같은 논리)**: ① **명시적** — "IP 경유" / "트렌드 속성 경유"라는 해석 가능한 경로만 엣지로 만든다(층 스택의 $A^2,A^3$는 모든 멀티홉을 뭉뚱그림). ② **1층 유지** — 3홉(`has_kw_ipip`)까지도 사전 압축이라 층을 더 쌓지 않는다(over-smoothing·파라미터 절약). ③ **DiffMG가 가중 학습** — 각각 별도 관계 타입이라 $\alpha_r$가 "IP/트렌드 경유 키워드를 얼마나 신뢰할지"를 데이터로 정한다. 단, exp41에서 이 4종의 $\alpha_r$은 모두 균등(≈0.023) 수준에 머물러(§8) 게이트가 아직 이들을 분화하지 못한 상태다 — 구조엔 들어와 있으나 신호 기여는 동반구매·sim 관계가 주도한다.</span>

---

## 6. Phase 5 — Bi-level 학습 루프 (`trainer.py`)

**학습 단위 — 전 그래프 full-batch, 동시다발 전파 / 손실은 product에만.**
매 epoch의 forward는 미니배치·이웃 샘플링 없이 **전체 그래프(모든 노드·모든 엣지)를 한 번에** 통과한다(HGT 논문의 HGSampling은 미사용). 즉 keyword(<span style="color:#c92a2a">2,063</span>)·ip(<span style="color:#c92a2a">335</span>)·product(5,033) 표현이 **동시다발적으로** 계산된다. 다만 학습 신호의 출처는 다르다:
- **지도 신호(레이블)는 `product` 노드에만** 존재한다(`성공여부` $y$). `keyword`·`ip`는 레이블이 없고, 그 임베딩은 product 예측으로부터 엣지를 타고 역전파된 그래디언트로 **간접 학습**된다.
- **transductive(전이적) 설정**: val/test 상품의 *엣지·피처는 학습 그래프에 그대로 포함*되어 메시지 전파에 쓰이지만, 그들의 *레이블은 손실에 절대 들어가지 않는다*(마스킹). 한 forward에서 전 노드 표현을 구한 뒤, **손실만 마스크로 잘라** Step1은 `train_mask`, Step2는 `val_mask` 노드에서 계산한다.

**논문(DiffMG §4.2)**: 네트워크 가중치 $\omega$와 아키텍처 파라미터 $\lambda$를 분리해 이중 최적화.

$$
\min_{\lambda}\ \mathcal L_{val}\big(\omega^*(\lambda), \lambda\big) \quad\text{s.t.}\quad \omega^*(\lambda) = \arg\min_{\omega} \mathcal L_{train}(\omega, \lambda)
$$

여기서 $\omega$ = 네트워크 가중치(임베딩·HGT·KGAT·Head 전부), $\lambda$ = DiffMG 아키텍처 파라미터($\alpha_r$의 로짓), $\mathcal L_{train}/\mathcal L_{val}$ = train/val 손실. 바깥 문제는 $\lambda$를 val로, 안쪽 문제는 $\omega$를 train으로 최적화한다(이중 최적화).

**왜 $\lambda$는 val, $\omega$는 train인가 — 부연.** 두 파라미터는 *역할이 다르다*. $\omega$(가중치)는 "주어진 관계 구조 안에서 데이터를 실제로 맞히는 근육"이고, $\lambda$(=어떤 관계 $\alpha_r$를 신뢰할지)는 "어떤 구조로 풀지 정하는 설계 결정"이다.

- **만약 둘 다 train으로 학습하면?** $\lambda$가 train 정답을 외우기 가장 쉬운 관계 조합(노이즈 관계 포함)을 골라버린다 → **구조의 과적합**. "트렌드 전이가 중요해서"가 아니라 "그 관계를 켜면 train 점수가 올라가서" $\alpha_r$이 커지는, XAI로서 신뢰할 수 없는 값이 된다.
- **그래서 역할을 분리한다.** $\omega$는 train으로 학습해 "이 구조로 train을 최대한 맞추는 최적 근육 $\omega^*(\lambda)$"을 만들고($\arg\min_\omega \mathcal L_{train}$ = 안쪽/제약 문제), $\lambda$는 그렇게 만들어진 모델이 **처음 보는 데이터(val)에서 얼마나 잘 일반화하는지**로 평가·갱신한다($\min_\lambda \mathcal L_{val}$ = 바깥/주 문제). val 손실을 낮추는 쪽으로 $\lambda$를 움직이면, "train 암기"가 아니라 "일반화에 진짜 기여하는 관계"만 $\alpha_r$이 커진다.
- **비유.** $\omega$ = 교과서(train)로 공부하는 학생, $\lambda$ = 공부 *전략/커리큘럼*. 전략의 좋고 나쁨을 학생이 외운 교과서(train)로 채점하면 안 된다 — 따로 둔 모의고사(val)로 채점해야 전략이 진짜 통하는지 안다. 그래서 학생은 교과서로 공부(inner, train), 전략은 모의고사로 평가(outer, val).
- **"바깥/안쪽" = 중첩 구조.** 식의 `s.t.`가 핵심이다. 바깥 문제 $\min_\lambda \mathcal L_{val}$은 *안쪽 문제의 해* $\omega^*(\lambda)$에 의존한다 — 즉 "어떤 $\lambda$를 고르든, 먼저 그 $\lambda$ 아래서 train 최적 $\omega^*$를 구한 뒤, 그 모델을 val로 평가"하는 2단 중첩이다. 본 구현은 이 중첩을 매 epoch **Step1(train으로 $\omega$ 한 발) → Step2(val로 $\lambda$ 한 발)** 로 번갈아 근사한다(1차 근사, 아래 코드).

**본 구현** — 두 Optimizer를 명시 분리(grad 간섭 차단):

```python
# Step 1: W(=ω) 갱신 — train 마스크
opt_w.zero_grad(); opt_a.zero_grad()
loss_w = BCE(model(...)[train_mask], y[train_mask])
loss_w.backward(); opt_w.step()              # W만 갱신

# Step 2: α(=λ, DiffMG 게이트) 갱신 — val 마스크
opt_w.zero_grad(); opt_a.zero_grad()
loss_a = BCE(model(...)[val_mask], y[val_mask])
loss_a.backward(); opt_a.step()              # α만 갱신
```

`_split_params`로 DiffMG 게이트 파라미터(α)와 그 외(W)를 id로 분리, 각각 lr이 다르다(확정: `lr_w=0.005`, `lr_alpha=0.02`).

> <span style="color:#2f9e44">**v2_sweepA 학습 = 멀티태스크 + DropEdge (`HINGNNv2`, `experiments/v2_multitask.py`)**: bi-level 구조(Step1 W/train · Step2 α/val)와 주 손실(아래 §6.1 weighted BCE)은 **그대로**이되, 위 `loss_w`(Step1)에 **보조 손실**이 더해진다 — 동반구매 link-prediction:
> <br>$\mathcal L_{train} = \mathcal L^{성공}_{BCE} + \lambda_{aux}\,\mathcal L^{동반구매}_{link}$, &nbsp; $\mathcal L^{동반구매}_{link} = \text{BCE}\big(\text{aux\_link\_logit}(p,q),\ \mathbb 1[(p,q)\ \text{동반구매쌍}]\big)$
> <br>여기서 `aux_link_logit(p,q)=\big(W_{aux}\,e_p\big)\cdot e_q`(`hin_gnn_v2.py`), 양성 쌍 = 동반구매 제품쌍, 음성 = 무작위 샘플. **양·음성 쌍을 train 제품으로만 한정해 누수 차단**(출시-후 동반구매를 *입력*이 아니라 *예측 타깃*으로만 사용). 또 학습 forward마다 **DropEdge**(엣지를 확률 $p$로 무작위 드롭, 추론 시 $p=0$)로 과적합을 억제한다. → 즉 v2는 "동반구매로 *성공을 직접 맞히는*" 누수 경로를 끊고, "공유 임베딩이 *동반구매 잠재력*을 부수적으로 학습"하게 만들어 **궁합 좋은 속성 = 잘 팔림**을 적법한 forward 신호로 흡수한다(설계 §9-1, `docs/final_model_leakfree_switch_plan.md`).</span>

### 6.1 손실 — 불균형 보정 weighted BCE

양성 23.8% 불균형 → 양성 오답에 더 큰 페널티:

$$
\mathcal L = -\frac{1}{N}\sum_i \Big[ w_{pos}\, y_i \log \sigma(z_i) + (1-y_i)\log(1-\sigma(z_i)) \Big], \quad w_{pos}=3.24 \approx \frac{\#\text{실패}}{\#\text{성공}}
$$

여기서 $N$ = 해당 마스크의 product 수, $y_i\in\{0,1\}$ = 상품 $i$의 성공 레이블, $z_i$ = 모델 출력 로짓, $\sigma$ = 시그모이드(로짓→확률), $w_{pos}$ = 양성(성공) 클래스 가중치.

### 6.2 평가·조기종료 — PR-AUC 주지표

불균형에서 ROC-AUC는 낙관적이라 **PR-AUC를 early stopping 기준**으로 사용(`success_predictor.py`). 랜덤 베이스라인 PR-AUC = 양성비율 = **0.236**.

```python
best_metric ← max val_pr_auc;  patience=30 epochs;  best_state 복원 후 test 평가
```

---

## 7. Phase 6 — 산출물 ①·② 추론과 추천

### 7.1 산출 ① 성공 확률 분류 (`success_predictor.py`)

$$
\hat p_p = \sigma\big(\text{Head}(\mathbf h_p^{(L)})\big), \qquad \text{Head} = \text{Linear}\!\to\!\text{ReLU}\!\to\!\text{Dropout}\!\to\!\text{Linear}
$$

여기서 $\hat p_p$ = product $p$의 예측 성공확률($[0,1]$), $\mathbf h_p^{(L)}$ = 최종 $L$층 표현(<span style="color:#1971c2">64</span>차원, <span style="color:#c92a2a">exp22는 $L=1$이라 1층 표현 $\mathbf h_p^{(1)}$</span>), $\sigma$ = 시그모이드, $\text{Head}$ = 2층 MLP(<span style="color:#1971c2">64→64→1</span>).

> <span style="color:#c92a2a">**"2층 MLP"의 2 vs `num_layers=1`은 서로 다른 층이다 — 혼동 주의.** Head는 코드상 `nn.Sequential(nn.Linear(`<span style="color:#1971c2">64,64</span>`), ReLU, Dropout, nn.Linear(`<span style="color:#1971c2">64</span>`,1))`로 **`nn.Linear`(가중치 층)가 2개**다 → "2층 MLP"가 맞다(MLP 깊이는 비선형 사이에 낀 Linear 개수로 센다). 반면 `num_layers=1`은 **GNN 메시지패싱(DiffMG+HGT+KGAT) 전파 층 수**로, 그래프에서 정보가 몇 홉 도는지를 뜻한다. 즉 "그래프 전파 1층 → 그 결과 $\mathbf h_p^{(1)}$를 입력받는 분류기 MLP 2층"으로, 둘은 완전히 별개의 구성요소다.</span>

`learned_product_scores.parquet`에 product별 `pred_success_prob`로 저장.

### 7.2 산출 ② 키워드 조합 추천 — 메타패스 순회 (`recommend.py`)

**메타패스** `keyword_s → product → keyword_t` 위에서, 시드 키워드 $k_s$와 잘 맞는 조합 키워드 점수:

$$
\text{score}(k_t \mid k_s) = \sum_{j\,:\,k_s,k_t \in \text{kw}(j)} \text{att}(j, k_s)\cdot \hat p_{\text{success}}(j) \cdot \text{att}(j, k_t)
$$

- $k_s$ = 시드 키워드, $k_t$ = 후보 조합 키워드, $j$ = $k_s$와 $k_t$를 **둘 다 보유한 상품**, $\text{kw}(j)$ = 상품 $j$의 키워드 집합.
- $\text{att}(j,\cdot)$ = 학습된 `(product, has_kw, keyword)` 엣지 어텐션(마지막 층, head 평균) — HGT가 남긴 `last_attention`.
- $\hat p_{\text{success}}(j)$ = 상품 $j$의 예측 성공 확률.

**$\text{att}(j,k)$ 가 정확히 무엇인가 — `last_attention` 상세.** 이 값은 추천 단계에서 새로 계산하는 게 아니라, **§4.2 HGT가 forward 중 이미 만든 어텐션 $\alpha$를 저장해 둔 것을 읽어오는** 것이다.

- **무엇**: §4.2의 $\alpha(s,e,t)=\text{Softmax}_{s\in N(t)}(\text{score})$ — 즉 "타겟 노드가 받는 incoming 엣지들 사이의 상대 중요도(합 1)". 추천에선 `(product, has_kw, keyword)` 관계의 $\alpha$만 쓴다.
- **어떻게 저장**: `hgt_layer.py:113` 에서 `last_attention[key] = alpha.mean(dim=1).detach()`. ① `.mean(dim=1)` = **$H$=4개 head를 평균**해 엣지마다 스칼라 1개로, ② `.detach()` = 그래디언트와 분리한 **읽기 전용 기록값**(학습이 아니라 추론 시 해석용).
- **언제 것**: `last_attention`은 forward마다 덮어쓰이므로, eval 시점 마지막 forward = **최종(마지막 층) 표현을 만든 그 어텐션**이다. (층이 여러 개면 마지막 층 값만 남는다.)
- **방향 뉘앙스**: `has_kw` 순방향은 src=product, dst=keyword라, $\alpha$는 **keyword $k$ 기준으로 그에 연결된 product들 사이에서 정규화**된다 → $\text{att}(j,k)$ = "키워드 $k$를 가진 상품들 중 $j$가 차지하는 상대적 어텐션 비중". 추천 코드(`recommend.py:26-37`)는 이 엣지별 $\alpha$ 벡터를 `prod2kw[j]`·`kw2prod[k]` 양방향 딕셔너리로 인덱싱해, 시드측 $\text{att}(j,k_s)$와 후보측 $\text{att}(j,k_t)$ 를 같은 테이블에서 꺼내 쓴다.
- **요약**: $\text{att}(j,k)$ = **학습이 끝난 모델이 상품 $j$–키워드 $k$ 결합에 부여한 신뢰도(마지막 층·head 평균·정규화된 HGT 어텐션)**. "성공한 상품에서 강하게 묶인 조합"을 점수에 반영하는 핵심 신호다.

> **"하이브리드 α×성공"의 의미**: 시드 키워드를 가진 상품들 중 **① 성공 확률이 높고(p) ② 그 키워드 결합이 강한(att)** 상품을 경유한 조합 키워드일수록 높은 점수. 단순 동시출현 빈도가 아니라 "성공한 상품에서 강하게 묶인" 조합을 추천.

**실제 추천 예시**(exp01_baseline, 시드→top 키워드):

```
마라   → 도쿠시마(0.036), 향라(0.036), 탄탄(0.034), 샹궈(0.023), 마라탕(0.014) ...
로제   → 떡볶퀸(0.017), 중독성(0.017), 마카로니(0.011), 당면(0.005), 파마산(0.004) ...
위스키 → 블랙서클(0.006), 산토리(0.006), 티처스위스키(0.006), 맥캘란(0.005) ...
```

→ "마라" 신제품 기획 시 *향라·탄탄·샹궈*(중화 마라 계열) 조합이 성공 상품에서 강하게 묶여 있음을 데이터로 제시.

계절 서브네트워크용은 `recommend_seasonal.py`가 동일 로직으로, 시드(예: "말차")에 대해 계절별 모델로 조합을 추천한다.

### 7.3 <span style="color:#c92a2a">대시보드 서빙 — 고정 K-P-K 점수 + 동적 이기종 워크 (`serve.py`)</span>

> <span style="color:#c92a2a">**핵심 답: 둘이 공존한다.** 대시보드는 (A) **꿀조합 점수**는 §7.2의 **고정 메타패스 `keyword→product→keyword`** 로 계산하고, (B) 그 조합을 화면에 **네트워크로 그릴 때**는 메타패스가 **고정이 아니라 동적으로 바뀌는 이기종 워크**를 쓴다. 즉 "무엇을 추천하나(점수)"는 K-P-K 고정, "어떻게 연결해 보여주나(시각화·근거 경로)"는 동적이다.</span>

<span style="color:#c92a2a">서빙 코어 `serve.py`(<span style="color:#2f9e44">현재 `SERVING_EXP="v2_sweepA"`</span>)는 학습이 끝난 모델의 export 산출물(성공확률·가중 엣지·`relation_importance.json`)만으로 동작한다(torch 불필요).</span>

#### <span style="color:#c92a2a">(A) 꿀조합 키워드 점수 — 고정 K-P-K (+서빙 보정)</span>

<span style="color:#c92a2a">`recommend_keywords`/`recommend_proposals`는 §7.2의 점수식 `score(k_t|k_s)=Σ_j att(j,k_s)·p̂(j)·att(j,k_t)`를 **그대로** 쓴다(경로 = 시드 키워드 → 경유 상품 → 후보 키워드, **항상 K-P-K**). 다만 서빙 품질을 위해 세 가지 보정을 더한다:</span>
- <span style="color:#c92a2a">**① 빈도 보정 `×deg(k_t)`**: `att`이 "키워드 기준 정규화"라 제품 수가 적은 희귀 키워드가 과대평가되는 편향이 있어, 후보 키워드의 보유 제품 수 `deg`를 곱해 상쇄(≈ 성공가중 동시출현).</span>
- <span style="color:#c92a2a">**② `min_support`(기본 3)**: 보유 제품이 3개 미만인 극희귀 키워드는 제외(노이즈 컷).</span>
- <span style="color:#c92a2a">**③ 카테고리 그라운딩(K_cat)**: `category` 지정 시 경유 상품 `j`를 해당 대분류로 한정 → 추천 키워드가 그 카테고리 속성에 머묾(예: '과자'에 '돈까스·고기' 유입 차단).</span>

<span style="color:#c92a2a">`recommend_paths`(빔 워크)는 K-P-K 한 스텝(`_kpk_next`)을 여러 번 이어 키워드 체인(`마라→마라탕→샹궈…`)을 만들지만, **각 스텝이 여전히 keyword→product→keyword**이므로 타입 시퀀스는 고정이다(상품은 매개자).</span>

#### <span style="color:#c92a2a">(B) 네트워크 시각화 — 동적 이기종 그리디 워크</span>

<span style="color:#c92a2a">`walk_network`/`walk_chain`/`_keyword_net`은 **통합 이기종 인접 `hadj`** 위에서 움직인다. `hadj`에는 타입이 섞인 모든 엣지가 들어있다 — `keyword↔product`(has_kw), `product↔ip`(has_ip), `ip↔keyword`(ip의 has_kw), `keyword↔keyword`(trend_to). 각 엣지의 **가중치**는:</span>

$$
w(s\to t) = \underbrace{\alpha_r}_{\text{relation\_importance.json}}\;\times\;\underbrace{\text{att}(s,t)}_{\text{학습 엣지 어텐션}}\;\times\;\underbrace{\hat p(t)}_{t\text{가 상품일 때만}}
$$

<span style="color:#c92a2a">그리디 워크(`_greedy_walk_from`)는 매 스텝 **방문 안 한 이웃 중 $w$가 최대인 노드를 — 타입에 상관없이 — 하나 고른다**. 그래서 경로의 **타입 시퀀스가 시드·데이터에 따라 매번 달라진다**. 예:</span>

```
[exp41 동적 워크 예시]  마라 ─(sim/has_kw)→ (제품)마라라면 ─(has_ip)→ (IP)진라면 ─(ip-has_kw)→ (키워드)얼큰 ─(trend_to)→ (키워드)국물 …
   keyword → product → ip → keyword → keyword   ← 타입 순서가 고정 K-P-K가 아님(동적)
```

<span style="color:#c92a2a">**"동적"의 정확한 의미 — 오해 주의**: 이는 모델이 *추론 때 메타패스를 새로 탐색·학습*한다는 뜻이 **아니다**. DiffMG의 메타패스 학습은 **학습 단계에서 끝났고**, 그 결과인 $\alpha_r$(관계 중요도)는 **고정**이다. 서빙의 워크는 그 **고정된 $\alpha_r$×어텐션 가중치**를 따라 *그리디하게 경로를 펼칠 뿐*이라, "추천 시드가 무엇이냐"에 따라 펼쳐지는 경로(노드 타입 순서)가 달라지는 것이다. → 점수의 메타패스는 K-P-K로 고정, **시각화 경로의 타입 시퀀스만 시드별로 가변**.</span>

<span style="color:#c92a2a">**그리디 워크는 언제 끝나나 — 종료 조건**: 이 워크는 "$w$가 최댓값일 때까지" 가는 게 아니라(국소 선택일 뿐 전역 최적화가 아님), 아래 **둘 중 먼저 닿는 조건**에서 멈춘다(`_greedy_walk_from`).</span>
- <span style="color:#c92a2a">**① 스텝 상한 `max_steps` 도달**: 루프가 `for _ in range(max_steps)`라 **최대 홉 수가 고정**돼 있다(기본값: `walk_chain`=6, `walk_network` 백본=4, `_keyword_net`=3). 여기 닿으면 $w$가 아직 커도 멈춘다.</span>
- <span style="color:#c92a2a">**② 막다른 길(`best is None`)**: 현재 노드의 이웃 중 **갈 수 있는 후보가 하나도 없으면** 즉시 종료. "갈 수 있는"에서 빠지는 경우 = **이미 방문한 노드**(`visited` 집합 — 재방문 금지 → 사이클·무한루프 원천 차단), 초일반 불용어(`GENERIC_STOPWORDS`)·제품 수 `min_support` 미만 키워드, 카테고리/`K_cat` 그라운딩에 안 맞는 노드, 또는 애초에 `hadj`에 나가는 엣지가 없는 노드.</span>

<span style="color:#c92a2a">즉 **경로의 끝 = "max_steps 횟수를 다 썼거나" 또는 "더 갈 미방문·허용 이웃이 없거나"**. $w$가 작아진다고 멈추는 임계는 이 워크엔 없다(매 스텝 가용 이웃 중 최대 $w$를 고를 뿐, 직전보다 작아도 진행). 다만 **별개 함수 `recommend_bundle`(①형 일관 묶음)** 은 예외로 `stop_ratio`(기본 0.3)를 둬 "다음 후보 점수가 첫 추가의 30% 미만이면 정지"하는 **가변 길이**다 — 약한 연결에서 자연스럽게 끊기게 한 것.</span>

<span style="color:#c92a2a">**`walk_chain` vs `_keyword_net` vs `walk_network` — 같은 그리디 워크, 다른 포장.** 셋 다 위의 동일한 종료 조건(`max_steps` 상한 + 막다른 길 + `visited` 재방문 금지)을 따르지만, **시작점 수·구조·max_steps·그라운딩**이 다르다.</span>

| <span style="color:#c92a2a">함수</span> | <span style="color:#c92a2a">시작점</span> | <span style="color:#c92a2a">구조</span> | <span style="color:#c92a2a">max_steps 기본</span> | <span style="color:#c92a2a">1-hop 가지</span> | <span style="color:#c92a2a">카테고리/K_cat 그라운딩</span> | <span style="color:#c92a2a">용도</span> |
|---|---|---|---|---|---|---|
| <span style="color:#c92a2a">`walk_chain`</span> | <span style="color:#c92a2a">단일(`seeds[0]`)</span> | <span style="color:#c92a2a">**선형 체인 1줄**(자체 루프)</span> | <span style="color:#c92a2a">6</span> | <span style="color:#c92a2a">없음</span> | <span style="color:#c92a2a">없음</span> | <span style="color:#c92a2a">"줄줄이 사탕" 단일 경로 스토리(`explain_chain`)</span> |
| <span style="color:#c92a2a">`_keyword_net`</span> | <span style="color:#c92a2a">단일</span> | <span style="color:#c92a2a">**백본 체인 + 각 백본 노드의 1-hop 가지**</span> | <span style="color:#c92a2a">3</span> | <span style="color:#c92a2a">**있음**(가중 상위 `TOP_BRANCH_1HOP`=10)</span> | <span style="color:#c92a2a">없음</span> | <span style="color:#c92a2a">한 키워드의 국소 네트워크(`attr_network`의 빌딩블록)</span> |
| <span style="color:#c92a2a">`walk_network`</span> | <span style="color:#c92a2a">**다중**(상위 `max_seeds`=12)</span> | <span style="color:#c92a2a">**여러 백본 체인을 공유 노드로 병합**(레이어드)</span> | <span style="color:#c92a2a">4</span> | <span style="color:#c92a2a">없음(병합 백본만)</span> | <span style="color:#c92a2a">**있음**</span> | <span style="color:#c92a2a">대시보드 메인 통합 네트워크(`recommend_proposals`가 카드마다 호출)</span> |

<span style="color:#c92a2a">정리하면: ① `walk_chain` = **한 줄짜리 그리디 체인**(가장 단순, 자체 루프, 그라운딩 없음). ② `_keyword_net` = **체인(백본) + 각 노드의 1-hop 곁가지** — 곁가지는 워크가 아니라 단순 1홉 이웃 top-10이라 종료 조건과 무관하게 "1홉·상위 10개"로 끊긴다. ③ `walk_network` = **여러 시드에서 각각 `_greedy_walk_from`을 돌려 합친 다중 출발 네트워크** + 카테고리 그라운딩. ②·③은 백본을 공통 헬퍼 `_greedy_walk_from`으로 만들고, ①만 인라인 루프(그래서 grounding 인자가 없다).</span>

#### <span style="color:#c92a2a">(C) 그 외 서빙 기능</span>
- <span style="color:#c92a2a">`infer_attrs`: 임의 검색어(트렌드·IP·인물)를 Gemma(Ollama)로 식품 속성 8~12개로 확장 후 그래프 키워드에 매칭 → K-P-K 시작점 확보.</span>
- <span style="color:#c92a2a">`recommend_proposals`: 추천 키워드를 조합 카드 k개로 묶고, 카드마다 `walk_network`로 고유 네트워크 + 설명(`explain_network`)을 생성. 제품명은 Gemma 배치 생성.</span>
- <span style="color:#c92a2a">`similar_success_products`·`diagnose`: 유사 성공제품 탐색, 부진 제품의 약한(저-att) 키워드 진단.</span>

> <span style="color:#c92a2a">요약: **추천의 "점수"는 학습된 고정 K-P-K**, **대시보드의 "그림·근거 경로"는 학습된 $\alpha_r$·어텐션을 따라가는 동적 이기종 워크**. 둘 다 **학습 후 고정된 가중치**를 쓰며, 모델 구조가 아니라 그 위에 얹은 **서빙 레이어**다.</span>

---

## 8. Phase 6 — 산출물 ③ XAI 관계 중요도 (`export_results.py`)

DiffMG 게이트의 층별 $\alpha_r$를 그대로 추출 → `relation_importance.json`.

```
layer 0 (exp01):
  product__has_kw__keyword : 0.1272   ← 가장 큰 관계
  ip__rev_has_ip__product  : 0.1250
  ... (8개 관계가 ≈0.125로 거의 균등)
```

> exp01($\tau$=1.0)은 α_r이 균등(≈1/8) → 게이트 미분화 신호. 그래서 <span style="color:#c92a2a">exp22도(exp06_3와 동일하게)</span> $\tau$=0.5, `lr_alpha` 0.005→0.02로 올려 관계 선택을 활성화했다. <span style="color:#1971c2">exp41은 엣지 12종(+역방향)이라 관계 수 **R=24**(균등 1/24≈**0.0417**), 1층이라 게이트는 **layer 0 하나뿐**이다.</span>

<span style="color:#1971c2">**exp41 실제 layer 0 α_r**(`relation_importance.json`):</span>

```
[exp41] layer 0 (R=24, 균등 0.0417):
  product__co_offline__product      : 0.1314   ← 최대 (오프라인 동반구매)
  product__rev_co_offline__product  : 0.0998
  product__rev_co_quick__product    : 0.0929
  product__co_quick__product        : 0.0787
  product__sim_ip__product          : 0.0690  (rev 0.0686)   ← 유사상품(IP 공유)
  product__sim_kw__product          : 0.0492  (rev 0.0400)   ← 유사상품(키워드 공유)
  나머지 16종(has_kw·trend_to·has_ip·has_kw_via_ip·ipip·trend 등) : ≈0.0231 (균등 수렴)
```

> <span style="color:#1971c2">→ exp41에서 게이트가 **분화시킨 관계는 동반구매(`co_offline`/`co_quick`)·유사상품(`sim_ip`/`sim_kw`) 8종뿐**이고, **키워드·IP 경로 관계(`has_kw` 포함, 신규 멀티홉 4종 포함)는 전부 균등 0.0231**에 머문다. 즉 "신상품 성패를 가르는 신호는 *매장에서 함께 팔리는 상품* + *유사한 기존 상품*이 주도"한다는 해석. (exp22까지의 "has_kw가 최대"와 반대 — 데이터·관계 구성이 바뀌며 게이트의 선택이 동반구매·sim 쪽으로 이동.)</span>

<span style="color:#2f9e44">**최종 모델 v2_sweepA 실제 layer 0 α_r**(`relation_importance.json`, R=22, 균등 1/22≈0.0455):</span>

```
[v2_sweepA] layer 0 (R=22, 균등 0.0455):
  product__sim_kw__product          : 0.6136   ← 압도적 최대 (유사상품·키워드 공유, 균등의 13.5배)
  product__rev_sim_ip__product      : 0.1049
  product__sim_ip__product          : 0.1035                ← 유사상품(IP 공유)
  product__rev_sim_kw__product      : 0.0486
  나머지 18종(has_kw·trend_to·has_ip·via_ip·ipip·trend·basket_comp 등) : ≈0.0072 (균등 이하로 도태)
```

> <span style="color:#2f9e44">→ **동반구매가 입력에서 빠지자(누수 제거), 게이트의 1순위가 동반구매 → 유사상품(공유키워드 `sim_kw`)으로 이동**했다. v2는 `sim_kw` 한 관계에 α의 61%를 몰아주고(균등의 13.5배), 그다음이 `sim_ip`(양방향 ≈0.10). `basket_comp`를 포함한 나머지는 균등(0.0455) 이하로 도태. 즉 **"신상품 성패 = 어떤 기존 상품과 키워드가 닮았는가(sim_kw) + IP가 닮았는가(sim_ip)"** — exp41의 동반구매 의존(누수)을 걷어낸 *leak-free* XAI 근거다. (해석 원칙 ①~⑤는 v2에도 동일 적용.)</span>

**$\alpha_r$ 을 어떻게 해석하나 — "가장 중요한 엣지 타입"이 정해졌을 때.**

먼저 **$\alpha_r$의 정확한 의미**: 한 층에서 메시지를 전파할 때 **관계 타입 $r$에 모델이 부여한 상대 신뢰 점유율**(층마다 $\sum_r\alpha_r=1$). "가장 큰 $\alpha_r$" = "성공 예측 신호가 이 관계를 타고 가장 많이 흘렀다".

올바른 해석 원칙:
- **① 절대값이 아니라 균등선($1/R$) 대비로 읽는다.** <span style="color:#1971c2">exp41은 $R$=24라 균등은 **0.0417**</span>. 어떤 관계가 균등선을 *얼마나 상회/하회*하는지가 신호다(숫자 자체보다 "균등의 몇 배"가 의미 — 예: exp41 `co_offline` 0.1314 ≈ 균등의 3.2배).
- **② "예측 기여"이지 "인과"가 아니다.** $\alpha_r$↑ 는 "이 관계 메시지가 성공/실패를 *가르는 데 유용했다*"는 뜻이지 "이 관계가 성공을 *유발한다*"가 아니다. MD 근거로 쓸 때 "이 신호가 성공 상품을 잘 설명한다"까지만 말하고 인과 주장은 피한다.
- **③ val 기준 학습값이라 "일반화 기여"다.** $\alpha_r$은 bi-level 바깥 문제(val)에서 갱신되므로(§6), train 암기가 아니라 **처음 보는 상품에도 통하는 관계**에 가중이 실린다 → XAI로서 신뢰 근거.
- **④ 층마다 따로 읽는다.** <span style="color:#c92a2a">**exp22는 1층이라 게이트가 layer 0 하나뿐**이다. 여기서 `has_kw`·`co_offline` 등 1홉 관계와 `sim_kw`·`sim_ip` 2홉 관계의 $\alpha_r$를 **같은 층에서** 비교한다 — "직접 속성 vs 유사 상품 맥락 중 무엇을 더 신뢰했나"를 한눈에 읽는다.</span> <span style="color:#1971c2">exp41도 1층이라 layer 0 하나에서 24개 관계(직접 속성·동반구매·sim·확장 멀티홉)를 모두 비교한다.</span> (이전 exp06_3처럼 2층이면 layer 0=1홉, layer 1=2홉으로 나눠 읽었다.)
- **⑤ 순방향·역방향을 구분한다.** `product→has_kw→keyword`와 `keyword→rev_has_kw→product`는 별도 $\alpha_r$ → "정보가 상품에서 키워드로 가는 흐름"과 "키워드 맥락이 상품으로 오는 흐름" 중 무엇이 중요했는지로 해석.

<span style="color:#1971c2">해석 예시(exp41 **실측**, 균등선 0.0417 기준): `product__co_offline__product`=**0.1314**(균등의 3.2배), `product__co_quick__product`=0.0787(+역방향 0.0929), `product__sim_ip__product`=0.0690·`product__sim_kw__product`=0.0492가 균등을 상회 → **"신상품 성패는 (a) 매장에서 무엇과 함께 팔리는가(동반구매) + (b) 어떤 기존 상품과 유사한가(공유 IP·키워드) 가 핵심 축"** 으로 MD에 보고. 반대로 `has_kw`·`has_ip`·`has_kw_trend` 등 키워드·IP **직접/경유 보유 관계는 α_r이 균등(0.0231)에 머물러** "키워드를 무엇을 보유했는지·IP 협찬 자체는 (이 게이트 기준) 성패 변별력이 약하다"는 근거. (※ 단 균등 수렴은 "관계가 무의미"가 아니라 "게이트가 아직 분화 못 함"일 수 있어 함정 ①·재학습 권고와 함께 읽는다.)</span>

해석 시 주의(함정):
- **균등(≈$1/R$)이면 해석하지 말 것.** 게이트가 "선택"을 못 한 상태(예: exp01)라 순위가 노이즈. 먼저 분포가 뾰족한지($\tau$↓·`lr_alpha`↑로 확보) 확인하고 해석한다.
- **$\alpha_r$은 "타입" 중요도**다. "어느 *개별* 키워드/상품이 중요한가"는 §4.2의 엣지 어텐션·§7.2 추천으로 본다(혼동 금지).
- **영향 총량 ≠ $\alpha_r$ 단독.** 실제 기여는 $\alpha_r \times$ (그 관계의 엣지 수·Lift)로 증폭된다. 엣지가 적은 관계는 $\alpha_r$이 커도 전체 영향은 작을 수 있다.
- **재현성 확인.** seed·실험 간 같은 관계가 반복적으로 상위인지 보고, 1회 학습값을 과신하지 않는다. 가능하면 그 관계를 뺀 ablation의 성능 하락폭과 교차검증한다.

영속화 산출물 4종:
1. `weighted_product_keyword_edges.parquet` — 학습된 어텐션 가중 네트워크(순회용)
2. `learned_product_scores.parquet` — product 성공확률 + 임베딩 norm
3. `relation_importance.json` — 층별 α_r (MD 대시보드)
4. `report.md` — 지표·α_r·추천 샘플 리포트

---

## 9. <span style="color:#2f9e44">최종 모델 — `v2_sweepA`</span> <span style="color:#1971c2">(직전: `exp41_trend_kw3_ip1`)</span>

> <span style="color:#1971c2">이 절은 exp41로 전면 교체됐었다(exp22_2hop_kw3 → exp41).</span> <span style="color:#2f9e44">**현재 서빙 기준점은 `serve.py: SERVING_EXP="v2_sweepA"`** (leak-free, 2026-06-21 승격). 아래 설정 표는 exp41 기준이며, v2 변경점은 초록 주석(노드/엣지 11종·R22·hidden32·HINGNNv2 멀티태스크·DropEdge)으로 표기. (이전 이력은 각 절 파랑/crimson 참고.)</span>

### 9.1 설정

| 구분 | 값 |
|---|---|
| 노드/엣지 | product·keyword·ip / <span style="color:#1971c2">**12종**(co_offline·co_quick·sim_kw·sim_ip·**has_kw_via_ip·has_kw_ipip·has_kw_trend·ip_has_ip**) + 역방향 = **R 24**</span> <span style="color:#2f9e44">→ v2: **11종**(co_* 제거, **`basket_comp`** 추가) + 역방향 = **R 22**</span> |
| hidden_dim / layers / heads | <span style="color:#1971c2">**64**</span> / **1** / 4 <span style="color:#2f9e44">(v2 hidden **32**)</span> |
| dropout | 0.3 |
| DiffMG | `use_diffmg_gate=true`, `temperature=0.5` |
| 2홉 sim 엣지 | <span style="color:#1971c2">`add_2hop_edges=true`, `sim_kw` 공유 키워드 ≥3, `sim_ip` 공유 IP **≥1**</span> |
| <span style="color:#1971c2">확장 멀티홉 엣지</span> | <span style="color:#1971c2">`add_via_ip_edges`·`add_ipip_kw_edges`·`add_trend_kw_edges`=true (P-I-K / P-I-I-K / P-K-K)</span> |
| readout | `final`(1층 표현 사용; `learned_hop_sum` 아님) |
| loss | weighted BCE (`pos_weight=3.24`) <span style="color:#2f9e44">(v2: 주 weighted BCE **+ 동반구매 link-pred 보조 손실**(멀티태스크) · 학습 시 **DropEdge**)</span> |
| optim | Adam, `lr_w=0.005`, `lr_alpha=0.02`, <span style="color:#1971c2">`weight_decay_w=5e-4`, `weight_decay_alpha=1e-3`</span> |
| early stop | val PR-AUC, patience=30, epochs=200 |
| product 부가피처 | `has_promo`, `insta_mention_30d` (**2차원**) — 앱 행동 7차원 제거 |
| Lift / IDF | `use_lift_weights=false`, `use_idf_keyword_weights=false` (둘 다 OFF) |

> <span style="color:#1971c2">**exp22 → exp41 변경점**: ① forward 엣지 8→**12**종(확장 멀티홉 4종 추가, R 16→**24**) ② `sim_ip` 임계 ≥2→**≥1** ③ hidden_dim 128→**64** ④ test PR-AUC 0.6852 → **0.6959**. (1층·2차원 피처·Lift OFF·bi-level·DiffMG/HGT/KGAT 한 층 내부는 불변.)</span>
>
> <span style="color:#2f9e44">**exp41 → v2_sweepA 변경점(최종 · leak-free)**: ① 동반구매 `co_offline`·`co_quick` **입력 제거**(target leakage) + `basket_comp`(K-K) 추가 → forward 12→**11**종, R 24→**22** ② 모델 `HINGNN`→**`HINGNNv2`**(주 forward 동일 + 동반구매 link-pred **보조 태스크**) ③ hidden_dim 64→**32**, **DropEdge**·dropout↑·weight_decay↑(과적합 억제) ④ test PR-AUC 0.6959→**0.6083**(하락분=증발한 누수; 운영점 P=R=F1 거의 동일) ⑤ α_r 최상위 `co_offline`→**`sim_kw`(0.61)**. (1층·heads4·τ0.5·content_aggregation·bi-level·§4 한 층 내부 수식은 불변.)</span>

### 9.2 성능

<span style="color:#2f9e44">**최종 모델 `v2_sweepA` (leak-free, `metrics.json`)**</span>

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | <span style="color:#2f9e44">0.7237</span> | <span style="color:#2f9e44">0.9076</span> | <span style="color:#2f9e44">0.6948</span> | <span style="color:#2f9e44">0.5880</span> |
| val | <span style="color:#2f9e44">0.5880</span> | <span style="color:#2f9e44">0.8115</span> | <span style="color:#2f9e44">0.5741</span> | <span style="color:#2f9e44">0.5593</span> |
| <span style="color:#2f9e44">**test**</span> | <span style="color:#2f9e44">**0.6083**</span> | <span style="color:#2f9e44">**0.8314**</span> | <span style="color:#2f9e44">**0.6121**</span> | <span style="color:#2f9e44">**0.4535**</span> |
| <span style="color:#2f9e44">운영점(operating point)</span> | <span style="color:#2f9e44">—</span> | <span style="color:#2f9e44">—</span> | <span style="color:#2f9e44">0.550 (P=R=0.55)</span> | <span style="color:#2f9e44">0.7049</span> |
| 랜덤 베이스라인 | <span style="color:#2f9e44">0.239</span> | — | — | — |

> <span style="color:#2f9e44">test PR-AUC **0.6083** = 랜덤(0.239) 대비 약 **2.55배**. exp41(0.6959)보다 낮지만 **그 차이는 동반구매 누수가 만든 랭킹 꼬리**(증발한 누수)이며, 생존율 동기화 운영점(THR=0.7049)에서 P=R=F1≈**0.55**로 exp47(0.544)과 동급·실사용 성능은 보존된다. **train–test gap ≈ 0.115**(`serve.py`; exp41 0.024보다 큰 것은 *누수로 부풀린 쉬운 신호*가 사라진 정직한 일반화 한계). 두 운영점 혼동행렬·구조 진단은 `eda/network_eda_원준/network_eda_report.md` 참조. 세부 지표·재현: `experiments/results/v2_sweepA/`(metrics.json·relation_importance.json·learned_product_scores.parquet).</span>

<details><summary><span style="color:#1971c2">직전 모델 exp41 (이력 · 동반구매 누수 포함)</span></summary>

| split | PR-AUC | AUC-ROC | F1@best | threshold |
|---|---|---|---|---|
| train | <span style="color:#1971c2">0.7528</span> | <span style="color:#1971c2">0.9056</span> | <span style="color:#1971c2">0.6897</span> | <span style="color:#1971c2">0.5996</span> |
| val | <span style="color:#1971c2">0.7288</span> | <span style="color:#1971c2">0.8625</span> | <span style="color:#1971c2">0.6469</span> | <span style="color:#1971c2">0.7535</span> |
| <span style="color:#1971c2">**test**</span> | <span style="color:#1971c2">**0.6959**</span> | <span style="color:#1971c2">**0.8734**</span> | <span style="color:#1971c2">**0.6528**</span> | <span style="color:#1971c2">**0.5552**</span> |
| 랜덤 베이스라인 | 0.236 | — | — | — |

> <span style="color:#1971c2">test PR-AUC **0.6959** = 랜덤(0.236) 대비 약 **2.95배**, val-test gap 0.033. 단 이 점수는 동반구매(`co_offline`) **target leakage**로 과대평가된 값(§2.4·§9 요약 박스) → v2_sweepA로 전환. 후보 비교: exp06_3 0.6729 → exp22 0.6852 → exp41 0.6959. 재현: `experiments/results/exp41_trend_kw3_ip1/`.</span>

</details>

---

## 10. 용어·표기 빠른참조

| 기호 | 의미 |
|---|---|
| $P, K, I$ | product / keyword / ip 노드 수 (5033 / <span style="color:#c92a2a">2063</span> / <span style="color:#c92a2a">335</span>) |
| $d$ | hidden 차원 (<span style="color:#1971c2">64; exp22까지 128</span>) |
| <span style="color:#c92a2a">`sim_kw`/`sim_ip`</span> | <span style="color:#c92a2a">2홉 유사도 엣지 (공유 키워드≥3 / </span><span style="color:#1971c2">공유 IP≥1 — exp41</span><span style="color:#c92a2a">, product↔product)</span> |
| <span style="color:#1971c2">`has_kw_via_ip`/`ipip`/`trend`</span> | <span style="color:#1971c2">exp41 확장 멀티홉 product→keyword 엣지 (P-I-K / P-I-I-K / P-K-K)</span> |
| <span style="color:#c92a2a">$R$</span> | <span style="color:#1971c2">DiffMG 관계 수 (exp41: 12 forward ×2 = **24**; exp22는 16)</span> |
| $H, d_k$ | 어텐션 head 수(4), head당 차원($d/H$=<span style="color:#1971c2">16</span>) |
| $\alpha_r$ | DiffMG 관계 게이트 (관계별 softmax 가중치, $\sum=1$) |
| $\tau$ | DiffMG temperature (작을수록 선택 날카로움) |
| $\text{att}(j,k)$ | HGT 학습 엣지 어텐션 (product $j$↔keyword $k$) |
| $\hat p_p$ | product $p$ 예측 성공 확률 |
| $\omega / \lambda$ | 네트워크 가중치 W / 아키텍처 파라미터 α (bi-level 분리 대상) |

---

## 부록 A. 파일↔단계 매핑

| 단계 | 파일 | 핵심 함수 |
|---|---|---|
| 구축 | `src/data_builder/build_hetero_data.py` | `build_graph`, `norm_id`, `_stratified_masks` |
| 계절 서브 | `src/data_builder/build_seasonal_graph.py` | `build_seasonal_graph`, `build_cumulative_graph` |
| DiffMG | `src/models/diffmg_pruner.py` | `DiffMGRelationGate` |
| HGT | `src/models/hgt_layer.py` | `HGTLayer` |
| KGAT | `src/models/kgat_layer.py` | `KGATUpdate` |
| 조립 | `src/models/hin_gnn.py` | `HINGNN`, `_init_product`, `forward` |
| 학습 | `src/train/trainer.py` | `train`(Step1/Step2 bi-level) |
| 평가 | `src/eval/success_predictor.py` | `predict_proba`, `evaluate_mask`, `compute_metrics` |
| 추천 | `src/eval/recommend.py` | `recommend_combinations` |
| 추천(계절) | `src/eval/recommend_seasonal.py` | `recommend_combinations_seasonal` |
| 산출/XAI | `src/eval/export_results.py` | `export_experiment` |
| 오케스트레이션 | `experiments/exp_utils.py` | `run_experiment`, `compare_experiments` |
</content>
</invoke>
