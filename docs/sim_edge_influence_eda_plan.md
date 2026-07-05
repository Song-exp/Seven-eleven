# sim 엣지 영향력 EDA 계획

> `network_eda_원준/network_eda_report.md` M0 발견(어텐션 α_r의 **87%가 sim_kw/sim_ip에 집중**)을
> 전용 연구로 확장. **목적**: 이 집중이 (a) 누수가 아닌 homophily임을 정량 확인하고, (b) sim_kw 퇴화·
> 전이적 낙관 여부를 가르고, (c) 처방/combo 시스템을 어느 모드(직접채널 vs 배포현실)로 둘지 결정.

## 0. 배경 — 이미 확정된 사실 (재논의 금지)

- sim_kw/sim_ip = **공유 키워드(≥3)/공유 IP(≥1)** 로만 구축 (`build_hetero_data._hop2`). 매출·동반구매·성공라벨 미사용 → **출시 전 정보 → 누수 아님**. 진짜 누수(co_offline)는 v2에서 제거됨.
- ★ **sim_kw = thresholded A²(P-K-P)**. `A_PK @ A_PKᵀ ≥ 3` 을 typed 엣지로 materialize+denoise 한 것. 즉 "sim vs 인접행렬 가중합(A^L)"은 *대체*가 아니라 **명시 materialize(typed 게이트 분리) vs 암묵 재귀**의 차이. → 깊이(L)를 늘려도 같은 P-K-P 정보가 게이트 분리 없이 섞일 뿐, sim이 하던 일이 사라지지 않음.
- 모델 제품 피처에 라벨 없음(키워드/IP 임베딩 집계 + promo/insta). 메시지패싱은 **임베딩 전파**(라벨 전파 아님).
- α_r: `sim_kw` 0.614(13.5×) · `sim_ip(+rev)` ~0.21 · 나머지(has_kw·via_ip·trend 등) ~0.007.
- ★ `score_concept(include_sim=)` 구현됨: 가상노드에 sim 즉석 재계산. 실측 — sim 켜면 콜드스타트 prob **0.11→0.21(≈2배)**, sim_kw 이웃수 **중앙 61·최대 791**, 배치 **drift=0**, margin(+누들) **+0.14→+0.19**.
- 이전 메타패스 L-결정(메모리): **L=2 · 엣지별 깊이 · 키워드 IDF 가중 · L3 스윕 예정**. 본 계획 S5가 그 L3 스윕을 흡수.

## 1. 핵심 질문 (5)

| # | 질문 | 가설 |
|---|---|---|
| Q1 | sim_kw가 **일반어 기반 준-완전그래프로 퇴화**했나? | 이웃 중앙 61·max 791, 변별력 평탄(0.40/0.38/0.32/0.28) → 퇴화 의심 |
| Q2 | **전이적 낙관** — train↔test sim 교차로 test metric이 부풀려졌나? | 누수의 사촌. 끊으면 PR-AUC 하락폭이 크기 |
| Q3 | sim_kw vs sim_ip 중 **진짜 변별자**는? | sim_ip(0.60/0.53/0.22/0.14)가 변별, sim_kw는 prior |
| Q4 | `include_sim`이 **처방/combo 신호**를 어떻게 바꾸나? | sim 켜면 Δprob가 모델 주력 채널을 타 강해짐(순위 변동) |
| Q5 | **깊이 vs 정규화** — 퇴화를 L=3 재귀로 풀까, 키워드 IDF/허브 정규화로 풀까? | L=3는 homophily·오버스무딩↑로 gap만 키움 / IDF 정규화가 변별 회복(우세 가설) |

## 2. 스테이지 (각 = 질문·방법·산출·결정게이트)

### S1 — sim 그래프 구조 진단 (Q1 퇴화)
- **방법**: sim_kw/sim_ip degree 분포(히스토그램·분위)·연결성분 수. 각 sim_kw 엣지의 **공유 키워드 집합**에서 generic(`GENERIC_STOPWORDS`+허브 상위) 비율. `min_shared` 3→4→5→6 sweep 시 엣지수·평균 degree 변화 곡선.
- **산출**: degree 히스토그램, "sim_kw를 만든 공유 키워드 top-30"(일반어 비중), min_shared sweep 표.
- **결정**: 평균 degree가 수십~수백이고 공유어가 일반어 위주면 → **퇴화 확정** → S5 후보(generic 제외/threshold↑).

### S2 — 변별력 분해 (Q3 sim_kw vs sim_ip)
- **방법**: 제품별 **이웃 성공률**(train+val 라벨, 누수 0)을 단일 피처로 두고 test 성공 예측 PR-AUC를 sim_kw·sim_ip 각각 측정. 4분면 이웃성공률(이미 리포트 M3 있음)을 effect-size(Cliff's δ)로 정량화.
- **산출**: 관계별 단독 변별력(PR-AUC·δ) 표.
- **결정**: sim_kw 변별력이 base 근처면 "어텐션 0.61인데 변별 0" 모순 명문화 → 어텐션 재배분 개선 정당화.

### S3 — 전이적 낙관 ablation (Q2, ★누수 직접 검증)
- **방법**: 두 가지.
  ① **inference-time edge drop**(재학습 X, 빠름): test 제품에 닿는 **train↔test sim 엣지를 제거**하고 `predict_proba` 재실행 → test PR-AUC 비교. (engine 캐시 eidx에서 cross-split sim 엣지 마스킹)
  ② test 제품당 **train-성공 sim 이웃 수** 분포 + 그 수와 prob의 상관.
- **산출**: PR-AUC(full) vs PR-AUC(cross-split sim 제거) / 제품별 train이웃수 히스토그램.
- **결정**: 하락폭 큼(예: 0.61→0.45) → metric이 **이웃 조회에 의존** → 별도 **cold-start hold-out 평가** 도입(완전 신규 제품군으로 split). 하락 작음 → 일반화 신호로 안심.

### S4 — include_sim의 처방 영향 (Q4, 우리 시스템 직결)
- **방법**: killer 장부와 combo Δprob/seed_partners를 `include_sim=False`(현행) vs `True`로 재생성 → ① killer 집합 순위 상관(Kendall τ)·뒤집힌 키워드 ② Δprob scatter(direct vs +sim) ③ 가상노드 sim 이웃수와 prob 상관.
- **산출**: 두 모드 killer 비교표, Δprob 산점도, 뒤집힘 키워드 목록.
- **결정**: combo/처방 **기본 모드 확정** — 직접채널(순수 키워드 효과, 해석가능) vs 배포현실(sim 포함, 모델정합). 권장: **EDA는 둘 다 보고, 서빙 기본은 배포현실(+sim)** — 단 generic 퇴화(S1) 해결 후.

### S5 — 깊이 vs 정규화 head-to-head (Q5, 재학습 필요)
"sim 대신 L=3 가져올까?"의 정직한 답 — *대체*가 아니라 **같은 측정틀 위 통제 비교**.

| 안 | 구성 | 가설 |
|---|---|---|
| **A** (현행) | sim materialized, L≤2 | 기준선 |
| **B** | sim 제거 + **L=3 재귀**(A^L 가중합) | homophily·오버스무딩↑ → gap↑·변별↓ |
| **C** | sim + **키워드 IDF/허브 정규화**(간식 다운웨이트) | 퇴화 해소·변별↑ (★우세 가설) |

- **C안 구현**: `build_hetero_data._hop2(hop2_kw_idf=True, hop2_kw_idf_tau=τ)` — `S = A·diag(idf)·Aᵀ ≥ τ`, idf=log(P/df_k). sim_ip는 불변(이미 변별자).
- **★ 재학습 전 미리보기**(노트북 §S5-prep, `sim_diag.idf_sim_sweep`/`idf_sim_discrimination`): 재학습 없이 τ 튜닝.
  - 실측: τ↑ 시 generic_only **0.66→0.04(τ12)→0.004(τ16)**, deg **208→11.6(τ12)→2.9(τ16)**. **후보 τ≈12** (deg 정상·노이즈 96%↓·보유율 0.83 유지).
  - 변별력은 0.332→**0.39(소폭)**, sim_ip 0.617엔 못 미침 → **IDF 가치는 sim_kw를 스타로 만드는 게 아니라 노이즈가 어텐션 61% 먹는 걸 막아 sim_ip·직접채널로 재분배**시키는 것.
- **방법**: 각 안 그래프 재빌드 → v2 재학습(`src.train.trainer`) → **공통 측정**: test PR-AUC · **val-test gap** · **α_r 재분배**(sim_kw↓·sim_ip/직접↑) · FP/FN 변별 · **S3 전이ablation 재적용**.
- **산출**: A/B/C 비교 표 (실험 대장 §10 등재).
- **결정**: 단순 PR-AUC 아니라 **"α_r이 sim_ip·직접채널로 분산되며 gap 안 키우고 변별 살았나"**. C가 이기면 정규화 채택, B가 이기면 깊이 채택 — *둘 다 시도한 근거가 남음*.

## 3. 구현 맵 — 단일 노트북 + .py 헬퍼

S1~S5 **결과를 한 노트북**(`eda/sim_edge_influence/sim_edge_influence.ipynb`)에 모은다.
"실행 위치"와 "결과 정리 위치"는 다르다 — 노트북 밖으로 나가는 건 **S5 학습 실행(긴 GPU 루프)** 하나뿐이고,
그 산출 체크포인트를 노트북이 로드해 A/B/C를 한 표로 비교한다. 로직은 `.py`, 노트북은 구동·표·그림만.

```
eda/sim_edge_influence/sim_edge_influence.ipynb   ← S1~S5 결과 단일 표면
   §0 setup     MDEngine(v2) 로드 · build_mass/ledger · import sim_diag
   §S1 구조진단  sim_diag.sim_degree_stats / min_shared_sweep / sim_kw_shared_keywords
   §S2 변별력    sim_diag.discrimination_table (sim_kw vs sim_ip 단독 PR-AUC)
   §S3 ablation  sim_diag.ablate_cross_split_sim / train_neighbor_counts
   §S4 처방토글  sim_diag.compare_include_sim (killer 후보 direct vs +sim)
   §S5 A/B/C     체크포인트 로드 → α_r·PR-AUC·gap 비교표
        └ (전제) A/B/C 학습은 터미널: python -m src.train.trainer (실험대장 §10)
```

| 계층 | 위치 | 내용 |
|---|---|---|
| 로직 (.py) | `src/eval/md/sim_diag.py` (신규) | S1~S4 헬퍼 + `ablate_cross_split_sim` |
| 프리미티브 | `engine.score_concept(include_sim=)` (구현됨) | S4 두 모드 |
| 구동/표/그림 | `sim_edge_influence.ipynb` (신규) | 단일 노트북 |
| 재학습 (CLI) | `build_hetero_data` 옵션 + `src.train.trainer` | S5 A/B/C → 체크포인트, 실험대장 등재 |

> **순서**: S1→S2→S3 (진단·누수검증 먼저) → S4 (처방 기본모드 결정) → S5 (깊이vs정규화, 재학습 후 표 갱신).
> **발견 누적**: sim 관련 인과·구조 발견은 `docs/findings/`에 한 발견=한 파일 ([findings/README.md](findings/README.md)).

## 4. 성공 기준 (이 EDA가 끝났다고 말하려면)

1. **Q1·Q3**: sim_kw 퇴화 여부 + 진짜 변별자(sim_ip 추정) 정량 확정.
2. **Q2**: 전이적 낙관 크기(ablation PR-AUC 하락폭) → cold-start 별도평가 필요성 판정.
3. **Q4**: combo/처방 **기본 모드**(direct vs +sim) 결정 + 근거.
4. **Q5**: A/B/C 재학습 비교 → **깊이 vs 정규화** 결론 (gap·변별 기준). 최소 C(IDF) 1건 + B(L=3) 대조군.

> 끝나면: 처방 시스템 기본 모드 확정(S4) + 그래프 구축 개선안 채택 여부(S5) + (필요시) cold-start 평가 프로토콜 도입(S3).
