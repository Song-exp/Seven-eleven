# MD 처방 시스템 — 운영 가이드 (as-built)

> 학습된 HIN-GNN에서 **성공/실패/일반 키워드 장부**를 뽑고, **흥행 유발 키워드를 인과적으로 검증**해 확정하고, **대시보드로 서빙**하는 전체 시스템의 사용 설명서.
> 설계 배경: [EDA·처방 계획](eda_channel_prescription_plan.md) · [모델 전환](final_model_leakfree_switch_plan.md) · [v2 전환](v2_serving_transition.md).

---

## 0. 한눈에 — 전체 데이터 흐름

```
[학습 모델]  experiments/results/{exp47_no_copurchase | v2_sweepA}/hin_gnn_best.pt
     │
     ▼  단일 추론 1회 (원칙 0)
[엔진]  src/eval/md/engine.py  →  prob·24관계 어텐션·Mass·장부(killer/mine/hub)·개입 머신(Δprob)
     │
     ├──▶ [EDA]  tasks.py (혼동행렬·A_diff) + prescription.py (처방 A~F)
     │         노트북: experiments/notebooks/md_prescription_pipeline.ipynb
     │
     ├──▶ [확정]  inspector.py (Δprob 증거) → keyword_final.csv
     │         노트북: experiments/notebooks/keyword_finalization.ipynb
     │
     ▼
[서빙]  src/eval/serve.py  ──(keyword_final.csv 훅: include 필터 + tag)──▶ _infer / _network
     │
     ▼  python -m scripts.export_dashboard
[대시보드]  Dashboard/config.js  →  Dashboard/dashboard.html
```

핵심: **학습은 한 번, 추론도 한 번**. 이후 EDA·확정·서빙은 그 캐시를 슬라이싱/조건부 집계로 처리.

---

## 1. 모듈 맵 (`src/eval/md/`)

| 모듈 | 핵심 객체/함수 | 역할 |
|---|---|---|
| `engine.py` | `MDEngine`, `EngineConfig` | 모델 로드(exp47/v2 자동분기) → 단일 추론 캐시 + `Mass` + `build_ledger`(killer/mine/hub) + `delta_prob`(개입 머신) |
| `tasks.py` | `stage_g1_macro`, `stage_g2_channel`, `cell_4a/4b/4c` | 혼동행렬 구조 진단 + 성공망/실패망·채널 차분 행렬(A_diff) |
| `prescription.py` | `MDPrescriptionEngine`, `get_md_prescription` | 시드→처방(승인/반려 + POS/인스타 파트너 + anti-partner + 장바구니 + 소생) |
| `validate.py` | `run_all` (Tier 1/2/4/5 + Precision@K) | 장부·조합의 통계·인과·hold-out 검증 |
| `inspector.py` | `keyword_evidence`, `evidence_table`, `export_keyword_final` | **키워드 확정용 4축 증거**(통계·인과·실증·지지도) |
| `export_keyword_final.py` | `python -m …` | `keyword_final.csv` 생성 CLI |

선택: `EngineConfig.exp47()` / `.v2_sweepA()` 로 모델 명시 선택 (장부는 모델 거의 불변, [v2 전환 §3.5](v2_serving_transition.md) 참조).

---

## 2. 마스터 장부 (killer / mine / hub)

4대 순방향 경로(#1 has_kw / #10 via_ip / #11 ipip / #12 trend)의 어텐션으로 키워드별 점수 산출:

```
Score_succ(k) = Σ_경로 Σ_{성공제품 p} att(p,k)
Purity(k)     = Score_succ / (Score_succ + Score_fail)          # base rate 0.238 기준
Hub_Score(k)  = Σ_경로 Σ_{p} att(p,k) × Mass[p]                 # Mass=z(sim_kw)+z(sim_ip)
```

| 장부 | 선정 (지지도≥3) | 의미 |
|---|---|---|
| **killer** | Purity≥0.50 ∧ Score_succ 상위 25% | 성공 특이 (대박 유도 후보) |
| **mine** | Purity≤0.15 ∧ Score_fail 상위 25% | 실패 특이 (악재 후보) |
| **hub** | Hub_Score 상위 20% ∧ **\|Purity−base\|<0.15** | 무색무취 백본 (균형 게이트가 검증) |

> 임계는 `EngineConfig`에서 조정 (커버리지↔신뢰도 트레이드오프). 확대 프리셋 적용 시 태그 ~337개(16%).

---

## 3. ★ 핵심 방법론 — 인과 killer (흥행 "유발" 검증)

**문제**: `Purity`(통계적 상관)가 높다고 그 키워드가 *흥행을 유발*하는 건 아니다. 성공 제품에 *붙어만* 있을 수 있다(다른 요인 때문).

**해법**: **개입 머신 `Δprob`** — 그 키워드를 *없는* 제품(가상 노드)에 추가했을 때 모델의 성공확률이 오르나? `Δprob > 0` 이면 **인과적 유발**, `≤ 0` 이면 **상관만**.

실측 대조 (exp47, killer 후보):

| 키워드 | 성공률 | Purity | **Δprob** | 판정 |
|---|---|---|---|---|
| 고창 | 0.80 | 0.80 | **+0.118** | ✅ 진짜 유발 |
| 공주 | 1.00 | 0.70 | **+0.070** | ✅ 진짜 유발 |
| 띠부씰 | 0.72 | 0.67 | **−0.024** | ⚠ 상관만 (K리그·KBO IP 후광) |
| 공룡 | 1.00 | 0.92 | **−0.067** | ⚠ 상관만 |

→ **확정 기준 = 통계(Purity·성공률) ∧ 인과(Δprob>0) ∧ 실증(매출).** `inspector.evidence_table`이 이 셋을 한 표로 제공.

### 데이터 품질 플래그
`강아지`처럼 **4경로 지지도(support_succ)와 직접 has_kw 성공률이 어긋나는**(우회 경로로만 성공 연결) 키워드는 확정에서 제외 후보. 인스펙터에서 `성공률` vs `support_succ` 불일치로 식별.

### 상호작용 — modifier vs base 강도 (캐리어별 절제)
평균 Δprob는 **여러 캐리어에 더해본 평균**이라 상호작용을 뭉갠다. `keyword_context_breakdown`(노트북 §3.5)은 *실제 보유 제품마다* 키워드를 빼본 기여(`contrib`)를 분리한다. `keyword_disentangle`은 동반 키워드(예: 고창↔꿀고구마) 중 진짜 드라이버를 가른다.
> 예: **고창** — 성공을 가르는 건 고창이 아니라 캐리어 base 강도. 고창은 +0.3쯤 일정 리프트를 주는 *증폭기*(충분조건 아님), 진짜 드라이버는 지역브랜딩 '고창'이지 꿀고구마가 아님. 상세: [`docs/findings/`](findings/README.md).

> **발견 누적**: 노트북에서 키워드를 드릴다운하며 얻은 인과·상호작용 인사이트는 `docs/findings/`에 한 발견 = 한 파일로 모은다 (인덱스 + 템플릿은 [findings/README.md](findings/README.md)).

---

## 4. 키워드 확정 → 대시보드 파이프라인 (운영)

### 워크플로우
```
1. experiments/notebooks/keyword_finalization.ipynb 열기
2. (튜닝) EngineConfig 임계 조정 → 장부 재생성
3. (검토) evidence_table 로 Δprob≤0 가짜 killer 식별 / 단일 키워드 drill-down(실제 제품·매출)
4. (반복) 임계 조정하며 진짜 유발 키워드만 남기기
5. (확정) export_keyword_final → data/processed/hin/keyword_final.csv
6. (편집) CSV에서 include(Y/N)·tag 손보기 — 'suggested'의 "강등검토" 우선 검토
7. (연결) python -m scripts.export_dashboard → Dashboard/config.js 재생성
8. dashboard.html 열기 → 확정 키워드·태그(색) 반영
```

### `keyword_final.csv` 스키마
`keyword | tag(killer/mine/hub/neutral) | include(Y/N) | 성공률 | purity | balance | support | delta_prob_mean | delta_prob_pos_rate | 매출중앙값 | suggested`

- **당신이 편집하는 단일 진실 소스.** include·tag만 손보면 됨.
- `suggested` 추천액션: killer는 `유지(흥행유발 확인)` / `강등검토(상관만 Δ≤0)`.

### serve.py 훅 (무코드 연결)
`serve._load_keyword_final()` 가 CSV를 읽어 ① `include=N` 키워드 그래프에서 제거 ② killer/mine/hub `tag`를 네트워크 노드에 부착(대시보드 색·뱃지). **파일 없으면 전체 통과(하위호환).**

### 프론트엔드 계약
`dashboard.html` ↔ 백엔드: `POST /infer{trend}→{attrs}` · `POST /network{trend,attrs}→{nodes,edges}`. 오프라인 모드(`config.js`의 `window.DASHBOARD_DATA`)면 서버 불필요.

---

## 5. 두 모델 운영

| | **v2_sweepA (현 서빙)** | exp47 (비교용) |
|---|---|---|
| 구조 | HINGNNv2 멀티태스크 + basket_comp | HINGNN, copurchase 제거 |
| held-out PR-AUC | **0.608** | 0.570 |
| 과적합 gap | **0.115** | 0.224 |
| 누수 | leak-free | leak-free |
| 선택 | `EngineConfig.v2_sweepA()` / serve `SERVING_EXP="v2_sweepA"` (기본) | `EngineConfig.exp47()` |

> **2026-06-21 v2 전면 승격**: 서빙·EDA·확정·대시보드 기본값 모두 `v2_sweepA`. v2 서빙 산출물은 `python -m experiments.v2_export_serving`로 생성(weighted 엣지 4종 + relation gate). serve.py는 torch 추론이 아니라 export parquet을 읽는 오프라인 서빙이라 어댑터 없이 산출물 생성만으로 전환됨. 상세: [v2 전환 §5](v2_serving_transition.md).

장부·A_diff·처방어휘는 **모델 거의 불변**(어텐션=구조 지배). 모델이 바꾸는 건 prob 기반 부분(혼동행렬·파트너 랭킹·Δprob). 상세: [v2 전환 §3.5](v2_serving_transition.md).

---

## 6. 빠른 참조 (명령)

```bash
# 키워드 확정 CSV 생성
python -m src.eval.md.export_keyword_final exp47      # 또는 v2_sweepA

# 대시보드 캐시 재생성 (확정 후)
python -m scripts.export_dashboard

# 노트북 (Jupyter/VSCode에서)
experiments/notebooks/md_prescription_pipeline.ipynb   # EDA·처방·검증 (MODEL 선택 셀)
experiments/notebooks/keyword_finalization.ipynb       # 키워드 확정 루프
```

```python
from src.eval.md.engine import MDEngine, EngineConfig
from src.eval.md.inspector import keyword_evidence, export_keyword_final
eng = MDEngine(EngineConfig.exp47()).run_single_inference(); eng.build_mass(); eng.build_ledger("full")
keyword_evidence(eng, "마라")          # 단일 키워드 4축 증거
```

---

## 7. 산출물 맵

| 파일 | 내용 |
|---|---|
| `experiments/results/md_prescription/{model}/` | inference_cache·ledgers·scoreboard·figures (모델별) |
| `experiments/results/md_prescription/{model}/keyword_scores_full.csv` | 전 키워드 점수표(2,063) |
| `data/processed/hin/keyword_final.csv` | **확정 마스터** (serve.py가 읽음) |
| `Dashboard/config.js` | 대시보드 오프라인 캐시 (export_dashboard 생성) |
