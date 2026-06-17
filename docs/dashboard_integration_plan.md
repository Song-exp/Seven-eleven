# 대시보드 ↔ 학습 네트워크 연동 설계 (Graph RAG 포함)

> **상태**: 설계 확정(2026-06-14) / 구현 착수
> **프론트**: `Dashboard/dashboard.html` (정적, config.js 로드, SVG 렌더)
> **백엔드**: 학습된 HIN-GNN best 체크포인트 + Gemma(Ollama) Graph RAG
> **관련**: `docs/methodology_metapath_search.md`(모델), `src/eval/recommend.py`(K-P-K)

---

## 1. 아키텍처 — 하이브리드 + 단일 서빙 코어 + Graph RAG

```
            ┌──────────────────────────────────────────────┐
            │  src/eval/serve.py  (서빙 코어, 단일 진실)       │
            │  best 체크포인트 + 7함수 (검색·생성·조회)         │
            └──────┬──────────────────────────┬──────────────┘
        ②정적 배치   │                          │  ③라이브
  scripts/export_dashboard.py            src/eval/api.py (FastAPI)
   → Dashboard/config.js                  /infer /recommend /hits /busts
        │                                       │
        └──────────────▶ dashboard.html ◀───────┘
            캐시 있으면 즉시 / 없으면 fetch

  [Graph RAG]  GNN(검색: 학습된 K-P-K 조합·점수·경유제품·성공확률)
               → Gemma(Ollama, 근거 기반 생성: 제품명·근거·진단)
```

원칙: 추론 로직은 `serve.py` 한 곳. config.js 배치와 API가 **같은 함수** 호출.

---

## 2. 확정 결정 사항

| 항목 | 결정 |
|---|---|
| 연동 방식 | **하이브리드** — config.js 프리캐시(기지 트렌드) + FastAPI fallback(임의 입력) |
| API 프레임워크 | **FastAPI** (도입 승인) |
| 카테고리 | product_nodes에 없음 → **forward-compatible resolver**. 세븐=POS `ITEM_MDDV_NM` 조인(2796/2924), CU·GS=임시 `'미분류'`. **추후 product_nodes에 `중분류` 컬럼 추가 시 코드 수정 없이 자동 우선 사용** |
| 매출 | **하드 의존성 아님**. 랭킹·"상위 N%"는 모델 `pred_success_prob`. 세븐 실매출(`sales_30d_amt`)은 있을 때만 보조 표시 |
| 서빙 모델 | **pluggable** — `SERVING_EXP` 한 줄로 교체. 골격=exp07(현재 그래프 재학습), best는 methodB 실험 후 교체 |
| Gemma | **Ollama** `gemma4:e4b` @ `localhost:11434`. 기존 `attribute_inferrer.infer_attributes` 재사용 |
| Graph RAG 범위 | **전체** — ① 제품명 ② 추천 근거 ③ 부진 진단 모두 그래프 근거 생성 |

---

## 3. 데이터 계약 (config.js ≡ API 응답, 동일 스키마)

기존 하드코딩 구조 그대로 (프론트 변경 최소):
```js
window.DASHBOARD_DATA = {
  categories: ["삼각김밥","도시락","냉장간편식"],
  trendAttrs: { "<trend>": ["<graph키워드>", ...] },                 // 탭1 추론 (graph 어휘 정합)
  proposals:  { "<trend>|<cat>": [ {name, category, score, scoreLabel,
                  attrs, unique, tpo, via, hops, rationale,           // rationale = RAG 생성 근거
                  network:{center,trend,attrs}} ] },                  // 탭1 조합
  hits:  { "<cat>": [ {name, sales|null, pct} ] },                    // 탭2 (pct=success_prob percentile)
  busts: { "<cat>": [ {name, sales|null, issue} ] },                  // 탭3
  diag:  { "<item>": {avoid:[...], rx:[...], text} }                  // 탭3 진단 (text = RAG 생성)
}
```

---

## 4. serve.py 함수 (7종)

| 함수 | 소스 | 비고 |
|---|---|---|
| `load_serving_model()` | `experiments/results/{SERVING_EXP}/hin_gnn_best.pt` | export_results._rebuild 재사용 |
| `get_category(row)` | product_nodes.`중분류`(있으면) → POS 조인 → `'미분류'` | **forward-compatible** |
| `infer_attrs(trend)` | `trend_keywords.parquet` 조회 → 없으면 Gemma + graph 어휘 필터 | 어휘 정합 핵심 |
| `top_products(cat,n)` | `learned_product_scores`(prob↑) + get_category | 탭2 |
| `bust_products(cat)` | `learned_product_scores`(prob↓) | 탭3 |
| `graph_retrieve(query)` | recommend_combinations + 경유제품 + success + α_r | **RAG 검색** |
| `gemma_generate(task,ctx)` | Ollama gemma, ctx=검색 근거 | **RAG 생성** |

조립: `recommend_proposals(cat,attrs)` = `graph_retrieve` → `gemma_generate("제품제안")` → 카드. `diagnose(item)` = `graph_retrieve(bust)` → `gemma_generate("진단")`.

---

## 5. Graph RAG 설계 (학습 네트워크 = 검색 근거)

```
[검색] graph_retrieve(query):
   - K-P-K 추천 키워드 + 학습 점수 (recommend_combinations)
   - 경유 제품(매개 product) + pred_success_prob
   - 연결 IP, 층별 α_r(관계 중요도, "왜")
   → context (구조화 근거 dict)
[증강] 프롬프트 = 역할 + context 근거 + "제공된 근거만 사용" 제약
[생성] Gemma(Ollama) → 근거 기반 텍스트 (할루시네이션 차단)
```

**3개 접점** (전부 적용):
1. **제품명** — 추천 조합·경유제품 근거로 신제품명 생성
2. **추천 근거** — "왜 이 조합이 성공 가능한지" (점수·경유 히트·α_r 인용)
3. **부진 진단** — 부진제품 약한 엣지·결핍 속성 근거로 avoid/rx 문장 생성

하이브리드 정합: 기지 트렌드는 RAG 결과를 **config.js 프리캐시**, 임의 트렌드는 **API 라이브 RAG**.

---

## 6. 로드맵 (S0~S5 구현 완료 2026-06-14)

| 단계 | 작업 | 산출 | 상태 |
|---|---|---|---|
| **S0** | exp07 현재 그래프 재학습 → 임시 pluggable 서빙 모델 | `results/exp07.../hin_gnn_best.pt` (test PR-AUC 0.6572) | ✅ |
| **S1** | `src/eval/serve.py` 코어 (offline, torch 불필요) | 히트·부진·K-P-K·entry-point 매처 | ✅ |
| **S2** | `scripts/export_dashboard.py` → `config.js` | trendAttrs 450·proposals 120·hits/busts/diag | ✅ |
| **S3** | `dashboard.html` 리팩터 (캐시 소비 + fetch fallback) | **서버 없이 데모 동작** | ✅ |
| **S4** | `src/eval/api.py` (FastAPI) | /infer /recommend /hits /busts /diagnose | ✅ (엔드포인트 검증) |
| **S5** | entry-point 매칭 + `gemma_generate` (제품명·근거·진단) | **WSL Gemma로 실제 생성 검증 완료** | ✅ |

### ✅ Gemma(Ollama) — WSL 우회로 해결
**문제**: Windows 네이티브 Ollama가 모든 gemma4(e4b·12b·26b) **CLIP 로드 실패** (한글 사용자명 경로 `C:\Users\송정현\...`의 멀티모달 projector blob 로드 버그).
**해결**: WSL(Linux) Ollama는 동일 버그 없음. **재다운로드 없이** Windows 기존 모델 blob을 심볼릭링크로 재사용 + WSL2 localhost 포워딩으로 Windows 백엔드가 도달.

**WSL Ollama 기동 (RAG 사용 시 1회)**:
```bash
wsl
ln -sfn /mnt/c/Users/*/.ollama/models /tmp/winmodels      # Windows blob → ASCII 심볼릭링크
OLLAMA_MODELS=/tmp/winmodels OLLAMA_HOST=0.0.0.0:11435 ollama serve
```
- Windows ollama(11434)와 충돌 회피 위해 **11435** 사용. Windows가 안 쓰는 포트라 WSL2가 `localhost:11435`로 자동 포워딩 (WSL IP 변동 무관).
- `serve.py._ollama_base()`가 Windows 실행 시 자동으로 `localhost:11435` 사용 (env `OLLAMA_BASE_URL`로 오버라이드 가능).
- 첫 호출은 9.6GB 모델 로드(/mnt/c 9p ≈ 83초), `keep_alive 30m`로 이후 RAM 캐시 → 빠름.

**검증 결과**: 제품명(`바지락마라감칠삼각김밥`)·근거(경유 제품 성공확률 0.281 인용)·진단 모두 그래프 근거 생성 확인.
config.js RAG 프리캐시는 `export_dashboard.py`의 `USE_RAG=True` 후 재실행 (배치 느림, 라이브 API는 즉시 RAG).

### 실행 방법
- **서버 없이**: `Dashboard/dashboard.html` 직접 열기 (config.js 캐시)
- **라이브 API**: `python -m uvicorn src.eval.api:app --port 8000` → 미캐시 트렌드 fetch fallback
- **config.js 재생성**: `python -m scripts.export_dashboard`

---

## 7. 의존성·주의

- **Ollama 필요**(S4·S5): `ollama serve` + `gemma4:e4b`. 미가동 시 기지 트렌드 조회로 fallback
- **어휘 정합**: Gemma 출력은 그래프 키워드(`k2i`)로 필터해야 recommend 동작. 기지 트렌드(trend_keywords)는 이미 정합
- **카테고리 컬럼**: 추후 product_nodes에 `중분류` 추가 시 `get_category`가 자동 우선 사용 (CU·GS도 그때 채워짐)
- **FastAPI**: 신규 의존성 (승인됨)
- **서빙 모델 교체**: methodB(exp07/08/09) 비교 후 best로 `SERVING_EXP`만 변경
