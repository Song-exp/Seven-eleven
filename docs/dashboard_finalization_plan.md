# 최종 키워드 확정 → 대시보드 연결 계획

> **✅ 2026-06-23 실행 완료 (옵션 B 채택).** killer 89·매개 79·mine 70(회피 뱃지)·가짜 35 강등. serve 추천에서 mine 제외, config.js 재생성, dashboard 색·범례 반영.
> 생성 2026-06-23 · 전제: 키워드 분류 확정(`keyword_final.csv`), 추가 확장 없음(누들·햇반 미추가).
> 관련: [연결 가이드](dashboard_connection_guide.md) · [발표등급](killer_evidence_tiers.md) · [빈도×purity](keyword_frequency_purity_groups.md) · [제품예시](keyword_examples_by_metric.md)

---

## 0. 확정 사항 (결정 완료)

| 항목 | 결정 |
|---|---|
| 후보 확장(누들·햇반) | **안 함** — purity 0.45 완화는 +2 modest·raw 약함이라 현 풀 유지 |
| ❌ 가짜(강등) 35개 | killer 뱃지 제거(→ neutral 취급) |
| **mine(지뢰) 70개** | **최종 서빙에서 제거** → 최종 = killer + 매개 (+ neutral 배경) |
| 발표등급 | killer를 강증거/보통/모델신호/조건부로 차등 표기 |

**최종 서빙 키워드 = killer 89 (❌제외) + 매개 79 = 203개 처방 키워드** (+ neutral 1,790 배경 노드).

---

## 1. 현 연결 구조 (as-is) — 배관은 이미 존재

```
data/processed/hin/keyword_final.csv      ← 단일 진실 소스 (당신이 편집)
        │  serve._load_keyword_final() : include(Y/N) 필터 + tag(killer/mine/매개/neutral) 맵
        ▼
src/eval/serve.py  (graph_kw &= include_set ;  노드에 tag 부착)
        ├──▶ [라이브]  api.py  (POST /infer·/network·/combo)  ──▶ dashboard.html
        └──▶ [오프라인]  python -m scripts.export_dashboard  ──▶ Dashboard/config.js  ──▶ dashboard.html
```

- **변경 지점은 단 하나**: `keyword_final.csv`의 `include`·`tag` 컬럼. 코드 수정 거의 없음.
- 현재 `include`는 전부 `Y` (2,063개 전부 통과) → 아직 아무것도 필터링 안 된 상태.

---

## 2. Phase 1 — keyword_final.csv 확정 편집 (핵심)

`include`·`tag`를 아래 규칙으로 갱신 (스크립트 1회):

| 현재 tag/판정 | n | → include | → tag | 대시보드 효과 |
|---|---|---|---|---|
| killer (❌가짜 아님) | 89 | Y | killer | 초록+강조 뱃지, 추천 대상 |
| killer 中 ❌가짜(강등) | 35 | Y | **neutral** | 뱃지 제거, 일반 노드로 강등 |
| 매개 | 79 | Y | 매개 | 보편증폭 뱃지 |
| **mine** | 70 | **(결정 필요 ↓)** | mine | (아래) |
| neutral | 1,790 | Y | neutral | 배경 노드 |

### ★ 결정 필요 — mine "제거"를 어떻게 구현할지
| 옵션 | 구현 | 장점 | 단점 |
|---|---|---|---|
| **A. 그래프에서 완전 제거** | mine `include=N` | 추천·시야에서 완전 배제, 가장 단순 | 그래프 연결성 변함(와인·찌개 등 실제 속성 노드 사라짐), **회피 처방(★★★) 시야에서 소실** |
| **B. (권장) 노드는 유지·빨강 회피뱃지 + 추천 제외** | mine `include=Y`, `tag=mine`, recommend 함수에서 mine 제외 | 연결성 보존, **"피해라" 회피 처방을 빨강으로 시각화**(리포트가 mine을 가장 신뢰), 추천엔 안 뜸 | recommend 함수에 mine 제외 한 줄 추가 필요 |

> 권장 = **B**. mine은 리포트에서 신뢰도 최고(★★★ 구조적 실패)라, *추천에서만 빼고* 대시보드엔 **빨강 "회피" 뱃지**로 남기는 게 정보 손실이 없음. "제거"의 의도(추천 안 함)는 충족하면서 회피 처방을 살림.
> A를 원하면 mine include=N 한 줄이면 끝.

### 발표등급 반영 (선택)
killer 89개에 `발표등급`(강증거 36/보통 18/모델신호 7/조건부 5)을 컬럼으로 추가 → 대시보드에서 **강증거=진한 뱃지, 모델신호=옅은 뱃지**로 신뢰도 차등 표시. 생성 로직은 `scripts/gen_killer_evidence_tiers_md.py`의 등급 함수 재사용.

**산출**: 편집된 `keyword_final.csv` (단일 진실 소스 갱신).

---

## 3. Phase 2 — serve 훅 검증 (코드 변경 최소)

1. `serve._load_keyword_final()`: include=N 필터·tag 맵 정상 동작 확인 (이미 구현됨).
2. (옵션 B 채택 시) `recommend_keywords`/`recommend_bundle`/`recommend_paths`에 **mine tag 제외** 한 줄 추가
   - 현 `exclude`/`GENERIC_STOPWORDS` 필터(serve.py:492) 옆에 `kw_tag.get(k)=='mine'` 제외 조건 삽입.
3. 헤드리스 스모크: `infer_attrs`·`recommend_keywords`가 mine 키워드를 추천에 안 내보내는지 확인.

---

## 4. Phase 3 — 오프라인 캐시 재생성

```bash
python -m scripts.export_dashboard          # serve.py 실행 → Dashboard/config.js 재생성
# (조합 데모 쓰면) python -m scripts.export_combo_dashboard <키워드들>   → Dashboard/combo_data.js
```
- `config.js`의 `window.DASHBOARD_DATA`에 확정 키워드·tag가 직렬화됨.
- `file://`로 `dashboard.html` 열어 오프라인 모드 동작 확인.

---

## 5. Phase 4 — dashboard.html 색/뱃지 (프론트 한 줄)

1. **매개 색 추가** (인수인계 §3.4 대기작업): 노드 렌더에서 `tag=='매개'` 색 분기 한 줄. 네트워크 렌더 로직엔 영향 없음.
2. (옵션 B) `tag=='mine'` 빨강 "회피" 뱃지 색 확인.
3. (선택) 발표등급별 뱃지 농도 분기.
4. 범례 갱신: 초록=killer / 보라=매개 / 빨강=mine(회피) / 회색=일반.

---

## 6. Phase 5 — 라이브 API (선택, MD 실무용)

```bash
python -m uvicorn src.eval.api:app --port 8000   # /infer·/network·/combo 라이브
```
- 임의 키워드 클릭 시 ~1-2s 동적 서브넷. `file://`면 캐시만 사용(데모 안전).

---

## 7. 검증 체크리스트

- [ ] keyword_final.csv: mine 70 처리(A 또는 B), ❌가짜 35 → neutral, killer 89·매개 79 tag 정상
- [ ] serve: mine이 추천에 안 뜸 / include 필터 반영
- [ ] config.js 재생성 후 dashboard.html 오프라인 로드 OK
- [ ] 색: killer 초록 / 매개 보라 / (B면) mine 빨강 / neutral 회색
- [ ] 강증거 36개가 강조 뱃지로 구분되는지 (발표등급 반영 시)
- [ ] 대표 키워드 스폿체크: 고창(강증거 killer)·즉석(매개)·와인(mine 회피) 표기 확인

---

## 8. 작업 순서 요약

```
1. (결정) mine 처리 A/B 선택
2. keyword_final.csv 편집 스크립트 작성·실행  (Phase 1)
3. serve 훅 검증 + (B면) recommend mine 제외 한 줄  (Phase 2)
4. python -m scripts.export_dashboard           (Phase 3)
5. dashboard.html 매개 색 한 줄 + 범례           (Phase 4)
6. 오프라인 로드 검증 → 끝                        (Phase 7 체크리스트)
```

> 핵심: **키워드 풀은 이미 확정.** 이 작업은 *분류 결과를 CSV 두 컬럼(include·tag)에 반영하고 재export*하는 연결 작업이며, 신규 모델 추론·재학습 없음.
