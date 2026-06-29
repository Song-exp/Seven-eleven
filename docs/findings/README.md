# 키워드 발견 노트 (Findings)

> `keyword_finalization.ipynb`에서 키워드를 드릴다운하며 발견한 **인과·상호작용 인사이트**를 모으는 곳.
> 한 발견 = 한 파일. 키워드 확정(`keyword_final.csv`)과 대시보드 메모의 근거가 된다.

## 어디서 나오나
`experiments/notebooks/keyword_finalization.ipynb` — `SEED`만 바꿔가며 돌리면 키워드별로 증거가 나온다.

| 셀 | 도구 | 보는 것 |
|---|---|---|
| §2 | `evidence_table` | 장부 전체 Δprob 스캔 (진짜 유발 vs 상관만) |
| §3 | `keyword_evidence` | 단일 키워드 4축 (통계·인과·실매출·지지도) |
| **§3.5** | `keyword_context_breakdown` | **캐리어별 절제** — 같은 키워드가 무엇에 붙느냐로 갈리는가 (상호작용) |
| **§3.5** | `keyword_disentangle` | **교란 분리** — 동반 키워드 중 진짜 드라이버 |

## 기록 규칙
- 파일명: `YYYY-MM-DD_키워드-한줄요지.md` (예: `2026-06-21_고창-modifier-vs-base.md`)
- 새 발견을 추가하면 아래 인덱스에 한 줄 추가.
- 키워드 확정에 영향을 주는 발견이면 `keyword_final.csv`의 `include`/`tag`/메모에도 반영.
- 모델은 기본 `exp47`(최종 채택). v2로 확인했으면 발견에 모델 명시.

## 템플릿
```markdown
# [키워드] — 한 줄 요지

- 모델: exp47 | 날짜: YYYY-MM-DD | 태그: killer/mine/hub
- 도구: keyword_context_breakdown / keyword_disentangle / ...

## 관찰
(표/수치 — 노트북 출력 붙여넣기)

## 해석
(왜 이런가 — 메커니즘)

## 확정 함의
(include/tag/메모 어떻게 할지)
```

---

## 인덱스

| 날짜 | 키워드 | 요지 | 태그 |
|---|---|---|---|
| 2026-06-21 | [고창](2026-06-21_고창-modifier-vs-base.md) | 성공을 가르는 건 고창이 아니라 **캐리어 base 강도**. 고창은 일정 리프트 주는 증폭기(충분조건 아님). 진짜 드라이버는 고창(지역브랜딩)이지 꿀고구마 아님 | killer 유지 |
| 2026-06-21 | [조합 포화·대체재](2026-06-21_조합-포화-substitution.md) | 강한 키워드는 **보완재가 아니라 대체재**(가상노드 ~2키워드서 포화). 깊은 빔·초가산성 selector 폐기 → `seed_partners`(headroom Δ) 인과 파트너표가 정답 | 방법론 정정 |
| 2026-06-22 | [sim 엣지](2026-06-22_sim엣지-퇴화-sim_ip가신호.md) | 어텐션 87% 먹는 sim 중 **sim_kw는 퇴화 노이즈**(공유≥3 과밀, 66% 일반어, 변별 0.33 평탄), **sim_ip가 진짜 신호**(단독 PR-AUC 0.617≈전체 0.608). train↔test ablation drop 0.0005 → **누수 아님**. 개선=정규화(L=3 아님) | 구조/누수해소 |
| 2026-06-22 | [synergy 단독강도 역전](2026-06-22_synergy-headroom-단독강도역전.md) | synergy=`margin(k\|seed)−margin(k\|∅)`는 **단독 약한 키워드의 회복분**이라 `corr(synergy,baseline)=−0.61`. → 시너지 쌍의 실제 공동성공률이 잠식 쌍보다 **낮음(0.25 vs 0.45)**. synergy=salvage 신호이지 흥행 신호 아님 → baseline 병기 필요. 분류기준(per-pair 2·SE)은 유효 | 해석 주의 |
| 2026-06-23 | [killer 스태킹 포화](2026-06-23_killer-스태킹-포화.md) | 확정 killer 89개로 재현: **첫 killer가 성공확률을 천장(~0.65)으로**(+0.565), 2번째부터 한계기여 ≈0(포화). killer 더미에 killer 추가 Δ≈0 vs 빈 베이스 +0.54. `corr(단독강도,synergy)=−1.0·synergy<0 100%`. **성공 키워드 무더기 합쳐도 시너지 안 좋아짐** = salvage이지 합산 아님 | 처방·발표 |
