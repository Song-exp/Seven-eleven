"""keyword_finalization.ipynb 생성기 — 키워드 확정 루프(증거 검토 + 파라미터 튜닝 + export).

실행: python -m src.eval.md._build_finalization_notebook
"""
import os
import nbformat as nbf

NB = "experiments/notebooks/keyword_finalization.ipynb"
md = lambda s: nbf.v4.new_markdown_cell(s)
code = lambda s: nbf.v4.new_code_cell(s)
cells = []

cells.append(md(r"""# 키워드 확정 루프 — 증거 기반 (흥행 *유발* 검증)

> 목적: 파라미터 조정 → 나온 키워드가 **진짜 흥행을 유발**하는지 4축 증거로 확인 → 확정 → 대시보드 연결.
> 4축: ① 통계(purity·성공률) ② **인과(Δprob: 더하면 성공확률 오르나)** ③ 실증(실매출) ④ 지지도.

```
[튜닝] EngineConfig 임계 조정 → 장부 재생성
   ↓
[검토] evidence_table 로 Δprob·매출 스캔 → 단일 키워드 drill-down
   ↓  (반복)
[확정] keyword_final.csv 저장 (include/tag + 증거)
   ↓
[연결] python -m scripts.export_dashboard → dashboard.html 반영
```
**당신이 하는 일**: 셀 돌리며 Δprob≤0인 가짜 killer 골라내고, 임계 조정해 재확인 → 마지막에 CSV 저장."""))

cells.append(code(r"""import os, sys
ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
if ROOT not in sys.path: sys.path.insert(0, ROOT)
os.chdir(ROOT)
import numpy as np, pandas as pd
pd.set_option("display.max_rows", 200)
from src.eval.md.engine import MDEngine, EngineConfig
from src.eval.md import inspector as I
from src.eval.md.inspector import keyword_evidence, evidence_table, ledger_keywords, export_keyword_final
from src.eval.md.inspector import keyword_context_breakdown, keyword_disentangle

MODEL = "v2_sweepA"   # "v2_sweepA" | "exp47"  (최종 채택=v2_sweepA. 장부는 모델 거의 불변)
print("model:", MODEL)"""))

cells.append(md("## 1. 엔진 로드 + 장부 (파라미터는 EngineConfig)"))
cells.append(code(r"""# ★ 파라미터 튜닝 지점 — 값 바꾸고 이 셀부터 다시 실행하면 장부가 바뀜
CFG = (EngineConfig.exp47() if MODEL=="exp47" else EngineConfig.v2_sweepA())
CFG.killer_purity = 0.50   # killer purity 하한 (base 0.238 위)
CFG.killer_top_q  = 0.75   # Score_succ 상위 25%
CFG.mine_purity   = 0.15   # mine purity 상한
CFG.hub_top_q     = 0.80   # Hub_Score 상위 20%
CFG.hub_balance_eps = 0.15 # |purity-base| 균형 게이트

eng = MDEngine(CFG).run_single_inference(); eng.build_mass(); lg = eng.build_ledger("full")
print(f"killer {len(lg.killer)} / mine {len(lg.mine)} / hub {len(lg.hub)} / neutral {eng.cache['K']-len(lg.killer|lg.mine|lg.hub)}")"""))

cells.append(md("## 2. 증거 스캔 — killer가 진짜 흥행을 유발하나 (Δprob)"))
cells.append(code(r"""killers = ledger_keywords(eng, "killer")
tbl = evidence_table(eng, killers)
# Δprob>0 = 흥행 유발(인과 확인) / Δprob≤0 = 상관만 (가짜 killer 의심)
tbl = tbl.sort_values("delta_prob_mean", ascending=False)
print(f"killer {len(tbl)}개 중 Δprob>0(진짜 유발): {(tbl['delta_prob_mean']>0).sum()}개 / Δprob≤0(상관만): {(tbl['delta_prob_mean']<=0).sum()}개")
display(tbl)"""))

cells.append(code(r"""# mine / hub 도 같은 방식
print("── MINE (Δprob<0 이어야 진짜 악재) ──"); display(evidence_table(eng, ledger_keywords(eng,"mine")).sort_values("delta_prob_mean"))
print("── HUB (balance 작아야 진짜 일반어) ──"); display(evidence_table(eng, ledger_keywords(eng,"hub")).sort_values("balance"))"""))

cells.append(md("## 3. 단일 키워드 drill-down (실제 제품·매출 확인)"))
cells.append(code(r"""SEED = "고창"   # 의심스러운 키워드 넣어 확인
e = keyword_evidence(eng, SEED)
for k,v in e.items():
    if k != "예시제품": print(f"  {k}: {v}")
display(e["예시제품"])"""))

cells.append(md(r"""## 3.5 캐리어별 절제 분해 — 상호작용(modifier) 판정

`keyword_evidence`의 Δprob는 캐리어 평균이라 상호작용을 뭉갠다. 여기선 **실제 보유 제품마다** 키워드를 빼본 기여(`contrib`)를 분리 → "비스킷엔 +, 캔디엔 ≈0" 같은 조건부 효과가 드러난다.
- `contrib` ≫ 0: 이 제품에서 진짜 일함 / ≈0: 무의미 / <0: 악재
- `keyword_disentangle`: 동반 키워드(예: 고창↔꿀고구마) 중 실제 드라이버 분리"""))
cells.append(code(r"""# 캐리어별 in-context 절제 — 같은 키워드가 '무엇에 붙느냐'로 성공이 갈리는지
bd = keyword_context_breakdown(eng, SEED)
print(f"[{SEED}] 보유 {len(bd)}개 제품 · contrib = 이 제품에서 키워드를 빼면 떨어지는 성공확률 (클수록 진짜 일함)")
display(bd)

# 교란 분리 — 동반 키워드 중 진짜 신호의 주인은? (예: 고창 vs 꿀고구마)
print(f"\n[{SEED}] 교란 분리 (동반키워드별 평균 기여 비교):")
display(keyword_disentangle(eng, SEED))"""))

cells.append(md(r"""## 3.7 성공 조합 마이닝 — 개방형 인과 전진 (★여기서 조합 검토)

⚠ 가상 올스타 노드는 ~2키워드서 **포화** → 깊은 빔·초가산성 selector 폐기. **headroom Δ(margin)** 로 판별.
- **`combo_grow`(메인)**: 시드에서 margin≥ε 파트너를 **데이터가 멈출 때까지** 전진(고정 depth 없음). hop별 `synergy`(레일이 그 키워드를 증폭하나)로 **★찐보완재 / 강한보조** 라벨. margin<0은 **Bypass 장부**(잠식 블랙리스트 = MD 뇌절 금지).
- `seed_partners`(상세): 한 base에서 전체 보강/anti 파트너표. `mine_pairs`: 조합쌍 시너지 진단.
- 배경: [findings/조합-포화-substitution](../../docs/findings/2026-06-21_조합-포화-substitution.md)"""))
cells.append(code(r"""# ★ combo_grow — 개방형 인과 전진 (데이터가 레일 깊이 결정 + Bypass 장부). 시드당 ~60s
from src.eval.md.combo import combo_grow, print_grow_log, _ConceptCache
SC = _ConceptCache(eng)   # score_concept 캐시 — §3.8 서브네트와 공유(재계산 방지)

res = combo_grow(eng, SEED, eps=0.02, max_hops=5, cand_pool=28, n_headroom=10, sc=SC)
print_grow_log(res)
# res['rail']=최종조합 / res['accepted']=hop별 margin·synergy·라벨(찐보완재|강한보조) / res['bypass']=잠식 블랙리스트
# ε 올리면 레일 짧아짐(정예) · max_hops로 상한 · 더 정밀하면 n_headroom↑(느려짐)"""))
cells.append(code(r"""# (상세) 인과 파트너표(비포화) + 시너지 진단
from src.eval.md.combo import seed_partners, mine_pairs

COMBO_SEED = SEED   # §3의 SEED 재사용 (바꿔도 됨)
print(f"[{COMBO_SEED}] 인과 파트너표 — Δ>0 보강 / Δ<0 피할 것 (headroom 제품 기준, ~50s)")
pt = seed_partners(eng, COMBO_SEED, cand_pool=40, n_headroom=15, top=0)
print("보강 파트너:"); display(pt.head(8))
print("anti 파트너(피할 조합):"); display(pt.tail(4))

# 조합쌍 시너지 진단 (logit) — 보완재(>0) vs 대체재(<0). selector 아님, 라벨용
print("\nkiller 쌍 시너지 진단 (강+강은 대체재라 음수 경향 — 정상):")
display(mine_pairs(eng, ledger_keywords(eng, "killer")[:20], top=8))"""))
cells.append(code(r"""# (확정) 조합 장부 export — 시드별 보강/anti 파트너 → combo_final.csv
#   ⚠ 시드 1개당 ~50s. COMBO_SEEDS 늘리면 비례해 느려짐 (확정 단계서 한 번만).
from src.eval.md.combo import export_combo_final
COMBO_SEEDS = ledger_keywords(eng, "killer")[:5]   # 확정할 시드 목록
cf = export_combo_final(eng, COMBO_SEEDS, cand_pool=35, n_headroom=12)
print("저장: data/processed/hin/combo_final.csv |", len(cf), "시드")
display(cf)"""))

cells.append(md(r"""## 3.8 서브네트워크 + 상호작용 (★키워드별 결론 — 대시보드로 나갈 형태)

레일(combo_grow)을 척추로, 각 마디의 **1-hop 문맥**(IP·트렌드·바스켓, 타입별 색)을 붙인 서브네트. 깊이는 레일이, 문맥은 얕게(IP만 2-hop).
- **①추천**: `recommend_within` — 시작 노드에 서브네트 내 뭘 붙이면 좋은지 (headroom Δ)
- **②시너지**: `pair_synergy` — 두 노드 보완(✅)/대체·잠식(⚠) 인과 판정
- **③최적패스**: `best_path` — 두 노드 구조 경로를 score_concept로 재랭킹 + 끝점 시너지 (없으면 글루 노드 제안)

→ 대시보드에선 ②③이 클릭 인터랙션, 무거운 인과계산(①②)은 오프라인 선계산. 설계: [combo_mining_plan](../../docs/combo_mining_plan.md)"""))
cells.append(code(r"""# 3.8a 서브네트워크 — 레일(척추) + 1-hop 타입별 문맥(IP/트렌드/바스켓) + IP 2-hop + 잠식
from src.eval.md import subnet as SN
net = SN.build_subnetwork(eng, SEED, eps=0.02, max_hops=4, sc=SC)   # SC 공유 → combo_grow 재계산 안 함
from collections import Counter
print(f"[{SEED}] 노드 {len(net['nodes'])} 엣지 {len(net['edges'])} | 타입 {dict(Counter(n['type'] for n in net['nodes']))}")
print("레일:", " ➔ ".join(net["rail"]))
SN.draw_subnetwork(net)"""))
cells.append(code(r"""# 3.8b 상호작용 — ① 시작노드 추천  ② 두 노드 시너지  ③ 두 노드 최적 패스
pool = [n["label"] for n in net["nodes"] if n["type"] in ("rail", "trend", "basket", "ip2")]

print(f"① recommend_within({SEED}) — 서브네트 내 붙이면 좋은 노드:")
display(SN.recommend_within(eng, SEED, pool, n_headroom=10, top=6, sc=SC))

A, B = SEED, net["rail"][2] if len(net["rail"]) > 2 else net["rail"][-1]   # 예: 마라 vs 짭조름함
print(f"② pair_synergy({A}, {B}):", SN.pair_synergy(eng, A, B, n_headroom=10, sc=SC))

print(f"③ best_path({A}, {B}):")
bp = SN.best_path(eng, A, B, net, sc=SC)
print("   경로:", " → ".join(bp["path"]) if bp.get("path") else f"직접경로 없음 → 글루 {bp.get('glue')}")
print("   끝점 시너지:", bp["synergy"]["label"], bp["synergy"]["synergy"])
SN.draw_subnetwork(net, highlight_path=bp.get("path"))   # 패스 강조해 다시 그림"""))

cells.append(md("## 4. 확정 export → keyword_final.csv"))
cells.append(code(r"""# 전 키워드 + 태그 + Δprob 증거 + 추천액션. (태그 키워드 Δprob 계산에 수 분)
df = export_keyword_final(eng)
print("저장: data/processed/hin/keyword_final.csv |", len(df), "키워드")
print("추천액션:"); print(df[df.tag!="neutral"]["suggested"].value_counts().to_string())
print("\n→ 이 CSV에서 include(Y/N)·tag 손보세요. '강등검토'는 Δprob≤0인 가짜 killer 후보입니다.")"""))

cells.append(md(r"""## 5. 대시보드 연결 (확정 후)
CSV 확정(include/tag 편집) 후 터미널에서:
```
python -m scripts.export_dashboard
```
→ `Dashboard/config.js` 재생성 → `dashboard.html` 열면 확정 키워드·태그(색) 반영. **코드 수정 없음.**"""))

cells.append(md(r"""## 6. MD 서빙 미리보기 — 확정 → 대시보드/MD 한 눈에

이 노트북에서 확정한 것(`keyword_final.csv` include/tag + `combo_final.csv` 파트너)이 `serve.py`를 통해 대시보드·MD 처방으로 나가는 모습을 바로 확인한다. (서빙 모델 = `v2_sweepA`)"""))
cells.append(code(r"""# 서빙 미리보기 — 확정 결과(keyword_final.csv)가 대시보드/MD에 어떻게 나가나
from src.eval import serve
serve._data.cache_clear()   # keyword_final.csv 갱신분 반영
d = serve._data()
PREVIEW_TREND = "마라"
attrs = serve.infer_attrs(PREVIEW_TREND)
net = serve._keyword_net(PREVIEW_TREND, d)
tagged = [n for n in net["nodes"] if n.get("tag")]
print(f"서빙 모델: {serve.SERVING_EXP}")
print(f"[{PREVIEW_TREND}] 추론 속성({len(attrs)}): {attrs[:8]}")
print(f"네트워크: 노드 {len(net['nodes'])} · 엣지 {len(net['edges'])} · tag(killer/mine/hub) 부착 {len(tagged)}")
print("→ include=N 키워드는 그래프에서 빠지고, tag는 노드 색으로 대시보드에 반영됨.")
print("   (전체 MD 처방문은 md_prescription_pipeline.ipynb Stage 3 = get_md_prescription)")"""))

nb = nbf.v4.new_notebook(cells=cells)
nb.metadata["kernelspec"] = {"display_name": "Python 3", "language": "python", "name": "python3"}
os.makedirs(os.path.dirname(NB), exist_ok=True)
with open(NB, "w", encoding="utf-8") as f:
    nbf.write(nb, f)
print("wrote", NB, "cells:", len(cells))
