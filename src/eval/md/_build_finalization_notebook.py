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

nb = nbf.v4.new_notebook(cells=cells)
nb.metadata["kernelspec"] = {"display_name": "Python 3", "language": "python", "name": "python3"}
os.makedirs(os.path.dirname(NB), exist_ok=True)
with open(NB, "w", encoding="utf-8") as f:
    nbf.write(nb, f)
print("wrote", NB, "cells:", len(cells))
