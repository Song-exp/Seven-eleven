import sys
try:
    sys.stdout.reconfigure(encoding='utf-8')
except AttributeError:
    pass

import matplotlib
matplotlib.use('Agg') # Disable GUI window

import os, sys
from pathlib import Path
from collections import defaultdict
import numpy as np, pandas as pd
import matplotlib.pyplot as plt, matplotlib
import networkx as nx
_anchor = Path(globals().get("__vsc_ipynb_file__", ".")).resolve().parent if globals().get("__vsc_ipynb_file__") else Path.cwd()
ROOT = next((str(p) for p in [_anchor, *_anchor.parents] if (p / "CLAUDE.md").exists()), str(_anchor))
sys.path.insert(0, ROOT); os.chdir(ROOT)
matplotlib.rcParams["font.family"] = "Malgun Gothic"; matplotlib.rcParams["axes.unicode_minus"] = False

from src.eval import serve
serve._ollama = lambda *args, **kwargs: ""
D = serve._data()
kn = pd.read_parquet("data/processed/hin/keyword_nodes.parquet")
TREND_KW = set(kn.loc[kn["is_trend_keyword"], "keyword"])
prod_succ = dict(zip(D["scores"]["ITEM_NM"], D["scores"]["y_true"]))
SUCC = [p for p in D["prod2kw"] if prod_succ.get(p) == 1]
FAIL = [p for p in D["prod2kw"] if prod_succ.get(p) == 0]
KW_OF = {p: set(k for k, _ in lst) for p, lst in D["prod2kw"].items()}  # 제품→키워드집합
SEEDS = [s for s in ["마라", "흑임자", "로제", "단백질", "딸기", "띠부씰", "감자", "매콤"] if s in D["graph_kw"]]
print("모델:", serve.SERVING_EXP, "| 성공", len(SUCC), "/ 실패", len(FAIL), "| 트렌드키워드", len(TREND_KW))
print("시드:", SEEDS)


# =====================================================================

# ── Phase A. walk 분석: 일관묶음 vs 탐색경로, 스텝수 드리프트 진단 ──
print("① 일관 묶음(bundle) vs ② 탐색 경로(paths) — 스텝별\n")
for seed in SEEDS[:4]:
    attrs = serve.infer_attrs(seed) or [seed]
    bundle = serve.recommend_bundle(attrs)
    print(f"[{seed}] 묶음: {bundle}")
    for st in [2, 3, 4]:
        ps = serve.recommend_paths(attrs, max_steps=st, n_paths=2, avoid=bundle)
        for p, _ in ps:
            print(f"   {st}스텝: " + " → ".join(p))
    print()

# 드리프트 지표: 스텝별 경로 키워드의 (a)신규율 (b)평균 제품빈도(=일반성)
print("스텝별 드리프트 진단 (전 시드 평균):")
print(f"{'스텝':>4}{'신규키워드율':>12}{'평균 제품빈도':>13}  (빈도↑=일반어로 드리프트)")
for st in [2, 3, 4]:
    new_r, gen = [], []
    for seed in SEEDS:
        attrs = serve.infer_attrs(seed) or [seed]
        bundle = set(serve.recommend_bundle(attrs))
        for p, _ in serve.recommend_paths(attrs, max_steps=st, n_paths=3, avoid=list(bundle)):
            kws = p[1:]
            if kws:
                new_r.append(np.mean([k not in bundle for k in kws]))
                gen.append(np.mean([D["deg"].get(k, 0) for k in kws]))
    print(f"{st:>4}{np.mean(new_r):>12.2f}{np.mean(gen):>13.0f}")
print("→ 신규율 높고 빈도 적당하면 좋은 탐색 / 빈도 급증하면 일반어 드리프트 (스텝 줄일 신호)")


# =====================================================================

# ── Phase B. 성공/실패 제품 주변 키워드의 트렌드 키워드 비율 ──
def trend_stats(prods):
    tot, tr, per = 0, 0, []
    for p in prods:
        kws = [k for k, _ in D["prod2kw"][p]]
        if not kws:
            continue
        t = sum(k in TREND_KW for k in kws)
        tot += len(kws); tr += t; per.append(t / len(kws))
    return tr / max(tot, 1), float(np.mean(per)), float(np.mean([len([k for k,_ in D["prod2kw"][p] if k in TREND_KW]) for p in prods]))

sr, sp, sc = trend_stats(SUCC)
fr, fp, fc = trend_stats(FAIL)
df = pd.DataFrame({
    "구분": ["성공", "실패"],
    "트렌드KW 비율(전체)": [f"{sr:.3f}", f"{fr:.3f}"],
    "제품당 트렌드비율(평균)": [f"{sp:.3f}", f"{fp:.3f}"],
    "제품당 트렌드KW수": [f"{sc:.2f}", f"{fc:.2f}"],
})
try:
    from IPython.display import display; display(df)
except Exception:
    print(df.to_string(index=False))
print(f"\n→ 성공/실패 트렌드비율 비율(lift) = {sr/max(fr,1e-9):.2f}  (>1이면 성공 제품이 트렌드 키워드를 더 많이 보유)")
fig, ax = plt.subplots(figsize=(5, 3))
ax.bar(["성공", "실패"], [sp, fp], color=["#00C896", "#FF5C5C"])
ax.set_ylabel("제품당 트렌드KW 비율"); ax.set_title("성공 vs 실패 — 트렌드 키워드 비율"); plt.tight_layout(); # plt.show()


# =====================================================================

# ── Phase C. 네트워크 허브 키워드 (중심성) ──
# 키워드 동시출현 그래프 (제품 매개) → degree / eigenvector 중심성
coc = defaultdict(int)
for p, lst in D["prod2kw"].items():
    ks = [k for k, _ in lst]
    for a in range(len(ks)):
        for b in range(a + 1, len(ks)):
            coc[tuple(sorted((ks[a], ks[b])))] += 1
G = nx.Graph()
for (a, b), w in coc.items():
    if w >= 3:                              # 노이즈 제거 임계
        G.add_edge(a, b, weight=w)
print(f"동시출현 그래프: 노드 {G.number_of_nodes()} / 엣지 {G.number_of_edges()}")

# 최대 연결요소에서 eigenvector 중심성
Gc = G.subgraph(max(nx.connected_components(G), key=len)).copy()
eig = nx.eigenvector_centrality_numpy(Gc, weight="weight")
deg_c = {k: D["deg"].get(k, 0) for k in G.nodes}      # 제품빈도(인기 허브)
cooc_deg = dict(G.degree())                            # 연결 키워드 수(커넥터 허브)

def top(dct, n=15):
    return sorted(dct.items(), key=lambda x: -x[1])[:n]

print("\n[degree 허브 — 제품 많이 보유]:", [k for k, _ in top(deg_c, 10)])
print("[connector 허브 — 많은 키워드와 동시출현]:", [k for k, _ in top(cooc_deg, 10)])
print("[eigenvector 허브 — 중심부 연결]:", [k for k, _ in top(eig, 10)])
HUBS = [k for k, _ in top(eig, 30)]      # Phase D 에서 사용할 허브 집합 (eigenvector 기준)


# =====================================================================

# ── Phase D. 허브 키워드 포함 여부 — 성공 vs 실패 비교 ──
succ_sets = [KW_OF[p] for p in SUCC]
fail_sets = [KW_OF[p] for p in FAIL]

rows = []
for h in HUBS:
    sf = np.mean([h in s for s in succ_sets])
    ff = np.mean([h in s for s in fail_sets])
    rows.append({"허브": h, "성공포함율": round(sf, 3), "실패포함율": round(ff, 3),
                 "lift": round(sf / (ff + 1e-9), 2), "제품수": D["deg"].get(h, 0)})
hub_df = pd.DataFrame(rows).sort_values("lift", ascending=False)
try:
    from IPython.display import display; display(hub_df.head(20))
except Exception:
    print(hub_df.head(20).to_string(index=False))

# 종합: 제품당 평균 허브 보유 수
hubset = set(HUBS)
s_hub = np.mean([len(s & hubset) for s in succ_sets])
f_hub = np.mean([len(s & hubset) for s in fail_sets])
print(f"\n제품당 평균 허브 키워드 수 — 성공 {s_hub:.2f} / 실패 {f_hub:.2f} (lift {s_hub/max(f_hub,1e-9):.2f})")
print("→ lift>1 이면 성공 제품이 허브 키워드를 더 많이 포함 = '허브 포함이 성공과 연관'")
fig, ax = plt.subplots(figsize=(5, 3))
ax.bar(["성공", "실패"], [s_hub, f_hub], color=["#00C896", "#FF5C5C"])
ax.set_ylabel("제품당 허브 키워드 수"); ax.set_title("성공 vs 실패 — 허브 키워드 포함"); plt.tight_layout(); # plt.show()
