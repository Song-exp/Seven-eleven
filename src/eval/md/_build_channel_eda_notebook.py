"""channel_fit_eda.ipynb 생성기 — 키워드 채널 적합도(인스타 vs POS) EDA.

목적: A안(배타 성공군 + ablation Δ + margin) 로직으로 키워드를 POS형/인스타형/범용으로
      분류해 **뽑힌 키워드를 눈으로 검토**하고, 이 로직을 **대시보드에 녹일지 말지** 판단한다.
      (데이터 산출이 목적이 아니라 의사결정 보조가 목적 — 마지막 §6 decision-aid 셀.)

방법론: docs/causal_intervention_and_metapath.md §2(분류기준) + 채널 배타 분할.
모델: v2_sweepA (현 서빙). 대시보드와 동일 프리미티브(score_concept) → 노이즈 플로어 ±0.01 통일.
실행: python -m src.eval.md._build_channel_eda_notebook
"""
import nbformat as nbf

NB_PATH = "experiments/notebooks/channel_fit_eda.ipynb"
md = lambda s: nbf.v4.new_markdown_cell(s)
code = lambda s: nbf.v4.new_code_cell(s)
cells = []

cells.append(md(r"""# 키워드 채널 적합도 EDA — 인스타(바이럴) vs POS(매출)

> 방법론: `docs/causal_intervention_and_metapath.md` §2 (관찰 게이트 → 인과 Δ 검증)
> 모델: **v2_sweepA** (현 서빙) · 대시보드와 동일 `score_concept` 프리미티브 · 노이즈 플로어 ±0.01 통일

## 무엇을 하나
1. **배타 성공군 분할**: POS 단독성공(`성공_소스=='POS'`) vs 인스타 단독성공(`인스타/CU/GS25`). **POS+인스타(둘 다 성공)는 채널 판별 노이즈라 제외.**
2. **관찰 게이트(커버리지)**: 키워드가 POS 성공군에 ≥3 / 인스타 성공군에 ≥1 등장하나. (killer/mine과 동일한 빈도≥3 base 위에서)
3. **인과 Δ (ablation)**: 그 키워드를 *보유한* 성공작에서 키워드를 **빼봤을 때** 성공확률 하락폭. 채널별로 평균.
   - `Δ_pos(k)`, `Δ_insta(k)` → **margin = Δ_insta − Δ_pos** (±0.01 노이즈 플로어로 끊음)
4. **태그**: POS형 / 인스타형 / 범용(양쪽) / 채널미정. 인스타 1~2개 표본은 **저신뢰 플래그**.

## 이 노트북의 진짜 목적
뽑힌 키워드가 **납득되는지**(§5) + 이 채널 태그가 기존 killer/mine/매개 태그와 **직교한 새 정보인지**(§6)를 보고, **대시보드에 녹일지 결정**한다."""))

cells.append(code(r"""# --- Setup ---
import os, sys
from collections import defaultdict
# repo 루트 자동 탐색 (cwd가 notebooks/든 repo 루트든 무관) — src/eval/md/engine.py 마커로 상향 검색
ROOT = os.path.abspath(os.getcwd())
for _ in range(8):
    if os.path.exists(os.path.join(ROOT, "src", "eval", "md", "engine.py")):
        break
    parent = os.path.dirname(ROOT)
    if parent == ROOT:
        raise RuntimeError("repo 루트를 못 찾음 — src/eval/md/engine.py 기준")
    ROOT = parent
if ROOT not in sys.path: sys.path.insert(0, ROOT)
os.chdir(ROOT)

import numpy as np, pandas as pd
import matplotlib.pyplot as plt
plt.rcParams["font.family"] = "Malgun Gothic"; plt.rcParams["axes.unicode_minus"] = False

from src.eval.md.engine import MDEngine, EngineConfig, PK_MAIN
print("repo:", ROOT)"""))

cells.append(md(r"""## ⚙️ 0. 파라미터 (튜닝 다이얼)

- `POS_FLOOR=3` / `INSTA_FLOOR=1`: 채널 게이트 (인스타는 표본이 얇어 1로 푼다 — 소실 vs 정밀 트레이드오프).
- `TOTAL_FLOOR=3`: killer/mine과 동일 base (소표본 거품 제거).
- `NOISE=0.01`: 단일 노이즈 플로어 — 대시보드 pair synergy / 전역 분류와 동일 기준선.
- `LOWCONF_INSTA=3`: 인스타 보유 < 이 값이면 저신뢰 플래그(버리지 않음)."""))

cells.append(code(r"""POS_FLOOR    = 3
INSTA_FLOOR  = 1
TOTAL_FLOOR  = 3
NOISE        = 0.01
LOWCONF_INSTA= 3
MODEL        = "v2_sweepA"   # 현 서빙. 비교하려면 "exp47"."""))

cells.append(md(r"""## 1. 단일 추론 + 채널 배타 마스크

`succ_src` 로 성공 제품을 배타 분할. POS+인스타(둘 다)는 양쪽에서 뺀다."""))

cells.append(code(r"""cfg = EngineConfig.v2_sweepA() if MODEL == "v2_sweepA" else EngineConfig.exp47()
eng = MDEngine(cfg).run_single_inference(); eng.build_mass()

y   = eng.cache["y"]
src = eng.cache["succ_src"]
K   = eng.cache["K"]

INSTA_SRC = ["인스타", "CU_인스타", "GS25_인스타"]
pos_idx   = np.where((y == 1) & (src == "POS"))[0]                  # POS 단독성공
insta_idx = np.where((y == 1) & np.isin(src, INSTA_SRC))[0]        # 인스타 단독성공
both_n    = int(((y == 1) & (src == "POS+인스타")).sum())          # 제외 대상
print(f"POS 단독성공 {len(pos_idx)} / 인스타 단독성공 {len(insta_idx)} / POS+인스타(제외) {both_n}")"""))

cells.append(md(r"""## 2. 관찰 게이트 — 채널별 보유수 + 커버리지 (소실률 리포트)

has_kw 엣지로 키워드별 (전체 / POS성공 / 인스타성공) 보유 제품 수를 센다."""))

cells.append(code(r"""ei = eng.cache["eidx"][PK_MAIN].numpy()   # [2, E]  (product, keyword)

def holder_count(prod_idx):
    m = np.isin(ei[0], prod_idx)
    return np.bincount(ei[1][m], minlength=K)

total_cnt = np.bincount(ei[1], minlength=K)
pos_cnt   = holder_count(pos_idx)
insta_cnt = holder_count(insta_idx)

names = np.array([eng.kw_name(k) for k in range(K)])
df = pd.DataFrame({"keyword": names, "total": total_cnt, "pos": pos_cnt, "insta": insta_cnt})
df = df[df["total"] >= TOTAL_FLOOR].reset_index(drop=True)   # killer/mine base

pos_ok = df["pos"] >= POS_FLOOR
ins_ok = df["insta"] >= INSTA_FLOOR
n = len(df)
print(f"base(총빈도>={TOTAL_FLOOR}) 키워드: {n}")
print(f"  POS 키워드(POS>={POS_FLOOR}):    {int(pos_ok.sum())} ({pos_ok.mean()*100:.1f}%)")
print(f"  인스타 키워드(insta>={INSTA_FLOOR}): {int(ins_ok.sum())} ({ins_ok.mean()*100:.1f}%)")
print(f"   ├ both:    {int((pos_ok&ins_ok).sum())}")
print(f"   ├ POS만:   {int((pos_ok&~ins_ok).sum())}")
print(f"   └ insta만: {int((~pos_ok&ins_ok).sum())}")
print(f"  채널 판정(둘 중 하나라도): {int((pos_ok|ins_ok).sum())} ({(pos_ok|ins_ok).mean()*100:.1f}%)")
print(f"  소실(둘 다 미달):          {int((~pos_ok&~ins_ok).sum())} ({(~pos_ok&~ins_ok).mean()*100:.1f}%)")"""))

cells.append(md(r"""## 3. 인과 Δ (ablation) — 채널별 기여도

**핵심 단계.** 키워드를 *보유한* 성공작에서 그 키워드를 빼보고(`score_concept`로 가상 재구성),
원래 대비 성공확률 하락폭 `contrib = prob(보유) − prob(제거)` 를 채널별로 평균한다.

- 정확도: 같은 제품의 `full`과 `ablate_k`를 **한 청크에 넣어** 동시 forward → 차분 기준 1차 상쇄(엔진 docstring).
- 대시보드 `combo`/`classify`와 동일 프리미티브 → 수치 정합.
- 후보(`cand`)는 게이트 통과 키워드만 — 비용 절약."""))

cells.append(code(r"""# 게이트 통과(둘 중 하나라도) 키워드만 Δ 계산 대상
cand_set = set(np.where((total_cnt >= TOTAL_FLOOR))[0].tolist())
cand_set = {k for k in cand_set if pos_cnt[k] >= POS_FLOOR or insta_cnt[k] >= INSTA_FLOOR}

HASIP = ("product", "has_ip", "ip")
have_ip = HASIP in eng.cache["eidx"]
union_prod = sorted(set(pos_idx.tolist()) | set(insta_idx.tolist()))
pk_set = {p: eng.product_keywords(p) for p in union_prod}
ip_set = {p: (eng.product_keywords(p, HASIP) if have_ip else []) for p in union_prod}

def product_contribs(p):
    '''제품 p에서 보유 후보 키워드를 각각 빼본 contrib dict {k: prob(full)-prob(full\\k)}.'''
    kws, ips = pk_set[p], ip_set[p]
    targets = [k for k in kws if k in cand_set]
    if not targets:
        return {}
    concepts = [(kws, ips)] + [([x for x in kws if x != k], ips) for k in targets]
    s = eng.score_concept_batch(concepts, chunk_size=len(concepts))  # 단일 청크 → 상쇄 최대
    base = s[0]
    return {k: float(base - s[i + 1]) for i, k in enumerate(targets)}

def channel_delta(prod_idx):
    acc = defaultdict(list)
    for p in prod_idx:
        for k, c in product_contribs(int(p)).items():
            acc[k].append(c)
    d = {k: float(np.mean(v)) for k, v in acc.items()}
    nn = {k: len(v) for k, v in acc.items()}
    return d, nn

import time
t0 = time.time()
d_pos, _   = channel_delta(pos_idx)
d_insta, _ = channel_delta(insta_idx)
print(f"Δ 계산 완료: POS {len(d_pos)}키 / insta {len(d_insta)}키  ({time.time()-t0:.0f}s)")"""))

cells.append(md(r"""## 4. 태그 분류 — margin 기준

- both(POS≥3 & insta≥1): `margin = Δ_insta − Δ_pos` → `>+0.01` 인스타형 / `<−0.01` POS형 / 그 사이 **범용**.
- 한쪽만 통과: 그 채널로 직판정 (반대쪽은 증거 0 = 측정 불가).
- 인스타 보유 < `LOWCONF_INSTA` 면 **저신뢰** 플래그 (드롭하지 않음)."""))

cells.append(code(r"""rows = []
for _, r in df.iterrows():
    k = int(np.where(names == r["keyword"])[0][0])
    dp = d_pos.get(k, np.nan); di = d_insta.get(k, np.nan)
    p_ok = r["pos"] >= POS_FLOOR; i_ok = r["insta"] >= INSTA_FLOOR
    margin = (di - dp) if (p_ok and i_ok) else np.nan
    if p_ok and i_ok:
        tag = "인스타형" if margin > NOISE else ("POS형" if margin < -NOISE else "범용")
    elif p_ok:
        tag = "POS형"
    elif i_ok:
        tag = "인스타형"
    else:
        tag = "채널미정"
    lowconf = (tag == "인스타형") and (r["insta"] < LOWCONF_INSTA)
    rows.append(dict(keyword=r["keyword"], tag=tag, total=int(r["total"]),
                     pos_n=int(r["pos"]), insta_n=int(r["insta"]),
                     d_pos=round(dp, 4) if not np.isnan(dp) else None,
                     d_insta=round(di, 4) if not np.isnan(di) else None,
                     margin=round(margin, 4) if not np.isnan(margin) else None,
                     저신뢰=lowconf))
res = pd.DataFrame(rows)
print(res["tag"].value_counts())
print(f"\n저신뢰(인스타 보유<{LOWCONF_INSTA}) 인스타형: {int(res['저신뢰'].sum())}")
res.head(20)"""))

cells.append(md(r"""## 5. 방법 검증 ① — 전역 채널 수치가 납득되나?

라이브로 노드에 붙일 채널 수치(ablation Δ)가 직관과 맞는지 먼저 본다. (가설: POS형=식감·내실 / 인스타형=트렌드·디저트)
> 채널 라벨은 정적 리스트가 아니라 **gtag(killer/mine)처럼 전역 1회 산출(§3 배치) → 서브네트 노드에 부착**할 속성이다. 여기선 그 산출값이 타당한지 본다."""))

cells.append(code(r"""def show(tag, by, asc, nshow=15):
    sub = res[res["tag"] == tag].sort_values(by, ascending=asc)
    print(f"=== {tag} ({len(res[res['tag']==tag])}개) ===")
    return sub.head(nshow)[["keyword","pos_n","insta_n","d_pos","d_insta","margin","저신뢰"]]
display(show("인스타형", "insta_n", False))
display(show("POS형",   "pos_n",   False))
display(show("범용",     "total",   False))"""))

cells.append(md(r"""## 6. ★ 방법 검증 ② — 라이브 서브네트워크 내 채널 구분 (핵심)

대시보드 사용 흐름 그대로: **시드 주입 → `subnet.build_subnetwork`로 양의-시너지 서브네트 생성 → 그 안의 키워드에 채널 라벨 부착.**
MD는 이 서브네트에서 5개를 뽑아 **인스타형=마케팅 카피 / POS형=제품 활용**으로 배분한다.

여기서 검증할 것:
- 서브네트 안의 키워드들이 **채널로 깔끔히 갈리나** (예시 납득도).
- **서브네트 내 채널미정 비율**이 낮은가 (전역 45.7%보다 높아야 라이브 부착 가치 ↑ — 양의 시너지 키워드는 빈도가 높아 더 잘 잡힐 것이라는 가설 검증).
- 비용 — gtag처럼 로드 1회 산출이라 부착은 dict 조회(≈0)."""))

cells.append(code(r"""from src.eval.md.combo import _ConceptCache
from src.eval.md import subnet as SN
sc = _ConceptCache(eng)
chan = dict(zip(res["keyword"], res["tag"]))
lowc = dict(zip(res["keyword"], res["저신뢰"]))

SEEDS = ["마라", "로제", "흑임자", "단백질", "고창", "약과"]
cov_rows = []
for seed in SEEDS:
    if eng.seed_to_idx(seed) is None:
        print(f"[{seed}] 그래프에 없음"); continue
    net = SN.build_subnetwork(eng, seed, sc=sc)
    if net.get("error"):
        print(f"[{seed}] {net['error']}"); continue
    kws = [n["label"] for n in net["nodes"] if n["type"] in ("rail","trend","basket","ip2")]
    kws = [k for k in dict.fromkeys(kws) if k != seed]
    ins = [f"{k}{'⚠' if lowc.get(k) else ''}" for k in kws if chan.get(k)=="인스타형"]
    pos = [k for k in kws if chan.get(k)=="POS형"]
    uni = [k for k in kws if chan.get(k)=="범용"]
    und = [k for k in kws if chan.get(k,"채널미정")=="채널미정"]
    ndet = len(kws)-len(und)
    print(f"\n● [{seed}] 서브네트 키워드 {len(kws)} | 채널판정 {ndet} ({ndet/max(len(kws),1)*100:.0f}%)")
    print(f"   📷 인스타형(카피): {ins}")
    print(f"   🔴 POS형(제품):    {pos}")
    print(f"   ⚪ 범용:           {uni}")
    print(f"   ❔ 채널미정:        {und}")
    cov_rows.append(dict(seed=seed, n=len(kws), 판정=ndet, 인스타=len(ins), POS=len(pos), 범용=len(uni), 미정=len(und)))

cov = pd.DataFrame(cov_rows)
if len(cov):
    display(cov)
    print(f"\n서브네트 평균 채널판정률: {cov['판정'].sum()/cov['n'].sum()*100:.0f}%  (전역 base 45.7% 대비)")"""))

cells.append(code(r"""# 라이브 비용 체크 — combo 서브네트 1회. 채널 부착은 gtag처럼 dict 조회라 추가비용 ≈0.
import time
t0 = time.time(); _ = SN.build_subnetwork(eng, "마라", sc=sc)
print(f"combo 서브네트 1회: {time.time()-t0:.2f}s  (+채널 부착 ≈0 → combo와 동일하게 라이브 가능)")"""))

cells.append(md(r"""## 7. 의사결정 — 라이브 `ctag`로 녹일까?

판단 재료:
1. **방법 타당성**(§5) — 전역 채널 수치가 직관과 맞나.
2. **서브네트 커버리지**(§6) — MD가 실제 보는 서브네트에서 채널미정 비율이 낮은가. ★ 이게 결정의 핵심 (전역 45.7%가 아니라 *서브네트 내* 판정률).
3. **직교성**(아래) — 채널 태그가 killer/mine/매개와 다른 새 정보인가.
4. **비용** — gtag처럼 로드 1회 → 라이브 부착 ≈0 (§6에서 확인).

→ §6 서브네트 커버리지가 충분 + §5 납득되면: `classify_channel_live`(로드 1회 캐시) + `combo_serve.build_seed`에서 노드 `ctag` 부착 = **gtag와 완전히 동일한 패턴**으로 연동. (single click = ctag 채널 / two click = 기존 synergy 그대로)"""))

cells.append(code(r"""# 직교성 — 기존 전역 분류와 교차 (새 정보인지)
from src.eval.md.classify import classify_keywords_live
gtags = classify_keywords_live(eng)
res["전역태그"] = res["keyword"].map(lambda x: gtags.get(x, "neutral"))
print("채널 태그 × 전역 태그 교차표:")
display(pd.crosstab(res["tag"], res["전역태그"]))

# 검증·연동 판단 자료 저장 (이 CSV가 산출물이 아니라, 라이브 계산법 검증 근거)
OUT = f"experiments/results/md_prescription/{MODEL}/channel_fit"
os.makedirs(OUT, exist_ok=True)
res.to_csv(os.path.join(OUT, "channel_fit_keywords.csv"), index=False, encoding="utf-8-sig")
print("saved:", os.path.join(OUT, "channel_fit_keywords.csv"))"""))

nb = nbf.v4.new_notebook()
nb["cells"] = cells
nb["metadata"] = {"language_info": {"name": "python"}}
import os as _os
_os.makedirs(_os.path.dirname(NB_PATH), exist_ok=True)
with open(NB_PATH, "w", encoding="utf-8") as f:
    nbf.write(nb, f)
print("written ->", NB_PATH)
