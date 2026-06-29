"""synergy 3분류 기준(시너지/잠식/보통) 검증 EDA — 복붙용 (eda/ipynb/ 에 노트북으로 저장).

검증 대상
---------
서브넷 내 조합 평가를, 두 부품으로 분해해 판정한다:
  · 베이스라인(키워드별) : margin(k|∅)        = 키워드 k의 '단독효과' (어느 컨텍스트든 동일)
  · 서브넷값(쌍별)       : margin(k|seed)     = 시드 컨셉 안에서 k의 효과
  · synergy             : 서브넷값 − 베이스라인 = "혼자보다 여기서 더 돕나"
  · 커트라인(쌍별)       : 2·SE               = 그 쌍 측정의 노이즈 바닥 (per-product 분산에서)

분류 규칙 (per-pair, 전역 상수 없음 → 대표 표본 불필요):
  synergy >  +2·SE → 시너지
  synergy <  −2·SE → 잠식
  |synergy| ≤ 2·SE → 보통(노이즈와 구분 불가)

핵심 검증 질문
-------------
E1  베이스라인 vs 서브넷값 분해 — synergy가 정말 '단독효과로부터의 이탈'인가 (멘탈모델 확인)
E2  노이즈 바닥 SE의 층화 — SE가 키워드 지지도에 따라 다른가 (다르면 전역 τ 불가 → per-pair 정당)
E3  분류 비율 — per-pair 2·SE가 합리적 3분할을 주는가
E4  ★ 포화 시드 핸들링 — 포화 시드는 '전부 보통', 풍부 시드는 뚜렷한 시너지로 나오는가 (대표성/포화 함정 회피 증명)
E5  전역 τ vs per-pair flip — 고-SE 쌍을 전역 τ가 오판하는가 (per-pair가 교정하는 오류)
E6  재현성 — headroom 표본을 바꿔도 분류가 안정적인가
E7  ★ 실제 데이터 앵커 — synergy가 현실인가: 두 키워드를 실제 같이 가진 제품의 성공률 대조
       (모델 prob이 아니라 raw 성공률로 보아 부분 순환을 완화)

주의: 콘솔 cp949에서 이모지 print 금지(노트북=UTF-8이라 무방). 데이터는 Read-Only.
"""

# %% ──────────────────────────────────────────── [Cell 1] Setup
import os, sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def _find_repo_root(start):
    """cwd가 repo 어디에 있든 src/eval + experiments 가진 루트를 위로 탐색."""
    p = os.path.abspath(start)
    while True:
        if os.path.isdir(os.path.join(p, "src", "eval")) and os.path.isdir(os.path.join(p, "experiments")):
            return p
        parent = os.path.dirname(p)
        if parent == p:
            return None
        p = parent


ROOT = _find_repo_root(os.getcwd())
assert ROOT, f"repo 루트 못 찾음 (cwd={os.getcwd()})"
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
os.chdir(ROOT)
plt.rcParams["font.family"] = "Malgun Gothic"
plt.rcParams["axes.unicode_minus"] = False

from src.eval.md.engine import MDEngine, EngineConfig, PK_MAIN
from src.eval.md.combo import _ConceptCache
from src.eval.md.subnet import build_subnetwork, _headroom_products

eng = MDEngine(EngineConfig.v2_sweepA()).run_single_inference()
eng.build_mass(); eng.build_ledger("full")
print("repo:", ROOT, "| K =", eng.cache["K"], "| base =", round(eng.cache["base_rate"], 3))

# 검증에 쓸 시드: 풍부/포화/지역/주류 섞어 다양성 확보
SEEDS = ["마라", "로제", "단백질", "흑임자", "위스키", "약과", "고창", "제로", "마늘", "흑맥주"]
KFACTOR = 2.0        # 커트라인 = KFACTOR · SE
GLOBAL_TAU = 0.005   # E5 비교용 전역 임계


# %% ──────────────────────────────────────────── [Cell 2] 진단 함수
def classify(synergy, se, k=KFACTOR):
    """per-pair 분류: |synergy| 가 자기 노이즈(k·SE)를 넘는지."""
    if synergy > k * se:
        return "시너지"
    if synergy < -k * se:
        return "잠식"
    return "보통"


def cooccur_success(eng, a_idx, b_idx):
    """E7 앵커: a·b를 실제로 같이 가진 제품의 raw 성공률 (모델 prob 아님)."""
    ei = eng.cache["eidx"][PK_MAIN].numpy()
    y = eng.cache["y"]
    pa = set(ei[0][ei[1] == a_idx].tolist())
    pb = set(ei[0][ei[1] == b_idx].tolist())
    both = list(pa & pb)
    return dict(n_both=len(both),
                succ_both=(float(y[both].mean()) if both else np.nan))


def subnet_synergy_table(eng, seed, n_headroom=20, rng_seed=0, max_nb=None):
    """시드 서브넷의 각 이웃에 대해 baseline·subnet·synergy·SE·분류·실제공동성공률 표.

    synergy_i = (margin(k|seed) on product i) − (margin(k|∅) on product i)  per product
    synergy = mean_i,  SE = std_i / sqrt(n)  → 커트라인 = 2·SE (쌍별).
    """
    si = eng.seed_to_idx(seed)
    if si is None:
        return pd.DataFrame()
    sc = _ConceptCache(eng)
    sn = build_subnetwork(eng, seed, sc=sc)
    if sn.get("error"):
        return pd.DataFrame()
    neigh = [n["label"] for n in sn["nodes"] if n["id"].startswith("kw:") and not n.get("seed")]
    cand = [(n, eng.seed_to_idx(n)) for n in dict.fromkeys(neigh)]
    cand = [(n, X) for n, X in cand if X is not None and X != si]
    if max_nb:
        cand = cand[:max_nb]
    sample = _headroom_products(eng, n_headroom, rng_seed=rng_seed)
    pk = {p: set(eng.product_keywords(p)) for p in sample}
    # 1 warm: {kp}, {kp|seed}, {kp|nb}, {kp|seed,nb}
    sc.warm([pk[p] for p in sample] + [pk[p] | {si} for p in sample]
            + [pk[p] | {X} for _, X in cand for p in sample]
            + [pk[p] | {si, X} for _, X in cand for p in sample])
    base0 = {p: sc(frozenset(pk[p])) for p in sample}
    base_s = {p: sc(frozenset(pk[p] | {si})) for p in sample}
    rows = []
    for n, X in cand:
        alone = np.array([sc(frozenset(pk[p] | {X})) - base0[p] for p in sample])    # margin(k|∅)
        withv = np.array([sc(frozenset(pk[p] | {si, X})) - base_s[p] for p in sample])  # margin(k|seed)
        syn = withv - alone
        m, se = float(syn.mean()), float(syn.std(ddof=1) / np.sqrt(len(syn)))
        co = cooccur_success(eng, si, X)
        rows.append(dict(seed=seed, 이웃=n, baseline=round(alone.mean(), 4),
                         subnet=round(withv.mean(), 4), synergy=round(m, 4),
                         SE=round(se, 4), 분류=classify(m, se),
                         n_공동=co["n_both"], 성공률_공동=co["succ_both"]))
    df = pd.DataFrame(rows)
    return df.sort_values("synergy", ascending=False).reset_index(drop=True) if len(df) else df


# %% ──────────────────────────────────────────── [Cell 3] E1 베이스라인 vs 서브넷값 분해
# 가설: synergy = 서브넷값 − 베이스라인. 대각선 위=시너지(여기서 더 도움), 아래=잠식.
df_e1 = subnet_synergy_table(eng, "마라", n_headroom=20)
print("[E1] 마라 서브넷 — 상위/하위 5")
print(df_e1.head(5).to_string(index=False))
print("...")
print(df_e1.tail(5).to_string(index=False))

fig, ax = plt.subplots(figsize=(7, 7))
colors = {"시너지": "#16A34A", "잠식": "#EF4444", "보통": "#9CA3AF"}
for cls, g in df_e1.groupby("분류"):
    ax.scatter(g["baseline"], g["subnet"], c=colors[cls], label=cls, s=60, alpha=0.8, edgecolors="white")
lim = [min(df_e1.baseline.min(), df_e1.subnet.min()), max(df_e1.baseline.max(), df_e1.subnet.max())]
ax.plot(lim, lim, "k--", lw=1, label="대각선(synergy=0)")
for _, r in df_e1.iterrows():
    ax.annotate(r["이웃"], (r["baseline"], r["subnet"]), fontsize=7, alpha=0.7)
ax.set_xlabel("baseline = 단독효과 margin(k|단독)  [혼자일 때]")
ax.set_ylabel("subnet = margin(k|seed)  [마라 안에서 효과]")
ax.set_title("[E1] 베이스라인 대비 서브넷값 — 대각선 위=시너지 (마라)")
ax.legend(); plt.tight_layout(); plt.show()


# %% ──────────────────────────────────────────── [Cell 4] E2 노이즈 바닥 SE의 층화
# 가설: SE는 키워드 지지도(보유 제품수)가 낮을수록 크다 → 전역 τ 불가, per-pair 정당.
all_pairs = pd.concat([subnet_synergy_table(eng, s, n_headroom=20) for s in SEEDS], ignore_index=True)
ei = eng.cache["eidx"][PK_MAIN].numpy()
deg = np.bincount(ei[1], minlength=eng.cache["K"])   # 키워드별 보유 제품수
all_pairs["지지도"] = all_pairs["이웃"].map(lambda n: deg[eng.seed_to_idx(n)] if eng.seed_to_idx(n) is not None else 0)
all_pairs["지지도_구간"] = pd.cut(all_pairs["지지도"], [0, 5, 20, 100, 99999],
                              labels=["희소(≤5)", "낮음(6-20)", "중간(21-100)", "높음(>100)"])
se_by = all_pairs.groupby("지지도_구간", observed=True)["SE"].agg(["median", "count"])
print("[E2] 지지도 구간별 SE 중앙값 (노이즈 바닥)")
print(se_by.to_string())

fig, ax = plt.subplots(figsize=(8, 4))
all_pairs.boxplot(column="SE", by="지지도_구간", ax=ax, grid=False)
ax.axhline(all_pairs["SE"].median(), color="r", ls="--", label=f"전체 SE 중앙값={all_pairs['SE'].median():.4f}")
ax.set_xlabel("키워드 지지도 구간"); ax.set_ylabel("per-pair SE (노이즈 바닥)")
ax.set_title("[E2] 지지도가 낮을수록 SE↑ → 전역 τ 부적절, per-pair 필요")
plt.suptitle(""); ax.legend(); plt.tight_layout(); plt.show()


# %% ──────────────────────────────────────────── [Cell 5] E3 분류 비율
# per-pair 2·SE 적용 시 전체/시드별 시너지·잠식·보통 비율.
print("[E3] 전체 분류 비율")
print((all_pairs["분류"].value_counts(normalize=True) * 100).round(1).astype(str) + "%")
ct = all_pairs.groupby(["seed", "분류"], observed=True).size().unstack(fill_value=0)
ct_pct = ct.div(ct.sum(axis=1), axis=0) * 100
fig, ax = plt.subplots(figsize=(9, 4))
ct_pct[["시너지", "보통", "잠식"]].plot(kind="bar", stacked=True, ax=ax,
                                       color=["#16A34A", "#9CA3AF", "#EF4444"])
ax.set_ylabel("비율 (%)"); ax.set_xlabel("시드")
ax.set_title("[E3] 시드별 시너지/보통/잠식 분류 비율 (per-pair 2·SE)")
ax.legend(title="분류"); plt.tight_layout(); plt.show()


# %% ──────────────────────────────────────────── [Cell 6] E4 ★ 포화 시드 핸들링
# 가설: 풍부 시드(마라)=뚜렷한 시너지 다수 / 포화 시드(로제·단백질)=대부분 보통.
#   오차막대(±2·SE)가 0선을 넘는지로 시각 판정 → per-pair가 포화를 정직하게 '보통'처리.
fig, axes = plt.subplots(1, 3, figsize=(16, 5), sharex=False)
for ax, sd in zip(axes, ["마라", "로제", "단백질"]):
    d = subnet_synergy_table(eng, sd, n_headroom=24).head(12).iloc[::-1]
    cols = [colors[c] for c in d["분류"]]
    ax.barh(d["이웃"], d["synergy"], xerr=KFACTOR * d["SE"], color=cols,
            error_kw=dict(ecolor="#333", lw=1, capsize=2))
    ax.axvline(0, color="k", lw=0.8)
    n_syn = (d["분류"] == "시너지").sum()
    ax.set_title(f"[{sd}] 시너지 {n_syn}/{len(d)}개")
    ax.set_xlabel("synergy ± 2·SE")
fig.suptitle("[E4] 포화 시드(로제·단백질)는 오차막대가 0을 덮음=보통 / 마라는 0 초과=시너지", y=1.02)
plt.tight_layout(); plt.show()


# %% ──────────────────────────────────────────── [Cell 7] E5 전역 τ vs per-pair flip
# 전역 τ(0.005)와 per-pair(2·SE) 분류가 갈리는 쌍 = 전역 τ가 오판하는 고-SE 지점.
def classify_global(s):
    return "시너지" if s > GLOBAL_TAU else ("잠식" if s < -GLOBAL_TAU else "보통")


all_pairs["분류_전역τ"] = all_pairs["synergy"].map(classify_global)
flips = all_pairs[all_pairs["분류"] != all_pairs["분류_전역τ"]]
print(f"[E5] 전역τ vs per-pair 불일치: {len(flips)}/{len(all_pairs)}쌍 ({len(flips)/len(all_pairs):.0%})")
print(flips.sort_values("SE", ascending=False)
      [["seed", "이웃", "synergy", "SE", "분류_전역τ", "분류"]].head(12).to_string(index=False))
print("→ 대개 SE 큰(노이즈 큰) 쌍: 전역τ는 '시너지/잠식'이라 했지만 per-pair는 '보통'으로 정정")


# %% ──────────────────────────────────────────── [Cell 8] E6 재현성
# headroom 표본(rng_seed)을 바꿔 분류가 유지되는지 — 표본 운빨이 아님을 확인.
base_cls = subnet_synergy_table(eng, "마라", n_headroom=20, rng_seed=0).set_index("이웃")["분류"]
agree = []
for rs in [1, 2, 3]:
    alt = subnet_synergy_table(eng, "마라", n_headroom=20, rng_seed=rs).set_index("이웃")["분류"]
    common = base_cls.index.intersection(alt.index)
    agree.append((base_cls[common] == alt[common]).mean())
print(f"[E6] 마라 분류 재현성 (rng 0 vs 1/2/3): 일치율 {[round(a,2) for a in agree]}  평균 {np.mean(agree):.2f}")
print("→ 1에 가까울수록 분류가 표본에 안 흔들림")


# %% ──────────────────────────────────────────── [Cell 9] E7 ★ 실제 데이터 앵커
# 가설: '시너지' 쌍은 두 키워드를 실제 같이 가진 제품의 raw 성공률이 base_rate보다 높다.
#   (모델 prob 아닌 npd_success_labels 기반 → 부분 순환 완화)
anchor = all_pairs[all_pairs["n_공동"] >= 3].copy()   # 공동보유 제품 3개 이상만
base_rate = eng.cache["base_rate"]
g = anchor.groupby("분류", observed=True)["성공률_공동"].agg(["mean", "median", "count"])
print(f"[E7] 분류별 '공동보유 제품 raw 성공률' (전체 base={base_rate:.3f})")
print(g.to_string())
print("→ 시너지 > base > 잠식 이면 synergy가 현실(실제 성공)을 반영")

fig, ax = plt.subplots(figsize=(7, 4))
order = ["시너지", "보통", "잠식"]
vals = [anchor[anchor["분류"] == c]["성공률_공동"].mean() for c in order]
ax.bar(order, vals, color=[colors[c] for c in order])
ax.axhline(base_rate, color="k", ls="--", label=f"전체 base={base_rate:.3f}")
ax.set_ylabel("공동보유 제품 raw 성공률"); ax.set_xlabel("synergy 분류")
ax.set_title("[E7] 시너지 쌍일수록 실제 공동제품 성공률↑ (현실 타당성)")
ax.legend(); plt.tight_layout(); plt.show()


# %% ──────────────────────────────────────────── [Cell 10] E8 ★ 단독강도 분해 + 기록용 표본 저장
# 발견: synergy는 '약한 걸 살리는 회복분'이라 단독강도와 음의 상관 → realized success와 역전.
#   docs/findings/2026-06-22_synergy-headroom-단독강도역전.md 의 근거 표본을 생성·저장.
ei = eng.cache["eidx"][PK_MAIN].numpy()
y = eng.cache["y"]


def solo_success(k_idx):
    """키워드 단독 raw 성공률 = 그 키워드 보유 제품의 성공 비율 (모델 prob 아님)."""
    ps = ei[0][ei[1] == k_idx]
    return float(y[ps].mean()) if len(ps) else np.nan


rec = all_pairs.copy()   # E2에서 만든 전 시드 354쌍
rec["이웃_단독성공률"] = rec["이웃"].map(lambda n: solo_success(eng.seed_to_idx(n)))
rec["시드_단독성공률"] = rec["seed"].map(lambda n: solo_success(eng.seed_to_idx(n)))

decomp = rec.groupby("분류", observed=True).agg(
    baseline=("baseline", "mean"), 이웃단독성공률=("이웃_단독성공률", "mean"),
    공동성공률=("성공률_공동", "mean"), n=("synergy", "size")).round(3)
print("[E8] 분류별 단독강도 분해 (base=%.3f)" % eng.cache["base_rate"])
print(decomp.to_string())
print(f"\ncorr(synergy, baseline)        = {rec['synergy'].corr(rec['baseline']):+.3f}")
print(f"corr(baseline, 이웃_단독성공률) = {rec['baseline'].corr(rec['이웃_단독성공률']):+.3f}")
print(f"corr(synergy, 이웃_단독성공률)  = {rec['synergy'].corr(rec['이웃_단독성공률']):+.3f}")
print("→ synergy↑ ⟺ 단독약함 ⟺ realized success↓ : synergy는 'salvage' 신호, '흥행' 신호 아님")

# 산점도: synergy vs 이웃 단독성공률 (음의 관계 시각화)
fig, ax = plt.subplots(figsize=(7, 5))
for cls, gg in rec.groupby("분류", observed=True):
    ax.scatter(gg["이웃_단독성공률"], gg["synergy"], c=colors[cls], label=cls, s=40, alpha=0.7, edgecolors="white")
ax.axhline(0, color="k", lw=0.6); ax.axvline(base_rate, color="k", ls=":", lw=0.8, label=f"base={base_rate:.3f}")
ax.set_xlabel("이웃 키워드 단독 raw 성공률"); ax.set_ylabel("synergy")
ax.set_title("[E8] synergy ↔ 단독강도 음의 상관 (시너지=단독 약한 쪽)")
ax.legend(); plt.tight_layout(); plt.show()

# 기록용 표본 저장 (재현·추적용)
OUT = os.path.join(ROOT, "eda", "outputs"); os.makedirs(OUT, exist_ok=True)
cols = ["seed", "이웃", "baseline", "subnet", "synergy", "SE", "분류",
        "이웃_단독성공률", "시드_단독성공률", "n_공동", "성공률_공동", "지지도"]
rec[cols].to_parquet(os.path.join(OUT, "synergy_threshold_sample.parquet"), index=False)
decomp.to_csv(os.path.join(OUT, "synergy_threshold_decomp.csv"), encoding="utf-8-sig")
print("\n기록용 표본 저장 →", os.path.join(OUT, "synergy_threshold_sample.parquet"), f"({len(rec)}쌍)")
