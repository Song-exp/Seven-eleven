"""
seasonal_peaks_viz.py
─────────────────────
seasonal_peaks_output.csv 를 기반으로 시각화 4종 생성:
  1. Lift-Specificity 산점도 (전체, 시기별 색상)
  2. 시기별 Lift 분포 (박스플롯)
  3. 시기별 Specificity 분포 (박스플롯)
  4. 시기별 Top 상품 목록 텍스트 패널
"""

import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec
from pathlib import Path

# ── 한글 폰트 설정 ────────────────────────────────────────────────────────
matplotlib.rcParams['font.family'] = 'Apple SD Gothic Neo'
matplotlib.rcParams['axes.unicode_minus'] = False

# ── 데이터 로드 ───────────────────────────────────────────────────────────
BASE = Path(__file__).parent
df = pd.read_csv(BASE / "seasonal_peaks_output.csv")

# lift = inf 인 행은 시각화 목적으로 cap (진짜 inf는 그래프에서 표현 불가)
LIFT_CAP = 250
df["lift_plot"] = df["lift"].replace(np.inf, LIFT_CAP)
df["is_inf"]    = df["lift"] == np.inf

# 시기 순서 (연도 흐름)
PERIOD_ORDER = [
    "SEOLLAL", "VALENTINE", "WHITE_DAY",
    "CHILDRENS_DAY", "PARENTS_DAY",
    "CHUSEOK", "PEPERO_DAY", "CSAT", "CHRISTMAS",
]
PERIOD_KO = {
    "SEOLLAL":       "설날",
    "VALENTINE":     "발렌타인데이",
    "WHITE_DAY":     "화이트데이",
    "CHILDRENS_DAY": "어린이날",
    "PARENTS_DAY":   "어버이날",
    "CHUSEOK":       "추석",
    "PEPERO_DAY":    "빼빼로데이",
    "CSAT":          "수능",
    "CHRISTMAS":     "크리스마스",
}

df["period_ko"]  = df["period_key"].map(PERIOD_KO)
df["period_ord"] = pd.Categorical(df["period_key"], categories=PERIOD_ORDER, ordered=True)
df = df.sort_values("period_ord")

PALETTE = plt.cm.tab10.colors
period_color = {p: PALETTE[i] for i, p in enumerate(PERIOD_ORDER)}
df["color"] = df["period_key"].map(period_color)

# ─────────────────────────────────────────────────────────────────────────────
# Figure 1: Lift-Specificity 산점도 + 시기별 박스플롯
# ─────────────────────────────────────────────────────────────────────────────
fig1, axes = plt.subplots(1, 3, figsize=(20, 7))
fig1.patch.set_facecolor("#F8F9FA")
for ax in axes:
    ax.set_facecolor("#F8F9FA")

# ── (A) Lift × Specificity 산점도 ───────────────────────────────────────────
ax = axes[0]
for pk in PERIOD_ORDER:
    sub = df[df["period_key"] == pk]
    ax.scatter(
        sub["specificity"], sub["lift_plot"],
        c=[period_color[pk]], label=PERIOD_KO[pk],
        s=80, alpha=0.85, edgecolors="white", linewidths=0.5, zorder=3
    )
# lift=inf 표시선
ax.axhline(LIFT_CAP, color="gray", ls="--", lw=1, alpha=0.6, zorder=2)
ax.text(0.01, LIFT_CAP + 3, f"lift = ∞ (상한 {LIFT_CAP}으로 표시)", fontsize=8, color="gray")

# 임계선
ax.axhline(1.5, color="#E74C3C", ls=":", lw=1.5, alpha=0.8, zorder=2)
ax.text(0.75, 1.55, "lift = 1.5 (최소 기준)", fontsize=8, color="#E74C3C")

ax.set_xlabel("Specificity (시기 종속성)", fontsize=11)
ax.set_ylabel("Lift (시기 효과)", fontsize=11)
ax.set_title("(A)  Lift × Specificity 분포", fontsize=13, fontweight="bold")
ax.legend(loc="upper left", fontsize=8, framealpha=0.8)
ax.set_xlim(-0.02, 1.05)
ax.set_ylim(-5, LIFT_CAP + 20)
ax.grid(axis="both", color="white", lw=1.2, zorder=1)

# ── (B) 시기별 Lift 박스플롯 ────────────────────────────────────────────────
ax = axes[1]
plot_data_lift = [
    df.loc[df["period_key"] == pk, "lift_plot"].values
    for pk in PERIOD_ORDER
]
bp = ax.boxplot(
    plot_data_lift,
    vert=True, patch_artist=True,
    medianprops=dict(color="black", lw=2),
    whiskerprops=dict(lw=1.2),
    capprops=dict(lw=1.2),
    flierprops=dict(marker="o", markersize=4, alpha=0.5),
)
for patch, pk in zip(bp["boxes"], PERIOD_ORDER):
    patch.set_facecolor(period_color[pk])
    patch.set_alpha(0.8)
for flier, pk in zip(bp["fliers"], PERIOD_ORDER):
    flier.set_markerfacecolor(period_color[pk])

ax.axhline(1.5, color="#E74C3C", ls=":", lw=1.5, alpha=0.8)
ax.axhline(LIFT_CAP, color="gray", ls="--", lw=1, alpha=0.5)
ax.set_xticks(range(1, len(PERIOD_ORDER) + 1))
ax.set_xticklabels([PERIOD_KO[p] for p in PERIOD_ORDER], rotation=40, ha="right", fontsize=9)
ax.set_ylabel("Lift (∞ → 250 cap)", fontsize=11)
ax.set_title("(B)  시기별 Lift 분포", fontsize=13, fontweight="bold")
ax.grid(axis="y", color="white", lw=1.2, zorder=1)

# ── (C) 시기별 Specificity 박스플롯 ─────────────────────────────────────────
ax = axes[2]
plot_data_spec = [
    df.loc[df["period_key"] == pk, "specificity"].values
    for pk in PERIOD_ORDER
]
bp2 = ax.boxplot(
    plot_data_spec,
    vert=True, patch_artist=True,
    medianprops=dict(color="black", lw=2),
    whiskerprops=dict(lw=1.2),
    capprops=dict(lw=1.2),
    flierprops=dict(marker="o", markersize=4, alpha=0.5),
)
for patch, pk in zip(bp2["boxes"], PERIOD_ORDER):
    patch.set_facecolor(period_color[pk])
    patch.set_alpha(0.8)
for flier, pk in zip(bp2["fliers"], PERIOD_ORDER):
    flier.set_markerfacecolor(period_color[pk])

ax.set_xticks(range(1, len(PERIOD_ORDER) + 1))
ax.set_xticklabels([PERIOD_KO[p] for p in PERIOD_ORDER], rotation=40, ha="right", fontsize=9)
ax.set_ylabel("Specificity (연간 판매 중 시기 비중)", fontsize=11)
ax.set_title("(C)  시기별 Specificity 분포", fontsize=13, fontweight="bold")
ax.grid(axis="y", color="white", lw=1.2, zorder=1)

fig1.suptitle("시기별 판매 집중 상품 분석  —  Lift & Specificity", fontsize=15, fontweight="bold", y=1.01)
fig1.tight_layout()
out1 = BASE / "viz_lift_specificity.png"
fig1.savefig(out1, dpi=150, bbox_inches="tight", facecolor=fig1.get_facecolor())
print(f"저장: {out1}")

# ─────────────────────────────────────────────────────────────────────────────
# Figure 2: 시기별 Top 상품 목록 (텍스트 패널 3×3)
# ─────────────────────────────────────────────────────────────────────────────
fig2, axes2 = plt.subplots(3, 3, figsize=(22, 18))
fig2.patch.set_facecolor("#F8F9FA")

for idx, pk in enumerate(PERIOD_ORDER):
    ax = axes2[idx // 3][idx % 3]
    ax.set_facecolor("#FFFFFF")

    sub = df[df["period_key"] == pk].copy()
    # 정렬: lift 유한 → 내림차순, inf는 specificity 내림차순으로 후순위
    finite_rows = sub[sub["lift"] != np.inf].sort_values("lift", ascending=False)
    inf_rows    = sub[sub["lift"] == np.inf].sort_values("specificity", ascending=False)
    sub = pd.concat([finite_rows, inf_rows]).head(20)

    # 헤더
    color = period_color[pk]
    ax.set_title(f"{PERIOD_KO[pk]}  ({pk})\n{sub['period_start'].iloc[0]}  ~  {sub['period_end'].iloc[0]}",
                 fontsize=12, fontweight="bold", color="white",
                 bbox=dict(boxstyle="round,pad=0.4", facecolor=color, edgecolor="none"))

    ax.axis("off")

    # 컬럼 헤더
    col_headers = ["#", "상품명", "Lift", "Spec."]
    col_x       = [0.02, 0.10, 0.75, 0.89]
    ax.text(col_x[0], 0.96, col_headers[0], transform=ax.transAxes,
            fontsize=8.5, fontweight="bold", color="#555")
    ax.text(col_x[1], 0.96, col_headers[1], transform=ax.transAxes,
            fontsize=8.5, fontweight="bold", color="#555")
    ax.text(col_x[2], 0.96, col_headers[2], transform=ax.transAxes,
            fontsize=8.5, fontweight="bold", color="#555", ha="right")
    ax.text(col_x[3], 0.96, col_headers[3], transform=ax.transAxes,
            fontsize=8.5, fontweight="bold", color="#555", ha="right")

    # 구분선
    ax.plot([0, 1], [0.94, 0.94], color=color, lw=1.5, transform=ax.transAxes, clip_on=False)

    for i, (_, row) in enumerate(sub.iterrows()):
        y = 0.90 - i * 0.044
        bg = "#F0F4FF" if i % 2 == 0 else "#FFFFFF"
        ax.add_patch(mpatches.FancyBboxPatch(
            (0, y - 0.018), 1, 0.038,
            boxstyle="square,pad=0", transform=ax.transAxes,
            facecolor=bg, edgecolor="none", zorder=0
        ))

        rank_color = {0: "#C0392B", 1: "#E67E22", 2: "#F1C40F"}.get(i, "#555")
        ax.text(col_x[0], y, f"{i+1}", transform=ax.transAxes,
                fontsize=8, fontweight="bold", color=rank_color, va="center")

        # 상품명 (20자 이상이면 자름)
        name = row["상품명"]
        if len(name) > 18:
            name = name[:17] + "…"
        ax.text(col_x[1], y, name, transform=ax.transAxes,
                fontsize=8, color="#222", va="center")

        lift_str = "∞" if row["lift"] == np.inf else f"{row['lift']:.1f}"
        ax.text(col_x[2], y, lift_str, transform=ax.transAxes,
                fontsize=8, color="#1A5276" if row["lift"] != np.inf else "#884EA0",
                va="center", ha="right", fontweight="bold")

        ax.text(col_x[3], y, f"{row['specificity']:.3f}", transform=ax.transAxes,
                fontsize=8, color="#1E8449", va="center", ha="right")

fig2.suptitle("시기별 판매 집중 상품 Top 20  (Lift 내림차순, ∞는 후순위)",
              fontsize=16, fontweight="bold", y=1.005)
fig2.tight_layout()
fig2.subplots_adjust(hspace=0.55, wspace=0.25)
out2 = BASE / "viz_period_top_products.png"
fig2.savefig(out2, dpi=150, bbox_inches="tight", facecolor=fig2.get_facecolor())
print(f"저장: {out2}")

plt.close("all")
print("완료")
