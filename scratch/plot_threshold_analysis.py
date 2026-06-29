import os
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats

sys.stdout.reconfigure(encoding='utf-8')

# Ensure directories exist
os.makedirs("C:/Users/송정현/.gemini/antigravity-cli/brain/04ea14b0-2f48-41ee-b4fd-8dc5781526d9", exist_ok=True)

# 1. Read Instagram engagement data
df_ie = pd.read_csv("data/processed/instagram_engagement_with_keywords_final.csv")
if '제외' in df_ie.columns:
    df_ie = df_ie[df_ie['제외'] != 1]

# Sum likes by store and normalized product name
df_likes = df_ie.groupby(['편의점명', '정규화명'])['좋아요 수'].sum().reset_index()

# Define stores and brand colors
stores = ["세븐일레븐", "CU", "GS25"]
colors = {
    "세븐일레븐": "#27AE60",  # Forest Green
    "CU": "#8E44AD",        # CU Purple
    "GS25": "#2980B9"       # GS Blue
}

# Set matplotlib style and font
plt.style.use('seaborn-v0_8-whitegrid')
plt.rc('font', family='Malgun Gothic', size=11)
plt.rc('axes', unicode_minus=False)

fig, axes = plt.subplots(1, 2, figsize=(15, 6.5))

# ----------------------------------------------------
# Subplot 1: Empirical Cumulative Distribution (ECDF)
# ----------------------------------------------------
ax1 = axes[0]
threshold_val = 3000

for store in stores:
    df_store = df_likes[df_likes['편의점명'] == store]
    likes = df_store['좋아요 수'].values
    sorted_likes = np.sort(likes)
    y_vals = np.arange(1, len(sorted_likes) + 1) / len(sorted_likes) * 100  # Convert to percentile
    
    # Plot ECDF curve
    ax1.plot(sorted_likes, y_vals, label=f"{store} (N={len(likes)})", color=colors[store], linewidth=2.5)
    
    # Calculate percentile at 3,000
    pct = stats.percentileofscore(likes, threshold_val)
    print(f"[{store}] Percentile of 3,000: {pct:.2f}% (상위 {100-pct:.2f}%)")
    
    # Draw horizontal guide line to the intersection
    ax1.axhline(y=pct, color=colors[store], linestyle=':', alpha=0.7)
    
    # Annotate intersection point
    ax1.scatter([threshold_val], [pct], color=colors[store], s=70, zorder=5)
    ax1.text(threshold_val * 1.15, pct - 2, f"상위 {100-pct:.1f}% ({pct:.1f}%ile)", 
             color=colors[store], fontsize=9.5, fontweight='bold')

# Vertical line at 3,000
ax1.axvline(x=threshold_val, color="#E74C3C", linestyle="--", linewidth=2.0, label="임계치 (3,000개)")

ax1.set_xscale('log')
ax1.set_title("인스타그램 좋아요 합산 누적백분위수 (ECDF)", fontsize=13, fontweight='bold', pad=15)
ax1.set_xlabel("좋아요 합산 수 (Log Scale)", fontsize=11, labelpad=8)
ax1.set_ylabel("누적 확률 백분위수 (%)", fontsize=11, labelpad=8)
ax1.set_ylim(0, 105)
ax1.set_xlim(10, 100000)
ax1.legend(loc="lower right", frameon=True, facecolor="white", edgecolor="#ddd")

# ----------------------------------------------------
# Subplot 2: Elbow Curve (Success Ratio Decay)
# ----------------------------------------------------
ax2 = axes[1]
grid_thresholds = [500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000]

for store in stores:
    df_store = df_likes[df_likes['편의점명'] == store]
    total_n = len(df_store)
    ratios = []
    
    for t in grid_thresholds:
        cnt = (df_store['좋아요 수'] >= t).sum()
        ratios.append(cnt / total_n * 100)
        
    ax2.plot(grid_thresholds, ratios, marker='o', label=store, color=colors[store], linewidth=2.5, markersize=6)
    
    # Annotate values at 3000
    idx_3000 = grid_thresholds.index(3000)
    pct_3000 = ratios[idx_3000]
    ax2.scatter([3000], [pct_3000], color=colors[store], s=80, zorder=5)
    ax2.text(3100, pct_3000 + 2, f"{pct_3000:.1f}%", color=colors[store], fontsize=10, fontweight='bold')

# Vertical line at 3,000
ax2.axvline(x=threshold_val, color="#E74C3C", linestyle="--", linewidth=2.0, label="임계치 (3,000개)")

# Highlight the L-curve bend/Elbow zone
ax2.axvspan(2000, 3500, color='#F39C12', alpha=0.1, label='변곡 영역 (Elbow Zone)')

ax2.set_title("임계값 증가에 따른 성공 제품 비율 감소 곡선 (Elbow Curve)", fontsize=13, fontweight='bold', pad=15)
ax2.set_xlabel("좋아요 합산 성공 임계치", fontsize=11, labelpad=8)
ax2.set_ylabel("성공 제품 비율 (%)", fontsize=11, labelpad=8)
ax2.set_xticks(grid_thresholds)
ax2.set_xticklabels([f"{x:,}" for x in grid_thresholds], rotation=45)
ax2.set_ylim(-2, 85)
ax2.legend(loc="upper right", frameon=True, facecolor="white", edgecolor="#ddd")

plt.tight_layout()

# Save plot to artifacts directory
save_path = "C:/Users/송정현/.gemini/antigravity-cli/brain/04ea14b0-2f48-41ee-b4fd-8dc5781526d9/threshold_analysis.png"
plt.savefig(save_path, dpi=300, bbox_inches='tight')
print(f"\nPlot successfully saved to: {save_path}")
