import os
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats

sys.stdout.reconfigure(encoding='utf-8')

# Paths
artifact_dir = "C:/Users/송정현/.gemini/antigravity-cli/brain/04ea14b0-2f48-41ee-b4fd-8dc5781526d9"
docs_dir = "C:/Users/송정현/Documents/Projects/박재홍교수님세미나/Projects/20기/7eleven_npd_framework/docs"

# Load data
df_ie = pd.read_csv("data/processed/instagram_engagement_with_keywords_final.csv")
if '제외' in df_ie.columns:
    df_ie = df_ie[df_ie['제외'] != 1]

# Group and sum likes
df_likes = df_ie.groupby(['편의점명', '정규화명'])['좋아요 수'].sum().reset_index()

# Brand configurations: 세븐일레븐(Blue), CU(Orange), GS25(Green)
stores_info = [
    {
        "name": "세븐일레븐",
        "color": "#2980B9",       # Blue (as requested)
        "z_score": 0.95,
        "pct": "상위 16.4%"
    },
    {
        "name": "CU",
        "color": "#E67E22",       # Orange (as requested)
        "z_score": 1.12,
        "pct": "상위 15.2%"
    },
    {
        "name": "GS25",
        "color": "#27AE60",       # Green (as requested)
        "z_score": 1.39,
        "pct": "상위 8.1%"
    }
]

# Set clean style
plt.style.use('seaborn-v0_8-whitegrid')
plt.rc('font', family='Malgun Gothic', size=11)
plt.rc('axes', unicode_minus=False)

# Initialize figure (1 row, 3 columns) with shared Y axis
fig, axes = plt.subplots(1, 3, figsize=(18, 5.5), sharey=True)

threshold_val = 3000
log_threshold = np.log(threshold_val)  # ~8.0064

for i, store_data in enumerate(stores_info):
    ax = axes[i]
    store = store_data["name"]
    color = store_data["color"]
    z_score = store_data["z_score"]
    pct = store_data["pct"]
    
    df_store = df_likes[df_likes['편의점명'] == store]
    likes = df_store['좋아요 수'].values
    valid_likes = likes[likes > 0]
    log_likes = np.log(valid_likes)
    
    mu = np.mean(log_likes)
    sigma = np.std(log_likes)
    
    # 1. Plot histogram (very clean, light gray)
    sns.histplot(log_likes, stat="density", color="#D5D8DC", alpha=0.6, bins=20, edgecolor="white", linewidth=0.5, ax=ax)
    
    # 2. Fit normal distribution curve
    x_range = np.linspace(log_likes.min() - 0.5, log_likes.max() + 0.5, 200)
    pdf_normal = stats.norm.pdf(x_range, mu, sigma)
    ax.plot(x_range, pdf_normal, color=color, linewidth=3)
    
    # 3. Fill success group area (outliers >= ln(3000))
    x_outliers = np.linspace(log_threshold, log_likes.max() + 0.5, 150)
    pdf_outliers = stats.norm.pdf(x_outliers, mu, sigma)
    ax.fill_between(x_outliers, pdf_outliers, color=color, alpha=0.25)
    
    # 4. Vertical cutoff line at ln(3000)
    ax.axvline(x=log_threshold, color="#E74C3C", linestyle="-", linewidth=2.0)
    
    # 5. Add minimal text annotations
    # (Removed Brand Name label at top-left as requested)
    
    # Threshold cut label near the line
    ax.text(log_threshold + 0.15, ax.get_ylim()[1] * 0.75, f"좋아요 3,000개\n{pct}\n(Z = +{z_score:.2f}σ)", 
            color="#C0392B", fontsize=10, fontweight="bold")
    
    # Set labels
    ax.set_xlabel("ln(좋아요 수)", fontsize=11, labelpad=8)
    if i == 0:
        ax.set_ylabel("확률 밀도 (Density)", fontsize=11, labelpad=8)
    else:
        ax.set_ylabel("")
        
    ax.set_xlim(log_likes.min() - 0.3, log_likes.max() + 0.3)
    ax.grid(True, linestyle="--", alpha=0.5)

plt.tight_layout()

# Save paths
filename = "combined_likes_lognormal.png"
artifact_path = f"{artifact_dir}/{filename}"
docs_path = f"{docs_dir}/{filename}"

plt.savefig(artifact_path, dpi=300, bbox_inches='tight')
plt.savefig(docs_path, dpi=300, bbox_inches='tight')
plt.close()

print("Combined plot generated successfully with requested colors and without brand names!")
print(f"  - Artifact: {artifact_path}")
print(f"  - Docs: {docs_path}")
