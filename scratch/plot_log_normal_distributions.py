import os
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats

sys.stdout.reconfigure(encoding='utf-8')

# Ensure directories exist
artifact_dir = "C:/Users/송정현/.gemini/antigravity-cli/brain/04ea14b0-2f48-41ee-b4fd-8dc5781526d9"
docs_dir = "C:/Users/송정현/Documents/Projects/박재홍교수님세미나/Projects/20기/7eleven_npd_framework/docs"
os.makedirs(artifact_dir, exist_ok=True)
os.makedirs(docs_dir, exist_ok=True)

# 1. Read Instagram engagement data
df_ie = pd.read_csv("data/processed/instagram_engagement_with_keywords_final.csv")
if '제외' in df_ie.columns:
    df_ie = df_ie[df_ie['제외'] != 1]

# Sum likes by store and normalized product name
df_likes = df_ie.groupby(['편의점명', '정규화명'])['좋아요 수'].sum().reset_index()

# Define stores, filenames, and brand colors
stores_info = [
    {
        "name": "세븐일레븐",
        "file_prefix": "seven",
        "color": "#27AE60",       # Forest Green
        "bg_color": "#E8F8F5"
    },
    {
        "name": "CU",
        "file_prefix": "cu",
        "color": "#8E44AD",       # Purple
        "bg_color": "#F4ECF7"
    },
    {
        "name": "GS25",
        "file_prefix": "gs25",
        "color": "#2980B9",       # Blue
        "bg_color": "#EAF2F8"
    }
]

# Set matplotlib style
plt.style.use('seaborn-v0_8-whitegrid')
plt.rc('font', family='Malgun Gothic', size=12)
plt.rc('axes', unicode_minus=False)

threshold_val = 3000
log_threshold = np.log(threshold_val)  # ~8.0064

for store_data in stores_info:
    store = store_data["name"]
    prefix = store_data["file_prefix"]
    color = store_data["color"]
    bg_color = store_data["bg_color"]
    
    df_store = df_likes[df_likes['편의점명'] == store]
    likes = df_store['좋아요 수'].values
    valid_likes = likes[likes > 0]
    log_likes = np.log(valid_likes)
    
    mu = np.mean(log_likes)
    sigma = np.std(log_likes)
    z_score = (log_threshold - mu) / sigma
    percentile = stats.percentileofscore(likes, threshold_val)
    outlier_pct = 100 - percentile
    
    # Start plot
    fig, ax = plt.subplots(figsize=(10, 6.5))
    
    # Plot histogram and KDE of actual log likes
    sns.histplot(log_likes, kde=False, stat="density", color="#BDC3C7", alpha=0.5, bins=25, label="실제 분포 (로그 스케일)", ax=ax)
    sns.kdeplot(log_likes, color="#7F8C8D", linewidth=1.5, linestyle="--", label="KDE (실제 밀도)", ax=ax)
    
    # Fit normal distribution curve
    x_range = np.linspace(log_likes.min() - 1, log_likes.max() + 1, 300)
    pdf_normal = stats.norm.pdf(x_range, mu, sigma)
    ax.plot(x_range, pdf_normal, color=color, linewidth=3, label=f"Log-Normal 적합 곡선")
    
    # Fill outlier region (likes >= 3000, i.e., log_likes >= log_threshold)
    x_outliers = np.linspace(log_threshold, log_likes.max() + 1, 200)
    pdf_outliers = stats.norm.pdf(x_outliers, mu, sigma)
    ax.fill_between(x_outliers, pdf_outliers, color=color, alpha=0.3, label="성공군 영역 (좋아요 3,000개 이상)")
    
    # Draw vertical cutoff line
    ax.axvline(x=log_threshold, color="#E74C3C", linestyle="-", linewidth=2.5)
    
    # Add annotations
    # Cut-off text annotation
    ax.text(log_threshold + 0.1, ax.get_ylim()[1] * 0.85, 
            f"임계값 3,000개\nln(3,000) = {log_threshold:.4f}\n(Z = {z_score:.2f} $\sigma$)", 
            color="#C0392B", fontsize=11, fontweight="bold", 
            bbox=dict(boxstyle="round,pad=0.4", facecolor="white", edgecolor="#E74C3C", alpha=0.9))
    
    # Statistics box
    stats_text = (
        f"편의점: {store}\n"
        f"모수 (N) = {len(likes)}개\n"
        f"로그 평균 (μ) = {mu:.4f}\n"
        f"로그 표준편차 (σ) = {sigma:.4f}\n"
        f"-----------------------\n"
        f"성공 제품 수: {len(df_store[df_store['좋아요 수'] >= threshold_val])}개\n"
        f"성공 비율 (상위): {outlier_pct:.2f}%\n"
        f"실패 비율 (하위): {percentile:.2f}%"
    )
    ax.text(0.05, 0.92, stats_text, transform=ax.transAxes, fontsize=11,
            verticalalignment='top', bbox=dict(boxstyle="round,pad=0.5", facecolor=bg_color, edgecolor=color, alpha=0.8))
    
    # Add a marker for the mean
    ax.axvline(x=mu, color="#2C3E50", linestyle=":", linewidth=1.5)
    ax.text(mu - 0.5, ax.get_ylim()[1] * 0.02, f"평균 μ={mu:.2f}", color="#2C3E50", fontsize=10, fontweight="bold")
    
    # Title and Labels
    ax.set_title(f"[{store}] 인스타그램 좋아요 로그분포 및 아웃라이어 임계치", fontsize=15, fontweight='bold', pad=18)
    ax.set_xlabel("좋아요 수의 자연로그 값 (ln(좋아요 수))", fontsize=12, labelpad=10)
    ax.set_ylabel("확률 밀도 (Density)", fontsize=12, labelpad=10)
    ax.set_xlim(log_likes.min() - 0.5, log_likes.max() + 0.5)
    ax.legend(loc="upper right", frameon=True, facecolor="white", edgecolor="#ddd")
    
    plt.tight_layout()
    
    # Save filenames
    filename = f"{prefix}_likes_lognormal.png"
    artifact_path = f"{artifact_dir}/{filename}"
    docs_path = f"{docs_dir}/{filename}"
    
    plt.savefig(artifact_path, dpi=300, bbox_inches='tight')
    plt.savefig(docs_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"[{store}] Saved to:")
    print(f"  - {artifact_path}")
    print(f"  - {docs_path}")
print("\nAll 3 brand log-normal plots successfully generated and saved!")
