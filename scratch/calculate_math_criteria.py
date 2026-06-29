import sys
import numpy as np
import pandas as pd
import scipy.stats as stats
sys.stdout.reconfigure(encoding='utf-8')

# Read Instagram engagement data
df_ie = pd.read_csv("data/processed/instagram_engagement_with_keywords_final.csv")
if '제외' in df_ie.columns:
    df_ie = df_ie[df_ie['제외'] != 1]

df_likes = df_ie.groupby(['편의점명', '정규화명'])['좋아요 수'].sum().reset_index()

stores = ["세븐일레븐", "CU", "GS25"]

print("=== Statistical Analysis of Likes Threshold 3,000 ===")

for store in stores:
    df_store = df_likes[df_likes['편의점명'] == store]
    # Filter out values <= 0 to get valid log scale
    likes = df_store['좋아요 수'].values
    valid_likes = likes[likes > 0]
    
    # 1. Percentile check for 3,000 (on original distribution)
    percentile_of_3000 = stats.percentileofscore(likes, 3000)
    print(f"\n[{store}] (N={len(likes)}, Valid N={len(valid_likes)})")
    print(f"  Likes 3,000 Percentile: {percentile_of_3000:.2f}% (상위 {100 - percentile_of_3000:.2f}%)")
    
    # 2. Log-normal Distribution Fitting
    log_likes = np.log(valid_likes)
    mu = np.mean(log_likes)
    sigma = np.std(log_likes)
    
    # Check log score of 3,000
    log_3000 = np.log(3000)
    z_score = (log_3000 - mu) / sigma
    
    print(f"  Log-normal Fit:")
    print(f"    mean(mu): {mu:.4f} | std(sigma): {sigma:.4f}")
    print(f"    Log(3,000): {log_3000:.4f} | Z-Score (Log-space): {z_score:.2f} sigma")
    print(f"    Formula: Log(3,000) = mu + {z_score:.2f} * sigma")
