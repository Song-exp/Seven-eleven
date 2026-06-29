import sys
import pandas as pd
import numpy as np

sys.stdout.reconfigure(encoding='utf-8')

# Load the final labels
df_labels = pd.read_csv('data/processed/npd_success_labels.csv')
df_se = df_labels[df_labels['편의점명'] == '세븐일레븐'].copy()
print(f"Total Seven-Eleven Products in npd_success_labels.csv: {len(df_se)}")
print(f"Current success count (threshold=3000): {(df_se['성공여부'] == '성공').sum()} ({(df_se['성공여부'] == '성공').sum() / len(df_se) * 100:.2f}%)")
print(f"Current failure count: {(df_se['성공여부'] == '실패').sum()}")

# Load Instagram engagement
df_ie = pd.read_csv("data/processed/instagram_engagement_with_keywords_final.csv")
if '제외' in df_ie.columns:
    df_ie = df_ie[df_ie['제외'] != 1]
df_likes = df_ie[df_ie['편의점명'] == '세븐일레븐'].groupby(['정규화명'])['좋아요 수'].sum().reset_index()

# Load POS features and calculate POS success
df_pos = pd.read_csv('data/processed/pos_product_features.csv')

# Pareto 80% logic for POS
df_pos_clean = df_pos.dropna(subset=['sales_30d_amt']).copy()
pos_success_cds = set()
for cat, sub_df in df_pos_clean.groupby('ITEM_MDDV_NM'):
    sub_df = sub_df.sort_values(by='sales_30d_amt', ascending=False).copy()
    total_amt = sub_df['sales_30d_amt'].sum()
    if total_amt <= 0:
        continue
    sub_df['cum_amt'] = sub_df['sales_30d_amt'].cumsum()
    sub_df['cum_pct'] = sub_df['cum_amt'] / total_amt
    success_cds = sub_df[sub_df['cum_pct'] <= 0.8]['ITEM_CD'].values
    border_df = sub_df[sub_df['cum_pct'] > 0.8].head(1)
    if len(border_df) > 0:
        success_cds = np.append(success_cds, border_df['ITEM_CD'].values)
    pos_success_cds.update(success_cds)

# Map POS success to items
df_pos['pos_success'] = df_pos['ITEM_CD'].apply(lambda x: 1 if x in pos_success_cds else 0)

# Merge POS and Instagram Engagement
# Let's see how they match.
# In the original workflow, they matched ITEM_CD to Instagram likes by normal name ('정규화명')
# Let's load the matching bridge
df_bridge = pd.read_csv('data/processed/pos_b4_insta_pool_final.csv')

# Merge bridge to POS
df_merged = pd.merge(df_pos, df_bridge, on='ITEM_CD', how='left')

# Merge with Instagram likes by 정규화명
df_merged = pd.merge(df_merged, df_likes, on='정규화명', how='left')
df_merged['좋아요 수'] = df_merged['좋아요 수'].fillna(0)

# Let's count totals
print("\n--- Running Sweep Simulation on Matched Dataset ---")
print(f"Total merged products size: {len(df_merged)}")

thresholds = [1500, 3000, 5000]
for t in thresholds:
    # Compute insta_success for this threshold
    df_merged[f'insta_success_{t}'] = df_merged['좋아요 수'].apply(lambda x: 1 if x >= t else 0)
    # Final success is POS success OR Instagram success
    df_merged[f'final_success_{t}'] = np.where((df_merged['pos_success'] == 1) | (df_merged[f'insta_success_{t}'] == 1), 1, 0)
    
    n_insta_success = (df_merged[f'insta_success_{t}'] == 1).sum()
    n_final_success = (df_merged[f'final_success_{t}'] == 1).sum()
    pct_final = n_final_success / len(df_merged) * 100
    
    print(f"Threshold >= {t}:")
    print(f"  Instagram success count: {n_insta_success} products")
    print(f"  POS success count: {(df_merged['pos_success'] == 1).sum()} products")
    # Let's check overlap
    overlap = ((df_merged['pos_success'] == 1) & (df_merged[f'insta_success_{t}'] == 1)).sum()
    print(f"  Overlap (both POS & Insta success): {overlap} products")
    print(f"  Union Success (POS OR Insta success): {n_final_success} products ({pct_final:.2f}%)")
