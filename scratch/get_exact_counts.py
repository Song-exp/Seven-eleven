import sys
import pandas as pd
import numpy as np

sys.stdout.reconfigure(encoding='utf-8')

print("=== 1. POS Channel Stats (Seven-Eleven Only) ===")
# POS success is computed on pos_product_features
df_pos = pd.read_csv('data/processed/pos_product_features.csv')
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

n_pos_appear = len(df_pos)
n_pos_success = len(pos_success_cds)
print(f"POS Appearance (등장 수): {n_pos_appear}개")
print(f"POS Success (성공 수): {n_pos_success}개 (성공률: {n_pos_success/n_pos_appear*100:.2f}%)")

print("\n=== 2. Instagram Channel Stats (All 3 Brands) ===")
df_ie = pd.read_csv('data/processed/instagram_engagement_with_keywords_final.csv')
if '제외' in df_ie.columns:
    df_ie = df_ie[df_ie['제외'] != 1]
df_likes = df_ie.groupby(['편의점명', '정규화명'])['좋아요 수'].sum().reset_index()

stores = ['세븐일레븐', 'CU', 'GS25']
for s in stores:
    df_s = df_likes[df_likes['편의점명'] == s]
    n_insta_appear = len(df_s)
    n_insta_success = (df_s['좋아요 수'] >= 3000).sum()
    print(f"[{s}]")
    print(f"  Instagram Appearance (등장 수): {n_insta_appear}개")
    print(f"  Instagram Success (성공 수): {n_insta_success}개 (성공률: {n_insta_success/n_insta_appear*100:.2f}%)")
