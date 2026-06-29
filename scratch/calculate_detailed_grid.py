import sys
import pandas as pd
sys.stdout.reconfigure(encoding='utf-8')

# Read Instagram engagement data
df_ie = pd.read_csv("data/processed/instagram_engagement_with_keywords_final.csv")

# Filter out excluded row if any
if '제외' in df_ie.columns:
    df_ie = df_ie[df_ie['제외'] != 1]

# We need to compute '좋아요합' grouped by 편의점명 and 정규화명
df_likes = df_ie.groupby(['편의점명', '정규화명'])['좋아요 수'].sum().reset_index()

stores = ["세븐일레븐", "CU", "GS25"]
thresholds = [3000, 3500, 4000, 4500, 5000]

print("=== 3,000 ~ 5,000 Grid Search Simulation (500 unit step) ===")

for store in stores:
    df_store = df_likes[df_likes['편의점명'] == store]
    # In notebook, the total N (base size) for insta is:
    # Seven-Eleven: 658, CU: 1,162, GS25: 985
    # Let's count store likes length
    total_n = len(df_store)
    print(f"\nStore: {store} (Total Insta Products: {total_n})")
    for t in thresholds:
        n_success = (df_store['좋아요 수'] >= t).sum()
        pct = (n_success / total_n) * 100
        print(f"  Threshold >= {t:,} : Success: {n_success}개 ({pct:.2f}%)")
