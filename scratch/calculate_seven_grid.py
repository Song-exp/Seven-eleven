import sys
import pandas as pd
sys.stdout.reconfigure(encoding='utf-8')

# Try to load POS product pool and instagram engagement data
try:
    # 1. Load POS success pool for Seven-Eleven
    # Check pos_product_features or pos_b4_product_pool to see where 'pos_success' or equivalent label is
    df_pos = pd.read_csv("data/processed/pos_product_features.csv")
    print("POS Features columns:", list(df_pos.columns))
except Exception as e:
    print("Error reading pos_product_features.csv:", e)

try:
    df_pool = pd.read_csv("data/processed/pos_b4_insta_pool_final.csv")
    print("Insta Pool columns:", list(df_pool.columns))
except Exception as e:
    print("Error reading pos_b4_insta_pool_final.csv:", e)
