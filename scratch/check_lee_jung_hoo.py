import sys
import pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
df = pd.read_parquet(r"data/processed/hin/product_nodes.parquet")
lee_products = df[df['ITEM_NM'].str.contains('이정후', na=False)]
print("HIN Product Nodes:")
for idx, row in lee_products.iterrows():
    print(f"ID: {row['ITEM_CD']} | NM: {row['ITEM_NM']} | SUCCESS: {row['성공여부']} | STORE: {row['편의점명']}")
