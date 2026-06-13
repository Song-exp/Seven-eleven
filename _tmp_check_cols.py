# -*- coding: utf-8 -*-
import pandas as pd
from pathlib import Path

BASE = Path(__file__).resolve().parent
pnode_path = BASE / "data" / "processed" / "hin" / "최종" / "product_nodes.parquet"

df = pd.read_parquet(pnode_path)
print("=== product_nodes columns ===")
for c in df.columns:
    print(" ", c)
print("shape:", df.shape)
print()
print("has_promo_30d 있음?", "has_promo_30d" in df.columns)
promo_cols = [c for c in df.columns if c.startswith("promo_")]
print("promo_* 컬럼:", promo_cols)
