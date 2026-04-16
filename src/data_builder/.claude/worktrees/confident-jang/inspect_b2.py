import polars as pl
import os

b2_path = "7eleven_npd_framework/data/processed/B2_POS_SALE.parquet"
cleaned_path = "7eleven_npd_framework/data/processed/B2_POS_SALE_CLEANED.parquet"

def analyze_diff(path, label):
    df = pl.scan_parquet(path)
    schema = df.schema
    count = df.select(pl.len()).collect().item()
    return count, schema

print(f"--- Comparison Analysis ---")
c1, s1 = analyze_diff(b2_path, "Original")
c2, s2 = analyze_diff(cleaned_path, "Cleaned")

print(f"Original Row Count: {c1:,}")
print(f"Cleaned Row Count: {c2:,}")
print(f"Row Reduction: {c1 - c2:,} ({(c1-c2)/c1*100:.2f}%)")

print("\n--- Schema Differences ---")
print(f"Original Schema: {s1}")
print(f"Cleaned Schema: {s2}")
