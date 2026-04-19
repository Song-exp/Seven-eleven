import polars as pl
import glob
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns

# 1. 데이터 로드
processed_files = glob.glob("data/processed/app_event_integrated/*.parquet")
df = pl.read_parquet(processed_files)

# 2. 기초 통계 분석
total_rows = len(df)
mapped_rows = df.filter(pl.col("item_name").is_not_null()).height
print(f"Total Rows: {total_rows}")
print(f"Mapped Product Rows: {mapped_rows} ({mapped_rows/total_rows*100:.2f}%)")

# 3. 유입 채널별 이벤트 분포
channel_counts = df.group_by("Media Source").agg(pl.len().alias("count")).sort("count", descending=True).head(10)
print("\nTop 10 Media Sources:")
print(channel_counts)

# 4. 가장 많이 조회된 상품 Top 10
top_items = df.filter(pl.col("item_name").is_not_null()) \
              .group_by("item_name") \
              .agg(pl.len().alias("count")) \
              .sort("count", descending=True) \
              .head(10)
print("\nTop 10 Interacted Items:")
print(top_items)

# 5. 월별 트렌드
df = df.with_columns([
    pl.col("Event Time").str.slice(0, 7).alias("Month")
])
monthly_trend = df.group_by("Month").agg(pl.len().alias("count")).sort("Month")
print("\nMonthly Event Trend:")
print(monthly_trend)

# 시각화 (이미지 저장)
try:
    plt.figure(figsize=(12, 6))
    sns.barplot(x="Month", y="count", data=monthly_trend.to_pandas())
    plt.title("Monthly App Event Trend (2025)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("eda/app_monthly_trend.png")
    
    plt.figure(figsize=(12, 6))
    sns.barplot(x="count", y="item_name", data=top_items.to_pandas())
    plt.title("Top 10 Interacted Items in App (2025)")
    plt.tight_layout()
    plt.savefig("eda/app_top_items.png")
    print("\nSaved charts to eda/app_monthly_trend.png and eda/app_top_items.png")
except Exception as e:
    print(f"Visualization error: {e}")
