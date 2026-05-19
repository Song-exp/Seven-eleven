"""
7-Eleven app event EDA starter.

Business goal
-------------
This script gives the first reliable read of app behavior before modeling:

1. Data quality
   Check row counts, user-id coverage, product mapping coverage, and campaign
   dimensions. Product fields are currently expected to be sparse or unmapped in
   some integrated files, so product-level charts are generated only when mapped
   names exist.

2. Funnel and intent proxy
   AppsFlyer event names are treated as behavioral states. Counts by event,
   month, platform, media source, and campaign show where app traffic is coming
   from and which interactions dominate.

3. User and graph-readiness diagnostics
   For HIN/GNN work, the relevant shape is a sparse user-content bipartite
   interaction graph. We therefore estimate user activity distribution, content
   node degree distribution, and top user-content edges using af_content as the
   item/content proxy when POS item mapping is unavailable.

Data safety
-----------
The script reads only from data/processed/app_event_integrated/*.parquet and
writes only to eda/app_event_yumi/outputs/starter_eda/.
"""

from __future__ import annotations

import glob
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
os.environ.setdefault("MPLCONFIGDIR", "/private/tmp/seven_eleven_mplconfig")
os.environ.setdefault("XDG_CACHE_HOME", "/private/tmp/seven_eleven_cache")

import matplotlib.pyplot as plt
import polars as pl
import seaborn as sns


INPUT_DIR = PROJECT_ROOT / "data" / "processed" / "app_event_integrated"
OUTPUT_DIR = Path(__file__).resolve().parent / "outputs" / "starter_eda"


def save_csv(df: pl.DataFrame, name: str) -> Path:
    """Write a small aggregated DataFrame to the EDA output directory."""
    path = OUTPUT_DIR / name
    df.write_csv(path)
    return path


def save_barplot(
    df: pl.DataFrame,
    x: str,
    y: str,
    title: str,
    xlabel: str,
    ylabel: str,
    name: str,
    figsize: tuple[int, int] = (12, 7),
) -> Path | None:
    """Save a readable bar plot for an already-aggregated result."""
    if df.is_empty():
        return None

    pdf = df.to_pandas()
    plt.figure(figsize=figsize)
    sns.barplot(data=pdf, x=x, y=y)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.tight_layout()

    path = OUTPUT_DIR / name
    plt.savefig(path, dpi=160)
    plt.close()
    return path


def app_event_lazyframe() -> pl.LazyFrame:
    parquet_files = sorted(glob.glob(str(INPUT_DIR / "*.parquet")))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {INPUT_DIR}")

    period = pl.col("Event Time").str.extract(r"(오전|오후)", 1)
    hour_12 = pl.col("Event Time").str.extract(r"(?:오전|오후) (\d{1,2}):", 1).cast(pl.Int8)
    hour_24 = (
        pl.when((period == "오후") & (hour_12 < 12))
        .then(hour_12 + 12)
        .when((period == "오전") & (hour_12 == 12))
        .then(0)
        .otherwise(hour_12)
        .cast(pl.Int8)
    )

    return (
        pl.scan_parquet(parquet_files)
        .with_columns(
            [
                pl.col("Event Time").str.slice(0, 10).alias("event_date_raw"),
                pl.col("Event Time").str.slice(0, 7).alias("event_month"),
                hour_24.alias("event_hour"),
                pl.col("Platform").str.to_lowercase().alias("platform_norm"),
                pl.col("Media Source").fill_null("unknown").alias("media_source_norm"),
                pl.col("Campaign").fill_null("unknown").alias("campaign_norm"),
                pl.when(pl.col("item_name").is_not_null())
                .then(pl.col("item_name"))
                .otherwise(pl.col("af_content"))
                .alias("content_proxy"),
            ]
        )
        .with_columns(
            pl.col("event_date_raw")
            .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
            .alias("event_date")
        )
    )


def collect_data_quality(lf: pl.LazyFrame) -> pl.DataFrame:
    summary = lf.select(
        [
            pl.len().alias("total_rows"),
            pl.col("event_date").is_not_null().sum().alias("parseable_event_date_rows"),
            pl.col("af_customer_user_id").is_not_null().sum().alias("user_id_rows"),
            pl.col("AppsFlyer ID").is_not_null().sum().alias("appsflyer_id_rows"),
            pl.col("af_content").is_not_null().sum().alias("af_content_rows"),
            pl.col("pos_item_code").is_not_null().sum().alias("pos_item_code_rows"),
            pl.col("item_name").is_not_null().sum().alias("item_name_rows"),
            pl.col("category_hierarchy").is_not_null().sum().alias("category_hierarchy_rows"),
            pl.col("Event Name").n_unique().alias("unique_event_names"),
            pl.col("af_customer_user_id").n_unique().alias("unique_customer_users"),
            pl.col("AppsFlyer ID").n_unique().alias("unique_appsflyer_ids"),
            pl.col("af_content").n_unique().alias("unique_af_content"),
        ]
    ).collect()

    total_rows = summary.item(0, "total_rows")
    rate_columns = [
        "parseable_event_date_rows",
        "user_id_rows",
        "appsflyer_id_rows",
        "af_content_rows",
        "pos_item_code_rows",
        "item_name_rows",
        "category_hierarchy_rows",
    ]
    return summary.with_columns(
        [(pl.col(col) / total_rows * 100).round(4).alias(f"{col}_pct") for col in rate_columns]
    )


def collect_user_distribution(lf: pl.LazyFrame) -> pl.DataFrame:
    user_counts = (
        lf.filter(pl.col("af_customer_user_id").is_not_null())
        .group_by("af_customer_user_id")
        .agg(pl.len().alias("event_count"))
    )
    return user_counts.select(
        [
            pl.len().alias("users"),
            pl.col("event_count").mean().round(3).alias("mean_events_per_user"),
            pl.col("event_count").median().alias("median_events_per_user"),
            pl.col("event_count").quantile(0.75).alias("p75_events_per_user"),
            pl.col("event_count").quantile(0.9).alias("p90_events_per_user"),
            pl.col("event_count").quantile(0.95).alias("p95_events_per_user"),
            pl.col("event_count").max().alias("max_events_per_user"),
        ]
    ).collect()


def collect_graph_sparsity(lf: pl.LazyFrame) -> pl.DataFrame:
    interactions = lf.filter(
        pl.col("af_customer_user_id").is_not_null() & pl.col("content_proxy").is_not_null()
    )
    content_degree = interactions.group_by("content_proxy").agg(
        [
            pl.len().alias("event_count"),
            pl.col("af_customer_user_id").n_unique().alias("unique_users"),
        ]
    )
    edge_count = interactions.select(["af_customer_user_id", "content_proxy"]).unique().select(pl.len())

    content_summary = content_degree.select(
        [
            pl.len().alias("content_nodes"),
            pl.col("event_count").sum().alias("content_event_rows"),
            pl.col("event_count").mean().round(3).alias("mean_events_per_content"),
            pl.col("unique_users").mean().round(3).alias("mean_unique_users_per_content"),
            pl.col("unique_users").quantile(0.5).alias("median_unique_users_per_content"),
            pl.col("unique_users").quantile(0.9).alias("p90_unique_users_per_content"),
            pl.col("unique_users").max().alias("max_unique_users_per_content"),
        ]
    ).collect()
    unique_edges = edge_count.collect().item(0, 0)
    return content_summary.with_columns(pl.lit(unique_edges).alias("unique_user_content_edges"))


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    sns.set_theme(style="whitegrid", font="AppleGothic")
    plt.rcParams["axes.unicode_minus"] = False

    lf = app_event_lazyframe()

    data_quality = collect_data_quality(lf)
    save_csv(data_quality, "data_quality_summary.csv")

    event_counts = (
        lf.group_by("Event Name")
        .agg(pl.len().alias("event_count"))
        .sort("event_count", descending=True)
        .collect()
    )
    save_csv(event_counts, "event_name_counts.csv")
    save_barplot(
        event_counts.head(20),
        x="event_count",
        y="Event Name",
        title="Top 20 App Event Names",
        xlabel="Event count",
        ylabel="Event name",
        name="top20_event_names.png",
    )

    monthly_platform = (
        lf.group_by(["event_month", "platform_norm"])
        .agg(pl.len().alias("event_count"))
        .sort(["event_month", "platform_norm"])
        .collect()
    )
    save_csv(monthly_platform, "monthly_platform_counts.csv")

    hourly_counts = (
        lf.filter(pl.col("event_hour").is_not_null())
        .group_by("event_hour")
        .agg(pl.len().alias("event_count"))
        .sort("event_hour")
        .collect()
    )
    save_csv(hourly_counts, "hourly_event_counts.csv")

    media_counts = (
        lf.group_by("media_source_norm")
        .agg(pl.len().alias("event_count"))
        .sort("event_count", descending=True)
        .collect()
    )
    save_csv(media_counts, "media_source_counts.csv")
    save_barplot(
        media_counts.head(20),
        x="event_count",
        y="media_source_norm",
        title="Top 20 Media Sources",
        xlabel="Event count",
        ylabel="Media source",
        name="top20_media_sources.png",
    )

    campaign_counts = (
        lf.group_by("campaign_norm")
        .agg(pl.len().alias("event_count"))
        .sort("event_count", descending=True)
        .head(100)
        .collect()
    )
    save_csv(campaign_counts, "top100_campaign_counts.csv")

    user_distribution = collect_user_distribution(lf)
    save_csv(user_distribution, "user_event_distribution_summary.csv")

    top_users = (
        lf.filter(pl.col("af_customer_user_id").is_not_null())
        .group_by("af_customer_user_id")
        .agg(
            [
                pl.len().alias("event_count"),
                pl.col("Event Name").n_unique().alias("unique_event_names"),
                pl.col("content_proxy").n_unique().alias("unique_content_proxy"),
                pl.col("event_date").min().alias("first_event_date"),
                pl.col("event_date").max().alias("last_event_date"),
            ]
        )
        .sort("event_count", descending=True)
        .head(50)
        .collect()
    )
    save_csv(top_users, "top50_users_by_event_count.csv")

    user_monthly = (
        lf.filter(pl.col("af_customer_user_id").is_not_null())
        .group_by(["event_month", "platform_norm"])
        .agg(pl.col("af_customer_user_id").n_unique().alias("unique_users"))
        .sort(["event_month", "platform_norm"])
        .collect()
    )
    save_csv(user_monthly, "monthly_unique_users_by_platform.csv")

    content_counts = (
        lf.filter(pl.col("content_proxy").is_not_null())
        .group_by("content_proxy")
        .agg(
            [
                pl.len().alias("event_count"),
                pl.col("af_customer_user_id").n_unique().alias("unique_users"),
            ]
        )
        .sort("event_count", descending=True)
        .head(100)
        .collect()
    )
    save_csv(content_counts, "top100_content_proxy_counts.csv")

    mapped_products = (
        lf.filter(pl.col("item_name").is_not_null())
        .group_by("item_name")
        .agg(
            [
                pl.len().alias("event_count"),
                pl.col("af_customer_user_id").n_unique().alias("unique_users"),
            ]
        )
        .sort("event_count", descending=True)
        .head(100)
        .collect()
    )
    save_csv(mapped_products, "top100_mapped_product_counts.csv")
    save_barplot(
        mapped_products.head(20),
        x="event_count",
        y="item_name",
        title="Top 20 Mapped Products in App Events",
        xlabel="Event count",
        ylabel="Item name",
        name="top20_mapped_products.png",
    )

    graph_sparsity = collect_graph_sparsity(lf)
    save_csv(graph_sparsity, "graph_sparsity_summary.csv")

    print(f"Saved app event EDA outputs to: {OUTPUT_DIR}")
    print(data_quality)
    print(user_distribution)
    print(graph_sparsity)


if __name__ == "__main__":
    main()
