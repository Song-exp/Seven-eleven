"""
Product-level app interest analysis.

Input
-----
eda/app_event_yumi/outputs/cleaned_app_events/clean_app_item_interactions.parquet

Output
------
eda/app_event_yumi/outputs/product_interest_analysis/

This analysis uses cleaned, product-matched app events. There are no matched
purchase events in this app-product dataset, so the main business target here is
not sales conversion. Instead, we estimate app interest and purchase intent from
views, add-to-cart, wishlist, share, and removal events.
"""

from __future__ import annotations

import os
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/private/tmp/seven_eleven_mplconfig")
os.environ.setdefault("XDG_CACHE_HOME", "/private/tmp/seven_eleven_cache")

import matplotlib.pyplot as plt
import polars as pl
import seaborn as sns


BASE_DIR = Path(__file__).resolve().parent
INPUT_FILE = BASE_DIR / "outputs" / "cleaned_app_events" / "clean_app_item_interactions.parquet"
OUTPUT_DIR = BASE_DIR / "outputs" / "product_interest_analysis"
MONTH_DIR = OUTPUT_DIR / "by_month"
PERIOD_DIR = OUTPUT_DIR / "by_period"
MONTHS = ["2025-03", "2025-04", "2025-05"]
PERIODS = [
    ("2025_03_early", "2025-03 초순", "2025-03-01", "2025-03-10"),
    ("2025_03_mid", "2025-03 중순", "2025-03-11", "2025-03-20"),
    ("2025_03_late", "2025-03 하순", "2025-03-21", "2025-03-31"),
    ("2025_04_early", "2025-04 초순", "2025-04-01", "2025-04-10"),
    ("2025_04_mid", "2025-04 중순", "2025-04-11", "2025-04-20"),
    ("2025_04_late", "2025-04 하순", "2025-04-21", "2025-04-30"),
    ("2025_05_early", "2025-05 초순", "2025-05-01", "2025-05-10"),
    ("2025_05_mid", "2025-05 중순", "2025-05-11", "2025-05-20"),
    ("2025_05_late", "2025-05 하순", "2025-05-21", "2025-05-31"),
]


EVENT_ALIASES = {
    "af_content_view_product": "view_count",
    "af_add_to_cart": "cart_add_count",
    "af_remove_from_cart": "cart_remove_count",
    "af_add_to_wishlist": "wishlist_add_count",
    "af_remove_from_wishlist": "wishlist_remove_count",
    "af_share": "share_count",
}


def write_csv(df: pl.DataFrame, filename: str, output_dir: Path = OUTPUT_DIR) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    df.write_csv(output_dir / filename)


def save_barplot(
    df: pl.DataFrame,
    x: str,
    y: str,
    title: str,
    xlabel: str,
    ylabel: str,
    filename: str,
    figsize: tuple[int, int] = (12, 8),
    output_dir: Path = OUTPUT_DIR,
) -> None:
    if df.is_empty():
        return
    output_dir.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=figsize)
    sns.barplot(data=df.to_pandas(), x=x, y=y)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.tight_layout()
    plt.savefig(output_dir / filename, dpi=160)
    plt.close()


def app_lf() -> pl.LazyFrame:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Missing cleaned app product data: {INPUT_FILE}")
    return pl.scan_parquet(INPUT_FILE)


def product_metrics(lf: pl.LazyFrame) -> pl.DataFrame:
    metrics = (
        lf.group_by(
            [
                "mapped_pos_item_code",
                "mapped_item_name",
                "mapped_category_hierarchy",
                "category_l",
                "category_m",
                "category_s",
                "item_type",
            ]
        )
        .agg(
            [
                pl.len().alias("total_events"),
                pl.col("af_customer_user_id").n_unique().alias("unique_users"),
                pl.col("event_weight").sum().round(2).alias("weighted_interest_score"),
                pl.col("Event Name").eq("af_content_view_product").sum().cast(pl.Int64).alias("view_count"),
                pl.col("Event Name").eq("af_add_to_cart").sum().cast(pl.Int64).alias("cart_add_count"),
                pl.col("Event Name").eq("af_remove_from_cart").sum().cast(pl.Int64).alias("cart_remove_count"),
                pl.col("Event Name").eq("af_add_to_wishlist").sum().cast(pl.Int64).alias("wishlist_add_count"),
                pl.col("Event Name").eq("af_remove_from_wishlist").sum().cast(pl.Int64).alias("wishlist_remove_count"),
                pl.col("Event Name").eq("af_share").sum().cast(pl.Int64).alias("share_count"),
                pl.col("event_date").min().alias("first_event_date"),
                pl.col("event_date").max().alias("last_event_date"),
                pl.col("list_price").median().alias("median_price"),
            ]
        )
        .with_columns(
            [
                (pl.col("cart_add_count") - pl.col("cart_remove_count")).alias("net_cart_count"),
                (pl.col("wishlist_add_count") - pl.col("wishlist_remove_count")).alias("net_wishlist_count"),
                (
                    pl.col("cart_add_count") * 3
                    + pl.col("wishlist_add_count") * 2
                    + pl.col("share_count") * 2
                    + pl.col("view_count")
                    - pl.col("cart_remove_count")
                    - pl.col("wishlist_remove_count")
                ).alias("intent_score"),
            ]
        )
        .with_columns(
            [
                (pl.col("cart_add_count") / pl.col("view_count").clip(1) * 100)
                .round(4)
                .alias("cart_add_per_100_views"),
                (pl.col("wishlist_add_count") / pl.col("view_count").clip(1) * 100)
                .round(4)
                .alias("wishlist_add_per_100_views"),
                (pl.col("net_cart_count") / pl.col("view_count").clip(1) * 100)
                .round(4)
                .alias("net_cart_per_100_views"),
                (pl.col("intent_score") / pl.col("unique_users").clip(1))
                .round(4)
                .alias("intent_score_per_user"),
            ]
        )
        .sort("intent_score", descending=True)
        .collect()
    )
    return metrics


def category_metrics(lf: pl.LazyFrame) -> pl.DataFrame:
    return (
        lf.group_by(["category_l", "category_m"])
        .agg(
            [
                pl.len().alias("total_events"),
                pl.col("mapped_pos_item_code").n_unique().alias("unique_products"),
                pl.col("af_customer_user_id").n_unique().alias("unique_users"),
                pl.col("event_weight").sum().round(2).alias("weighted_interest_score"),
                pl.col("Event Name").eq("af_content_view_product").sum().cast(pl.Int64).alias("view_count"),
                pl.col("Event Name").eq("af_add_to_cart").sum().cast(pl.Int64).alias("cart_add_count"),
                pl.col("Event Name").eq("af_remove_from_cart").sum().cast(pl.Int64).alias("cart_remove_count"),
                pl.col("Event Name").eq("af_add_to_wishlist").sum().cast(pl.Int64).alias("wishlist_add_count"),
                pl.col("Event Name").eq("af_remove_from_wishlist").sum().cast(pl.Int64).alias("wishlist_remove_count"),
            ]
        )
        .with_columns(
            [
                (pl.col("cart_add_count") / pl.col("view_count").clip(1) * 100)
                .round(4)
                .alias("cart_add_per_100_views"),
                (
                    (pl.col("cart_add_count") + pl.col("wishlist_add_count"))
                    / pl.col("view_count").clip(1)
                    * 100
                )
                .round(4)
                .alias("intent_actions_per_100_views"),
                (pl.col("weighted_interest_score") / pl.col("unique_products").clip(1))
                .round(4)
                .alias("score_per_product"),
            ]
        )
        .sort("weighted_interest_score", descending=True)
        .collect()
    )


def monthly_product_metrics(lf: pl.LazyFrame) -> pl.DataFrame:
    return (
        lf.group_by(["event_month", "mapped_pos_item_code", "mapped_item_name", "category_l", "category_m"])
        .agg(
            [
                pl.len().alias("total_events"),
                pl.col("af_customer_user_id").n_unique().alias("unique_users"),
                pl.col("event_weight").sum().round(2).alias("weighted_interest_score"),
                pl.col("Event Name").eq("af_content_view_product").sum().cast(pl.Int64).alias("view_count"),
                pl.col("Event Name").eq("af_add_to_cart").sum().cast(pl.Int64).alias("cart_add_count"),
                pl.col("Event Name").eq("af_add_to_wishlist").sum().cast(pl.Int64).alias("wishlist_add_count"),
                pl.col("event_date").min().alias("first_event_date_in_month"),
            ]
        )
        .with_columns(
            (pl.col("cart_add_count") / pl.col("view_count").clip(1) * 100)
            .round(4)
            .alias("cart_add_per_100_views")
        )
        .sort(["event_month", "weighted_interest_score"], descending=[False, True])
        .collect()
    )


def monthly_movers(monthly: pl.DataFrame) -> pl.DataFrame:
    prev = monthly.select(
        [
            pl.col("event_month").alias("prev_month"),
            "mapped_pos_item_code",
            pl.col("weighted_interest_score").alias("prev_weighted_interest_score"),
            pl.col("unique_users").alias("prev_unique_users"),
        ]
    )
    month_order = {"2025-03": "2025-04", "2025-04": "2025-05"}
    prev = prev.with_columns(pl.col("prev_month").replace(month_order).alias("event_month")).drop("prev_month")
    return (
        monthly.join(prev, on=["event_month", "mapped_pos_item_code"], how="left")
        .with_columns(
            [
                pl.col("prev_weighted_interest_score").fill_null(0),
                pl.col("prev_unique_users").fill_null(0),
            ]
        )
        .with_columns(
            [
                (pl.col("weighted_interest_score") - pl.col("prev_weighted_interest_score")).alias(
                    "score_delta_vs_prev_month"
                ),
                (pl.col("unique_users") - pl.col("prev_unique_users")).alias("unique_users_delta_vs_prev_month"),
            ]
        )
        .filter(pl.col("event_month").is_in(["2025-04", "2025-05"]))
        .sort(["event_month", "score_delta_vs_prev_month"], descending=[False, True])
    )


def first_seen_products(monthly: pl.DataFrame) -> pl.DataFrame:
    first_month = monthly.group_by("mapped_pos_item_code").agg(pl.col("event_month").min().alias("first_month"))
    return (
        monthly.join(first_month, on="mapped_pos_item_code", how="left")
        .filter(pl.col("event_month") == pl.col("first_month"))
        .sort(["first_month", "weighted_interest_score"], descending=[False, True])
    )


def write_product_analysis_outputs(
    products: pl.DataFrame,
    categories: pl.DataFrame,
    monthly: pl.DataFrame,
    output_dir: Path,
) -> None:
    write_csv(products, "product_interest_metrics.csv", output_dir)
    write_csv(products.head(200), "top200_products_by_intent_score.csv", output_dir)
    write_csv(
        products.sort("cart_add_per_100_views", descending=True)
        .filter(pl.col("view_count") >= 500)
        .head(200),
        "top200_products_by_cart_rate_min500views.csv",
        output_dir,
    )
    write_csv(products.sort("unique_users", descending=True).head(200), "top200_products_by_unique_users.csv", output_dir)

    write_csv(categories, "category_interest_metrics.csv", output_dir)
    write_csv(monthly, "monthly_product_interest_metrics.csv", output_dir)
    write_csv(monthly.group_by("event_month").head(50), "monthly_top50_products_by_interest.csv", output_dir)

    save_barplot(
        products.head(20),
        x="intent_score",
        y="mapped_item_name",
        title="Top 20 Products by App Intent Score",
        xlabel="Intent score",
        ylabel="Product",
        filename="top20_products_by_intent_score.png",
        output_dir=output_dir,
    )
    save_barplot(
        categories.head(20),
        x="weighted_interest_score",
        y="category_m",
        title="Top 20 Middle Categories by Weighted App Interest",
        xlabel="Weighted interest score",
        ylabel="Middle category",
        filename="top20_middle_categories_by_interest.png",
        output_dir=output_dir,
    )


def write_by_month_outputs(lf: pl.LazyFrame, monthly: pl.DataFrame, movers: pl.DataFrame, first_seen: pl.DataFrame) -> None:
    for month in MONTHS:
        month_key = month.replace("-", "_")
        month_dir = MONTH_DIR / month_key
        month_lf = lf.filter(pl.col("event_month") == month)

        overview = month_lf.select(
            [
                pl.lit(month).alias("event_month"),
                pl.len().alias("clean_product_event_rows"),
                pl.col("mapped_pos_item_code").n_unique().alias("unique_products"),
                pl.col("af_customer_user_id").n_unique().alias("unique_users"),
                pl.col("category_l").n_unique().alias("unique_large_categories"),
                pl.col("event_date").min().alias("start_date"),
                pl.col("event_date").max().alias("end_date"),
            ]
        ).collect()
        write_csv(overview, "product_analysis_overview.csv", month_dir)

        event_mix = (
            month_lf.group_by("Event Name")
            .agg(pl.len().alias("event_count"))
            .with_columns(
                (pl.col("event_count") / pl.col("event_count").sum() * 100).round(4).alias("event_share_pct")
            )
            .sort("event_count", descending=True)
            .collect()
        )
        write_csv(event_mix, "product_event_mix.csv", month_dir)

        products = product_metrics(month_lf)
        categories = category_metrics(month_lf)
        month_monthly = monthly.filter(pl.col("event_month") == month)
        write_product_analysis_outputs(products, categories, month_monthly, month_dir)

        write_csv(
            movers.filter(pl.col("event_month") == month).head(100),
            "top100_rising_products.csv",
            month_dir,
        )
        write_csv(
            first_seen.filter(pl.col("first_month") == month).head(100),
            "top100_first_seen_products.csv",
            month_dir,
        )


def period_product_metrics(lf: pl.LazyFrame, period_label: str) -> pl.DataFrame:
    return (
        lf.group_by(["mapped_pos_item_code", "mapped_item_name", "category_l", "category_m"])
        .agg(
            [
                pl.len().alias("total_events"),
                pl.col("af_customer_user_id").n_unique().alias("unique_users"),
                pl.col("event_weight").sum().round(2).alias("weighted_interest_score"),
                pl.col("Event Name").eq("af_content_view_product").sum().cast(pl.Int64).alias("view_count"),
                pl.col("Event Name").eq("af_add_to_cart").sum().cast(pl.Int64).alias("cart_add_count"),
                pl.col("Event Name").eq("af_add_to_wishlist").sum().cast(pl.Int64).alias("wishlist_add_count"),
                pl.col("event_date").min().alias("first_event_date_in_period"),
            ]
        )
        .with_columns(
            [
                pl.lit(period_label).alias("period_label"),
                (pl.col("cart_add_count") / pl.col("view_count").clip(1) * 100)
                .round(4)
                .alias("cart_add_per_100_views"),
            ]
        )
        .sort("weighted_interest_score", descending=True)
        .collect()
    )


def write_by_period_outputs(lf: pl.LazyFrame) -> None:
    period_summaries = []

    for period_key, period_label, start_date, end_date in PERIODS:
        period_dir = PERIOD_DIR / period_key
        period_lf = lf.filter(
            (pl.col("event_date") >= pl.date(*[int(part) for part in start_date.split("-")]))
            & (pl.col("event_date") <= pl.date(*[int(part) for part in end_date.split("-")]))
        )

        overview = period_lf.select(
            [
                pl.lit(period_key).alias("period_key"),
                pl.lit(period_label).alias("period_label"),
                pl.lit(start_date).alias("start_date"),
                pl.lit(end_date).alias("end_date"),
                pl.len().alias("clean_product_event_rows"),
                pl.col("mapped_pos_item_code").n_unique().alias("unique_products"),
                pl.col("af_customer_user_id").n_unique().alias("unique_users"),
                pl.col("category_l").n_unique().alias("unique_large_categories"),
            ]
        ).collect()
        write_csv(overview, "product_analysis_overview.csv", period_dir)
        period_summaries.append(overview)

        event_mix = (
            period_lf.group_by("Event Name")
            .agg(pl.len().alias("event_count"))
            .with_columns(
                (pl.col("event_count") / pl.col("event_count").sum() * 100).round(4).alias("event_share_pct")
            )
            .sort("event_count", descending=True)
            .collect()
        )
        write_csv(event_mix, "product_event_mix.csv", period_dir)

        products = product_metrics(period_lf)
        categories = category_metrics(period_lf)
        period_metrics = period_product_metrics(period_lf, period_label)

        write_csv(products, "product_interest_metrics.csv", period_dir)
        write_csv(products.head(200), "top200_products_by_intent_score.csv", period_dir)
        write_csv(
            products.sort("cart_add_per_100_views", descending=True)
            .filter(pl.col("view_count") >= 500)
            .head(200),
            "top200_products_by_cart_rate_min500views.csv",
            period_dir,
        )
        write_csv(products.sort("unique_users", descending=True).head(200), "top200_products_by_unique_users.csv", period_dir)
        write_csv(categories, "category_interest_metrics.csv", period_dir)
        write_csv(period_metrics, "period_product_interest_metrics.csv", period_dir)
        write_csv(period_metrics.head(50), "period_top50_products_by_interest.csv", period_dir)

        save_barplot(
            products.head(20),
            x="intent_score",
            y="mapped_item_name",
            title=f"Top 20 Products by App Intent Score ({period_label})",
            xlabel="Intent score",
            ylabel="Product",
            filename="top20_products_by_intent_score.png",
            output_dir=period_dir,
        )
        save_barplot(
            categories.head(20),
            x="weighted_interest_score",
            y="category_m",
            title=f"Top 20 Middle Categories by Weighted App Interest ({period_label})",
            xlabel="Weighted interest score",
            ylabel="Middle category",
            filename="top20_middle_categories_by_interest.png",
            output_dir=period_dir,
        )

    if period_summaries:
        write_csv(pl.concat(period_summaries, how="vertical"), "period_overview.csv", PERIOD_DIR)


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    sns.set_theme(style="whitegrid", font="AppleGothic")
    plt.rcParams["axes.unicode_minus"] = False

    lf = app_lf()
    overview = lf.select(
        [
            pl.len().alias("clean_product_event_rows"),
            pl.col("mapped_pos_item_code").n_unique().alias("unique_products"),
            pl.col("af_customer_user_id").n_unique().alias("unique_users"),
            pl.col("category_l").n_unique().alias("unique_large_categories"),
            pl.col("event_date").min().alias("start_date"),
            pl.col("event_date").max().alias("end_date"),
        ]
    ).collect()
    write_csv(overview, "product_analysis_overview.csv")

    event_mix = (
        lf.group_by("Event Name")
        .agg(pl.len().alias("event_count"))
        .with_columns((pl.col("event_count") / pl.col("event_count").sum() * 100).round(4).alias("event_share_pct"))
        .sort("event_count", descending=True)
        .collect()
    )
    write_csv(event_mix, "product_event_mix.csv")

    products = product_metrics(lf)
    categories = category_metrics(lf)
    monthly = monthly_product_metrics(lf)
    write_product_analysis_outputs(products, categories, monthly, OUTPUT_DIR)

    movers = monthly_movers(monthly)
    write_csv(movers.group_by("event_month").head(100), "monthly_top100_rising_products.csv")

    first_seen = first_seen_products(monthly)
    write_csv(first_seen.group_by("first_month").head(100), "monthly_top100_first_seen_products.csv")

    write_by_month_outputs(lf, monthly, movers, first_seen)
    write_by_period_outputs(lf)

    print(f"Saved product interest analysis outputs to: {OUTPUT_DIR}")
    print(f"Saved month-separated outputs to: {MONTH_DIR}")
    print(f"Saved period-separated outputs to: {PERIOD_DIR}")
    print(overview)
    print(event_mix)
    print(products.head(10))
    print(categories.head(10))


if __name__ == "__main__":
    main()
