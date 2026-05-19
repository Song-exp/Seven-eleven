"""
Recover app product matching and remove abnormal users.

Outputs are kept inside eda/app_event_yumi/outputs/cleaned_app_events/.
The source app parquet files and raw mapping CSV are read-only inputs.

Matching logic
--------------
The previous integrated parquet has empty pos_item_code/item_name fields because
it tried to match app `af_content` to POS item code. In the current app logs,
product-level events carry `af_content_id`, and the mapping file stores that key
as `온라인상품번호`. Therefore the reliable product recovery key is:

    app_event.af_content_id == 상품코드목록_260416.csv.온라인상품번호

Cleaning logic
--------------
`L00000000000` accounts for more than 3.2M events while the next user has fewer
than 5K events. It behaves like a placeholder/system ID, so it is excluded from
the product interaction dataset. Null user IDs are also excluded from the graph
ready product interaction parquet because user-product edges require a user node.
"""

from __future__ import annotations

import glob
from pathlib import Path

import polars as pl


PROJECT_ROOT = Path(__file__).resolve().parents[2]
APP_EVENT_DIR = PROJECT_ROOT / "data" / "processed" / "app_event_integrated"
MAPPING_FILE = PROJECT_ROOT / "data" / "raw" / "app" / "상품코드목록_260416.csv"
OUTPUT_DIR = Path(__file__).resolve().parent / "outputs" / "cleaned_app_events"

ABNORMAL_USER_IDS = {"L00000000000"}

EVENT_WEIGHT_EXPR = (
    pl.when(pl.col("Event Name").is_in(["af_purchase", "af_delivery_purchase"]))
    .then(5.0)
    .when(pl.col("Event Name") == "af_add_to_cart")
    .then(3.0)
    .when(pl.col("Event Name") == "af_add_to_wishlist")
    .then(2.0)
    .when(pl.col("Event Name").str.contains("content_view"))
    .then(1.0)
    .when(pl.col("Event Name").str.contains("remove"))
    .then(-1.0)
    .otherwise(0.5)
)


def load_mapping() -> pl.DataFrame:
    mapping = pl.read_csv(MAPPING_FILE, encoding="cp949", infer_schema_length=0)
    return (
        mapping.rename(
            {
                "상품코드": "mapped_pos_item_code",
                "온라인상품번호": "online_item_code",
                "상품구분명": "item_type",
                "상품명": "mapped_item_name",
                "상품분류카테고리": "mapped_category_hierarchy",
                "정가": "list_price",
            }
        )
        .with_columns(
            [
                pl.col("online_item_code").cast(pl.Utf8).str.strip_chars(),
                pl.col("mapped_pos_item_code").cast(pl.Utf8).str.strip_chars(),
                pl.col("mapped_category_hierarchy").str.split_exact(">", 2).alias("category_parts"),
                pl.col("list_price").cast(pl.Int64, strict=False),
            ]
        )
        .with_columns(
            [
                pl.col("category_parts").struct.field("field_0").alias("category_l"),
                pl.col("category_parts").struct.field("field_1").alias("category_m"),
                pl.col("category_parts").struct.field("field_2").alias("category_s"),
            ]
        )
        .drop("category_parts")
        .unique("online_item_code", keep="first")
    )


def app_event_lf() -> pl.LazyFrame:
    parquet_files = sorted(glob.glob(str(APP_EVENT_DIR / "*.parquet")))
    if not parquet_files:
        raise FileNotFoundError(f"No app event parquet files found in {APP_EVENT_DIR}")

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
                pl.col("Event Time")
                .str.slice(0, 10)
                .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
                .alias("event_date"),
                pl.col("Platform").str.to_lowercase().alias("platform_norm"),
                pl.col("Media Source").fill_null("unknown").alias("media_source_norm"),
                pl.col("Campaign").fill_null("unknown").alias("campaign_norm"),
                pl.col("af_content_id").cast(pl.Utf8).str.strip_chars().alias("online_item_code"),
            ]
        )
    )


def write_csv(df: pl.DataFrame, filename: str) -> None:
    df.write_csv(OUTPUT_DIR / filename)


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    mapping = load_mapping()
    mapping_lf = mapping.lazy()
    lf = app_event_lf()
    enriched = lf.join(mapping_lf, on="online_item_code", how="left").with_columns(
        [
            pl.col("mapped_item_name").is_not_null().alias("is_product_matched"),
            pl.col("af_customer_user_id").is_in(ABNORMAL_USER_IDS).alias("is_abnormal_user"),
            EVENT_WEIGHT_EXPR.alias("event_weight"),
        ]
    )

    quality = enriched.select(
        [
            pl.len().alias("total_rows"),
            pl.col("af_customer_user_id").is_not_null().sum().alias("user_id_rows"),
            pl.col("online_item_code").is_not_null().sum().alias("online_item_code_rows"),
            pl.col("is_product_matched").sum().alias("matched_product_rows"),
            pl.col("is_abnormal_user").sum().alias("abnormal_user_rows"),
            pl.col("mapped_pos_item_code").n_unique().alias("matched_unique_pos_items"),
            pl.col("online_item_code").filter(pl.col("is_product_matched")).n_unique().alias(
                "matched_unique_online_items"
            ),
            pl.col("af_customer_user_id")
            .filter(~pl.col("is_abnormal_user") & pl.col("af_customer_user_id").is_not_null())
            .n_unique()
            .alias("clean_unique_users"),
        ]
    ).collect()
    total_rows = quality.item(0, "total_rows")
    quality = quality.with_columns(
        [
            (pl.col("matched_product_rows") / total_rows * 100).round(4).alias("matched_product_rows_pct"),
            (pl.col("abnormal_user_rows") / total_rows * 100).round(4).alias("abnormal_user_rows_pct"),
        ]
    )
    write_csv(quality, "cleaning_quality_summary.csv")

    event_matching = (
        enriched.group_by("Event Name")
        .agg(
            [
                pl.len().alias("rows"),
                pl.col("online_item_code").is_not_null().sum().alias("online_item_code_rows"),
                pl.col("is_product_matched").sum().alias("matched_product_rows"),
                pl.col("is_abnormal_user").sum().alias("abnormal_user_rows"),
            ]
        )
        .with_columns((pl.col("matched_product_rows") / pl.col("rows") * 100).round(4).alias("match_rate_pct"))
        .sort("rows", descending=True)
        .collect()
    )
    write_csv(event_matching, "matching_quality_by_event.csv")

    abnormal_users = (
        enriched.filter(pl.col("is_abnormal_user"))
        .group_by("af_customer_user_id")
        .agg(
            [
                pl.len().alias("event_count"),
                pl.col("Event Name").n_unique().alias("unique_event_names"),
                pl.col("event_date").min().alias("first_event_date"),
                pl.col("event_date").max().alias("last_event_date"),
            ]
        )
        .sort("event_count", descending=True)
        .collect()
    )
    write_csv(abnormal_users, "removed_abnormal_users.csv")

    clean_item_interactions = enriched.filter(
        pl.col("is_product_matched")
        & pl.col("af_customer_user_id").is_not_null()
        & ~pl.col("is_abnormal_user")
    ).select(
        [
            "event_date",
            "event_month",
            "event_hour",
            "Event Time",
            "Event Name",
            "event_weight",
            "af_customer_user_id",
            "AppsFlyer ID",
            "platform_norm",
            "media_source_norm",
            "campaign_norm",
            "online_item_code",
            "mapped_pos_item_code",
            "mapped_item_name",
            "item_type",
            "mapped_category_hierarchy",
            "category_l",
            "category_m",
            "category_s",
            "list_price",
        ]
    )
    clean_path = OUTPUT_DIR / "clean_app_item_interactions.parquet"
    clean_item_interactions.sink_parquet(clean_path)

    clean_lf = pl.scan_parquet(clean_path)
    top_products = (
        clean_lf.group_by(["mapped_pos_item_code", "mapped_item_name", "mapped_category_hierarchy"])
        .agg(
            [
                pl.len().alias("event_count"),
                pl.col("af_customer_user_id").n_unique().alias("unique_users"),
                pl.col("event_weight").sum().round(2).alias("weighted_interaction_score"),
                pl.col("event_date").min().alias("first_event_date"),
                pl.col("event_date").max().alias("last_event_date"),
            ]
        )
        .sort("weighted_interaction_score", descending=True)
        .head(200)
        .collect()
    )
    write_csv(top_products, "top200_matched_products.csv")

    monthly_category = (
        clean_lf.group_by(["event_month", "category_l"])
        .agg(
            [
                pl.len().alias("event_count"),
                pl.col("af_customer_user_id").n_unique().alias("unique_users"),
                pl.col("mapped_pos_item_code").n_unique().alias("unique_products"),
                pl.col("event_weight").sum().round(2).alias("weighted_interaction_score"),
            ]
        )
        .sort(["event_month", "weighted_interaction_score"], descending=[False, True])
        .collect()
    )
    write_csv(monthly_category, "monthly_category_interest.csv")

    graph_edges = (
        clean_lf.group_by(["af_customer_user_id", "mapped_pos_item_code"])
        .agg(
            [
                pl.len().alias("event_count"),
                pl.col("event_weight").sum().round(2).alias("edge_weight"),
                pl.col("event_date").min().alias("first_event_date"),
                pl.col("event_date").max().alias("last_event_date"),
            ]
        )
        .sort("edge_weight", descending=True)
    )
    graph_edges.sink_parquet(OUTPUT_DIR / "clean_user_product_edges.parquet")

    print(f"Saved cleaned app event outputs to: {OUTPUT_DIR}")
    print(quality)
    print(event_matching.head(12))
    print(abnormal_users)


if __name__ == "__main__":
    main()
