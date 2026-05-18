"""
Inventory search signal analysis for app events.

Business question
-----------------
Can inventory-related app behavior be used as an early interest signal before
POS sales?

Important logging constraint
----------------------------
The current inventory events do not carry a direct product id:

* af_search_inventory: Event Value has af_search_string only.
* af_content_view_inventory: Event Value has af_content only.

Therefore product-level analysis is possible only through a conservative
search-string-to-product-name candidate match. These outputs should be treated
as hypothesis-generation artifacts, not as confirmed product attribution.

Inputs are read-only:
* data/processed/app_event_integrated/*.parquet
* data/raw/app/상품코드목록_260416.csv
* data/processed/B2_POS_SALE.parquet

Outputs are written to:
* eda/app_event_yumi/outputs/inventory_signal_analysis/
"""

from __future__ import annotations

import glob
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
os.environ.setdefault("MPLCONFIGDIR", "/private/tmp/seven_eleven_mplconfig")
os.environ.setdefault("XDG_CACHE_HOME", "/private/tmp/seven_eleven_cache")

import matplotlib.pyplot as plt
import polars as pl
import seaborn as sns


APP_EVENT_DIR = PROJECT_ROOT / "data" / "processed" / "app_event_integrated"
MAPPING_FILE = PROJECT_ROOT / "data" / "raw" / "app" / "상품코드목록_260416.csv"
POS_FILE = PROJECT_ROOT / "data" / "processed" / "B2_POS_SALE.parquet"
OUTPUT_DIR = Path(__file__).resolve().parent / "outputs" / "inventory_signal_analysis"

ABNORMAL_USER_IDS = {"L00000000000"}
INVENTORY_EVENTS = ["af_search_inventory", "af_content_view_inventory"]
DEFAULT_TOP_TERMS = 500
DEFAULT_MAX_LAG_DAYS = 14


@dataclass(frozen=True)
class MatchCandidate:
    inventory_search_string: str
    search_string_norm: str
    mapped_pos_item_code: str
    mapped_item_name: str
    mapped_category_hierarchy: str | None
    category_l: str | None
    category_m: str | None
    match_type: str
    match_score: int


def normalize_text(value: str | None) -> str:
    """Normalize Korean/product strings for simple deterministic matching."""
    if value is None:
        return ""
    value = value.lower()
    value = re.sub(r"\([^)]*\)", "", value)
    value = re.sub(r"[^0-9a-z가-힣]+", "", value)
    return value


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
                pl.col("Event Time")
                .str.slice(0, 10)
                .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
                .alias("event_date"),
                pl.col("Event Time").str.slice(0, 7).alias("event_month"),
                hour_24.alias("event_hour"),
                pl.col("Platform").str.to_lowercase().alias("platform_norm"),
                pl.col("Media Source").fill_null("unknown").alias("media_source_norm"),
            ]
        )
    )


def inventory_lf(lf: pl.LazyFrame | None = None) -> pl.LazyFrame:
    source = app_event_lf() if lf is None else lf
    return source.filter(pl.col("Event Name").is_in(INVENTORY_EVENTS)).with_columns(
        [
            pl.col("Event Value")
            .str.json_path_match("$.af_search_string")
            .str.strip_chars()
            .alias("inventory_search_string"),
            pl.col("af_customer_user_id").is_in(ABNORMAL_USER_IDS).alias("is_abnormal_user"),
        ]
    )


def load_mapping() -> pl.DataFrame:
    mapping = pl.read_csv(MAPPING_FILE, encoding="cp949", infer_schema_length=0)
    return (
        mapping.rename(
            {
                "상품코드": "mapped_pos_item_code",
                "상품명": "mapped_item_name",
                "상품분류카테고리": "mapped_category_hierarchy",
                "정가": "list_price",
            }
        )
        .with_columns(
            [
                pl.col("mapped_pos_item_code").cast(pl.Utf8).str.strip_chars(),
                pl.col("mapped_item_name").cast(pl.Utf8).str.strip_chars(),
                pl.col("mapped_category_hierarchy").str.split_exact(">", 2).alias("category_parts"),
                pl.col("list_price").cast(pl.Int64, strict=False),
            ]
        )
        .with_columns(
            [
                pl.col("category_parts").struct.field("field_0").alias("category_l"),
                pl.col("category_parts").struct.field("field_1").alias("category_m"),
                pl.col("category_parts").struct.field("field_2").alias("category_s"),
                pl.col("mapped_item_name").map_elements(normalize_text, return_dtype=pl.Utf8).alias("item_name_norm"),
            ]
        )
        .drop("category_parts")
        .filter(pl.col("item_name_norm").str.len_chars() >= 2)
        .unique(["mapped_pos_item_code", "mapped_item_name"], keep="first")
    )


def save_csv(df: pl.DataFrame, filename: str, output_dir: Path = OUTPUT_DIR) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / filename
    df.write_csv(path)
    return path


def save_barplot(
    df: pl.DataFrame,
    x: str,
    y: str,
    title: str,
    xlabel: str,
    ylabel: str,
    filename: str,
    output_dir: Path = OUTPUT_DIR,
    figsize: tuple[int, int] = (12, 8),
) -> Path | None:
    if df.is_empty():
        return None
    output_dir.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=figsize)
    sns.barplot(data=df.to_pandas(), x=x, y=y)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.tight_layout()
    path = output_dir / filename
    plt.savefig(path, dpi=160)
    plt.close()
    return path


def collect_inventory_event_summary(inv: pl.LazyFrame) -> pl.DataFrame:
    return (
        inv.group_by("Event Name")
        .agg(
            [
                pl.len().alias("event_count"),
                pl.col("af_customer_user_id").n_unique().alias("unique_users"),
                pl.col("is_abnormal_user").sum().alias("abnormal_user_events"),
                pl.col("inventory_search_string").is_not_null().sum().alias("search_string_rows"),
                pl.col("af_content_id").is_not_null().sum().alias("af_content_id_rows"),
                pl.col("af_content").is_not_null().sum().alias("af_content_rows"),
            ]
        )
        .with_columns(
            [
                (pl.col("search_string_rows") / pl.col("event_count") * 100).round(4).alias(
                    "search_string_coverage_pct"
                ),
                (pl.col("af_content_id_rows") / pl.col("event_count") * 100).round(4).alias(
                    "af_content_id_coverage_pct"
                ),
                (pl.col("abnormal_user_events") / pl.col("event_count") * 100).round(4).alias(
                    "abnormal_user_event_pct"
                ),
            ]
        )
        .sort("event_count", descending=True)
        .collect()
    )


def collect_daily_inventory_funnel(inv: pl.LazyFrame) -> pl.DataFrame:
    daily = (
        inv.group_by(["event_date", "Event Name"])
        .agg(
            [
                pl.len().alias("event_count"),
                pl.col("af_customer_user_id").filter(~pl.col("is_abnormal_user")).n_unique().alias("clean_unique_users"),
            ]
        )
        .collect()
        .pivot(
            values=["event_count", "clean_unique_users"],
            index="event_date",
            on="Event Name",
            aggregate_function="first",
        )
        .fill_null(0)
        .sort("event_date")
    )

    search_col = "event_count_af_search_inventory"
    view_col = "event_count_af_content_view_inventory"
    if search_col in daily.columns and view_col in daily.columns:
        daily = daily.with_columns(
            (pl.col(view_col) / pl.col(search_col).clip(1) * 100).round(4).alias(
                "inventory_view_per_100_searches"
            )
        )
    return daily


def collect_search_terms(inv: pl.LazyFrame) -> pl.DataFrame:
    return (
        inv.filter(
            (pl.col("Event Name") == "af_search_inventory")
            & pl.col("inventory_search_string").is_not_null()
            & ~pl.col("is_abnormal_user")
        )
        .with_columns(
            pl.col("inventory_search_string")
            .map_elements(normalize_text, return_dtype=pl.Utf8)
            .alias("search_string_norm")
        )
        .filter(pl.col("search_string_norm").str.len_chars() >= 2)
        .group_by(["inventory_search_string", "search_string_norm"])
        .agg(
            [
                pl.len().alias("search_count"),
                pl.col("af_customer_user_id").n_unique().alias("unique_users"),
                pl.col("event_date").min().alias("first_search_date"),
                pl.col("event_date").max().alias("last_search_date"),
            ]
        )
        .sort("search_count", descending=True)
        .collect()
    )


def collect_daily_search_terms(inv: pl.LazyFrame, search_terms: pl.DataFrame) -> pl.DataFrame:
    top_terms = search_terms.select("search_string_norm")
    return (
        inv.filter(
            (pl.col("Event Name") == "af_search_inventory")
            & pl.col("inventory_search_string").is_not_null()
            & ~pl.col("is_abnormal_user")
        )
        .with_columns(
            pl.col("inventory_search_string")
            .map_elements(normalize_text, return_dtype=pl.Utf8)
            .alias("search_string_norm")
        )
        .join(top_terms.lazy(), on="search_string_norm", how="inner")
        .group_by(["event_date", "search_string_norm"])
        .agg(
            [
                pl.len().alias("search_count"),
                pl.col("af_customer_user_id").n_unique().alias("unique_users"),
            ]
        )
        .sort(["search_string_norm", "event_date"])
        .collect()
    )


def classify_match(search_norm: str, item_norm: str) -> tuple[str, int] | None:
    if not search_norm or not item_norm:
        return None
    if search_norm == item_norm:
        return "exact_norm", 100
    if item_norm.startswith(search_norm):
        return "item_startswith_search", 90
    if search_norm.startswith(item_norm) and len(item_norm) >= 4:
        return "search_startswith_item", 85
    if search_norm in item_norm:
        return "search_in_item", 80
    if item_norm in search_norm and len(item_norm) >= 4:
        return "item_in_search", 75
    return None


def match_search_terms_to_products(
    search_terms: pl.DataFrame,
    mapping: pl.DataFrame,
    top_n_terms: int = DEFAULT_TOP_TERMS,
) -> pl.DataFrame:
    terms = search_terms.head(top_n_terms).select(
        ["inventory_search_string", "search_string_norm", "search_count", "unique_users"]
    )
    products = mapping.select(
        [
            "mapped_pos_item_code",
            "mapped_item_name",
            "mapped_category_hierarchy",
            "category_l",
            "category_m",
            "item_name_norm",
        ]
    )

    candidates: list[MatchCandidate] = []
    product_rows = products.iter_rows(named=True)
    product_cache = list(product_rows)

    for term in terms.iter_rows(named=True):
        search_norm = term["search_string_norm"]
        if len(search_norm) < 2:
            continue
        for product in product_cache:
            classified = classify_match(search_norm, product["item_name_norm"])
            if classified is None:
                continue
            match_type, match_score = classified
            candidates.append(
                MatchCandidate(
                    inventory_search_string=term["inventory_search_string"],
                    search_string_norm=search_norm,
                    mapped_pos_item_code=product["mapped_pos_item_code"],
                    mapped_item_name=product["mapped_item_name"],
                    mapped_category_hierarchy=product["mapped_category_hierarchy"],
                    category_l=product["category_l"],
                    category_m=product["category_m"],
                    match_type=match_type,
                    match_score=match_score,
                )
            )

    if not candidates:
        return pl.DataFrame(
            schema={
                "inventory_search_string": pl.Utf8,
                "search_string_norm": pl.Utf8,
                "mapped_pos_item_code": pl.Utf8,
                "mapped_item_name": pl.Utf8,
                "mapped_category_hierarchy": pl.Utf8,
                "category_l": pl.Utf8,
                "category_m": pl.Utf8,
                "match_type": pl.Utf8,
                "match_score": pl.Int64,
                "search_count": pl.Int64,
                "unique_users": pl.Int64,
            }
        )

    return (
        pl.DataFrame([candidate.__dict__ for candidate in candidates])
        .join(terms, on=["inventory_search_string", "search_string_norm"], how="left")
        .sort(["search_count", "search_string_norm", "match_score"], descending=[True, False, True])
    )


def best_product_matches(candidates: pl.DataFrame) -> pl.DataFrame:
    if candidates.is_empty():
        return candidates
    return (
        candidates.sort(
            ["search_count", "search_string_norm", "match_score", "mapped_item_name"],
            descending=[True, False, True, False],
        )
        .group_by("search_string_norm", maintain_order=True)
        .head(1)
    )


def collect_matched_daily_searches(
    daily_search_terms: pl.DataFrame,
    best_matches: pl.DataFrame,
) -> pl.DataFrame:
    if daily_search_terms.is_empty() or best_matches.is_empty():
        return pl.DataFrame()
    return (
        daily_search_terms.join(
            best_matches.select(
                [
                    "search_string_norm",
                    "inventory_search_string",
                    "mapped_pos_item_code",
                    "mapped_item_name",
                    "category_l",
                    "category_m",
                    "match_type",
                    "match_score",
                ]
            ),
            on="search_string_norm",
            how="inner",
        )
        .group_by(
            [
                "event_date",
                "mapped_pos_item_code",
                "mapped_item_name",
                "category_l",
                "category_m",
                "match_type",
                "match_score",
            ]
        )
        .agg(
            [
                pl.col("search_count").sum().alias("inventory_search_count"),
                pl.col("unique_users").sum().alias("inventory_search_users"),
                pl.col("inventory_search_string").first().alias("representative_search_string"),
            ]
        )
        .sort(["mapped_pos_item_code", "event_date"])
    )


def collect_pos_daily_sales(product_codes: list[str], start_date, end_date) -> pl.DataFrame:
    if not product_codes:
        return pl.DataFrame()
    return (
        pl.scan_parquet(POS_FILE)
        .filter(
            pl.col("상품코드").is_in(product_codes)
            & (pl.col("판매일자") >= start_date)
            & (pl.col("판매일자") <= end_date)
        )
        .group_by(["판매일자", "상품코드"])
        .agg(
            [
                pl.col("판매수량").sum().alias("pos_sales_qty"),
                pl.col("판매금액").sum().alias("pos_sales_amount"),
                pl.col("거래번호").n_unique().alias("pos_receipts"),
            ]
        )
        .rename({"판매일자": "event_date", "상품코드": "mapped_pos_item_code"})
        .sort(["mapped_pos_item_code", "event_date"])
        .collect()
    )


def collect_lead_lag_correlations(
    matched_daily_searches: pl.DataFrame,
    pos_daily_sales: pl.DataFrame,
    max_lag_days: int = DEFAULT_MAX_LAG_DAYS,
) -> pl.DataFrame:
    if matched_daily_searches.is_empty() or pos_daily_sales.is_empty():
        return pl.DataFrame()

    rows = []
    search_base = matched_daily_searches.select(
        [
            "event_date",
            "mapped_pos_item_code",
            "mapped_item_name",
            "category_l",
            "category_m",
            "inventory_search_count",
            "inventory_search_users",
        ]
    )
    for lag in range(max_lag_days + 1):
        joined = (
            search_base.with_columns((pl.col("event_date") + pl.duration(days=lag)).alias("sales_date"))
            .join(
                pos_daily_sales.rename({"event_date": "sales_date"}),
                on=["mapped_pos_item_code", "sales_date"],
                how="inner",
            )
            .filter(pl.col("inventory_search_count") > 0)
        )
        if joined.is_empty():
            continue
        corr = (
            joined.group_by(["mapped_pos_item_code", "mapped_item_name", "category_l", "category_m"])
            .agg(
                [
                    pl.lit(lag).alias("lag_days"),
                    pl.len().alias("paired_days"),
                    pl.col("inventory_search_count").sum().alias("total_searches"),
                    pl.col("pos_sales_qty").sum().alias("total_sales_qty"),
                    pl.corr("inventory_search_count", "pos_sales_qty").round(5).alias("search_sales_qty_corr"),
                    pl.corr("inventory_search_users", "pos_sales_qty").round(5).alias("user_sales_qty_corr"),
                ]
            )
            .filter(pl.col("paired_days") >= 7)
        )
        rows.append(corr)

    if not rows:
        return pl.DataFrame()
    return (
        pl.concat(rows, how="vertical")
        .filter(pl.col("search_sales_qty_corr").is_finite())
        .sort(["search_sales_qty_corr", "paired_days"], descending=[True, True])
    )


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    sns.set_theme(style="whitegrid", font="AppleGothic")
    plt.rcParams["axes.unicode_minus"] = False

    inv = inventory_lf()

    summary = collect_inventory_event_summary(inv)
    save_csv(summary, "inventory_event_summary.csv")

    daily_funnel = collect_daily_inventory_funnel(inv)
    save_csv(daily_funnel, "daily_inventory_funnel.csv")

    search_terms = collect_search_terms(inv)
    save_csv(search_terms, "inventory_search_terms.csv")
    save_csv(search_terms.head(200), "top200_inventory_search_terms.csv")

    save_barplot(
        search_terms.head(30),
        x="search_count",
        y="inventory_search_string",
        title="Top 30 Inventory Search Terms",
        xlabel="Search count",
        ylabel="Search term",
        filename="top30_inventory_search_terms.png",
    )

    mapping = load_mapping()
    candidates = match_search_terms_to_products(search_terms, mapping)
    save_csv(candidates, "inventory_search_product_candidates.csv")

    best_matches = best_product_matches(candidates)
    save_csv(best_matches, "inventory_search_best_product_matches.csv")

    daily_search_terms = collect_daily_search_terms(inv, search_terms.head(DEFAULT_TOP_TERMS))
    save_csv(daily_search_terms, "daily_top_inventory_search_terms.csv")

    matched_daily_searches = collect_matched_daily_searches(daily_search_terms, best_matches)
    save_csv(matched_daily_searches, "daily_inventory_searches_matched_products.csv")

    if not matched_daily_searches.is_empty():
        start_date = matched_daily_searches["event_date"].min()
        end_date = matched_daily_searches["event_date"].max()
        product_codes = matched_daily_searches["mapped_pos_item_code"].unique().to_list()
        pos_daily_sales = collect_pos_daily_sales(product_codes, start_date, end_date)
    else:
        pos_daily_sales = pl.DataFrame()
    save_csv(pos_daily_sales, "pos_daily_sales_for_inventory_matched_products.csv")

    lead_lag = collect_lead_lag_correlations(matched_daily_searches, pos_daily_sales)
    save_csv(lead_lag, "inventory_search_pos_lead_lag_correlations.csv")
    if not lead_lag.is_empty():
        save_csv(
            lead_lag.filter(pl.col("lag_days") > 0).head(100),
            "top100_positive_lag_inventory_search_pos_correlations.csv",
        )

    notes = {
        "inventory_event_product_id_status": (
            "Inventory events do not expose a direct product id in af_content_id. "
            "af_search_inventory has af_search_string; af_content_view_inventory has only af_content."
        ),
        "matching_policy": (
            "Top inventory search strings are normalized and matched to product names with exact, prefix, "
            "or substring rules. Results are candidate attributions."
        ),
        "lead_lag_policy": (
            "Correlation compares daily inventory search counts with POS sales for the same candidate product "
            "after lag_days. lag_days=1 means search today vs POS sales tomorrow."
        ),
    }
    (OUTPUT_DIR / "analysis_notes.json").write_text(json.dumps(notes, ensure_ascii=False, indent=2) + "\n")

    print(f"Saved inventory signal analysis outputs to: {OUTPUT_DIR}")
    print(summary)
    print(search_terms.head(20))
    print(best_matches.head(20))
    print(lead_lag.head(20) if not lead_lag.is_empty() else "No lead-lag rows produced")


if __name__ == "__main__":
    main()
