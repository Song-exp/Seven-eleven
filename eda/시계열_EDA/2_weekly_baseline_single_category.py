"""
요청사항 반영 스크립트
- 대상 대분류: 가공식품, 냉장, 냉동, 면, 미반
- 그래프 구성: (전체 상품 평균 베이스라인 + 대분류 1개) × 5개
- 시간 단위: 주(weekly, W-MON)
- 정규화: 시계열별 Min-Max(0~1)
- 출력 파일명 접두어: 2_
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Dict, Tuple

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import pyarrow.parquet as pq
from matplotlib import font_manager


ROOT = Path(__file__).resolve().parents[2]
DATA_PATH = ROOT / "data" / "pos_data_food_final_상품단위변환전.parquet"
OUT_DIR = Path(__file__).resolve().parent

DATE_COL = "영업일자"
CATEGORY_COL = "상품대분류명"
QTY_COL = "매출수량"

TARGET_CATEGORIES = ["가공식품", "냉장", "냉동", "면", "미반"]


def _set_korean_font() -> None:
    """OS별 한글 폰트 자동 설정."""
    candidate_fonts = ["AppleGothic", "Malgun Gothic", "NanumGothic"]
    installed = {f.name for f in font_manager.fontManager.ttflist}

    for font in candidate_fonts:
        if font in installed:
            plt.rcParams["font.family"] = font
            break

    plt.rcParams["axes.unicode_minus"] = False


def min_max_normalize_series(series: pd.Series) -> pd.Series:
    """1개 시계열 Min-Max 정규화(0~1)."""
    s = series.astype(float)
    min_v = s.min()
    max_v = s.max()
    if pd.isna(min_v) or pd.isna(max_v) or max_v == min_v:
        return pd.Series(0.0, index=s.index, name=series.name)
    return ((s - min_v) / (max_v - min_v)).rename(series.name)


def min_max_normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """열(시계열)별 Min-Max 정규화(0~1)."""
    out = pd.DataFrame(index=df.index)
    for col in df.columns:
        out[col] = min_max_normalize_series(df[col])
    return out


def build_daily_category_agg(parquet_path: Path) -> pd.DataFrame:
    """Parquet를 배치로 읽어 [date, category] 판매수량을 집계한다."""
    agg_map: Dict[Tuple[pd.Timestamp, str], float] = defaultdict(float)

    pf = pq.ParquetFile(parquet_path)
    batch_iter = pf.iter_batches(batch_size=1_000_000, columns=[DATE_COL, CATEGORY_COL, QTY_COL])

    for batch in batch_iter:
        df = batch.to_pandas()
        if df.empty:
            continue

        dates = pd.to_datetime(df[DATE_COL].astype("Int64").astype(str), format="%Y%m%d", errors="coerce")
        categories = df[CATEGORY_COL].astype("string")
        qty = pd.to_numeric(df[QTY_COL], errors="coerce").fillna(0.0)

        valid_mask = dates.notna() & categories.notna()
        if not valid_mask.any():
            continue

        chunk = pd.DataFrame(
            {
                "date": dates[valid_mask].dt.normalize(),
                "category": categories[valid_mask].astype(str),
                "qty": qty[valid_mask].astype(float),
            }
        )

        grouped = chunk.groupby(["date", "category"], as_index=False, sort=False)["qty"].sum()
        for row in grouped.itertuples(index=False):
            agg_map[(row.date, row.category)] += float(row.qty)

    records = [{"date": d, "category": c, "qty": q} for (d, c), q in agg_map.items()]
    out_df = pd.DataFrame(records)

    if out_df.empty:
        return pd.DataFrame(columns=["date", "category", "qty"])

    return out_df.sort_values(["date", "category"]).reset_index(drop=True)


def prepare_weekly_pivot(df_daily: pd.DataFrame) -> pd.DataFrame:
    """일별 집계 -> 주별(월요일 기준) 집계 피벗."""
    if df_daily.empty:
        return pd.DataFrame()

    daily_pivot = (
        df_daily.pivot_table(
            index="date",
            columns="category",
            values="qty",
            aggfunc="sum",
            fill_value=0.0,
        )
        .sort_index()
        .sort_index(axis=1)
    )

    full_daily_index = pd.date_range(daily_pivot.index.min(), daily_pivot.index.max(), freq="D")
    daily_pivot = daily_pivot.reindex(full_daily_index, fill_value=0.0)
    daily_pivot.index.name = "date"

    weekly_pivot = daily_pivot.resample("W-MON").sum()
    weekly_pivot.index.name = "week"
    return weekly_pivot


def plot_single_category_vs_baseline(
    weekly_norm: pd.DataFrame,
    baseline_norm: pd.Series,
    category: str,
    output_path: Path,
) -> None:
    """베이스라인 + 단일 대분류 라인차트."""
    if category not in weekly_norm.columns:
        return

    plt.figure(figsize=(15, 8))

    plt.plot(
        baseline_norm.index,
        baseline_norm.values,
        color="black",
        linewidth=3.0,
        label="전체 상품 판매량 평균(정규화)",
        zorder=5,
    )

    plt.plot(
        weekly_norm.index,
        weekly_norm[category],
        linewidth=2.3,
        label=f"{category}(정규화)",
    )

    plt.title(f"{category} vs 전체 평균 베이스라인 (주간 판매량, 정규화)")
    plt.xlabel("주(Week)")
    plt.ylabel("정규화 판매수량 (0~1)")

    ax = plt.gca()
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    plt.xticks(rotation=45)

    plt.legend(title="범례")
    plt.grid(alpha=0.25)
    plt.tight_layout()
    plt.savefig(output_path, dpi=220)
    plt.close()


def main() -> None:
    _set_korean_font()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    daily_df = build_daily_category_agg(DATA_PATH)
    weekly_raw = prepare_weekly_pivot(daily_df)
    weekly_norm = min_max_normalize_df(weekly_raw)

    baseline_raw = weekly_raw.mean(axis=1).rename("전체상품_주평균_원시")
    baseline_norm = min_max_normalize_series(baseline_raw).rename("전체상품_주평균_정규화")

    # 산출물 저장
    raw_csv_path = OUT_DIR / "2_weekly_sales_by_top_category_raw.csv"
    norm_csv_path = OUT_DIR / "2_weekly_sales_by_top_category_normalized.csv"
    baseline_csv_path = OUT_DIR / "2_weekly_sales_baseline_normalized.csv"

    weekly_raw.reset_index().to_csv(raw_csv_path, index=False, encoding="utf-8-sig")
    weekly_norm.reset_index().to_csv(norm_csv_path, index=False, encoding="utf-8-sig")
    baseline_norm.reset_index().to_csv(baseline_csv_path, index=False, encoding="utf-8-sig")

    for category in TARGET_CATEGORIES:
        output_path = OUT_DIR / f"2_weekly_baseline_vs_{category}.png"
        plot_single_category_vs_baseline(weekly_norm, baseline_norm, category, output_path)

    print("[DONE] 2_ 주간 베이스라인 비교 그래프 생성 완료")
    print(f"- RAW CSV: {raw_csv_path}")
    print(f"- NORM CSV: {norm_csv_path}")
    print(f"- BASELINE CSV: {baseline_csv_path}")
    for category in TARGET_CATEGORIES:
        print(f"- PNG: {OUT_DIR / f'2_weekly_baseline_vs_{category}.png'}")


if __name__ == "__main__":
    main()
