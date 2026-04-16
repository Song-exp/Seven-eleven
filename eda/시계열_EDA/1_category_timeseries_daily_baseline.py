"""
대분류 기준 2025년 일별 판매량 시계열 EDA (정규화 + 전체평균 베이스라인 + 3개 그룹 분리).

요약
- 기준 데이터: pos_data_food_final_상품단위변환전.parquet
- 기간: 2025-01-01 ~ 2025-12-31 (일 단위)
- 지표: 매출수량
- 정규화: 열(시계열)별 Min-Max 정규화 (0~1)
- 베이스라인: 전체 대분류의 일별 평균 판매량 시계열(정규화), 검정색 라인
- 그룹 분리: 식품 / 간식류 / 음료

출력 파일 (모두 1_ 접두어)
- 1_daily_sales_by_top_category_2025_raw.csv
- 1_daily_sales_by_top_category_2025_normalized.csv
- 1_daily_sales_baseline_2025_normalized.csv
- 1_category_sales_trend_daily_normalized_2025_식품.png
- 1_category_sales_trend_daily_normalized_2025_간식류.png
- 1_category_sales_trend_daily_normalized_2025_음료.png
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

YEAR_START = pd.Timestamp("2025-01-01")
YEAR_END = pd.Timestamp("2025-12-31")
DAILY_INDEX_2025 = pd.date_range(YEAR_START, YEAR_END, freq="D")

CATEGORY_GROUPS = {
    "식품": [
        "가공식품",
        "냉동",
        "냉장",
        "면",
        "미반",
        "빵",
        "신선",
        "조리빵",
        "조미료/건물",
        "즉석 식품",
    ],
    "간식류": [
        "간식",
        "건강/기호식품",
        "과자",
        "디저트",
        "아이스크림",
        "안주",
    ],
    "음료": [
        "맥주",
        "양주와인",
        "유음료",
        "음료",
        "전통주",
        "즉석음료",
    ],
}


def _set_korean_font() -> None:
    """OS별 한글 폰트 자동 설정."""
    candidate_fonts = ["AppleGothic", "Malgun Gothic", "NanumGothic"]
    installed = {f.name for f in font_manager.fontManager.ttflist}

    for font in candidate_fonts:
        if font in installed:
            plt.rcParams["font.family"] = font
            break

    plt.rcParams["axes.unicode_minus"] = False


def build_daily_category_agg_2025(parquet_path: Path) -> pd.DataFrame:
    """Parquet를 배치로 읽어 2025년 [date, category] 판매수량을 집계한다."""
    agg_map: Dict[Tuple[pd.Timestamp, str], float] = defaultdict(float)

    pf = pq.ParquetFile(parquet_path)
    batch_iter = pf.iter_batches(
        batch_size=1_000_000,
        columns=[DATE_COL, CATEGORY_COL, QTY_COL],
    )

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

        chunk = chunk[(chunk["date"] >= YEAR_START) & (chunk["date"] <= YEAR_END)]
        if chunk.empty:
            continue

        grouped = chunk.groupby(["date", "category"], as_index=False, sort=False)["qty"].sum()
        for row in grouped.itertuples(index=False):
            agg_map[(row.date, row.category)] += float(row.qty)

    records = [{"date": d, "category": c, "qty": q} for (d, c), q in agg_map.items()]
    out_df = pd.DataFrame(records)

    if out_df.empty:
        return pd.DataFrame(columns=["date", "category", "qty"])

    return out_df.sort_values(["date", "category"]).reset_index(drop=True)


def prepare_daily_pivot(df_2025: pd.DataFrame) -> pd.DataFrame:
    """2025년 일자 x 대분류 피벗 생성 (누락일 0 보정)."""
    if df_2025.empty:
        return pd.DataFrame(index=DAILY_INDEX_2025)

    daily_pivot = (
        df_2025.pivot_table(
            index="date",
            columns="category",
            values="qty",
            aggfunc="sum",
            fill_value=0.0,
        )
        .sort_index()
        .sort_index(axis=1)
    )

    daily_pivot = daily_pivot.reindex(DAILY_INDEX_2025, fill_value=0.0)
    daily_pivot.index.name = "date"
    return daily_pivot


def min_max_normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """열(시계열)별 Min-Max 정규화 (0~1)."""
    if df.empty:
        return df.copy()

    normalized = pd.DataFrame(index=df.index)
    for col in df.columns:
        series = df[col].astype(float)
        min_v = series.min()
        max_v = series.max()
        if pd.isna(min_v) or pd.isna(max_v) or max_v == min_v:
            normalized[col] = 0.0
        else:
            normalized[col] = (series - min_v) / (max_v - min_v)

    return normalized


def min_max_normalize_series(series: pd.Series) -> pd.Series:
    """1개 시계열 Min-Max 정규화 (0~1)."""
    s = series.astype(float)
    min_v = s.min()
    max_v = s.max()
    if pd.isna(min_v) or pd.isna(max_v) or max_v == min_v:
        return pd.Series(0.0, index=s.index, name=series.name)
    return ((s - min_v) / (max_v - min_v)).rename(series.name)


def plot_group_daily(
    daily_norm: pd.DataFrame,
    baseline_norm: pd.Series,
    group_name: str,
    categories: list[str],
    output_path: Path,
) -> None:
    """그룹별 일 단위 정규화 라인차트(베이스라인 포함) 저장."""
    cols = [c for c in categories if c in daily_norm.columns]

    plt.figure(figsize=(16, 9))

    # 전체 상품 평균(대분류 평균) 베이스라인: 검정색
    plt.plot(
        baseline_norm.index,
        baseline_norm.values,
        color="black",
        linewidth=3.0,
        label="전체 상품 판매량 평균(정규화)",
        zorder=5,
    )

    for col in cols:
        plt.plot(daily_norm.index, daily_norm[col], linewidth=1.7, alpha=0.92, label=col)

    plt.title(f"{group_name} 대분류 일별 판매량 추이 (정규화, 2025)")
    plt.xlabel("일(Date)")
    plt.ylabel("정규화 판매수량 (0~1)")
    ax = plt.gca()
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    plt.xticks(rotation=45)
    plt.legend(title="범례", ncol=2, fontsize=9)
    plt.grid(alpha=0.25)
    plt.tight_layout()
    plt.savefig(output_path, dpi=220)
    plt.close()


def main() -> None:
    _set_korean_font()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    daily_2025 = build_daily_category_agg_2025(DATA_PATH)
    daily_raw = prepare_daily_pivot(daily_2025)
    daily_norm = min_max_normalize_df(daily_raw)

    baseline_raw = daily_raw.mean(axis=1).rename("전체상품_일평균_원시")
    baseline_norm = min_max_normalize_series(baseline_raw).rename("전체상품_일평균_정규화")

    raw_csv_path = OUT_DIR / "1_daily_sales_by_top_category_2025_raw.csv"
    norm_csv_path = OUT_DIR / "1_daily_sales_by_top_category_2025_normalized.csv"
    baseline_csv_path = OUT_DIR / "1_daily_sales_baseline_2025_normalized.csv"

    daily_raw.reset_index().to_csv(raw_csv_path, index=False, encoding="utf-8-sig")
    daily_norm.reset_index().to_csv(norm_csv_path, index=False, encoding="utf-8-sig")
    baseline_norm.reset_index().to_csv(baseline_csv_path, index=False, encoding="utf-8-sig")

    for group_name, categories in CATEGORY_GROUPS.items():
        output_png = OUT_DIR / f"1_category_sales_trend_daily_normalized_2025_{group_name}.png"
        plot_group_daily(daily_norm, baseline_norm, group_name, categories, output_png)

    print("[DONE] 일 단위 정규화 + 베이스라인 그래프 생성 완료")
    print(f"- 기간: {YEAR_START.date()} ~ {YEAR_END.date()} (일 단위)")
    print(f"- RAW CSV: {raw_csv_path}")
    print(f"- NORM CSV: {norm_csv_path}")
    print(f"- BASELINE CSV: {baseline_csv_path}")
    for group_name in CATEGORY_GROUPS:
        print(f"- PNG: {OUT_DIR / f'1_category_sales_trend_daily_normalized_2025_{group_name}.png'}")


if __name__ == "__main__":
    main()
