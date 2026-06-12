"""
대분류 기준 2025년 판매량 시계열 EDA 스크립트 (정규화 + 3개 그룹 분리).

분석 목적
- POS 판매 데이터에서 `상품대분류명` 단위의 판매량 추이를 2025년 1~12월 구간으로 확인한다.
- 판매수량 절댓값 대신 정규화된 값(0~1 Min-Max)으로 시각화하여,
  카테고리 간 스케일 차이를 줄이고 상대적 추세를 비교한다.
- 대분류를 아래 3개 묶음으로 나누어 각 1개 이미지씩 저장한다.
  1) 식품  2) 간식류  3) 음료

출력물
- monthly_sales_by_top_category_2025_raw.csv
- monthly_sales_by_top_category_2025_normalized.csv
- category_sales_trend_normalized_2025_식품.png
- category_sales_trend_normalized_2025_간식류.png
- category_sales_trend_normalized_2025_음료.png

주의
- data/raw, data/processed는 읽기 전용으로 취급한다.
- 대용량 메모리 사용을 피하기 위해 parquet를 배치 단위로 읽는다.
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
MONTH_START_INDEX = pd.date_range("2025-01-01", "2025-12-01", freq="MS")

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
    """OS 환경에 따라 사용 가능한 한글 폰트를 자동으로 설정한다."""
    candidate_fonts = ["AppleGothic", "Malgun Gothic", "NanumGothic"]
    installed = {f.name for f in font_manager.fontManager.ttflist}

    for font in candidate_fonts:
        if font in installed:
            plt.rcParams["font.family"] = font
            break

    plt.rcParams["axes.unicode_minus"] = False


def build_daily_category_agg_2025(parquet_path: Path) -> pd.DataFrame:
    """Parquet를 배치로 읽어 2025년 [date, category] 판매수량 합계를 구성한다."""
    agg_map: Dict[Tuple[pd.Timestamp, str], float] = defaultdict(float)

    pf = pq.ParquetFile(parquet_path)
    batch_iter = pf.iter_batches(
        batch_size=500_000,
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

        batch_daily = pd.DataFrame(
            {
                "date": dates[valid_mask].dt.normalize(),
                "category": categories[valid_mask].astype(str),
                "qty": qty[valid_mask].astype(float),
            }
        )

        batch_daily = batch_daily[(batch_daily["date"] >= YEAR_START) & (batch_daily["date"] <= YEAR_END)]
        if batch_daily.empty:
            continue

        grouped = batch_daily.groupby(["date", "category"], as_index=False, sort=False)["qty"].sum()
        for row in grouped.itertuples(index=False):
            agg_map[(row.date, row.category)] += float(row.qty)

    records = [{"date": d, "category": c, "qty": q} for (d, c), q in agg_map.items()]
    out_df = pd.DataFrame(records)

    if out_df.empty:
        return pd.DataFrame(columns=["date", "category", "qty"])

    return out_df.sort_values(["date", "category"]).reset_index(drop=True)


def prepare_monthly_pivot(df_2025: pd.DataFrame) -> pd.DataFrame:
    """2025년 월 x 대분류 피벗 생성 (누락값 0 보정)."""
    if df_2025.empty:
        return pd.DataFrame(index=MONTH_START_INDEX)

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

    full_daily_index = pd.date_range(YEAR_START, YEAR_END, freq="D")
    daily_pivot = daily_pivot.reindex(full_daily_index, fill_value=0.0)
    daily_pivot.index.name = "date"

    monthly_pivot = daily_pivot.resample("MS").sum().reindex(MONTH_START_INDEX, fill_value=0.0)
    monthly_pivot.index.name = "month"
    return monthly_pivot


def min_max_normalize(df: pd.DataFrame) -> pd.DataFrame:
    """열(카테고리)별 Min-Max 정규화 (0~1)."""
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


def plot_group_trend(normalized_monthly: pd.DataFrame, group_name: str, categories: list[str], output_path: Path) -> None:
    """카테고리 그룹별 정규화 월간 추이 라인차트 저장."""
    cols = [c for c in categories if c in normalized_monthly.columns]

    plt.figure(figsize=(14, 8))
    for col in cols:
        plt.plot(normalized_monthly.index, normalized_monthly[col], marker="o", linewidth=2.0, label=col)

    plt.title(f"{group_name} 대분류 월간 판매수량 추이 (정규화, 2025-01~2025-12)")
    plt.xlabel("월(Month)")
    plt.ylabel("정규화 판매수량 (0~1)")
    plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    plt.xticks(rotation=45)
    plt.legend(title="상품대분류", ncol=2, fontsize=9)
    plt.grid(alpha=0.25)
    plt.tight_layout()
    plt.savefig(output_path, dpi=220)
    plt.close()


def main() -> None:
    _set_korean_font()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    daily_2025 = build_daily_category_agg_2025(DATA_PATH)
    monthly_raw = prepare_monthly_pivot(daily_2025)
    monthly_norm = min_max_normalize(monthly_raw)

    raw_csv_path = OUT_DIR / "monthly_sales_by_top_category_2025_raw.csv"
    norm_csv_path = OUT_DIR / "monthly_sales_by_top_category_2025_normalized.csv"

    monthly_raw.reset_index().to_csv(raw_csv_path, index=False, encoding="utf-8-sig")
    monthly_norm.reset_index().to_csv(norm_csv_path, index=False, encoding="utf-8-sig")

    for group_name, categories in CATEGORY_GROUPS.items():
        output_png = OUT_DIR / f"category_sales_trend_normalized_2025_{group_name}.png"
        plot_group_trend(monthly_norm, group_name, categories, output_png)

    all_group_categories = {c for cols in CATEGORY_GROUPS.values() for c in cols}
    discovered_categories = set(monthly_raw.columns)
    unmapped = sorted(discovered_categories - all_group_categories)

    print("[DONE] 정규화 시계열 EDA 결과 생성 완료")
    print(f"- 기간: {YEAR_START.date()} ~ {YEAR_END.date()} (월 단위)")
    print(f"- RAW CSV: {raw_csv_path}")
    print(f"- NORM CSV: {norm_csv_path}")
    for group_name in CATEGORY_GROUPS:
        print(f"- PNG: {OUT_DIR / f'category_sales_trend_normalized_2025_{group_name}.png'}")
    if unmapped:
        print(f"- [주의] 미매핑 대분류: {unmapped}")
    else:
        print("- 미매핑 대분류 없음")


if __name__ == "__main__":
    main()
