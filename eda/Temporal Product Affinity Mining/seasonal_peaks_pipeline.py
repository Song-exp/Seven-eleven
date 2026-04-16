"""
seasonal_peaks_pipeline.py
--------------------------
특정 시기(기념일·명절)에 집중 판매되는 상품을 추출하여
지식 그래프의 PEAKS_IN 엣지 후보를 생성한다.

출력: DataFrame  (상품코드, 상품명, 시기키, lift, specificity, period_qty)
      → 이후 Neo4j PEAKS_IN 엣지로 적재

기준 데이터: pos_data_food_final_상품단위변환전.parquet (2025년)
"""

import pandas as pd
import numpy as np
from pathlib import Path

# ── 경로 설정 ──────────────────────────────────────────────────────────────
DATA_DIR = Path(__file__).parent.parent / "Database"
PARQUET_PATH = DATA_DIR / "pos_data_food_final_상품단위변환전.parquet"
OUTPUT_PATH  = Path(__file__).parent / "seasonal_peaks_output.csv"

# ── 시기 정의 (2025년 기준, 1주 창) ────────────────────────────────────────
SEASONAL_PERIODS = {
    "VALENTINE":     ("2025-02-08", "2025-02-14"),
    "WHITE_DAY":     ("2025-03-08", "2025-03-14"),
    "CHILDRENS_DAY": ("2025-04-29", "2025-05-05"),
    "PARENTS_DAY":   ("2025-05-02", "2025-05-08"),
    "PEPERO_DAY":    ("2025-11-05", "2025-11-11"),
    "CSAT":          ("2025-11-07", "2025-11-13"),
    "CHRISTMAS":     ("2025-12-19", "2025-12-25"),
    "SEOLLAL":       ("2025-01-23", "2025-01-29"),
    "CHUSEOK":       ("2025-09-30", "2025-10-06"),
}

# ── 파라미터 ────────────────────────────────────────────────────────────────
LIFT_THRESHOLD    = 1.5   # lift 최소값
ABS_MIN_QTY       = 10    # 시기 내 총 판매량 절대 하한선
REL_RATIO         = 0.5   # 평소 7일치 대비 비율
TOP_K             = 20    # 시기별 상위 K개 상품


def load_daily_sales() -> pd.DataFrame:
    """parquet → 상품·날짜 단위 일별 전국 집계 반환."""
    print("[1/4] parquet 로딩 중...")
    df = pd.read_parquet(
        PARQUET_PATH,
        columns=["영업일자", "상품코드", "상품명", "매출수량"]
    )

    # 날짜 변환: YYYYMMDD int → date
    df["date"] = pd.to_datetime(df["영업일자"].astype(str), format="%Y%m%d").dt.date

    # 전국 집계 (점포 합산)
    daily = (
        df.groupby(["date", "상품코드", "상품명"], observed=True)["매출수량"]
        .sum()
        .reset_index()
        .rename(columns={"매출수량": "qty"})
    )
    daily["date"] = pd.to_datetime(daily["date"])
    print(f"    일별 집계 완료: {len(daily):,} 행")
    return daily


def get_annual_qty(daily: pd.DataFrame) -> pd.Series:
    """상품별 연간 총 판매량 (specificity 분모)."""
    return daily.groupby("상품코드")["qty"].sum()


def analyze_period(
    period_key: str,
    start: str,
    end: str,
    daily: pd.DataFrame,
    annual_qty: pd.Series,
) -> pd.DataFrame:
    """
    단일 시기에 대해 lift·specificity를 계산하고 필터링된 상품 목록을 반환.
    """
    p_start = pd.Timestamp(start)
    p_end   = pd.Timestamp(end)
    period_days = (p_end - p_start).days + 1

    months_in_period = sorted(
        daily.loc[
            (daily["date"] >= p_start) & (daily["date"] <= p_end), "date"
        ].dt.month.unique()
    )

    # ── 시기 내 판매량 집계 ────────────────────────────────────────────────
    mask_period = (daily["date"] >= p_start) & (daily["date"] <= p_end)
    period_df = (
        daily[mask_period]
        .groupby(["상품코드", "상품명"])["qty"]
        .sum()
        .reset_index()
        .rename(columns={"qty": "period_qty"})
    )
    period_df["period_daily_avg"] = period_df["period_qty"] / period_days

    # ── 평소 기간: 동일 월 내 시기 제외 구간 ─────────────────────────────
    mask_baseline = (
        daily["date"].dt.month.isin(months_in_period)
        & ~((daily["date"] >= p_start) & (daily["date"] <= p_end))
    )
    baseline_df_raw = daily[mask_baseline]
    baseline_days = baseline_df_raw["date"].nunique()

    if baseline_days == 0:
        print(f"  [{period_key}] 평소 기간 없음, 건너뜀")
        return pd.DataFrame()

    baseline_df = (
        baseline_df_raw
        .groupby(["상품코드", "상품명"])["qty"]
        .sum()
        .reset_index()
        .rename(columns={"qty": "baseline_qty"})
    )
    baseline_df["baseline_daily_avg"] = baseline_df["baseline_qty"] / baseline_days

    # ── 병합 ──────────────────────────────────────────────────────────────
    merged = period_df.merge(baseline_df, on=["상품코드", "상품명"], how="left")
    merged["baseline_daily_avg"] = merged["baseline_daily_avg"].fillna(0)
    merged["baseline_qty"]       = merged["baseline_qty"].fillna(0)

    # ── lift 계산 ─────────────────────────────────────────────────────────
    # 평소 일평균 = 0 인 상품은 lift = inf (신규 시기 상품)
    merged["lift"] = np.where(
        merged["baseline_daily_avg"] > 0,
        merged["period_daily_avg"] / merged["baseline_daily_avg"],
        np.inf
    )

    # ── 최소 판매량 N ─────────────────────────────────────────────────────
    merged["N"] = np.maximum(
        merged["baseline_daily_avg"] * 7 * REL_RATIO,
        ABS_MIN_QTY
    )

    # ── specificity ───────────────────────────────────────────────────────
    merged["annual_qty"]    = merged["상품코드"].map(annual_qty).fillna(0)
    merged["specificity"]   = np.where(
        merged["annual_qty"] > 0,
        merged["period_qty"] / merged["annual_qty"],
        0.0
    )

    # ── 최종 필터 (AND) ───────────────────────────────────────────────────
    result = merged[
        (merged["lift"] >= LIFT_THRESHOLD) &
        (merged["period_qty"] >= merged["N"])
    ].copy()

    result["period_key"] = period_key
    result["period_start"] = start
    result["period_end"]   = end

    result = result.sort_values("lift", ascending=False)
    print(f"  [{period_key}] 유효 상품 {len(result)}개  (lift≥{LIFT_THRESHOLD}, qty≥N)")
    return result


def build_peaks_in_edges(results: pd.DataFrame) -> pd.DataFrame:
    """
    시기별 상위 K개만 남겨 PEAKS_IN 엣지 테이블 생성.
    컬럼: 상품코드, 상품명, period_key, lift, specificity, period_qty
    """
    edges = (
        results
        .sort_values(["period_key", "lift"], ascending=[True, False])
        .groupby("period_key", group_keys=False)
        .head(TOP_K)
        [["상품코드", "상품명", "period_key", "period_start", "period_end",
          "lift", "specificity", "period_qty", "period_daily_avg",
          "baseline_daily_avg", "annual_qty"]]
        .reset_index(drop=True)
    )
    return edges


def main():
    daily      = load_daily_sales()
    annual_qty = get_annual_qty(daily)

    print("[2/4] 시기별 분석 시작...")
    all_results = []
    for key, (start, end) in SEASONAL_PERIODS.items():
        res = analyze_period(key, start, end, daily, annual_qty)
        if not res.empty:
            all_results.append(res)

    if not all_results:
        print("유효 결과 없음.")
        return

    results = pd.concat(all_results, ignore_index=True)

    print("[3/4] PEAKS_IN 엣지 테이블 생성...")
    edges = build_peaks_in_edges(results)

    print("[4/4] 저장 중...")
    edges.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
    print(f"    저장 완료: {OUTPUT_PATH}")
    print(f"    총 엣지 수: {len(edges)}")

    # ── 요약 출력 ──────────────────────────────────────────────────────────
    print("\n=== 시기별 엣지 수 ===")
    print(edges.groupby("period_key")["상품코드"].count().to_string())

    print("\n=== 샘플 (lift 상위 10개) ===")
    top = edges.nlargest(10, "lift")[
        ["period_key", "상품명", "lift", "specificity", "period_qty"]
    ]
    print(top.to_string(index=False))

    return edges


if __name__ == "__main__":
    edges = main()
