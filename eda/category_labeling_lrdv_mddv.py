"""
B4_ITEM_DV_INFO.csv에서 대분류-중분류 관계를 라벨링/정리하는 스크립트.

요구사항 반영
1) 대분류 라벨링
2) 대분류 안에 중분류를 넣어서 확인 가능한 형태 생성
3) 같은 중분류는 하나의 대분류 열에서 1번만 나타나도록 중복 제거

출력
- eda/outputs_category_labeling/lrdv_label_map.csv
- eda/outputs_category_labeling/lrdv_mddv_long_with_label.csv
- eda/outputs_category_labeling/lrdv_mddv_wide.csv
- eda/outputs_category_labeling/lrdv_mddv_summary.csv
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


INPUT_CSV = Path(
    "/Users/hyunoworld/Desktop/Seminar/세븐일레븐/Database/B4_ITEM_DV_INFO.csv"
)
OUTPUT_DIR = Path(__file__).resolve().parent / "outputs_category_labeling"

TOP_COL_CANDIDATES = [
    "ITEM_LRDV_NM",
    "상품대분류명",
    "대분류명",
]
MID_COL_CANDIDATES = [
    "ITEM_MDDV_NM",
    "상품중분류명",
    "중분류명",
]


def pick_column(columns: list[str], candidates: list[str], kind: str) -> str:
    for c in candidates:
        if c in columns:
            return c
    raise ValueError(f"{kind} 컬럼을 찾을 수 없습니다. columns={columns}")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(INPUT_CSV, dtype=str)
    df.columns = [c.strip() for c in df.columns]

    top_col = pick_column(df.columns.tolist(), TOP_COL_CANDIDATES, "대분류")
    mid_col = pick_column(df.columns.tolist(), MID_COL_CANDIDATES, "중분류")

    # 문자열 정리 + 결측 제거 + (대분류, 중분류) 중복 제거
    pair_df = (
        df[[top_col, mid_col]]
        .assign(
            **{
                top_col: lambda x: x[top_col].astype(str).str.strip(),
                mid_col: lambda x: x[mid_col].astype(str).str.strip(),
            }
        )
        .replace({top_col: {"": pd.NA}, mid_col: {"": pd.NA}})
        .dropna(subset=[top_col, mid_col])
        .drop_duplicates(subset=[top_col, mid_col])
        .sort_values([top_col, mid_col])
        .reset_index(drop=True)
    )

    # 대분류 라벨링 (0부터 순차 부여)
    top_values = sorted(pair_df[top_col].unique().tolist())
    label_map = pd.DataFrame(
        {
            "TOP_LABEL": range(len(top_values)),
            top_col: top_values,
        }
    )

    # 라벨 포함 long 테이블
    long_with_label = pair_df.merge(label_map, on=top_col, how="left")
    long_with_label = long_with_label[["TOP_LABEL", top_col, mid_col]].sort_values(
        ["TOP_LABEL", mid_col]
    )

    # 대분류별 중분류를 열(column)로 펼친 wide 테이블
    # 같은 중분류는 pair_df에서 이미 중복 제거되었으므로 열 내 유일하게 유지됨
    grouped = long_with_label.groupby(top_col)[mid_col].apply(list)
    wide_dict = {top: pd.Series(mids) for top, mids in grouped.items()}
    wide_df = pd.DataFrame(wide_dict)

    # 요약
    summary_df = (
        long_with_label.groupby(["TOP_LABEL", top_col], as_index=False)[mid_col]
        .nunique()
        .rename(columns={mid_col: "중분류_개수"})
        .sort_values("TOP_LABEL")
    )

    label_map_path = OUTPUT_DIR / "lrdv_label_map.csv"
    long_path = OUTPUT_DIR / "lrdv_mddv_long_with_label.csv"
    wide_path = OUTPUT_DIR / "lrdv_mddv_wide.csv"
    summary_path = OUTPUT_DIR / "lrdv_mddv_summary.csv"

    label_map.to_csv(label_map_path, index=False, encoding="utf-8-sig")
    long_with_label.to_csv(long_path, index=False, encoding="utf-8-sig")
    wide_df.to_csv(wide_path, index=False, encoding="utf-8-sig")
    summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")

    print("[DONE] 대분류-중분류 라벨링 테이블 생성 완료")
    print(f"- input: {INPUT_CSV}")
    print(f"- top_col: {top_col}, mid_col: {mid_col}")
    print(f"- 대분류 개수: {len(label_map)}")
    print(f"- (대분류,중분류) 유니크 쌍: {len(long_with_label)}")
    print(f"- label_map: {label_map_path}")
    print(f"- long: {long_path}")
    print(f"- wide: {wide_path}")
    print(f"- summary: {summary_path}")


if __name__ == "__main__":
    main()
