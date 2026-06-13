"""
raw 폴더의 CSV/XLSX 파일을 data/processed/에 parquet으로 변환하고
B4_ITEM_DV_INFO_with_survival.csv를 생성합니다.
"""

import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RAW = os.path.join(BASE, "data", "raw")
PROCESSED = os.path.join(BASE, "data", "processed")


def convert_b4():
    src = os.path.join(RAW, "B4_ITEM_DV_INFO.csv")
    dst = os.path.join(PROCESSED, "B4_ITEM_DV_INFO.parquet")
    print("[B4] CSV → parquet 변환 중...")
    df = pd.read_csv(src, encoding="utf-8", dtype={"ITEM_CD": str})
    df["ITEM_CD"] = df["ITEM_CD"].str.zfill(6)
    df.to_parquet(dst, index=False, engine="pyarrow")
    print(f"  완료: {dst}  ({len(df):,}행, {df.columns.tolist()})")
    return df


def convert_b5():
    src = os.path.join(RAW, "B5_MNM_DATA.xlsx")
    dst = os.path.join(PROCESSED, "B5_MNM_DATA.parquet")
    print("[B5] XLSX → parquet 변환 중...")
    df = pd.read_excel(src)
    # 상품코드 컬럼 str 정규화
    for col in df.columns:
        if "상품코드" in col or "ITEM_CD" in col:
            df[col] = df[col].astype(str).str.strip()
            break
    df.to_parquet(dst, index=False, engine="pyarrow")
    print(f"  완료: {dst}  ({len(df):,}행)")


def make_b4_with_survival(df_b4):
    pareto_src = os.path.join(RAW, "카테고리필터링_pareto기준자르기.xlsx")
    dst = os.path.join(PROCESSED, "B4_ITEM_DV_INFO_with_survival.csv")
    print("[B4_with_survival] 생성 중...")

    df_pareto = pd.read_excel(pareto_src)
    # 마지막 컬럼이 사용여부 (값: 'O' = 생존)
    usage_col = df_pareto.columns[-1]
    survived_cats = set(
        df_pareto.loc[df_pareto[usage_col] == "O", "ITEM_MDDV_NM"].tolist()
    )
    print(f"  생존 카테고리 {len(survived_cats)}개: {sorted(survived_cats)[:5]} ...")

    df_out = df_b4.copy()
    df_out["생존여부"] = df_out["ITEM_MDDV_NM"].apply(
        lambda x: "생존" if x in survived_cats else "퇴출"
    )
    survived_n = (df_out["생존여부"] == "생존").sum()
    df_out.to_csv(dst, index=False, encoding="utf-8-sig")
    print(f"  완료: {dst}  (생존 {survived_n:,}개 / 전체 {len(df_out):,}개)")


if __name__ == "__main__":
    df_b4 = convert_b4()
    convert_b5()
    make_b4_with_survival(df_b4)
    print("\n모든 변환 완료.")
