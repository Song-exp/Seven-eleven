# -*- coding: utf-8 -*-
"""product_nodes.parquet 수동 패치.

동일 편의점 내 ITEM_NM 중복(구버전 바코드 + 신버전 바코드)을
POS 코드 기준으로 통합하고 키워드를 합산한다.

패턴:
  - in_pos=False(구버전) + in_pos=True(신버전) → 신버전 ITEM_CD로 통일
  - 둘 다 in_pos=True → 첫_등장일 빠른 쪽(원본)으로 통일
  키워드_final은 두 행의 합집합.

실행:
  python -m src.data_builder.patch_product_nodes
"""

import numpy as np
import pandas as pd
from pathlib import Path

PN_PATH = Path("data/processed/hin/product_nodes.parquet")
POS_PATH = Path("data/processed/pos_product_features.parquet")


def norm_id(x):
    try:
        return str(int(float(x)))
    except Exception:
        return str(x)


def klen(x):
    if isinstance(x, (list, np.ndarray)):
        return len(x)
    return 0


def merge_kws(*lists):
    """여러 키워드 리스트를 순서 유지 합집합으로 합산."""
    seen, out = set(), []
    for lst in lists:
        if isinstance(lst, (list, np.ndarray)):
            for k in lst:
                if k not in seen:
                    seen.add(k)
                    out.append(k)
    return out


def main():
    pn = pd.read_parquet(PN_PATH)
    pos = pd.read_parquet(POS_PATH)
    pos_ids = set(pos["ITEM_CD"].map(norm_id))

    pn["ITEM_CD_n"] = pn["ITEM_CD"].map(norm_id)
    pn["in_pos"] = pn["ITEM_CD_n"].isin(pos_ids)

    seven = pn[pn["편의점명"] == "세븐일레븐"]
    dup_nms = seven[seven.duplicated("ITEM_NM", keep=False)]["ITEM_NM"].unique()

    drop_ids = []
    patches = {}   # ITEM_CD_n → {"키워드_final": [...]}

    for nm in dup_nms:
        rows = seven[seven["ITEM_NM"] == nm].copy()
        rows = rows.sort_values(["in_pos", "첫_등장일"], ascending=[False, True])

        canonical = rows.iloc[0]   # POS 있는 것 우선, 동률이면 첫판매일 빠른 것
        duplicates = rows.iloc[1:]

        merged_kws = merge_kws(
            *[r["키워드_final"] for _, r in rows.iterrows()]
        )

        can_id = canonical["ITEM_CD_n"]
        patches[can_id] = {"키워드_final": merged_kws}

        for _, dup in duplicates.iterrows():
            drop_ids.append(dup["ITEM_CD_n"])
            print(f"  [{nm}] DROP {dup['ITEM_CD']}(in_pos={dup['in_pos']}) "
                  f"→ KEEP {canonical['ITEM_CD']}(in_pos={canonical['in_pos']}), "
                  f"kw합산 {len(merged_kws)}개")

    # 패치 적용
    for idx, row in pn.iterrows():
        nid = row["ITEM_CD_n"]
        if nid in patches:
            pn.at[idx, "키워드_final"] = patches[nid]["키워드_final"]

    # 중복 행 제거
    before = len(pn)
    pn = pn[~pn["ITEM_CD_n"].isin(drop_ids)]
    print(f"\n제거된 행: {before - len(pn)}개 | 최종: {len(pn)}행")

    pn = pn.drop(columns=["ITEM_CD_n", "in_pos"])
    pn.to_parquet(PN_PATH, index=False)
    print("product_nodes.parquet 저장 완료.")

    # CSV 갱신
    def join_list(x):
        if isinstance(x, (list, np.ndarray)):
            return ", ".join(map(str, x))
        return x

    csv_path = PN_PATH.with_suffix(".csv")
    pn_csv = pn.copy()
    pn_csv.insert(
        pn_csv.columns.get_loc("키워드_final"),
        "키워드_개수",
        pn_csv["키워드_final"].map(klen),
    )
    for col in ["키워드_final", "인스타_언급일자"]:
        pn_csv[col] = pn_csv[col].map(join_list)
    pn_csv.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print("product_nodes.csv 갱신 완료.")


if __name__ == "__main__":
    main()
