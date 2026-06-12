# -*- coding: utf-8 -*-
"""blog_keywords_with_pos 연결 누락 패치.

blog_keywords_processed 에 키워드가 있는데
blog_keywords_with_pos 에 ITEM_CD 연결이 없거나 키워드가 비어있는 제품을 수정.

매칭: pos_product_features.ITEM_NM == blog_keywords_processed.상품명 (직접 매칭)

처리:
  - ITEM_CD 없음 → append
  - ITEM_CD 있으나 키워드 빈 리스트 → UPDATE (build-blog-with-pos-code 재생성 대응)

실행: python -m src.data_builder.patch_blog_with_pos
"""
import numpy as np
import pandas as pd
from pathlib import Path

BASE = Path("data/processed")


def norm_id(x):
    try:
        return str(int(float(x)))
    except Exception:
        return str(x)


def klen(x):
    if isinstance(x, (list, np.ndarray)):
        return len(x)
    return 0


def main():
    pos     = pd.read_parquet(BASE / "pos_product_features.parquet")
    blog_wp = pd.read_parquet(BASE / "blog_keywords_with_pos.parquet")
    blog_pr = pd.read_parquet(BASE / "blog_keywords_processed.parquet")
    pn      = pd.read_parquet(BASE / "hin" / "product_nodes.parquet")

    pos["ITEM_CD_n"]     = pos["ITEM_CD"].map(norm_id)
    blog_wp["ITEM_CD_n"] = blog_wp["ITEM_CD"].map(norm_id)
    pn["n_kw"]           = pn["키워드_final"].map(klen)
    pn["ITEM_CD_n"]      = pn["ITEM_CD"].map(norm_id)

    # blog_processed에 있고 키워드도 있는 전체 후보 (product_nodes 의존 제거)
    blog_pr["n_kw"] = blog_pr["확정키워드_정제"].map(klen)
    blog_pr_has = blog_pr[blog_pr["n_kw"] > 0].copy()

    # pos에서 ITEM_CD 붙이기 (상품명 직접 매칭)
    pos_nm_map = (
        pos[["ITEM_NM", "ITEM_CD", "ITEM_CD_n"]]
        .drop_duplicates("ITEM_NM")
        .set_index("ITEM_NM")
    )
    candidates = blog_pr_has[blog_pr_has["상품명"].isin(pos_nm_map.index)].copy()
    candidates["ITEM_CD"]   = candidates["상품명"].map(pos_nm_map["ITEM_CD"])
    candidates["ITEM_CD_n"] = candidates["상품명"].map(pos_nm_map["ITEM_CD_n"])

    # blog_with_pos 현재 상태 분류
    # - 키워드 있는 것: 건드리지 않음
    # - 키워드 빈 것: UPDATE 대상
    # - 없는 것: APPEND 대상
    wp_has_kw   = set(blog_wp[blog_wp["키워드"].map(klen) > 0]["ITEM_CD_n"])
    wp_empty_kw = set(blog_wp[blog_wp["키워드"].map(klen) == 0]["ITEM_CD_n"])

    to_append = candidates[~candidates["ITEM_CD_n"].isin(set(blog_wp["ITEM_CD_n"]))].copy()
    to_update = candidates[candidates["ITEM_CD_n"].isin(wp_empty_kw)].copy()

    if len(to_append) == 0 and len(to_update) == 0:
        print("패치 대상 없음 (이미 모두 키워드 있음)")
        return

    # UPDATE: 빈 키워드 행을 blog_processed 값으로 덮어쓰기
    updated = 0
    if len(to_update):
        update_map = to_update.set_index("ITEM_CD_n")["확정키워드_정제"].to_dict()
        mask = blog_wp["ITEM_CD_n"].isin(update_map)
        blog_wp.loc[mask, "키워드"]       = blog_wp.loc[mask, "ITEM_CD_n"].map(update_map)
        blog_wp.loc[mask, "키워드_final"] = blog_wp.loc[mask, "ITEM_CD_n"].map(update_map)
        updated = mask.sum()
        print(f"키워드 업데이트(빈→채움): {updated}개")
        for _, r in to_update.iterrows():
            kws = list(r["확정키워드_정제"]) if isinstance(r["확정키워드_정제"], (list, np.ndarray)) else []
            print(f"  [UPDATE] [{r['ITEM_CD']}] {r['상품명']} → {kws[:6]}")

    # APPEND: 아예 없는 행 추가
    appended = 0
    if len(to_append):
        append_df = pd.DataFrame({
            "ITEM_CD":      to_append["ITEM_CD"].values,
            "ITEM_NM":      to_append["상품명"].values,
            "정규화명":     to_append["상품명"].values,
            "키워드":       to_append["확정키워드_정제"].values,
            "키워드_final": to_append["확정키워드_정제"].values,
        })
        blog_wp = pd.concat([blog_wp.drop(columns=["ITEM_CD_n"]), append_df],
                            ignore_index=True)
        appended = len(append_df)
        print(f"ITEM_CD 연결 추가: {appended}개")
        for _, r in append_df.iterrows():
            kws = list(r["키워드"]) if isinstance(r["키워드"], (list, np.ndarray)) else []
            print(f"  [APPEND] [{r['ITEM_CD']}] {r['ITEM_NM']} → {kws[:6]}")
    else:
        blog_wp = blog_wp.drop(columns=["ITEM_CD_n"])

    path = BASE / "blog_keywords_with_pos.parquet"
    blog_wp.to_parquet(path, index=False)
    print(f"\nblog_keywords_with_pos 저장 완료 (업데이트 {updated}개 / 추가 {appended}개)")


if __name__ == "__main__":
    main()
