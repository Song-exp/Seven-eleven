# -*- coding: utf-8 -*-
"""
build_keyword_datasets.py

Dataset 1: product_instagram_engagement + 인스타 키워드
  출력: data/processed/instagram_engagement_with_keywords.parquet

Dataset 2: B4(POS) + 블로그 제품명 + 블로그 키워드 (정규화명 브릿지)
  출력: data/processed/pos_blog_keywords_bridge.parquet

※ 동일 정규화명에 블로그 원본명이 여러 개인 경우 키워드는 union,
   블로그_원본명은 리스트로 보존.
   이후 전체 소스 병합 + 키워드 정규화는 별도 단계에서 수행.
"""

import os
import pandas as pd
import polars as pl

BASE_DIR = r"C:\Users\송정현\Documents\Projects\박재홍교수님세미나\Projects\20기\7eleven_npd_framework"

# ── 경로 ─────────────────────────────────────────────────────────────────────
ENG_PATH    = os.path.join(BASE_DIR, "data", "processed", "product_instagram_engagement.csv")
INSTA_PATH  = os.path.join(BASE_DIR, "data", "processed", "insta_keywords_processed.parquet")
REVIEW_PATH = os.path.join(BASE_DIR, "data", "processed", "product_name_review_final.xlsx")
B4_PATH     = os.path.join(BASE_DIR, "data", "processed", "B4_ITEM_DV_INFO_filtered.parquet")
BLOG_PATH   = os.path.join(BASE_DIR, "data", "processed", "blog_keywords_processed.parquet")

OUT_DS1 = os.path.join(BASE_DIR, "data", "processed", "instagram_engagement_with_keywords.parquet")
OUT_DS2 = os.path.join(BASE_DIR, "data", "processed", "pos_blog_keywords_bridge.parquet")


def _union_keywords(series):
    """List[List|ndarray[str]] → 중복 제거 + 순서 유지 union"""
    seen, result = set(), []
    for lst in series:
        try:
            it = iter(lst)
        except TypeError:
            continue
        for kw in it:
            kw = str(kw) if kw is not None else ""
            if kw and kw not in seen:
                seen.add(kw)
                result.append(kw)
    return result


# ── Dataset 1 ─────────────────────────────────────────────────────────────────
def build_dataset1():
    print("[Dataset 1] Instagram Engagement + 인스타 키워드")

    eng = pd.read_csv(ENG_PATH)
    insta = pl.read_parquet(INSTA_PATH).to_pandas()[["제품명", "확정키워드_정제"]]
    insta = insta.rename(columns={"제품명": "원본명"})

    ds1 = eng.merge(insta, on="원본명", how="left")

    matched = ds1["확정키워드_정제"].notna().sum()
    print(f"  전체: {len(ds1):,}행 | 키워드 매칭: {matched:,} ({matched/len(ds1)*100:.1f}%)")
    print(f"  컬럼: {ds1.columns.tolist()}")

    ds1.to_parquet(OUT_DS1, index=False, engine="pyarrow")
    print(f"  저장: {OUT_DS1}\n")
    return ds1


# ── Dataset 2 ─────────────────────────────────────────────────────────────────
def build_dataset2():
    print("[Dataset 2] B4(POS) + 블로그 제품명 + 블로그 키워드 (정규화명 브릿지)")

    rev  = pd.read_excel(REVIEW_PATH)[["소스", "원본명", "정규화명"]]
    b4   = pd.read_parquet(B4_PATH)[["ITEM_CD", "ITEM_NM", "ITEM_LRDV_NM", "ITEM_MDDV_NM", "ITEM_SMDV_NM", "생존여부", "is_npd"]]
    b4   = b4[b4["is_npd"]].drop(columns=["is_npd"]).reset_index(drop=True)
    blog = pl.read_parquet(BLOG_PATH).to_pandas()

    # POS 쪽 브릿지: ITEM_NM → 정규화명
    rev_pos = (rev[rev["소스"] == "POS"]
               [["원본명", "정규화명"]]
               .rename(columns={"원본명": "ITEM_NM"})
               .drop_duplicates("ITEM_NM"))

    # 블로그 쪽 브릿지: 상품명 → 정규화명
    rev_blog = (rev[rev["소스"] == "블로그"]
                [["원본명", "정규화명"]]
                .rename(columns={"원본명": "블로그_원본명"})
                .drop_duplicates("블로그_원본명"))

    # B4 → 정규화명
    b4_norm = b4.merge(rev_pos, on="ITEM_NM", how="inner")
    print(f"  B4 정규화명 매칭: {len(b4_norm):,} / {len(b4):,}개")

    # blog → 정규화명
    blog_norm = (blog
                 .merge(rev_blog, left_on="상품명", right_on="블로그_원본명", how="inner")
                 [["정규화명", "블로그_원본명", "확정키워드_정제"]])
    print(f"  블로그 정규화명 매칭: {len(blog_norm):,} / {len(blog):,}개")

    # 같은 정규화명의 블로그 원본명 여러 개 → union
    blog_agg = (blog_norm
                .groupby("정규화명", as_index=False)
                .agg(
                    블로그_원본명_목록=("블로그_원본명", list),
                    확정키워드_정제=("확정키워드_정제", _union_keywords),
                ))

    # 최종 join: B4 기준 left join
    ds2 = b4_norm.merge(blog_agg, on="정규화명", how="left")

    matched = ds2["확정키워드_정제"].notna().sum()
    print(f"  블로그 키워드 최종 매핑: {matched:,} / {len(ds2):,}개 ({matched/len(ds2)*100:.1f}%)")
    print(f"  컬럼: {ds2.columns.tolist()}")

    ds2.to_parquet(OUT_DS2, index=False, engine="pyarrow")
    print(f"  저장: {OUT_DS2}\n")
    return ds2


if __name__ == "__main__":
    ds1 = build_dataset1()
    ds2 = build_dataset2()

    print("=== 완료 ===")
    print(f"  DS1: {OUT_DS1}")
    print(f"  DS2: {OUT_DS2}")
