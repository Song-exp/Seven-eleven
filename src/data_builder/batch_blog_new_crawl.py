# -*- coding: utf-8 -*-
"""
batch_blog_new_crawl.py — 블로그 키워드 미추출 NPD 신규 크롤링

크롤링 대상 자동 계산:
  - pos_product_features 전체 NPD 중
  - blog_keywords_with_pos 에 키워드 없고
  - blog_keywords_processed 에도 없는 상품
  (blog_proc 에 있지만 ITEM_CD 연결만 없는 건 build_blog_with_pos 셀이 처리)
  - 노벨티 중분류 제외

입력 (자동):
  data/processed/pos_product_features.parquet
  data/processed/blog_keywords_with_pos.parquet
  data/processed/blog_keywords_processed.parquet

출력:
  data/processed/blog_keywords_new_crawl.parquet  (ITEM_CD | ITEM_NM | 확정키워드_정제)
  data/processed/blog_crawl_new_checkpoint.csv    (체크포인트, 10개마다)

실행: python src/data_builder/batch_blog_new_crawl.py
"""

import ast
import os
import re
import sys

import numpy as np
import pandas as pd

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "tests", "네이버블로그크롤링"))

from keyword_extractor import extract_keywords_blog
from keyword_pipeline import run_pipeline
from naver_blog_crawler import run_naver_blog_crawler
from preprocess_blog_merge import compute_quality_features, is_high_quality, merge_bodies

# ─── 경로 ────────────────────────────────────────────────────────────────────
POS_PATH       = os.path.join(PROJECT_ROOT, "data", "processed", "pos_product_features.parquet")
BLOG_WITH_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "blog_keywords_with_pos.parquet")
BLOG_PROC_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "blog_keywords_processed.parquet")
CHECKPOINT_CSV = os.path.join(PROJECT_ROOT, "data", "processed", "blog_crawl_new_checkpoint.csv")
OUTPUT_PARQUET = os.path.join(PROJECT_ROOT, "data", "processed", "blog_keywords_new_crawl.parquet")

# ─── 파라미터 ────────────────────────────────────────────────────────────────
EXCLUDE_CATS        = ["노벨티"]
CRAWL_START_DATE    = "2025-01-01"
RESULTS_LIMIT       = 15
MIN_BLOGS_PER_LAYER = 3
CHECKPOINT_EVERY    = 10

# ─── 유틸 ────────────────────────────────────────────────────────────────────
_SPEC_RE = re.compile(r"\d+(\.\d+)?\s*[gGmlML개입봉팩P]")


def _strip_spec(name: str) -> str:
    return _SPEC_RE.sub("", name).strip()


def norm_id(x):
    try:
        return str(int(float(x)))
    except Exception:
        return str(x)


def norm_name(s: str) -> str:
    """브랜드 접두사 제거 + 특수문자·공백 제거 + 소문자."""
    s = re.sub(r"^[^)]+\)", "", str(s)).strip()
    s = re.sub(r"[\s!&\(\)\[\].·★☆▶▷]", "", s).lower()
    return s


def has_kw(v) -> bool:
    return len(v) > 0 if isinstance(v, (list, np.ndarray)) else False


def build_query_layers(name: str) -> list:
    core = _strip_spec(name)
    layers = [f"세븐일레븐 {name}", name]
    if core and core != name:
        layers.append(core)
        layers.append(f"{core} 편의점")
    if len(core) >= 2:
        layers.append(f"세븐일레븐 {core}")
    return list(dict.fromkeys(layers))


def crawl_with_fallback(name: str):
    layers = build_query_layers(name)
    best_df, best_query = pd.DataFrame(), layers[0]
    for query in layers:
        raw = run_naver_blog_crawler(
            [query],
            results_limit=RESULTS_LIMIT,
            sort="sim",
            start_date=CRAWL_START_DATE,
            save_csv=False,
        )
        if raw is None or raw.empty:
            continue
        body_col = "content" if "content" in raw.columns else "본문내용"
        df_good = raw[raw[body_col].apply(
            lambda t: is_high_quality(compute_quality_features(str(t or "")))
        )].copy()
        if len(df_good) > len(best_df):
            best_df, best_query = df_good, query
        if len(best_df) >= MIN_BLOGS_PER_LAYER:
            break
    return best_df, best_query


# ─── 크롤링 대상 계산 ────────────────────────────────────────────────────────
def compute_targets() -> pd.DataFrame:
    pos = pd.read_parquet(POS_PATH)
    blog_with = pd.read_parquet(BLOG_WITH_PATH)
    blog_proc = pd.read_parquet(BLOG_PROC_PATH)

    pos["ITEM_CD"] = pos["ITEM_CD"].apply(norm_id)
    blog_with["ITEM_CD"] = blog_with["ITEM_CD"].apply(norm_id)

    blog_has_kw_ids = set(blog_with[blog_with["키워드"].apply(has_kw)]["ITEM_CD"])
    blog_proc_exact = set(blog_proc["상품명"].astype(str).str.strip())
    blog_proc_norm  = set(blog_proc["상품명"].apply(norm_name))

    targets = []
    for _, row in pos.iterrows():
        if row["ITEM_MDDV_NM"] in EXCLUDE_CATS:
            continue
        if row["ITEM_CD"] in blog_has_kw_ids:
            continue
        name = str(row["ITEM_NM"]).strip()
        # blog_proc에 이미 있으면 01b build_blog_with_pos 셀이 ITEM_CD 연결 처리
        if name in blog_proc_exact or norm_name(name) in blog_proc_norm:
            continue
        targets.append({
            "ITEM_CD":       row["ITEM_CD"],
            "ITEM_NM":       name,
            "ITEM_MDDV_NM":  row["ITEM_MDDV_NM"],
        })

    return pd.DataFrame(targets)


# ─── 메인 ────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("블로그 신규 크롤링 파이프라인")
    print(f"  start={CRAWL_START_DATE}  limit={RESULTS_LIMIT}  min_blogs={MIN_BLOGS_PER_LAYER}")
    print("=" * 60)

    if os.path.exists(CHECKPOINT_CSV):
        print(f"\n[1] 체크포인트 로드: {CHECKPOINT_CSV}")
        df = pd.read_csv(CHECKPOINT_CSV, encoding="utf-8-sig")
        df["_processed"] = df["_processed"].astype(bool)
        empty_mask = df["확정키워드_정제"].apply(
            lambda v: str(v).strip() in ("", "[]", "nan")
        )
        if empty_mask.sum():
            df.loc[empty_mask, "_processed"] = False
            print(f"  키워드 빈 행 {empty_mask.sum()}개 → _processed 리셋")
    else:
        print("\n[1] 크롤링 대상 계산 중...")
        df = compute_targets()
        print(f"  크롤링 대상: {len(df)}개")
        print("  중분류 분포 (상위 10):")
        print(df["ITEM_MDDV_NM"].value_counts().head(10).to_string())

        df["used_query"]        = ""
        df["crawled_blog_count"] = 0
        df["확정키워드_정제"]     = ""
        df["_processed"]         = False

    total   = len(df)
    done    = int(df["_processed"].sum())
    pending = df.index[~df["_processed"]].tolist()
    print(f"\n  총 {total}개 / 완료: {done}개 / 남은: {len(pending)}개")

    if pending:
        print("\n[2] 크롤링 + 키워드 추출 시작...\n")
        success = fail = processed_since_save = 0

        for i, idx in enumerate(pending, start=1):
            row  = df.loc[idx]
            name = row["ITEM_NM"]
            print(f"  [{done + i}/{total}] {name} [{row['ITEM_MDDV_NM']}]", flush=True)

            df_good, used_query = crawl_with_fallback(name)
            body_col = "content" if (not df_good.empty and "content" in df_good.columns) else "본문내용"
            merged_body = merge_bodies(df_good[body_col]) if not df_good.empty else ""

            df.at[idx, "used_query"]         = used_query
            df.at[idx, "crawled_blog_count"] = len(df_good)

            result = extract_keywords_blog(name, merged_body, cutoff=5000, num_ctx=8192)
            if result:
                hin_kws = result.get("hin_keywords") or []
                refined = run_pipeline(hin_kws, product_name=name)
                df.at[idx, "확정키워드_정제"] = str(refined)
                print(f"      → {len(df_good)}건 [쿼리: {used_query}] | kws={hin_kws[:3]}")
                success += 1
            else:
                print(f"      → {len(df_good)}건 | 키워드 추출 실패")
                fail += 1

            df.at[idx, "_processed"] = True
            processed_since_save += 1
            if processed_since_save >= CHECKPOINT_EVERY:
                df.to_csv(CHECKPOINT_CSV, index=False, encoding="utf-8-sig")
                print(f"\n  [체크포인트] {done + i}건 완료\n")
                processed_since_save = 0

        if processed_since_save > 0:
            df.to_csv(CHECKPOINT_CSV, index=False, encoding="utf-8-sig")

        print(f"\n처리 완료: 성공 {success}건 / 실패(키워드) {fail}건")

    # ─── 최종 저장 ──────────────────────────────────────────────────────────
    def _parse_list(v):
        if isinstance(v, list):
            return v
        try:
            return ast.literal_eval(str(v))
        except Exception:
            return []

    parquet_df = pd.DataFrame({
        "ITEM_CD":         df["ITEM_CD"],
        "ITEM_NM":         df["ITEM_NM"],
        "확정키워드_정제": df["확정키워드_정제"].apply(_parse_list),
    })
    parquet_df.to_parquet(OUTPUT_PARQUET, index=False, engine="pyarrow")

    filled = parquet_df["확정키워드_정제"].apply(bool).sum()
    print(f"\n=== 저장 완료 ===")
    print(f"  {OUTPUT_PARQUET}")
    print(f"  총 {len(parquet_df)}개 / 키워드 있음: {filled}개 / 없음: {len(parquet_df) - filled}개")
    print(f"\n다음 단계: 01b_matching_diagnostics.ipynb → build-blog-with-pos 셀 실행")


if __name__ == "__main__":
    main()
