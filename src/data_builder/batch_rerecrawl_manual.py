# -*- coding: utf-8 -*-
"""
batch_rerecrawl_manual.py — 보정검색어 기반 블로그 재추출

입력 : data/processed/blog_reextract_result_final.csv  (보정검색어 컬럼 있는 것만 처리)
출력 :
  data/processed/blog_reextract_manual_checkpoint.csv
  data/processed/blog_keywords_reextracted.parquet     (기존 parquet에 병합, 중복 시 덮어씀)

실행: python src/data_builder/batch_rerecrawl_manual.py
"""

import ast
import os
import sys

import pandas as pd

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "tests", "네이버블로그크롤링"))

from keyword_extractor import extract_keywords_blog
from keyword_pipeline import run_pipeline
from naver_blog_crawler import run_naver_blog_crawler
from preprocess_blog_merge import compute_quality_features, is_high_quality, merge_bodies

# ─── 경로 ────────────────────────────────────────────────────────────────────
INPUT_CSV      = os.path.join(PROJECT_ROOT, "data", "processed", "blog_reextract_result_final.csv")
CHECKPOINT_CSV = os.path.join(PROJECT_ROOT, "data", "processed", "blog_reextract_manual_checkpoint.csv")
OUTPUT_CSV     = os.path.join(PROJECT_ROOT, "data", "processed", "blog_reextract_manual_result.csv")
OUTPUT_PARQUET = os.path.join(PROJECT_ROOT, "data", "processed", "blog_keywords_reextracted.parquet")

# ─── 파라미터 ────────────────────────────────────────────────────────────────
RESULTS_LIMIT    = 15
CHECKPOINT_EVERY = 10


# ─── 메인 ────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("보정검색어 기반 블로그 재추출")
    print("=" * 60)

    # 1. 체크포인트 우선, 없으면 입력 CSV 로드
    if os.path.exists(CHECKPOINT_CSV):
        print(f"\n[1] 체크포인트 로드: {CHECKPOINT_CSV}")
        df = pd.read_csv(CHECKPOINT_CSV, encoding="utf-8-sig")
        df["_processed"] = df["_processed"].astype(bool)
        # 키워드 빈 행 리셋 (crawled_blog_count 필터 없음 — 처리 완료 행 유지)
        empty_mask = df["확정키워드_정제"].apply(
            lambda v: str(v).strip() in ("", "[]", "nan")
        )
        reset_count = empty_mask.sum()
        if reset_count:
            df.loc[empty_mask, "_processed"] = False
            print(f"  키워드 빈 행 {reset_count}개 → _processed 리셋")
    else:
        print(f"\n[1] 입력 로드: {INPUT_CSV}")
        raw = pd.read_csv(INPUT_CSV, encoding="utf-8-sig")

        # crawled_blog_count == 0 인 행만 (미수집)
        mask = raw["crawled_blog_count"].fillna(0).astype(int) == 0
        target = raw[mask].copy()
        print(f"  crawled_blog_count=0 행: {len(target)}개")

        df = target[["최종명", "보정검색어"]].reset_index(drop=True)
        df["crawled_blog_count"] = 0
        df["merged_body_len"]    = 0
        df["review_keywords"]    = ""
        df["hin_keywords"]       = ""
        df["확정키워드_정제"]    = ""
        df["_processed"]         = False

    total   = len(df)
    done    = int(df["_processed"].sum())
    pending = df.index[~df["_processed"]].tolist()
    print(f"  총 {total}개 / 완료: {done}개 / 남은: {len(pending)}개")

    if not pending:
        print("  모든 상품 처리 완료 → 최종 저장 진행")
    else:
        print("\n[2~4] 크롤링 + 키워드 추출 시작...\n")
        success = fail = processed_since_save = 0

        for i, idx in enumerate(pending, start=1):
            row        = df.loc[idx]
            최종명     = row["최종명"]
            검색어     = str(row["보정검색어"]).strip()

            print(f"  [{done + i}/{total}] {최종명}  (쿼리: {검색어})", flush=True)

            # 크롤링
            raw_result = run_naver_blog_crawler(
                [검색어],
                results_limit=RESULTS_LIMIT,
                sort="sim",
                save_csv=False,
            )

            if raw_result is None or raw_result.empty:
                merged_body   = ""
                crawled_count = 0
            else:
                body_col = "content" if "content" in raw_result.columns else "본문내용"

                def _passes(text):
                    return is_high_quality(compute_quality_features(str(text or "")))

                df_good       = raw_result[raw_result[body_col].apply(_passes)].copy()
                merged_body   = merge_bodies(df_good[body_col]) if not df_good.empty else ""
                crawled_count = len(df_good)

            df.at[idx, "crawled_blog_count"] = crawled_count
            df.at[idx, "merged_body_len"]    = len(merged_body)

            # LLM 키워드 추출
            result = extract_keywords_blog(최종명, merged_body, cutoff=5000, num_ctx=8192)

            if result:
                hin_kws  = result.get("hin_keywords") or []
                rv_kws   = result.get("review_keywords") or []
                refined  = run_pipeline(hin_kws, product_name=최종명)

                df.at[idx, "review_keywords"]  = str(rv_kws)
                df.at[idx, "hin_keywords"]     = str(hin_kws)
                df.at[idx, "확정키워드_정제"]  = str(refined)
                print(f"      → {crawled_count}건 | hin={hin_kws[:3]}")
                success += 1
            else:
                print(f"      → {crawled_count}건 | 키워드 추출 실패")
                fail += 1

            df.at[idx, "_processed"] = True
            processed_since_save    += 1

            if processed_since_save >= CHECKPOINT_EVERY:
                os.makedirs(os.path.dirname(CHECKPOINT_CSV), exist_ok=True)
                df.to_csv(CHECKPOINT_CSV, index=False, encoding="utf-8-sig")
                print(f"\n  [체크포인트] {done + i}건 완료\n")
                processed_since_save = 0

        if processed_since_save > 0:
            os.makedirs(os.path.dirname(CHECKPOINT_CSV), exist_ok=True)
            df.to_csv(CHECKPOINT_CSV, index=False, encoding="utf-8-sig")

        print(f"\n처리 완료: 성공 {success}건 / 실패(키워드) {fail}건")

    # ─── parquet 병합 저장 ────────────────────────────────────────────────────
    def _parse_list(v):
        if isinstance(v, list):
            return v
        try:
            return ast.literal_eval(str(v))
        except Exception:
            return []

    new_df = pd.DataFrame({
        "상품명":         df["최종명"],
        "확정키워드_정제": df["확정키워드_정제"].apply(_parse_list),
    })

    # 기존 parquet 있으면 병합, 중복 상품명은 새 결과 우선
    if os.path.exists(OUTPUT_PARQUET):
        existing = pd.read_parquet(OUTPUT_PARQUET)
        merged   = (
            pd.concat([existing, new_df], ignore_index=True)
            .drop_duplicates(subset="상품명", keep="last")
            .reset_index(drop=True)
        )
    else:
        merged = new_df

    os.makedirs(os.path.dirname(OUTPUT_PARQUET), exist_ok=True)
    merged.to_parquet(OUTPUT_PARQUET, index=False, engine="pyarrow")
    merged.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    filled = merged["확정키워드_정제"].apply(lambda x: len(x) > 0 if hasattr(x, "__len__") else False).sum()
    print("\n=== 최종 저장 완료 ===")
    print(f"  csv    : {OUTPUT_CSV}")
    print(f"  parquet: {OUTPUT_PARQUET}")
    print(f"  전체 {len(merged)}개 상품 / 키워드 채워진 행: {filled}개")

    # ─── blog_reextract_result_final.csv 업데이트 ─────────────────────────────
    update_cols = ["crawled_blog_count", "merged_body_len", "review_keywords", "hin_keywords", "확정키워드_정제"]
    original   = pd.read_csv(INPUT_CSV, encoding="utf-8-sig")
    update_map = df.set_index("최종명")[update_cols].to_dict(orient="index")
    for col in update_cols:
        original[col] = original.apply(
            lambda row: update_map[row["최종명"]][col]
            if row["최종명"] in update_map else row[col],
            axis=1,
        )
    original.to_csv(INPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"  원본 업데이트: {INPUT_CSV} ({len(update_map)}개 행 반영)")


if __name__ == "__main__":
    main()
