# -*- coding: utf-8 -*-
"""
batch_rerecrawl_attrs.py — 블로그 재추출 파이프라인 (고도화)

입력 : data/processed/product_name_review_final.xlsx  (블로그후기재추출=O 행)
출력 :
  data/processed/blog_reextract_checkpoint.csv     체크포인트
  data/processed/blog_reextract_result.csv          중간 결과 (디버깅용)
  data/processed/blog_keywords_reextracted.parquet  Phase 4 통합용

스키마 (parquet): 상품명 | 확정키워드_정제(list)
  → blog_keywords_processed.parquet 와 동일 스키마
  → Phase 4 에서 concat 후 중복 상품명은 재추출본 우선(keep='last')

실행: python src/data_builder/batch_rerecrawl_attrs.py
"""

import ast
import os
import re
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
NAME_REVIEW_PATH = os.path.join(
    PROJECT_ROOT, "data", "processed", "product_name_review_final.xlsx"
)
CHECKPOINT_CSV = os.path.join(
    PROJECT_ROOT, "data", "processed", "blog_reextract_checkpoint.csv"
)
OUTPUT_CSV = os.path.join(
    PROJECT_ROOT, "data", "processed", "blog_reextract_result.csv"
)
OUTPUT_PARQUET = os.path.join(
    PROJECT_ROOT, "data", "processed", "blog_keywords_reextracted.parquet"
)

# ─── 파라미터 ────────────────────────────────────────────────────────────────
CRAWL_START_DATE    = "2025-01-01"
RESULTS_LIMIT       = 15   # 쿼리 1개당 최대 수집
MIN_BLOGS_PER_LAYER = 3    # 이 이상 확보되면 다음 레이어로 넘어가지 않음
BODY_LEN_MAX        = 3000 # 개별 포스트 본문 최대 길이
CHECKPOINT_EVERY    = 10

# ─── 유틸 ────────────────────────────────────────────────────────────────────
_SPEC_RE = re.compile(r"\d+(\.\d+)?\s*[gGmlML개입봉팩P]")


def _strip_spec(name: str) -> str:
    """규격 표기 제거: '허니버터아몬드 120g' → '허니버터아몬드'"""
    return _SPEC_RE.sub("", name).strip()


def build_query_layers(최종명: str) -> list:
    """5-레이어 검색어 생성 (삽입 순서 유지, 중복 제거)."""
    core = _strip_spec(최종명)
    layers = [
        f"세븐일레븐 {최종명}",     # L1: 브랜드 + 풀네임
        최종명,                      # L2: 풀네임 단독
    ]
    if core and core != 최종명:
        layers.append(core)                  # L3: 규격 제거
        layers.append(f"{core} 편의점")      # L4: 규격 제거 + 편의점
    if len(core) >= 2:
        layers.append(f"세븐일레븐 {core}")  # L5: 브랜드 + 규격 제거
    return list(dict.fromkeys(layers))


def is_relevant_to_product(text: str, product_name: str) -> bool:
    """본문에 제품명 핵심 한글 형태소가 1개 이상 포함되는지 확인."""
    core = _strip_spec(product_name)
    words = re.findall(r"[가-힣]{2,}", core)
    return any(w in text for w in words)


# ─── 레이어 폴백 크롤링 ──────────────────────────────────────────────────────
def crawl_with_fallback(최종명: str):
    """레이어 순서대로 검색 → MIN_BLOGS_PER_LAYER 이상 확보 시 조기 종료."""
    layers = build_query_layers(최종명)
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

        def _passes(text):
            t = str(text or "")
            return is_high_quality(compute_quality_features(t))

        df_good = raw[raw[body_col].apply(_passes)].copy()
        if len(df_good) > len(best_df):
            best_df, best_query = df_good, query
        if len(best_df) >= MIN_BLOGS_PER_LAYER:
            break

    return best_df, best_query


# ─── 메인 ────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("블로그 재추출 파이프라인 (고도화)")
    print(
        f"  start={CRAWL_START_DATE} / limit={RESULTS_LIMIT}"
        f" / min_blogs={MIN_BLOGS_PER_LAYER}"
    )
    print("=" * 60)

    # 1. 체크포인트 우선, 없으면 원본 로드
    if os.path.exists(CHECKPOINT_CSV):
        print(f"\n[1] 체크포인트 로드: {CHECKPOINT_CSV}")
        df = pd.read_csv(CHECKPOINT_CSV, encoding="utf-8-sig")
        for _col in ["used_query", "review_keywords", "hin_keywords", "확정키워드_정제"]:
            df[_col] = df[_col].fillna("").astype(str)
        df["_processed"] = df["_processed"].astype(bool)
        # 키워드가 비어있는 행은 재처리 대상으로 리셋
        empty_mask = df["확정키워드_정제"].apply(
            lambda v: str(v).strip() in ("", "[]", "nan")
        )
        reset_count = empty_mask.sum()
        if reset_count:
            df.loc[empty_mask, "_processed"] = False
            print(f"  키워드 빈 행 {reset_count}개 → _processed 리셋")
    else:
        print(f"\n[1] 재추출 대상 로드: {NAME_REVIEW_PATH}")
        raw = pd.read_excel(NAME_REVIEW_PATH)

        # 블로그후기재추출 컬럼 자동 감지
        flag_col = next(
            (c for c in raw.columns if "블로그" in c and "재추출" in c), None
        )
        if flag_col is None:
            raise ValueError("product_name_review_final.xlsx에서 '블로그후기재추출' 컬럼을 찾을 수 없습니다.")

        target = raw[raw[flag_col].astype(str).str.strip().str.upper() == "O"].copy()
        print(f"  블로그후기재추출=O 행: {len(target)}개")

        def resolve_name(row):
            수정명 = str(row.get("수정명", "")).strip() if pd.notna(row.get("수정명")) else ""
            if 수정명 and 수정명.upper() != "O":
                return 수정명
            return str(row["정규화명"]).strip()

        df = pd.DataFrame(
            {
                "최종명": target.apply(resolve_name, axis=1),
                "원본명": target["원본명"].astype(str).str.strip(),
            }
        ).drop_duplicates(subset="최종명").reset_index(drop=True)

        df["used_query"] = ""
        df["crawled_blog_count"] = 0
        df["merged_body_len"] = 0
        df["review_keywords"] = ""
        df["hin_keywords"] = ""
        df["확정키워드_정제"] = ""
        df["_processed"] = False

    total = len(df)
    done = int(df["_processed"].sum())
    pending = df.index[~df["_processed"]].tolist()
    print(f"  총 {total}개 / 완료: {done}개 / 남은: {len(pending)}개")

    if not pending:
        print("  모든 상품 처리 완료 → 최종 저장 진행")
    else:
        print("\n[2~5] 상품별 크롤링 + 키워드 추출 시작...\n")
        success = fail = processed_since_save = 0

        for i, idx in enumerate(pending, start=1):
            row = df.loc[idx]
            최종명 = row["최종명"]

            print(f"  [{done + i}/{total}] {최종명}", flush=True)

            # 크롤링 (레이어 폴백)
            df_good, used_query = crawl_with_fallback(최종명)
            body_col = (
                "content"
                if not df_good.empty and "content" in df_good.columns
                else "본문내용"
            )
            merged_body = (
                merge_bodies(df_good[body_col]) if not df_good.empty else ""
            )
            crawled_count = len(df_good)

            df.at[idx, "used_query"] = used_query
            df.at[idx, "crawled_blog_count"] = crawled_count
            df.at[idx, "merged_body_len"] = len(merged_body)

            # LLM 키워드 추출
            result = extract_keywords_blog(
                최종명, merged_body, cutoff=5000, num_ctx=8192
            )

            if result:
                hin_kws = result.get("hin_keywords") or []
                rv_kws = result.get("review_keywords") or []
                refined = run_pipeline(hin_kws, product_name=최종명)

                df.at[idx, "review_keywords"] = str(rv_kws)
                df.at[idx, "hin_keywords"] = str(hin_kws)
                df.at[idx, "확정키워드_정제"] = str(refined)

                print(
                    f"      → {crawled_count}건 [쿼리: {used_query}]"
                    f" | hin={hin_kws[:3]}"
                )
                success += 1
            else:
                print(f"      → {crawled_count}건 | 키워드 추출 실패")
                fail += 1

            df.at[idx, "_processed"] = True
            processed_since_save += 1

            if processed_since_save >= CHECKPOINT_EVERY:
                os.makedirs(os.path.dirname(CHECKPOINT_CSV), exist_ok=True)
                df.to_csv(CHECKPOINT_CSV, index=False, encoding="utf-8-sig")
                print(f"\n  [체크포인트] {done + i}건 완료\n")
                processed_since_save = 0

        if processed_since_save > 0:
            os.makedirs(os.path.dirname(CHECKPOINT_CSV), exist_ok=True)
            df.to_csv(CHECKPOINT_CSV, index=False, encoding="utf-8-sig")

        print(f"\n처리 완료: 성공 {success}건 / 실패(키워드) {fail}건")

    # ─── 최종 저장 ────────────────────────────────────────────────────────────
    result_df = df.drop(columns=["_processed"])
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    result_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    def _parse_list(v):
        # numpy array str 포맷 "['a' 'b']" → ast.literal_eval이 'a''b' 암묵 연결
        # → 쉼표 없는 numpy str은 regex로 직접 추출
        import re as _re, numpy as _np
        if isinstance(v, (_np.ndarray, list)):
            return list(v)
        s = str(v).strip()
        if not s or s in ("", "[]", "nan"):
            return []
        if _re.search(r"'\s+'", s) and "', '" not in s:
            tokens = _re.findall(r"'([^']*)'", s)
            return [t for t in tokens if t]
        try:
            return ast.literal_eval(s)
        except Exception:
            tokens = _re.findall(r"'([^']*)'", s)
            return [t for t in tokens if t] or []

    parquet_df = pd.DataFrame(
        {
            "상품명": result_df["최종명"],
            "확정키워드_정제": result_df["확정키워드_정제"].apply(_parse_list),
        }
    )
    parquet_df.to_parquet(OUTPUT_PARQUET, index=False, engine="pyarrow")

    filled = parquet_df["확정키워드_정제"].apply(bool).sum()
    print("\n=== 최종 저장 완료 ===")
    print(f"  csv    : {OUTPUT_CSV}")
    print(f"  parquet: {OUTPUT_PARQUET}")
    print(f"  총 {len(parquet_df)}개 상품 / 키워드 채워진 행: {filled}개")


if __name__ == "__main__":
    main()
