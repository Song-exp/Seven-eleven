"""
블로그 데이터 전처리: 검색어별 본문 통합
- 동일 검색어에 대한 여러 블로그 본문을 하나로 합칩니다.
- 입력:  data/raw/블로그_전체상품_통합(분석식품대상).csv  (23,016행)
- 출력:  data/processed/blog_merged.csv                  (검색어별 1행)
"""
import os
import re

import pandas as pd

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

INPUT_FILE = os.path.join(
    PROJECT_ROOT, "data", "raw", "블로그_전체상품_통합(분석식품대상).csv"
)
OUTPUT_FILE = os.path.join(PROJECT_ROOT, "data", "processed", "blog_merged.csv")

BODY_SEP = "\n\n---\n\n"  # 본문 구분자


def parse_product_name(search_query: str) -> str:
    """'세븐일레븐 브랜드)제품명용량' → '브랜드)제품명용량'"""
    if not isinstance(search_query, str):
        return ""
    match = re.match(r"세븐일레븐\s+(.+)", search_query.strip())
    return match.group(1).strip() if match else search_query.strip()


def merge_bodies(series: pd.Series) -> str:
    """비어있지 않은 본문들을 구분자로 이어 붙임."""
    parts = [t.strip() for t in series.fillna("") if t.strip()]
    return BODY_SEP.join(parts)


def main():
    print("=" * 60)
    print("블로그 데이터 전처리: 검색어별 본문 통합")
    print("=" * 60)

    print(f"\n[1/3] 원본 파일 로드: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE, encoding="utf-8-sig")
    print(f"      {len(df)}행 로드 완료 / 고유 검색어: {df['검색어'].nunique()}개")

    print("\n[2/3] 검색어별 본문 통합 중...")
    merged = (
        df.groupby("검색어", sort=False)
        .agg(
            중분류명=("중분류명", "first"),
            블로그링크=("블로그링크", lambda s: ", ".join(s.dropna().unique())),
            블로그수=("블로그링크", "count"),
            본문내용=("본문내용", merge_bodies),
        )
        .reset_index()
    )

    merged["product_name"] = merged["검색어"].apply(parse_product_name)

    # 컬럼 순서 정리
    merged = merged[["중분류명", "검색어", "product_name", "블로그수", "블로그링크", "본문내용"]]

    print(f"      통합 완료: {len(merged)}행 (원본 {len(df)}행 → {len(merged)}행)")

    print(f"\n[3/3] 저장: {OUTPUT_FILE}")
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    merged.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    print("\n완료!")
    print(f"  - 총 검색어 수: {len(merged)}")
    print(f"  - 블로그수 분포:\n{merged['블로그수'].value_counts().sort_index().to_string()}")
    body_empty = (merged["본문내용"] == "").sum()
    print(f"  - 본문 비어있는 검색어: {body_empty}개")


if __name__ == "__main__":
    main()
