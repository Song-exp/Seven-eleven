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

# 품질 필터링 임계값 (EDA 결과 기반)
FILTER_CONFIG = {
    "body_len_min": 200,
    "body_len_max": 30000,
    "mean_line_len_min": 10,
    "ttr_min": 0.3,
    "num_ratio_max": 0.25,
    "korean_ratio_min": 0.3,
}


def parse_product_name(search_query: str) -> str:
    """'세븐일레븐 브랜드)제품명용량' → '브랜드)제품명용량'"""
    if not isinstance(search_query, str):
        return ""
    match = re.match(r"세븐일레븐\s+(.+)", search_query.strip())
    return match.group(1).strip() if match else search_query.strip()


def compute_quality_features(body: str) -> dict:
    """포스트 본문의 품질 피처 계산"""
    if not body or not isinstance(body, str) or not body.strip():
        return {
            "body_len": 0,
            "mean_line_len": 0,
            "ttr": 0,
            "num_ratio": 0,
            "korean_ratio": 0,
        }

    lines = [l for l in body.split("\n") if l.strip()]
    line_lens = [len(l) for l in lines]
    mean_line_len = sum(line_lens) / len(line_lens) if line_lens else 0

    tokens = re.findall(r"[가-힣a-zA-Z0-9]+", body)
    ttr = len(set(tokens)) / len(tokens) if tokens else 0

    total_chars = len(body)
    num_ratio = len(re.findall(r"[0-9]", body)) / total_chars
    kor_ratio = len(re.findall(r"[가-힣]", body)) / total_chars

    return {
        "body_len": total_chars,
        "mean_line_len": mean_line_len,
        "ttr": round(ttr, 4),
        "num_ratio": round(num_ratio, 4),
        "korean_ratio": round(kor_ratio, 4),
    }


def is_high_quality(features: dict) -> bool:
    """임계값을 기준으로 고품질 포스트 여부 판별"""
    return (
        features["body_len"] >= FILTER_CONFIG["body_len_min"]
        and features["body_len"] <= FILTER_CONFIG["body_len_max"]
        and features["mean_line_len"] >= FILTER_CONFIG["mean_line_len_min"]
        and features["ttr"] >= FILTER_CONFIG["ttr_min"]
        and features["num_ratio"] <= FILTER_CONFIG["num_ratio_max"]
        and features["korean_ratio"] >= FILTER_CONFIG["korean_ratio_min"]
    )


def merge_bodies(series: pd.Series) -> str:
    """비어있지 않은 본문들을 구분자로 이어 붙임."""
    parts = [t.strip() for t in series.fillna("") if t.strip()]
    return BODY_SEP.join(parts)


def main():
    print("=" * 60)
    print("블로그 데이터 전처리: 품질 필터링 및 검색어별 본문 통합")
    print("=" * 60)

    print(f"\n[1/4] 원본 파일 로드: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE, encoding="utf-8-sig")
    df["본문내용"] = df["본문내용"].fillna("")
    print(f"      {len(df)}행 로드 완료 / 고유 검색어: {df['검색어'].nunique()}개")

    print("\n[2/4] 품질 필터링 수행 중...")
    # 피처 계산 및 필터 적용
    quality_df = df["본문내용"].apply(compute_quality_features).apply(pd.Series)
    mask_ok = quality_df.apply(is_high_quality, axis=1)

    df_filtered = df[mask_ok].copy()
    dropped_count = len(df) - len(df_filtered)

    print(f"      필터링 완료: {len(df_filtered)}행 통과 (불량 {dropped_count}행 제거)")
    print(f"      생존율: {len(df_filtered) / len(df) * 100:.1f}%")

    print("\n[3/4] 검색어별 본문 통합 중...")
    merged = (
        df_filtered.groupby("검색어", sort=False)
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

    print(f"      통합 완료: {len(merged)}행 (필터링 후 {len(df_filtered)}행 → {len(merged)}행)")

    print(f"\n[4/4] 저장: {OUTPUT_FILE}")
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    merged.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    print("\n완료!")
    print(f"  - 총 검색어 수: {len(merged)}")
    print(
        f"  - 블로그수 분포:\n{merged['블로그수'].value_counts().sort_index().to_string()}"
    )
    body_empty = (merged["본문내용"] == "").sum()
    print(f"  - 본문 비어있는 검색어: {body_empty}개")


if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
