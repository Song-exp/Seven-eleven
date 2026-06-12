# -*- coding: utf-8 -*-
"""
build_trend_engagement.py

1. knewnew 크롤링 원본 2개 병합 → data/raw/knewnew_merged.csv
2. trend_keywords_processed.parquet의 트렌드_제품명을 포스트 keywords와 매칭
3. 출력: data/processed/trend_engagement_with_keywords.parquet

스키마: 트렌드(키워드) | 속성보유여부 | 속성(키워드) | 좋아요 수 | 언급일 | url | body
"""

import os
import pandas as pd

BASE_DIR = r"C:\Users\송정현\Documents\Projects\박재홍교수님세미나\Projects\20기\7eleven_npd_framework"

RAW_F1    = os.path.join(BASE_DIR, "data", "raw", "knewnew_without_ad_with_keywords.csv")
RAW_F2    = os.path.join(BASE_DIR, "data", "raw", "knewnew_1-01_04-22_with_keywords.csv")
TREND_PATH = os.path.join(BASE_DIR, "data", "processed", "trend_keywords_processed.parquet")
OUT_RAW    = os.path.join(BASE_DIR, "data", "raw", "knewnew_merged.csv")
OUT_PROC   = os.path.join(BASE_DIR, "data", "processed", "trend_engagement_with_keywords.parquet")

KEEP_COLS = ["date", "body", "likes", "url", "keywords"]


def _load_raw(path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except UnicodeDecodeError:
        df = pd.read_csv(path, encoding="cp949")

    # 공통 컬럼만 유지
    available = [c for c in KEEP_COLS if c in df.columns]
    df = df[available].copy()

    # 날짜 정규화 → YYYY-MM-DD
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return df


def merge_raw() -> pd.DataFrame:
    print("[1] 원본 파일 병합")
    df1 = _load_raw(RAW_F1)
    df2 = _load_raw(RAW_F2)
    print(f"  파일1: {len(df1)}행 ({df1['date'].min()} ~ {df1['date'].max()})")
    print(f"  파일2: {len(df2)}행 ({df2['date'].min()} ~ {df2['date'].max()})")

    merged = pd.concat([df2, df1], ignore_index=True)  # 오래된 것 먼저

    before = len(merged)
    # URL 있는 행은 URL 기준 dedup, URL 없는 행은 date+body 기준 dedup
    has_url  = merged["url"].notna()
    dedup_url  = merged[has_url].drop_duplicates(subset="url", keep="first")
    dedup_body = merged[~has_url].drop_duplicates(subset=["date", "body"], keep="first")
    merged = pd.concat([dedup_url, dedup_body], ignore_index=True)
    merged = merged.sort_values("date").reset_index(drop=True)

    print(f"  병합 후: {before}행 → dedup 후: {len(merged)}행")
    os.makedirs(os.path.dirname(OUT_RAW), exist_ok=True)
    merged.to_csv(OUT_RAW, index=False, encoding="utf-8-sig")
    print(f"  원본 저장 → {OUT_RAW}")
    return merged


def build_trend_engagement(merged: pd.DataFrame) -> pd.DataFrame:
    print("\n[2] 트렌드 키워드 매칭")
    trend = pd.read_parquet(TREND_PATH)

    # 트렌드_제품명 → 속성키워드 맵
    trend_map = {
        row["트렌드_제품명"]: row["확정키워드_정제"]
        for _, row in trend.iterrows()
    }
    trend_names = set(trend_map.keys())
    print(f"  트렌드 키워드 수: {len(trend_names)}개")

    rows = []
    for _, post in merged.iterrows():
        raw_kws = str(post.get("keywords", "") or "")
        post_kws = [k.strip() for k in raw_kws.split(",") if k.strip()]

        matched = [kw for kw in post_kws if kw in trend_names]
        for kw in matched:
            attr_kws = trend_map[kw]
            rows.append({
                "트렌드(키워드)": kw,
                "속성보유여부":   len(attr_kws) > 0,
                "속성(키워드)":   attr_kws,
                "좋아요 수":      post.get("likes"),
                "언급일":         post.get("date"),
                "url":            post.get("url"),
                "body":           post.get("body"),
            })

    result = pd.DataFrame(rows)
    matched_posts = result["url"].nunique() if not result.empty else 0
    print(f"  매칭된 (트렌드키워드, 포스트) 쌍: {len(result)}개 / 포스트 수: {matched_posts}개")

    if not result.empty:
        print(f"  트렌드 키워드 커버리지: {result['트렌드(키워드)'].nunique()}개 / {len(trend_names)}개")

    os.makedirs(os.path.dirname(OUT_PROC), exist_ok=True)
    result.to_parquet(OUT_PROC, index=False, engine="pyarrow")
    print(f"  저장 → {OUT_PROC}")
    return result


if __name__ == "__main__":
    merged = merge_raw()
    result = build_trend_engagement(merged)
    print("\n=== 완료 ===")
    print(f"  원본: {OUT_RAW}")
    print(f"  트렌드 engagement: {OUT_PROC}")
