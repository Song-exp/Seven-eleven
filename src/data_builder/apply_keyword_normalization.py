# -*- coding: utf-8 -*-
"""
keyword_frequency_review_final.xlsx 의 '정규화' 컬럼을 각 소스 파일에 적용.

원본 키워드 컬럼은 보존하고, 정규화 결과를 '키워드_final' (또는 '추출_속성_final') 컬럼으로 추가.

정규화 규칙:
  정규화 = 'O'        → 삭제
  정규화 = '새이름'   → 1:1 대체 (쉼표 없음)
  정규화 = 'A, B'     → SPLIT (쉼표로 분리 후 대체)
  정규화 = NaN        → 원본 그대로
"""

import re
import numpy as np
import pandas as pd
from pathlib import Path

BASE    = Path("data/processed")
REVIEW  = BASE / "keyword_frequency_review_final.xlsx"

SOURCES = {
    "blog":   BASE / "blog_keywords_with_pos.parquet",
    "insta":  BASE / "instagram_engagement_with_keywords.parquet",
    "ip":     BASE / "ip_keywords.parquet",
    "trend":  BASE / "trend_keywords.parquet",
}


# ─────────────────────────────────────────────
# 매핑 딕셔너리 구성
# ─────────────────────────────────────────────

def build_mapping(review_path):
    df = pd.read_excel(review_path)
    mapping = {}
    for _, row in df.iterrows():
        kw   = str(row["키워드"]).strip()
        norm = row["정규화"]
        if pd.isna(norm) or str(norm).strip() == "":
            continue
        norm = str(norm).strip()
        if norm == "O":
            mapping[kw] = "DELETE"
        elif "," in norm:
            mapping[kw] = [x.strip() for x in norm.split(",") if x.strip()]
        else:
            mapping[kw] = [norm]
    return mapping


# ─────────────────────────────────────────────
# 행별 적용 함수
# ─────────────────────────────────────────────

def apply_norm(kw_value, mapping):
    """키워드 리스트(또는 ndarray/문자열)에 매핑 적용 → 정규화된 리스트 반환."""
    # 입력값을 list로 통일
    if kw_value is None or (isinstance(kw_value, float) and np.isnan(kw_value)):
        return []
    if isinstance(kw_value, (list, np.ndarray)):
        kws = [str(k).strip() for k in kw_value if str(k).strip()]
    else:
        s = str(kw_value).strip()
        if not s or s in ("nan", "None", "[]"):
            return []
        if s.startswith("["):
            kws = re.findall(r"'([^']*)'", s)
        else:
            kws = [x.strip() for x in s.split(",") if x.strip()]

    result = []
    for kw in kws:
        rule = mapping.get(kw)
        if rule is None:
            result.append(kw)           # 변경 없음
        elif rule == "DELETE":
            pass                        # 삭제
        else:
            result.extend(rule)         # RENAME or SPLIT

    # 순서 유지 + 중복 제거
    seen = set()
    deduped = []
    for k in result:
        if k not in seen:
            seen.add(k)
            deduped.append(k)
    return deduped


# ─────────────────────────────────────────────
# 소스별 처리
# ─────────────────────────────────────────────

def process_blog(path, mapping):
    df = pd.read_parquet(path)
    df["키워드_final"] = df["키워드"].apply(lambda v: apply_norm(v, mapping))
    df.to_parquet(path, index=False)
    changed = (df["키워드_final"].apply(str) != df["키워드"].apply(str)).sum()
    print(f"  [blog] {len(df)}행 | 키워드_final 추가 | 변경 {changed}행")


def process_insta(path, mapping):
    df = pd.read_parquet(path)

    def get_final(row):
        brand = row.get("편의점명", "")
        if brand == "세븐일레븐":
            # 키워드_정제 우선, None이면 키워드 fallback
            src = row.get("키워드_정제")
            if src is None or (isinstance(src, float) and np.isnan(src)):
                src = row.get("키워드")
        else:
            src = row.get("키워드")
        result = apply_norm(src, mapping)
        # 전 브랜드 공통: 키워드_final이 비면 키워드_정제로 재시도
        if not result:
            src2 = row.get("키워드_정제")
            if src2 is not None and not (isinstance(src2, float) and np.isnan(src2)):
                result = apply_norm(src2, mapping)
        return result

    df["키워드_final"] = df.apply(get_final, axis=1)
    df.to_parquet(path, index=False)

    for brand in ["세븐일레븐", "CU", "GS25"]:
        sub = df[df["편의점명"] == brand]
        print(f"  [insta/{brand}] {len(sub)}행 | 키워드_final 추가")


def process_ip(path, mapping):
    df = pd.read_parquet(path)
    df["키워드_final"] = df["키워드"].apply(lambda v: apply_norm(v, mapping))
    df.to_parquet(path, index=False)
    changed = (df["키워드_final"].apply(str) != df["키워드"].apply(str)).sum()
    print(f"  [ip] {len(df)}행 | 키워드_final 추가 | 변경 {changed}행")


def process_trend(path, mapping):
    df = pd.read_parquet(path)
    # 트렌드_키워드 자체는 건드리지 않음 → 추출_속성만 정규화
    df["추출_속성_final"] = df["추출_속성"].apply(lambda v: apply_norm(v, mapping))
    df.to_parquet(path, index=False)
    changed = (df["추출_속성_final"].apply(str) != df["추출_속성"].apply(str)).sum()
    print(f"  [trend] {len(df)}행 | 추출_속성_final 추가 | 변경 {changed}행")


# ─────────────────────────────────────────────

def main():
    print("=== 키워드 정규화 적용 ===\n")

    mapping = build_mapping(REVIEW)
    delete_cnt = sum(1 for v in mapping.values() if v == "DELETE")
    rename_cnt = sum(1 for v in mapping.values() if v != "DELETE" and len(v) == 1)
    split_cnt  = sum(1 for v in mapping.values() if v != "DELETE" and len(v) > 1)
    print(f"매핑 로드: 총 {len(mapping)}개")
    print(f"  DELETE {delete_cnt} / RENAME {rename_cnt} / SPLIT {split_cnt}\n")

    process_blog(SOURCES["blog"],   mapping)
    process_insta(SOURCES["insta"], mapping)
    process_ip(SOURCES["ip"],       mapping)
    process_trend(SOURCES["trend"], mapping)

    print("\n완료.")


if __name__ == "__main__":
    main()
