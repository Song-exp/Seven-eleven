"""
LLM_추출_대상_블로그_관련여부_O.xlsx →
{str([상품명, 가격, 용량]): [keywords]} 형식의 JSON 변환기

NOTE: 원본 파일에 가격/용량 컬럼이 없어 null 처리.
      추후 가격/용량 데이터가 생기면 PRICE_COL / VOLUME_COL 상수를 수정.
JSON 키는 리스트를 직렬화한 문자열("["상품명", null, null]")로 저장.
동일 상품명이 여러 행인 경우 키워드를 합산·중복 제거함.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

# ─── 설정 ──────────────────────────────────────────────
INPUT_FILE  = "LLM_추출_대상_블로그_관련여부_O.xlsx"
OUTPUT_FILE = "product_keywords.json"

COL_NAME    = "상품명"
COL_KW      = "total_keywords"
PRICE_COL   = None   # 가격 컬럼명. 없으면 None
VOLUME_COL  = None   # 용량 컬럼명. 없으면 None
KW_SEP      = ","    # total_keywords 구분자
# ───────────────────────────────────────────────────────


def load_df(path: Path) -> pd.DataFrame:
    if not path.exists():
        print(f"[ERROR] 파일 없음: {path}", file=sys.stderr)
        sys.exit(1)

    df = pd.read_excel(path)

    for col in (COL_NAME, COL_KW):
        if col not in df.columns:
            print(f"[ERROR] 필수 컬럼 누락: '{col}'\n현재 컬럼: {df.columns.tolist()}", file=sys.stderr)
            sys.exit(1)

    print(f"로드 완료: {len(df)}행, 고유 상품명 {df[COL_NAME].nunique()}개")
    return df


def parse_keywords(raw: object) -> list[str]:
    if pd.isna(raw) or str(raw).strip() == "":
        return []
    return [kw.strip() for kw in str(raw).split(KW_SEP) if kw.strip()]


def get_scalar(df: pd.DataFrame, col: str | None, group_df: pd.DataFrame) -> object:
    """컬럼이 있으면 첫 번째 non-null 값, 없으면 None."""
    if col is None or col not in df.columns:
        return None
    vals = group_df[col].dropna()
    return vals.iloc[0] if len(vals) else None


def build_dict(df: pd.DataFrame) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}

    for name, group in df.groupby(COL_NAME, sort=False):
        price  = get_scalar(df, PRICE_COL,  group)
        volume = get_scalar(df, VOLUME_COL, group)

        # 키: JSON 직렬화 가능한 문자열 (리스트 표현)
        key = json.dumps([name, price, volume], ensure_ascii=False)

        # 키워드 합산 + 순서 유지 중복 제거
        seen: dict[str, None] = {}
        for raw in group[COL_KW]:
            for kw in parse_keywords(raw):
                seen[kw] = None

        result[key] = list(seen.keys())

    return result


def main() -> None:
    base = Path(__file__).parent
    df   = load_df(base / INPUT_FILE)
    data = build_dict(df)

    out_path = base / OUTPUT_FILE
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"저장 완료: {out_path}")
    print(f"총 상품 수: {len(data)}")

    # 샘플 3개 출력
    print("\n── 샘플 출력 ──")
    for i, (k, v) in enumerate(data.items()):
        if i >= 3:
            break
        print(f"  키  : {k}")
        print(f"  값  : {v}")
        print()


if __name__ == "__main__":
    main()
