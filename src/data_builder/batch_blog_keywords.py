"""
블로그 후기 배치 키워드 추출 스크립트
- data/raw/블로그_전체상품_통합(분석식품대상).csv 를 읽어
  extract_keywords_blog() 를 각 행에 적용합니다.
- 검색어 컬럼에서 '세븐일레븐 ' 이후 문자열을 제품명 컨텍스트로 활용합니다.
- 결과는 review_keywords / hin_keywords 열로 추가되어
  data/processed/blog_with_keywords.csv 에 저장됩니다.
- 중단 시 체크포인트에서 이어서 진행할 수 있습니다.
"""
import os
import re
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from keyword_extractor import extract_keywords_blog

# ==========================================
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

INPUT_FILE = os.path.join(
    PROJECT_ROOT, "data", "processed", "blog_merged.csv"
)
OUTPUT_CSV = os.path.join(
    PROJECT_ROOT, "data", "processed", "blog_with_keywords.csv"
)
CHECKPOINT_CSV = os.path.join(
    PROJECT_ROOT, "data", "processed", "blog_keywords_checkpoint.csv"
)

CHECKPOINT_EVERY = 20
# ==========================================


def parse_product_name(search_query: str) -> str:
    """'세븐일레븐 브랜드)제품명용량' → '브랜드)제품명용량' 형태로 제품명 추출."""
    if not isinstance(search_query, str):
        return ""
    # '세븐일레븐 ' 이후 문자열 추출
    match = re.match(r"세븐일레븐\s+(.+)", search_query.strip())
    return match.group(1).strip() if match else search_query.strip()


def load_input() -> pd.DataFrame:
    df = pd.read_csv(INPUT_FILE, encoding="utf-8-sig")
    df["본문내용"] = df["본문내용"].fillna("")
    # blog_merged.csv에 product_name 컬럼이 없는 경우 대비
    if "product_name" not in df.columns:
        df["product_name"] = df["검색어"].apply(parse_product_name)
    return df


def load_checkpoint() -> pd.DataFrame | None:
    if os.path.exists(CHECKPOINT_CSV):
        return pd.read_csv(CHECKPOINT_CSV, encoding="utf-8-sig")
    return None


def save_checkpoint(df: pd.DataFrame) -> None:
    os.makedirs(os.path.dirname(CHECKPOINT_CSV), exist_ok=True)
    df.to_csv(CHECKPOINT_CSV, index=False, encoding="utf-8-sig")


def main():
    print("=" * 60)
    print("블로그 후기 배치 키워드 추출 시작")
    print("=" * 60)

    # 1. 체크포인트 우선 로드, 없으면 원본 로드
    checkpoint_df = load_checkpoint()
    kw_cols = ["review_keywords", "hin_keywords"]

    if checkpoint_df is not None and all(c in checkpoint_df.columns for c in kw_cols):
        df = checkpoint_df.copy()
        df["본문내용"] = df["본문내용"].fillna("")
        print(f"\n[1/4] 체크포인트 로드: {CHECKPOINT_CSV}")
    else:
        print(f"\n[1/4] 원본 파일 로드: {INPUT_FILE}")
        df = load_input()
        df["review_keywords"] = ""
        df["hin_keywords"] = ""

    total = len(df)
    print(f"      총 {total}건 로드 완료")

    # 2. 미처리 행 선별: review_keywords가 비어있는 행만
    pending_mask = df["review_keywords"].isna() | (df["review_keywords"] == "")
    done_count = (~pending_mask).sum()
    print(f"\n[2/4] 이미 처리된 {done_count}건 건너뜀")

    pending_count = pending_mask.sum()
    print(f"\n[3/4] 처리 대상: {pending_count}건 (전체 {total}건 중)")

    if pending_count == 0:
        print("      모든 행이 이미 처리되었습니다. 최종 저장만 진행합니다.")
    else:
        print("\n[4/4] 키워드 추출 시작...\n")
        success = 0
        fail = 0
        processed_since_last_save = 0

        pending_indices = df.index[pending_mask].tolist()

        # 환경변수 LIMIT로 처리 건수 제한 (샘플 배치용, 기본은 무제한)
        limit_env = os.environ.get("LIMIT")
        if limit_env and limit_env.isdigit():
            limit_n = int(limit_env)
            pending_indices = pending_indices[:limit_n]
            pending_count = len(pending_indices)
            print(f"      [LIMIT={limit_n}] 이번 실행에서 {pending_count}건만 처리")

        for i, idx in enumerate(pending_indices, start=1):
            row = df.loc[idx]
            product_name = row.get("product_name", "")
            search_query = row.get("검색어", "")

            print(
                f"  [{i}/{pending_count}] {search_query} 처리 중...",
                end=" ",
                flush=True,
            )

            result = extract_keywords_blog(product_name, row["본문내용"])

            if result:
                review_kws = ", ".join(result.get("review_keywords") or [])
                hin_kws = ", ".join(result.get("hin_keywords") or [])
                df.at[idx, "review_keywords"] = review_kws
                df.at[idx, "hin_keywords"] = hin_kws
                print(f"→ [{review_kws[:50]}]")
                success += 1
            else:
                df.at[idx, "review_keywords"] = ""
                df.at[idx, "hin_keywords"] = ""
                print("→ (추출 실패)")
                fail += 1

            processed_since_last_save += 1

            if processed_since_last_save >= CHECKPOINT_EVERY:
                save_checkpoint(df)
                print(f"\n  [체크포인트 저장] {i}건 처리 완료\n")
                processed_since_last_save = 0

        if processed_since_last_save > 0:
            save_checkpoint(df)

        print(f"\n처리 완료: 성공 {success}건 / 실패 {fail}건")

    # 5. 최종 CSV 저장
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"\n최종 결과 저장 완료: {OUTPUT_CSV}")
    filled = (df["review_keywords"].fillna("") != "").sum()
    print(f"총 행 수: {len(df)}, review_keywords 채워진 행: {filled}건")


if __name__ == "__main__":
    main()
