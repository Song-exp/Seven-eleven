"""
인스타그램 게시글 배치 키워드 추출 스크립트
- data/raw/knewnew/knewnew_without_ad_1-01_04-22.csv 를 읽어
  extract_keywords_instagram() 를 각 행에 적용합니다.
- 결과는 keywords 열로 추가되어 data/processed/knewnew_1-01_04-22_with_keywords.csv 에 저장됩니다.
- 중단 시 체크포인트에서 이어서 진행할 수 있습니다.
"""
import os
import sys

import pandas as pd

# keyword_extractor 가 같은 디렉토리에 있으므로 경로 추가
sys.path.insert(0, os.path.dirname(__file__))
from keyword_extractor import extract_keywords_instagram

# ==========================================
# 설정 (필요 시 조정)
# ==========================================
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# INPUT_FILE = os.path.join(
#     PROJECT_ROOT, "data", "raw", "knewnew", "knewnew_without_ad_4-22_12-31.csv"
# )
# OUTPUT_CSV = os.path.join(
#     PROJECT_ROOT, "data", "processed", "knewnew_without_ad_with_keywords.csv"
# )
# CHECKPOINT_CSV = os.path.join(
#     PROJECT_ROOT, "data", "processed", "knewnew_keywords_checkpoint.csv"
# )

INPUT_FILE = os.path.join(
    PROJECT_ROOT, "data", "raw", "knewnew", "knewnew_without_ad_1-01_04-22.csv"
)
OUTPUT_CSV = os.path.join(
    PROJECT_ROOT, "data", "processed", "knewnew_1-01_04-22_with_keywords.csv"
)
CHECKPOINT_CSV = os.path.join(
    PROJECT_ROOT, "data", "processed", "knewnew_1-01_04-22_keywords_checkpoint.csv"
)

CHECKPOINT_EVERY = 10   # N 행마다 중간 저장
# ==========================================


def load_input() -> pd.DataFrame:
    """원본 파일 로드."""
    df = pd.read_csv(INPUT_FILE, encoding="utf-8-sig")
    # 컬럼명 통일: 새 파일은 'body(텍스트기반)', title 없음
    df = df.rename(columns={"body(텍스트기반)": "body"})
    df["title"] = ""
    df["body"] = df["body"].fillna("")
    # post_id: url 마지막 세그먼트 추출
    df["post_id"] = df["url"].str.rstrip("/").str.split("/").str[-1]
    # url이 없거나 추출 결과가 null/빈 경우 row index fallback
    # (NaN post_id는 체크포인트 isin/merge 매칭이 안 돼서 무한 재처리됨)
    null_mask = df["post_id"].isna() | (df["post_id"] == "")
    df.loc[null_mask, "post_id"] = [f"__row_{i}__" for i in df.index[null_mask]]
    return df


def load_checkpoint() -> pd.DataFrame | None:
    """체크포인트 파일이 있으면 읽어서 반환, 없으면 None."""
    if os.path.exists(CHECKPOINT_CSV):
        df = pd.read_csv(CHECKPOINT_CSV, encoding="utf-8-sig")
        return df
    return None


def save_checkpoint(df: pd.DataFrame) -> None:
    os.makedirs(os.path.dirname(CHECKPOINT_CSV), exist_ok=True)
    df.to_csv(CHECKPOINT_CSV, index=False, encoding="utf-8-sig")


def main():
    print("=" * 60)
    print("인스타그램 배치 키워드 추출 시작")
    print("=" * 60)

    # 1. 체크포인트 우선 로드, 없으면 원본 로드
    checkpoint_df = load_checkpoint()
    if checkpoint_df is not None and "keywords" in checkpoint_df.columns:
        df = checkpoint_df.copy()
        df["body"] = df["body"].fillna("")
        df["title"] = df["title"].fillna("") if "title" in df.columns else ""
        print(f"\n[1/4] 체크포인트 로드: {CHECKPOINT_CSV}")
    else:
        print(f"\n[1/4] 원본 파일 로드: {INPUT_FILE}")
        df = load_input()
        df["keywords"] = ""

    total = len(df)
    print(f"      총 {total}건 로드 완료")

    # 2. 미처리 행 선별: keywords가 비어있는 행만
    pending_mask = df["keywords"].isna() | (df["keywords"] == "")
    done_count = (~pending_mask).sum()
    print(f"\n[2/4] 이미 처리된 {done_count}건 건너뜀")

    pending_count = pending_mask.sum()
    print(f"\n[3/4] 처리 대상: {pending_count}건 (전체 {total}건 중)")

    if pending_count == 0:
        print("      모든 행이 이미 처리되었습니다. 최종 저장만 진행합니다.")
    else:
        # 4. 배치 처리
        print("\n[4/4] 키워드 추출 시작...\n")

        success = 0
        fail = 0
        processed_since_last_save = 0

        pending_indices = df.index[pending_mask].tolist()

        for i, idx in enumerate(pending_indices, start=1):
            row = df.loc[idx]
            post_id = row["post_id"]

            print(f"  [{i}/{pending_count}] post_id={post_id} 처리 중...", end=" ", flush=True)

            keywords = extract_keywords_instagram(row["title"], row["body"])

            if keywords:
                kw_str = ", ".join(keywords)
                df.at[idx, "keywords"] = kw_str
                print(f"→ {kw_str[:60]}")
                success += 1
            else:
                df.at[idx, "keywords"] = ""
                print("→ (추출 실패)")
                fail += 1

            processed_since_last_save += 1

            # 체크포인트 저장
            if processed_since_last_save >= CHECKPOINT_EVERY:
                save_checkpoint(df)
                print(f"\n  [체크포인트 저장] {i}건 처리 완료\n")
                processed_since_last_save = 0

        # 마지막 남은 것 저장
        if processed_since_last_save > 0:
            save_checkpoint(df)

        print(f"\n처리 완료: 성공 {success}건 / 실패 {fail}건")

    # 5. 최종 CSV 저장
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"\n최종 결과 저장 완료: {OUTPUT_CSV}")
    print(f"총 행 수: {len(df)}, keywords 열 채워진 행: {(df['keywords'] != '').sum()}건")


if __name__ == "__main__":
    main()
