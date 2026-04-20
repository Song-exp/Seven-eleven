"""
GS25 공식 인스타그램 게시글 배치 키워드 추출 스크립트
- data/raw/편의점/instagram_gs25_official_*.csv 두 파일을 병합하여
  extract_keywords_gs25() 를 각 행에 적용합니다.
- 결과는 keywords_json 열(JSON 문자열)로 추가되어
  data/processed/gs25_official_with_keywords.csv 에 저장됩니다.
- 중단 시 체크포인트에서 이어서 진행할 수 있습니다.
"""
import json
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from keyword_extractor import extract_keywords_gs25

# ==========================================
# 설정 (필요 시 조정)
# ==========================================
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
GS25_DIR = os.path.join(PROJECT_ROOT, "data", "raw", "편의점")

INPUT_FILES = [
    os.path.join(GS25_DIR, "instagram_gs25_official_2025-01-01_to_2025-01-31.csv"),
    os.path.join(GS25_DIR, "instagram_gs25_official_2025-02-01_to_2025-12-31.csv"),
]
OUTPUT_CSV = os.path.join(
    PROJECT_ROOT, "data", "processed", "gs25_official_with_keywords.csv"
)
CHECKPOINT_CSV = os.path.join(
    PROJECT_ROOT, "data", "processed", "gs25_keywords_checkpoint.csv"
)

CHECKPOINT_EVERY = 10   # N 행마다 중간 저장
# ==========================================


def load_input() -> pd.DataFrame:
    """두 CSV 파일을 병합하고 중복 제거.

    파일1(1월)은 post_id·title·timestamp·comments 컬럼이 없는 구버전 포맷이므로
    - post_id: url 값으로 대체
    - title: 빈 문자열로 채움
    """
    frames = []
    for f in INPUT_FILES:
        df_part = pd.read_csv(f, encoding="utf-8")
        if "post_id" not in df_part.columns:
            df_part["post_id"] = df_part["url"]
        if "title" not in df_part.columns:
            df_part["title"] = ""
        frames.append(df_part)

    df = pd.concat(frames, ignore_index=True)
    df = df[df["post_id"].notna()].drop_duplicates(subset="post_id")
    df["title"] = df["title"].fillna("").str.strip()
    df["body"] = df["body"].fillna("")
    return df.reset_index(drop=True)


def load_checkpoint() -> pd.DataFrame | None:
    """체크포인트 파일이 있으면 읽어서 반환, 없으면 None."""
    if os.path.exists(CHECKPOINT_CSV):
        return pd.read_csv(CHECKPOINT_CSV, encoding="utf-8-sig")
    return None


def save_checkpoint(df: pd.DataFrame) -> None:
    os.makedirs(os.path.dirname(CHECKPOINT_CSV), exist_ok=True)
    df.to_csv(CHECKPOINT_CSV, index=False, encoding="utf-8-sig")


def summarize(result: dict) -> str:
    """로그 출력용 한 줄 요약."""
    names = [m.get("name") or "" for m in result.get("metadata", [])]
    flavors = result.get("flavor_and_category", [])
    parts = []
    if names:
        parts.append(f"상품:{'/'.join(names[:2])}")
    if flavors:
        parts.append(f"맛/카테고리:{', '.join(flavors[:3])}")
    return " | ".join(parts) if parts else "(내용 없음)"


def main():
    print("=" * 60)
    print("GS25 공식 인스타 배치 키워드 추출 시작")
    print("=" * 60)

    # 1. 원본 데이터 로드
    print(f"\n[1/4] 파일 로드 및 병합...")
    df = load_input()
    total = len(df)
    print(f"      총 {total}건 (중복 제거 후)")

    # 2. 체크포인트 확인 → 이미 처리된 post_id 제외
    checkpoint_df = load_checkpoint()
    if checkpoint_df is not None and "keywords_json" in checkpoint_df.columns:
        done_ids = set(
            checkpoint_df.loc[
                checkpoint_df["keywords_json"].notna()
                & (checkpoint_df["keywords_json"] != ""),
                "post_id",
            ]
        )
        print(f"\n[2/4] 체크포인트 발견 → 이미 처리된 {len(done_ids)}건 건너뜀")
        df = df.merge(
            checkpoint_df[["post_id", "keywords_json"]],
            on="post_id",
            how="left",
        )
    else:
        print("\n[2/4] 체크포인트 없음 → 처음부터 시작")
        df["keywords_json"] = ""
        done_ids = set()

    # 3. 미처리 행 선별
    pending_mask = ~df["post_id"].isin(done_ids)
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

            result = extract_keywords_gs25(row["title"], row["body"])

            if result:
                df.at[idx, "keywords_json"] = json.dumps(result, ensure_ascii=False)
                print(f"→ {summarize(result)}")
                success += 1
            else:
                df.at[idx, "keywords_json"] = ""
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
    print(f"총 행 수: {len(df)}, keywords_json 채워진 행: {(df['keywords_json'] != '').sum()}건")


if __name__ == "__main__":
    main()
