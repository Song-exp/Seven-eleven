"""
블로그 핵심어 재추출 스크립트
흐름:
  1. blog_with_keywords.csv 에서 재추출 대상만 뽑아 blog_reextract_targets.csv 생성
     (이미 존재하면 체크포인트에서 이어서 실행)
  2. targets CSV 만 대상으로 5,000자 / num_ctx 8192 로 재추출
  3. 완료 후 결과를 원본 blog_with_keywords.csv 에 merge 반영

재추출 대상 조건:
  ① review_keywords OR hin_keywords 하나라도 3개 이하 + 본문 1000자 이상
  ② 가짜 키워드가 포함된 행 (해당 없음, 정보 없음, 후기본문추출실패, 없음 등)

실행: python src/data_builder/batch_blog_reextract.py
"""
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from keyword_extractor import extract_keywords_blog

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
ORIGIN_CSV     = os.path.join(PROJECT_ROOT, "data", "processed", "blog_with_keywords.csv")
TARGETS_CSV    = os.path.join(PROJECT_ROOT, "data", "processed", "blog_reextract_targets.csv")
CHECKPOINT_CSV = os.path.join(PROJECT_ROOT, "data", "processed", "blog_reextract_checkpoint.csv")

CHECKPOINT_EVERY    = 20
REEXTRACT_THRESHOLD = 1000  # 본문 이 이상이어야 재추출 대상 (키워드 부족 케이스)

FAKE_SUBSTRINGS = {"해당없음", "해당사항없음", "관련없음", "정보없음", "후기본문추출실패", "n/a"}
FAKE_EXACT      = {"없음"}

CUTOFF  = 5000
NUM_CTX = 8192


def kw_count(s: str) -> int:
    if not s or not s.strip():
        return 0
    return len([k for k in s.split(",") if k.strip()])


def has_fake(s: str) -> bool:
    """토큰 정규화 후 substring 포함 여부로 fake 탐지. '없음' 단독은 exact match."""
    if not s:
        return False
    for k in s.split(","):
        norm = k.strip().lower().replace(" ", "")
        if not norm:
            continue
        if norm in FAKE_EXACT:
            return True
        if any(sub in norm for sub in FAKE_SUBSTRINGS):
            return True
    return False


def is_target(row) -> bool:
    body_len = len(str(row.get("본문내용", "") or "").strip())
    either_low = (
        (kw_count(row["review_keywords"]) <= 3 or kw_count(row["hin_keywords"]) <= 3)
        and body_len >= REEXTRACT_THRESHOLD
    )
    fake = has_fake(row["review_keywords"]) or has_fake(row["hin_keywords"])
    return either_low or fake


def build_targets(origin: pd.DataFrame) -> pd.DataFrame:
    """원본에서 재추출 대상 행만 뽑아 orig_idx 컬럼을 추가해 반환."""
    mask = origin.apply(is_target, axis=1)
    targets = origin[mask].copy()
    targets["orig_idx"] = targets.index  # 원본 index 보존
    targets = targets.reset_index(drop=True)
    return targets


def merge_back(origin: pd.DataFrame, targets: pd.DataFrame) -> pd.DataFrame:
    """targets 결과를 orig_idx 기준으로 원본에 반영."""
    for _, row in targets.iterrows():
        idx = int(row["orig_idx"])
        origin.at[idx, "review_keywords"] = row["review_keywords"]
        origin.at[idx, "hin_keywords"]    = row["hin_keywords"]
    return origin


def main():
    print("=" * 60)
    print("블로그 핵심어 재추출 시작")
    print(f"  cutoff={CUTOFF}자 / num_ctx={NUM_CTX}")
    print("=" * 60)

    # 1. targets 로드 (체크포인트 > targets > 신규 생성 순)
    if os.path.exists(CHECKPOINT_CSV):
        df = pd.read_csv(CHECKPOINT_CSV, encoding="utf-8-sig")
        df["본문내용"] = df["본문내용"].fillna("")
        print(f"\n[1/4] 체크포인트 로드: {CHECKPOINT_CSV}")
    elif os.path.exists(TARGETS_CSV):
        df = pd.read_csv(TARGETS_CSV, encoding="utf-8-sig")
        df["본문내용"] = df["본문내용"].fillna("")
        print(f"\n[1/4] targets 로드: {TARGETS_CSV}")
    else:
        print(f"\n[1/4] 원본에서 재추출 대상 추출: {ORIGIN_CSV}")
        origin = pd.read_csv(ORIGIN_CSV, encoding="utf-8-sig")
        origin["본문내용"]       = origin["본문내용"].fillna("")
        origin["review_keywords"] = origin["review_keywords"].fillna("")
        origin["hin_keywords"]    = origin["hin_keywords"].fillna("")
        df = build_targets(origin)
        os.makedirs(os.path.dirname(TARGETS_CSV), exist_ok=True)
        df.to_csv(TARGETS_CSV, index=False, encoding="utf-8-sig")
        print(f"      재추출 대상 {len(df):,}건 → {TARGETS_CSV}")

    print(f"      총 {len(df):,}건 로드 완료")

    # 2. 미처리 행 선별 (_processed 컬럼으로 추적)
    if "_processed" not in df.columns:
        df["_processed"] = False

    pending_mask = ~df["_processed"].astype(bool)
    pending_idx  = df.index[pending_mask].tolist()

    print(f"\n[2/4] 이미 완료: {df['_processed'].sum():,}건 / 처리 대상: {len(pending_idx):,}건")

    if not pending_idx:
        print("      모든 대상 처리 완료 → 원본에 merge합니다.")
    else:
        print(f"\n[3/4] 재추출 시작 (cutoff={CUTOFF}자, num_ctx={NUM_CTX})...\n")
        success = fail = processed_since_save = 0
        total = len(pending_idx)

        for i, idx in enumerate(pending_idx, start=1):
            row          = df.loc[idx]
            search_query = row.get("검색어", "")
            product_name = row.get("product_name", "")

            print(f"  [{i}/{total}] {search_query} 처리 중...", end=" ", flush=True)

            result = extract_keywords_blog(
                product_name, row["본문내용"], cutoff=CUTOFF, num_ctx=NUM_CTX
            )

            if result:
                review_kws = ", ".join(result.get("review_keywords") or [])
                hin_kws    = ", ".join(result.get("hin_keywords") or [])
                df.at[idx, "review_keywords"] = review_kws
                df.at[idx, "hin_keywords"]    = hin_kws
                print(f"→ [{review_kws[:50]}]")
                success += 1
            else:
                print("→ (추출 실패)")
                fail += 1

            df.at[idx, "_processed"] = True

            processed_since_save += 1
            if processed_since_save >= CHECKPOINT_EVERY:
                df.to_csv(CHECKPOINT_CSV, index=False, encoding="utf-8-sig")
                print(f"\n  [체크포인트 저장] {i}건 처리 완료\n")
                processed_since_save = 0

        if processed_since_save > 0:
            df.to_csv(CHECKPOINT_CSV, index=False, encoding="utf-8-sig")

        print(f"\n처리 완료: 성공 {success}건 / 실패 {fail}건")

    # 4. 원본에 merge 반영
    print(f"\n[4/4] 원본에 merge 반영: {ORIGIN_CSV}")
    origin = pd.read_csv(ORIGIN_CSV, encoding="utf-8-sig")
    origin["본문내용"]       = origin["본문내용"].fillna("")
    origin["review_keywords"] = origin["review_keywords"].fillna("")
    origin["hin_keywords"]    = origin["hin_keywords"].fillna("")

    origin = merge_back(origin, df)
    origin.to_csv(ORIGIN_CSV, index=False, encoding="utf-8-sig")

    filled = (origin["review_keywords"].fillna("") != "").sum()
    print(f"merge 완료: 총 {len(origin):,}건 / review_keywords 채워진 행: {filled:,}건")


if __name__ == "__main__":
    main()
