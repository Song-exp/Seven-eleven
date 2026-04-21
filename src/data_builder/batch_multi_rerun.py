"""
다중 상품 게시글 키워드 재추출 배치 스크립트 (3사 통합)
- 기존 처리 완료된 파일에서 metadata 2개 이상인 행만 필터링
- v2 프롬프트(상품별 속성 귀속)로 재추출
- body 기준 체크포인트로 중단/재개 가능
- 최종 결과를 각 브랜드 output 파일에 반영
"""
import json
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from keyword_extractor import (
    extract_keywords_seveneleven_v2,
    extract_keywords_cu_v2,
    extract_keywords_gs25_v2,
)

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
PROCESSED = os.path.join(PROJECT_ROOT, "data", "processed")

CHECKPOINT_EVERY = 10

BRANDS = [
    {
        "name": "7eleven",
        "source": os.path.join(PROCESSED, "instagram_7elevenkorea_2025-01-01_to_2025-12-31.csv"),
        "kw_col": "hin_pipeline_json",
        "output": os.path.join(PROCESSED, "instagram_7elevenkorea_2025-01-01_to_2025-12-31.csv"),
        "checkpoint": os.path.join(PROCESSED, "7eleven_multi_rerun_checkpoint.csv"),
        "extractor": extract_keywords_seveneleven_v2,
    },
    {
        "name": "cu",
        "source": os.path.join(PROCESSED, "cu_official_with_keywords.csv"),
        "kw_col": "keywords_json",
        "output": os.path.join(PROCESSED, "cu_official_with_keywords.csv"),
        "checkpoint": os.path.join(PROCESSED, "cu_multi_rerun_checkpoint.csv"),
        "extractor": extract_keywords_cu_v2,
    },
    {
        "name": "gs25",
        "source": os.path.join(PROCESSED, "gs25_official_with_keywords.csv"),
        "kw_col": "keywords_json",
        "output": os.path.join(PROCESSED, "gs25_official_with_keywords.csv"),
        "checkpoint": os.path.join(PROCESSED, "gs25_multi_rerun_checkpoint.csv"),
        "extractor": extract_keywords_gs25_v2,
    },
]


def is_multi_product(kw_json_str: str) -> bool:
    try:
        kw = json.loads(kw_json_str)
        return isinstance(kw.get("metadata"), list) and len(kw["metadata"]) >= 2
    except Exception:
        return False


def load_checkpoint(path: str) -> dict:
    """body → keywords_json 매핑 반환."""
    if not os.path.exists(path):
        return {}
    df = pd.read_csv(path, encoding="utf-8-sig")
    df["body"] = df["body"].fillna("").str.strip()
    done = df[df["keywords_json_v2"].notna() & (df["keywords_json_v2"] != "")]
    return dict(zip(done["body"], done["keywords_json_v2"]))


def save_checkpoint(path: str, df_ckpt: pd.DataFrame) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df_ckpt.to_csv(path, index=False, encoding="utf-8-sig")


def run_brand(brand: dict) -> None:
    name = brand["name"]
    print(f"\n{'='*60}")
    print(f"  [{name.upper()}] 다중 상품 재추출 시작")
    print(f"{'='*60}")

    # 1. 소스 파일 로드
    if not os.path.exists(brand["source"]):
        print(f"  소스 파일 없음, 건너뜀: {brand['source']}")
        return

    df = pd.read_csv(brand["source"], encoding="utf-8-sig")
    df["body"] = df["body"].fillna("").str.strip()
    kw_col = brand["kw_col"]

    # 2. 다중 상품 행 필터링
    multi_mask = df[kw_col].notna() & df[kw_col].apply(
        lambda x: is_multi_product(x) if isinstance(x, str) else False
    )
    multi_indices = df.index[multi_mask].tolist()
    print(f"\n  다중 상품 대상: {len(multi_indices)}건 / 전체 {len(df)}건")

    if not multi_indices:
        print("  다중 상품 행 없음. 종료.")
        return

    # 3. 체크포인트 로드 (body 기준)
    body_to_v2 = load_checkpoint(brand["checkpoint"])
    print(f"  체크포인트 발견: {len(body_to_v2)}건 재사용")

    multi_bodies = set(df.loc[multi_indices, "body"])
    pending_bodies = multi_bodies - set(body_to_v2.keys())
    if not pending_bodies:
        print("  모든 다중 상품 행 처리 완료 (체크포인트 전체 커버). 저장만 진행합니다.")
        for idx in multi_indices:
            body = df.at[idx, "body"]
            if body in body_to_v2:
                df.at[idx, kw_col] = body_to_v2[body]
        df.to_csv(brand["output"], index=False, encoding="utf-8-sig")
        print(f"  저장 완료: {brand['output']}")
        return

    # 체크포인트 DataFrame 초기화
    if os.path.exists(brand["checkpoint"]):
        df_ckpt = pd.read_csv(brand["checkpoint"], encoding="utf-8-sig")
        df_ckpt["body"] = df_ckpt["body"].fillna("").str.strip()
    else:
        df_ckpt = df.loc[multi_indices, ["body"]].copy()
        df_ckpt["keywords_json_v2"] = ""

    # 4. 재추출
    pending_indices = [
        idx for idx in multi_indices
        if not (df.at[idx, "body"] in body_to_v2 and body_to_v2[df.at[idx, "body"]])
    ]
    # 체크포인트 히트 행은 미리 복원
    for idx in multi_indices:
        body = df.at[idx, "body"]
        if body in body_to_v2 and body_to_v2[body]:
            df.at[idx, kw_col] = body_to_v2[body]

    pending_total = len(pending_indices)
    print(f"\n  재추출 대상: {pending_total}건 (체크포인트 재사용: {len(multi_indices) - pending_total}건)\n")
    success, fail = 0, 0
    pending_since_save = 0

    for i, idx in enumerate(pending_indices, start=1):
        row = df.loc[idx]
        body = row["body"]

        print(f"  [{i}/{pending_total}] 처리 중...", end=" ", flush=True)
        title = row.get("title", "") or ""
        result = brand["extractor"](title, body)

        if result and result.get("metadata"):
            kw_str = json.dumps(result, ensure_ascii=False)
            df.at[idx, kw_col] = kw_str
            body_to_v2[body] = kw_str
            # 체크포인트 df 업데이트
            match = df_ckpt["body"] == body
            if match.any():
                df_ckpt.loc[match, "keywords_json_v2"] = kw_str
            else:
                df_ckpt = pd.concat(
                    [df_ckpt, pd.DataFrame([{"body": body, "keywords_json_v2": kw_str}])],
                    ignore_index=True,
                )
            names = [m.get("name", "") for m in result.get("metadata", [])]
            print(f"→ {len(names)}개 상품 완료")
            success += 1
        else:
            print("→ (추출 실패)")
            fail += 1

        pending_since_save += 1
        if pending_since_save >= CHECKPOINT_EVERY:
            save_checkpoint(brand["checkpoint"], df_ckpt)
            print(f"\n  [체크포인트 저장] {i}건 처리\n")
            pending_since_save = 0

    if pending_since_save > 0:
        save_checkpoint(brand["checkpoint"], df_ckpt)

    skipped = len(multi_indices) - pending_total
    print(f"\n  완료 — 성공:{success} / 실패:{fail} / 체크포인트 재사용:{skipped}")

    # 5. 최종 파일 저장
    df.to_csv(brand["output"], index=False, encoding="utf-8-sig")
    print(f"  저장 완료: {brand['output']}")


def main():
    print("=" * 60)
    print("  3사 다중 상품 게시글 v2 재추출 배치")
    print("=" * 60)

    for brand in BRANDS:
        run_brand(brand)

    print(f"\n{'='*60}")
    print("  전체 완료")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
