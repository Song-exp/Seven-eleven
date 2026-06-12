# -*- coding: utf-8 -*-
"""
refine_fill_keywords.py — keyword_fill_edges를 기존 vocab 기준으로 정제

정제 규칙:
  1. fill 키워드가 vocab에 존재 → 유지
  2. fill 키워드가 vocab에 없지만, vocab 키워드가 fill 키워드의 부분문자열
     → 포함된 vocab 키워드들로 쪼개어 대체 (긴 것 우선)
  3. 둘 다 아니면 → 제거

입력:
  data/processed/keyword_fill_edges.parquet
  data/processed/hin/keyword_nodes.parquet
  data/processed/keyword_fill_checkpoint.csv   (ITEM_NM 조회용)
출력:
  data/processed/keyword_fill_edges_refined.parquet
  data/processed/keyword_fill_review.xlsx       (정제 전/후 비교 리뷰)

실행: python src/data_builder/refine_fill_keywords.py
"""

import os
import pandas as pd

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

FILL_PATH   = os.path.join(PROJECT_ROOT, "data", "processed", "keyword_fill_edges.parquet")
KW_PATH     = os.path.join(PROJECT_ROOT, "data", "processed", "hin", "keyword_nodes.parquet")
CKPT_PATH   = os.path.join(PROJECT_ROOT, "data", "processed", "keyword_fill_checkpoint.csv")
OUTPUT_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "keyword_fill_edges_refined.parquet")
REVIEW_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "keyword_fill_review.xlsx")

MIN_VOCAB_LEN = 2


def refine_against_vocab(kw: str, vocab_set: set, vocab_sorted: list) -> list:
    """fill 키워드 하나를 vocab 기준으로 정제.

    vocab_sorted: 길이 내림차순 정렬 — 긴 vocab 키워드를 먼저 매칭.
    """
    kw = str(kw).strip()
    if not kw:
        return []

    # 1. 정확히 존재
    if kw in vocab_set:
        return [kw]

    # 2. fill 키워드 안에 vocab 키워드가 포함 → 분해
    contained = [
        vkw for vkw in vocab_sorted
        if vkw in kw and len(vkw) >= MIN_VOCAB_LEN and vkw != kw
    ]
    if contained:
        return contained

    # 3. 해당 없음 → 제거
    return []


def main():
    print("=" * 60)
    print("키워드 정제: keyword_fill_edges → vocab 기준 필터링")
    print("=" * 60)

    # vocab 로드
    kw_nodes = pd.read_parquet(KW_PATH)
    vocab_set = set(kw_nodes["keyword"].tolist())
    vocab_sorted = sorted(vocab_set, key=len, reverse=True)
    print(f"\n[0] vocab: {len(vocab_set)}개")

    # fill edges 로드
    fill = pd.read_parquet(FILL_PATH)
    print(f"[1] fill edges: {len(fill)}행 / {fill['ITEM_CD'].nunique()}개 제품")
    print(f"    소스별: {fill.groupby('source').size().to_dict()}")

    # ITEM_NM 매핑 (체크포인트에서)
    ckpt = pd.read_csv(CKPT_PATH, encoding="utf-8-sig")
    id_to_nm = ckpt.set_index("ITEM_CD")["ITEM_NM"].to_dict()
    id_to_store = ckpt.set_index("ITEM_CD")["편의점명"].to_dict()
    id_to_mddv = ckpt.set_index("ITEM_CD").get("ITEM_MDDV_NM", pd.Series(dtype=str)).to_dict()

    # ── 정제 ────────────────────────────────────────────────────────────────
    records = []
    for _, row in fill.iterrows():
        refined = refine_against_vocab(row["keyword"], vocab_set, vocab_sorted)
        for kw in refined:
            records.append({
                "ITEM_CD":          row["ITEM_CD"],
                "keyword":          kw,
                "source":           row["source"],
                "original_keyword": row["keyword"],
            })

    if not records:
        print("\n[경고] 정제 결과가 없습니다.")
        return

    refined_df = pd.DataFrame(records).drop_duplicates(["ITEM_CD", "keyword"])

    # ── 통계 ────────────────────────────────────────────────────────────────
    before_prods = fill["ITEM_CD"].nunique()
    after_prods  = refined_df["ITEM_CD"].nunique()
    removed_rows = len(fill) - len(refined_df)
    print(f"\n[2] 정제 결과:")
    print(f"  원본: {len(fill):,}행 ({before_prods}개 제품)")
    print(f"  정제: {len(refined_df):,}행 ({after_prods}개 제품)")
    print(f"  제거: {removed_rows:,}행 ({removed_rows / len(fill) * 100:.1f}%)")
    print(f"\n  소스별 정제 후:")
    print(refined_df.groupby("source").size().to_string())
    per_prod = refined_df.groupby("ITEM_CD")["keyword"].count()
    print(f"\n  제품당 평균 키워드: {per_prod.mean():.1f}개 (min={per_prod.min()}, max={per_prod.max()})")

    # ── parquet 저장 ─────────────────────────────────────────────────────────
    refined_df.to_parquet(OUTPUT_PATH, index=False, engine="pyarrow")
    print(f"\n[3] parquet 저장: {OUTPUT_PATH}")

    # ── 리뷰 파일 생성 (제품별 정제 전/후 비교) ─────────────────────────────
    print(f"\n[4] 리뷰 파일 생성 중...")

    # 제품별 원본 키워드 집계
    before_map = (
        fill.groupby("ITEM_CD")["keyword"]
        .apply(list)
        .to_dict()
    )
    # 제품별 정제 후 키워드 집계
    after_map = (
        refined_df.groupby("ITEM_CD")["keyword"]
        .apply(list)
        .to_dict()
    )
    # 제품별 소스 집계
    source_map = (
        fill.groupby("ITEM_CD")["source"]
        .apply(lambda x: ", ".join(sorted(set(x))))
        .to_dict()
    )

    all_ids = sorted(set(before_map) | set(after_map))
    review_rows = []
    for icd in all_ids:
        before_kws = sorted(set(before_map.get(icd, [])))
        after_kws  = sorted(set(after_map.get(icd, [])))
        removed    = sorted(set(before_kws) - set(after_kws))
        kept       = sorted(set(before_kws) & set(after_kws))
        split_from = [
            ok for ok in before_kws
            if ok not in after_kws and any(ak in ok for ak in after_kws)
        ]
        review_rows.append({
            "ITEM_CD":      icd,
            "ITEM_NM":      id_to_nm.get(str(icd), id_to_nm.get(icd, "")),
            "편의점명":      id_to_store.get(str(icd), id_to_store.get(icd, "")),
            "중분류":        id_to_mddv.get(str(icd), id_to_mddv.get(icd, "")),
            "소스":          source_map.get(icd, ""),
            "정제전_키워드수": len(before_kws),
            "정제후_키워드수": len(after_kws),
            "제거수":        len(removed),
            "정제전_키워드": ", ".join(before_kws),
            "정제후_키워드": ", ".join(after_kws),
            "제거된_키워드": ", ".join(removed),
            "쪼개진_원본":   ", ".join(split_from),
        })

    review_df = pd.DataFrame(review_rows).sort_values(
        ["편의점명", "중분류", "ITEM_NM"]
    ).reset_index(drop=True)

    review_df.to_excel(REVIEW_PATH, index=False, engine="openpyxl")
    print(f"    리뷰 저장: {REVIEW_PATH}")
    print(f"    제품 수: {len(review_df)}개")
    print(f"\n다음 단계: 04_hin_graph_builder.ipynb → keyword-fill 셀에서 refined 파일 사용")


if __name__ == "__main__":
    main()
