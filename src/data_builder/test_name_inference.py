"""
infer_keywords_from_name() 추론 성능 평가 스크립트

평가 방법:
- blog_with_keywords_filtered.csv 에서 품질이 검증된 항목을 ground truth로 사용
- 동일 제품명으로 infer_keywords_from_name() 실행 후 비교
- 지표: Exact Jaccard / Soft Jaccard (어간 정규화 후 비교)

실행:
  python test_name_inference.py          # 기본 10개 샘플
  python test_name_inference.py --n 20   # 20개 샘플
  python test_name_inference.py --n 20 --seed 42
"""
import os
import sys
import argparse
import random
import re
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from keyword_extractor import infer_keywords_from_name

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
BLOG_KEYWORDS_FILE = os.path.join(PROJECT_ROOT, "data", "processed", "blog_with_keywords_filtered.csv")

# ── GT 품질 필터 ────────────────────────────────────────────
# 블로그 포스팅이 제품 경험이 아닌 가격·이벤트·결제 정보 위주인 경우 제외
_NOISY_TERMS = {
    "할인적용", "가격정보", "신규제품", "기존제품", "구독", "현장할인",
    "쿠폰", "적립", "영수증", "결제", "포인트", "이벤트참여",
}


def is_noisy_gt(review_kw_str: str) -> bool:
    kws = {k.strip() for k in review_kw_str.split(",") if k.strip()}
    return len(kws & _NOISY_TERMS) >= 2


# ── 어간 정규화 (soft match용) ───────────────────────────────
# 공통 한국어 형용사/명사 접미사 제거 → 의미가 같지만 표현이 다른 키워드 매칭
# 예: 달달함 → 달달, 달콤함 → 달콤, 부드러움 → 부드러, 바삭함 → 바삭
_SUFFIXES = ["스러움", "스러운", "함이", "함", "한맛", "한", "인", "임", "움", "운"]


def stem(kw: str) -> str:
    kw = re.sub(r"\s+", "", kw.lower())
    for suf in _SUFFIXES:
        if kw.endswith(suf) and len(kw) - len(suf) >= 2:
            return kw[: -len(suf)]
    return kw


def normalize_exact(kw_str: str) -> set[str]:
    if not isinstance(kw_str, str) or not kw_str.strip():
        return set()
    return {k.strip().lower().replace(" ", "") for k in kw_str.split(",") if k.strip()}


def normalize_soft(kw_str: str) -> set[str]:
    return {stem(k) for k in normalize_exact(kw_str)}


# ── 지표 함수 ───────────────────────────────────────────────
def jaccard(a: set, b: set) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def recall(actual: set, pred: set) -> float:
    if not actual:
        return 1.0
    return len(actual & pred) / len(actual)


def precision(actual: set, pred: set) -> float:
    if not pred:
        return 0.0
    return len(actual & pred) / len(pred)


# ── 카테고리 추정 ────────────────────────────────────────────
_CAT_HINTS = [
    ("삼각", "삼각김밥"), ("김밥", "김밥"), ("도시락", "도시락"),
    ("샌드", "샌드위치"), ("버거", "버거"), ("떡볶이", "분식"),
    ("떡", "디저트/떡"), ("쿠키", "디저트/과자"), ("케이크", "디저트/과자"),
    ("ml", "음료"), ("g", "스낵/과자"),
]


def guess_category(name: str) -> str:
    for hint, cat in _CAT_HINTS:
        if hint in name:
            return cat
    return "기타"


# ── 카테고리별 균등 샘플링 ───────────────────────────────────
def stratified_sample(df: pd.DataFrame, n: int, seed: int) -> pd.DataFrame:
    df = df.copy()
    df["_cat"] = df["product_name"].apply(guess_category)
    cats = df["_cat"].unique().tolist()
    per_cat = max(1, n // len(cats))

    rng = random.Random(seed)
    sampled = []
    for cat in cats:
        pool = df[df["_cat"] == cat].to_dict("records")
        sampled.extend(rng.sample(pool, min(per_cat, len(pool))))

    sampled_names = {r["product_name"] for r in sampled}
    rest = [r for r in df.to_dict("records") if r["product_name"] not in sampled_names]
    need = n - len(sampled)
    if need > 0 and rest:
        sampled.extend(rng.sample(rest, min(need, len(rest))))

    return pd.DataFrame(sampled[:n]).drop(columns=["_cat"])


def print_div(char="─", w=64):
    print(char * w)


# ── 메인 ─────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=10)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    print("=" * 64)
    print("infer_keywords_from_name() 추론 성능 평가")
    print(f"샘플 수: {args.n}  /  시드: {args.seed}")
    print("=" * 64)

    blog_df = pd.read_csv(BLOG_KEYWORDS_FILE, encoding="utf-8-sig")
    gt = blog_df[
        blog_df["review_keywords"].notna() & (blog_df["review_keywords"] != "")
    ].drop_duplicates("product_name")[["product_name", "review_keywords", "hin_keywords"]]

    # GT 품질 필터 적용
    noisy_mask = gt["review_keywords"].apply(is_noisy_gt)
    gt_clean = gt[~noisy_mask].reset_index(drop=True)
    print(f"GT 풀: {len(gt)}종 → 노이즈 {noisy_mask.sum()}종 제외 → {len(gt_clean)}종 사용\n")

    sample_df = stratified_sample(gt_clean, args.n, args.seed)
    print(f"선택 샘플: {len(sample_df)}종\n")

    results = []

    for i, row in enumerate(sample_df.itertuples(index=False), start=1):
        name = row.product_name
        cat = guess_category(name)

        print_div()
        print(f"[{i}/{len(sample_df)}]  {name}  [{cat}]")
        print_div()

        gt_rev_exact = normalize_exact(row.review_keywords)
        gt_hin_exact = normalize_exact(row.hin_keywords)
        gt_rev_soft = normalize_soft(row.review_keywords)
        gt_hin_soft = normalize_soft(row.hin_keywords)

        print(f"  [실제 review] {row.review_keywords}")
        print(f"  [실제 hin   ] {row.hin_keywords}")

        print("  → 추론 중...", end=" ", flush=True)
        result = infer_keywords_from_name(name)

        if not result:
            print("실패\n")
            results.append({
                "product_name": name, "category": cat, "success": False,
                **{k: 0.0 for k in [
                    "rev_jaccard_exact", "rev_recall_exact", "rev_prec_exact",
                    "hin_jaccard_exact", "hin_recall_exact", "hin_prec_exact",
                    "rev_jaccard_soft",  "rev_recall_soft",  "rev_prec_soft",
                    "hin_jaccard_soft",  "hin_recall_soft",  "hin_prec_soft",
                ]},
            })
            continue

        inf_rev_raw = ", ".join(result.get("review_keywords", []))
        inf_hin_raw = ", ".join(result.get("hin_keywords", []))
        inf_rev_exact = normalize_exact(inf_rev_raw)
        inf_hin_exact = normalize_exact(inf_hin_raw)
        inf_rev_soft = normalize_soft(inf_rev_raw)
        inf_hin_soft = normalize_soft(inf_hin_raw)

        print("완료")
        print(f"  [추론 review] {inf_rev_raw}")
        print(f"  [추론 hin   ] {inf_hin_raw}")

        # 정확 일치 표시
        rev_exact_match = gt_rev_exact & inf_rev_exact
        hin_exact_match = gt_hin_exact & inf_hin_exact
        if rev_exact_match:
            print(f"  [exact 일치 review] {', '.join(sorted(rev_exact_match))}")
        if hin_exact_match:
            print(f"  [exact 일치 hin   ] {', '.join(sorted(hin_exact_match))}")

        # 어간 기준 추가 일치 표시 (exact 제외)
        rev_soft_only = (gt_rev_soft & inf_rev_soft) - {stem(k) for k in rev_exact_match}
        hin_soft_only = (gt_hin_soft & inf_hin_soft) - {stem(k) for k in hin_exact_match}
        if rev_soft_only:
            print(f"  [soft  추가 review] {', '.join(sorted(rev_soft_only))}  ← 어간 기준")
        if hin_soft_only:
            print(f"  [soft  추가 hin   ] {', '.join(sorted(hin_soft_only))}  ← 어간 기준")

        # 지표 계산
        rje = jaccard(gt_rev_exact, inf_rev_exact)
        rre = recall(gt_rev_exact, inf_rev_exact)
        rpe = precision(gt_rev_exact, inf_rev_exact)
        hje = jaccard(gt_hin_exact, inf_hin_exact)
        hre = recall(gt_hin_exact, inf_hin_exact)
        hpe = precision(gt_hin_exact, inf_hin_exact)

        rjs = jaccard(gt_rev_soft, inf_rev_soft)
        rrs = recall(gt_rev_soft, inf_rev_soft)
        rps = precision(gt_rev_soft, inf_rev_soft)
        hjs = jaccard(gt_hin_soft, inf_hin_soft)
        hrs = recall(gt_hin_soft, inf_hin_soft)
        hps = precision(gt_hin_soft, inf_hin_soft)

        print(
            f"  [review] Exact Jaccard={rje:.2f}  Recall={rre:.2f}  Prec={rpe:.2f}"
            f"  │  Soft Jaccard={rjs:.2f}  Recall={rrs:.2f}  Prec={rps:.2f}"
        )
        print(
            f"  [hin   ] Exact Jaccard={hje:.2f}  Recall={hre:.2f}  Prec={hpe:.2f}"
            f"  │  Soft Jaccard={hjs:.2f}  Recall={hrs:.2f}  Prec={hps:.2f}"
        )
        print()

        results.append({
            "product_name": name, "category": cat, "success": True,
            "rev_jaccard_exact": rje, "rev_recall_exact": rre, "rev_prec_exact": rpe,
            "hin_jaccard_exact": hje, "hin_recall_exact": hre, "hin_prec_exact": hpe,
            "rev_jaccard_soft":  rjs, "rev_recall_soft":  rrs, "rev_prec_soft":  rps,
            "hin_jaccard_soft":  hjs, "hin_recall_soft":  hrs, "hin_prec_soft":  hps,
        })

    # ── 집계 요약 ──────────────────────────────────────────────
    print_div("═")
    print("집계 요약")
    print_div("═")

    res_df = pd.DataFrame(results)
    ok = res_df[res_df["success"]]
    print(f"  성공률: {len(ok)}/{len(res_df)} ({len(ok)/len(res_df)*100:.1f}%)\n")

    if not ok.empty:
        header = f"  {'지표':<28}  {'Exact':>6}  {'Soft':>6}  {'향상':>6}"
        print(header)
        print("  " + "-" * 52)
        pairs = [
            ("review_keywords  Jaccard",   "rev_jaccard_exact", "rev_jaccard_soft"),
            ("review_keywords  Recall",    "rev_recall_exact",  "rev_recall_soft"),
            ("review_keywords  Precision", "rev_prec_exact",    "rev_prec_soft"),
            ("hin_keywords     Jaccard",   "hin_jaccard_exact", "hin_jaccard_soft"),
            ("hin_keywords     Recall",    "hin_recall_exact",  "hin_recall_soft"),
            ("hin_keywords     Precision", "hin_prec_exact",    "hin_prec_soft"),
        ]
        for label, ecol, scol in pairs:
            e = ok[ecol].mean()
            s = ok[scol].mean()
            print(f"  {label:<28}  {e:>6.3f}  {s:>6.3f}  {s-e:>+6.3f}")

        print()
        print("  카테고리별 review Soft Jaccard 평균:")
        cat_sum = ok.groupby("category")["rev_jaccard_soft"].mean().sort_values(ascending=False)
        for cat, val in cat_sum.items():
            bar = "█" * int(val * 20)
            print(f"    {cat:<16} {val:.3f}  {bar}")

    print_div("═")


if __name__ == "__main__":
    main()
