"""
블로그 데이터 미매칭 신제품 대상 제품명 기반 속성 추론 스크립트
- 00_TRUE_NPD_LIST_FILTERED.xlsx 필터 리스트와 blog_with_keywords_filtered.csv 대조
- 블로그 미매칭 제품(약 259종)에 대해 제품명만으로 review_keywords + hin_keywords 추론
- 추론 결과와 블로그 결과를 병합하여 blog_keywords_all.csv 저장 (전체 2,282종 완전체)
"""
import os
import sys
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from keyword_extractor import infer_keywords_from_name

# ==========================================
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

FILTER_LIST_FILE = os.path.join(PROJECT_ROOT, "data", "processed", "블로그크롤링", "00_TRUE_NPD_LIST_FILTERED.xlsx")
BLOG_KEYWORDS_FILE = os.path.join(PROJECT_ROOT, "data", "processed", "blog_with_keywords_filtered.csv")

OUTPUT_CSV = os.path.join(PROJECT_ROOT, "data", "processed", "attr_inferred_unmatched.csv")
CHECKPOINT_CSV = os.path.join(PROJECT_ROOT, "data", "processed", "attr_infer_checkpoint.csv")
MERGED_OUTPUT_CSV = os.path.join(PROJECT_ROOT, "data", "processed", "blog_keywords_all.csv")

CHECKPOINT_EVERY = 10
# ==========================================


def main():
    print("=" * 60)
    print("미매칭 신제품 제품명 기반 속성 추론 시작")
    print("=" * 60)

    # 1. 필터 리스트 로드
    print(f"[1/5] 필터 리스트 로드: {FILTER_LIST_FILE}")
    filter_df = pd.read_excel(FILTER_LIST_FILE)
    name_col = [c for c in filter_df.columns if "ITEM_NM" in c][0]
    target_products = set(filter_df[name_col].dropna().unique())
    print(f"      전체 필터 대상: {len(target_products)}종")

    # 2. 블로그 키워드 결과 로드
    print(f"\n[2/5] 블로그 키워드 결과 로드: {BLOG_KEYWORDS_FILE}")
    blog_df = pd.read_csv(BLOG_KEYWORDS_FILE, encoding="utf-8-sig")
    matched_products = set(blog_df["product_name"].dropna().unique())
    print(f"      블로그 매칭 완료: {len(matched_products)}종")

    # 3. 미매칭 상품 추출
    unmatched_names = sorted(target_products - matched_products)
    print(f"\n[3/5] 미매칭 상품 추출: {len(unmatched_names)}종")

    if not unmatched_names:
        print("      미매칭 상품이 없습니다. 병합 단계로 이동합니다.")
        df = pd.DataFrame(columns=["product_name", "review_keywords", "hin_keywords", "infer_source"])
        pending = []
    else:
        # 4. 체크포인트 로드 또는 초기화
        if os.path.exists(CHECKPOINT_CSV):
            print(f"\n[4/5] 체크포인트 로드: {CHECKPOINT_CSV}")
            df = pd.read_csv(CHECKPOINT_CSV, encoding="utf-8-sig")
            done_names = set(
                df[df["review_keywords"].notna() & (df["review_keywords"] != "")]["product_name"]
            )
            pending = [n for n in unmatched_names if n not in done_names]
        else:
            print("\n[4/5] 신규 추론 시작")
            df = pd.DataFrame({
                "product_name": unmatched_names,
                "review_keywords": "",
                "hin_keywords": "",
                "infer_source": "name_only",
            })
            pending = unmatched_names[:]

        print(f"      처리 대상: {len(pending)}건 / 전체 {len(unmatched_names)}건")

        success = 0
        fail = 0

        for i, name in enumerate(pending, start=1):
            print(f"  [{i}/{len(pending)}] {name} 추론 중...", end=" ", flush=True)

            result = infer_keywords_from_name(name)

            if result:
                rev_kws = ", ".join(result.get("review_keywords", []))
                hin_kws = ", ".join(result.get("hin_keywords", []))
                idx = df.index[df["product_name"] == name].tolist()
                if idx:
                    df.at[idx[0], "review_keywords"] = rev_kws
                    df.at[idx[0], "hin_keywords"] = hin_kws
                    df.at[idx[0], "infer_source"] = "name_only"
                else:
                    new_row = pd.DataFrame([{
                        "product_name": name,
                        "review_keywords": rev_kws,
                        "hin_keywords": hin_kws,
                        "infer_source": "name_only",
                    }])
                    df = pd.concat([df, new_row], ignore_index=True)
                print(
                    f"→ 성공 "
                    f"(review {len(result.get('review_keywords', []))}개 / "
                    f"hin {len(result.get('hin_keywords', []))}개)"
                )
                success += 1
            else:
                print("→ 실패")
                fail += 1

            if i % CHECKPOINT_EVERY == 0:
                df.to_csv(CHECKPOINT_CSV, index=False, encoding="utf-8-sig")
                print(f"      >>> {i}건 처리 후 중간 저장 완료")

        df.to_csv(CHECKPOINT_CSV, index=False, encoding="utf-8-sig")
        print(f"\n추론 완료: 성공 {success}건 / 실패 {fail}건")

    # 5. 최종 저장 + 병합
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"\n[5/5] 미매칭 결과 저장: {OUTPUT_CSV}")

    blog_slim = blog_df[["product_name", "review_keywords", "hin_keywords"]].copy()
    blog_slim["infer_source"] = "blog"

    inferred_slim = df[["product_name", "review_keywords", "hin_keywords", "infer_source"]].copy()

    merged = pd.concat([blog_slim, inferred_slim], ignore_index=True)
    merged = merged.drop_duplicates("product_name", keep="first")
    merged.to_csv(MERGED_OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"      병합 완료: {MERGED_OUTPUT_CSV} (총 {len(merged)}종)")
    print(f"        - blog:      {(merged['infer_source'] == 'blog').sum()}종")
    print(f"        - name_only: {(merged['infer_source'] == 'name_only').sum()}종")


if __name__ == "__main__":
    main()
