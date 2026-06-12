"""
필터링된 신제품 리스트 전용 블로그 키워드 추출 스크립트
- data/processed/블로그크롤링/00_TRUE_NPD_LIST_FILTERED.xlsx 에 포함된 상품만 대상으로 함
- data/processed/blog_merged.csv 에서 해당 상품의 블로그 후기만 필터링
- 상향된 모델 설정(3,000자 / 8,192 토큰)을 사용하여 키워드 추출 수행
"""
import os
import re
import sys
import pandas as pd

# 로컬 모듈 참조를 위해 경로 추가
sys.path.insert(0, os.path.dirname(__file__))
from keyword_extractor import extract_keywords_blog

# ==========================================
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# 입력 파일 경로
FILTER_LIST_FILE = os.path.join(PROJECT_ROOT, "data", "processed", "블로그크롤링", "00_TRUE_NPD_LIST_FILTERED.xlsx")
BLOG_INPUT_FILE = os.path.join(PROJECT_ROOT, "data", "processed", "blog_merged.csv")

# 결과 파일 경로
OUTPUT_CSV = os.path.join(PROJECT_ROOT, "data", "processed", "blog_with_keywords_filtered.csv")
CHECKPOINT_CSV = os.path.join(PROJECT_ROOT, "data", "processed", "blog_keywords_filtered_checkpoint.csv")

CHECKPOINT_EVERY = 10
# ==========================================

def parse_product_name(search_query: str) -> str:
    """'세븐일레븐 브랜드)제품명용량' → '브랜드)제품명용량' 형태로 제품명 추출."""
    if not isinstance(search_query, str):
        return ""
    match = re.match(r"세븐일레븐\s+(.+)", search_query.strip())
    return match.group(1).strip() if match else search_query.strip()

def main():
    print("=" * 60)
    print("필터링된 신제품 블로그 키워드 추출 시작")
    print("=" * 60)

    # 1. 필터 리스트 로드 (ITEM_NM 컬럼 기준)
    print(f"[1/4] 신제품 필터 리스트 로드: {FILTER_LIST_FILE}")
    filter_df = pd.read_excel(FILTER_LIST_FILE)
    # 컬럼명이 깨져서 보였으므로 유연하게 매칭 (ITEM_NM 포함된 컬럼 찾기)
    name_col = [c for c in filter_df.columns if 'ITEM_NM' in c]
    if not name_col:
        print("에러: 필터 리스트에서 ITEM_NM 컬럼을 찾을 수 없습니다.")
        return
    target_products = set(filter_df[name_col[0]].dropna().unique())
    print(f"      추출 대상 상품 수: {len(target_products)}종")

    # 2. 블로그 데이터 로드 및 필터링
    print(f"\n[2/4] 블로그 데이터 로드: {BLOG_INPUT_FILE}")
    blog_df = pd.read_csv(BLOG_INPUT_FILE, encoding="utf-8-sig")
    blog_df["본문내용"] = blog_df["본문내용"].fillna("")
    
    # 상품명 정규화 및 필터링
    blog_df["product_name"] = blog_df["검색어"].apply(parse_product_name)
    
    # TARGET 리스트에 포함된 상품만 필터링
    # (부분 일치 가능성을 고려하여 교집합 확인)
    df = blog_df[blog_df["product_name"].isin(target_products)].copy()
    
    print(f"      필터링 결과: {len(blog_df)}건 중 {len(df)}건 매칭 완료")

    if len(df) == 0:
        print("      매칭된 데이터가 없습니다. 종료합니다.")
        return

    # 3. 체크포인트 확인 및 초기화
    if os.path.exists(CHECKPOINT_CSV):
        print(f"\n[3/4] 체크포인트 로드: {CHECKPOINT_CSV}")
        checkpoint_df = pd.read_csv(CHECKPOINT_CSV, encoding="utf-8-sig")
        # 기존 처리된 데이터 병합 (결과값이 있는 경우 유지)
        df = pd.merge(df, checkpoint_df[["검색어", "review_keywords", "hin_keywords"]], on="검색어", how="left", suffixes=('', '_old'))
        df["review_keywords"] = df["review_keywords"].fillna(df.get("review_keywords_old", ""))
        df["hin_keywords"] = df["hin_keywords"].fillna(df.get("hin_keywords_old", ""))
        df.drop(columns=[c for c in df.columns if '_old' in c], inplace=True)
    else:
        print("\n[3/4] 신규 추출 시작")
        df["review_keywords"] = ""
        df["hin_keywords"] = ""

    # 4. 미처리 행 선별 및 추출
    pending_mask = (df["review_keywords"].isna()) | (df["review_keywords"] == "")
    pending_indices = df.index[pending_mask].tolist()
    pending_count = len(pending_indices)

    print(f"\n[4/4] 처리 대상: {pending_count}건 (전체 필터링 대상 {len(df)}건 중)")

    if pending_count == 0:
        print("      모든 행이 이미 처리되었습니다.")
    else:
        success = 0
        fail = 0
        
        for i, idx in enumerate(pending_indices, start=1):
            row = df.loc[idx]
            p_name = row["product_name"]
            body = row["본문내용"]

            print(f"  [{i}/{pending_count}] {p_name} ({len(body)}자) 추출 중...", end=" ", flush=True)

            # keyword_extractor.py의 상향된 설정 사용
            result = extract_keywords_blog(p_name, body)

            if result:
                rev_kws = ", ".join(result.get("review_keywords", []))
                hin_kws = ", ".join(result.get("hin_keywords", []))
                df.at[idx, "review_keywords"] = rev_kws
                df.at[idx, "hin_keywords"] = hin_kws
                print(f"→ 성공 (키워드 {len(result.get('review_keywords', []))}개)")
                success += 1
            else:
                print("→ 실패")
                fail += 1

            if i % CHECKPOINT_EVERY == 0:
                df.to_csv(CHECKPOINT_CSV, index=False, encoding="utf-8-sig")
                print(f"      >>> {i}건 처리 후 중간 저장 완료")

        df.to_csv(CHECKPOINT_CSV, index=False, encoding="utf-8-sig")
        print(f"\n추출 완료: 성공 {success}건 / 실패 {fail}건")

    # 5. 최종 저장
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"\n최종 결과 저장 완료: {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
