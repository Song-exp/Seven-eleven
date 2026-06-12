"""
CU 공식 인스타그램 키워드 추출 테스트 스크립트
- 게시글 유형별(신상 소개 / 이벤트·프로모션 / 콜라보 공지) 샘플을 골라
  extract_keywords_seveneleven() 결과를 JSON 구조 그대로 출력합니다.
- 전체 배치 실행 전 프롬프트 품질 검증용입니다.
- 주의: CU 데이터는 제목(title)이 거의 공백이므로 분류를 body 기준으로 수행합니다.
"""
import json
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from keyword_extractor import extract_keywords_seveneleven, preprocess_instagram_text

# ==========================================
# 설정
# ==========================================
SAMPLE_PER_TYPE = 2   # 유형별 샘플 수 (총 6건 테스트)
RANDOM_SEED = 42
# ==========================================

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
CU_DIR = os.path.join(PROJECT_ROOT, "data", "raw", "편의점")
CU_FILES = [
    "instagram_cu_official_2025-02-14_to_2025-11-12.csv",
    "instagram_cu_official_2025-11-12_to_2025-12-31.csv",
]


def load_data() -> pd.DataFrame:
    frames = [
        pd.read_csv(os.path.join(CU_DIR, f), encoding="utf-8")
        for f in CU_FILES
    ]
    df = pd.concat(frames, ignore_index=True).drop_duplicates(subset="post_id")
    df["title"] = df["title"].fillna("").str.strip()
    df["body"] = df["body"].fillna("")
    return df


def classify_post_type(row: pd.Series) -> str:
    # CU 제목은 대부분 공백이므로 body 기준으로 분류
    body = str(row["body"])
    body_head = body[:200]
    if " X CU" in body or "CU X " in body or ("콜라보" in body_head and "CU" in body_head):
        return "콜라보 공지"
    if "신상" in body_head and ("#EVENT" in body_head or "주차" in body_head or "Pick" in body_head):
        return "신상 소개"
    return "이벤트·프로모션"


def print_result(idx: int, total: int, row: pd.Series, post_type: str, result: dict):
    preprocessed = preprocess_instagram_text(row["title"], row["body"])
    sep = "=" * 70

    print(sep)
    print(f"[{idx}/{total}] 유형: {post_type}  |  날짜: {row['date']}  |  likes: {row['likes']}")
    print(f"TITLE : {row['title'][:80]}")
    print(f"BODY  : {row['body'][:150].replace(chr(10), ' ')}...")
    print(f"전처리 : {preprocessed[:120]}...")
    print()

    if not result:
        print("  ⚠  추출 실패 — Ollama 연결 또는 JSON 파싱을 확인하세요.")
    else:
        # metadata
        metadata = result.get("metadata", [])
        if metadata:
            print("  [상품 메타데이터]")
            for item in metadata:
                name = item.get("name", "-")
                price = item.get("price")
                cap = item.get("capacity")
                price_str = f"{price:,}원" if price else "가격 미표기"
                cap_str = cap if cap else "용량 미표기"
                print(f"    • {name} | {price_str} | {cap_str}")
        else:
            print("  [상품 메타데이터] 없음")

        print()

        # 트렌드 필드
        fields = [
            ("flavor_and_category", "맛·식감·카테고리"),
            ("collab_and_brand",    "콜라보·IP·브랜드"),
            ("promotion_type",      "프로모션 유형"),
            ("tpo_context",         "TPO·소비 맥락"),
        ]
        for key, label in fields:
            values = result.get(key, [])
            if values:
                print(f"  [{label}] {', '.join(values)}")
            else:
                print(f"  [{label}] (없음)")

        print()
        print("  [원본 JSON]")
        print("  " + json.dumps(result, ensure_ascii=False, indent=2).replace("\n", "\n  "))

    print()


def main():
    print("데이터 로딩 중...")
    df = load_data()
    print(f"전체 게시글: {len(df)}건\n")

    df["post_type"] = df.apply(classify_post_type, axis=1)

    # 유형별 샘플 추출
    samples = []
    for post_type in ["신상 소개", "이벤트·프로모션", "콜라보 공지"]:
        subset = df[df["post_type"] == post_type]
        n = min(SAMPLE_PER_TYPE, len(subset))
        samples.append(subset.sample(n, random_state=RANDOM_SEED))
        print(f"  {post_type}: 전체 {len(subset)}건 중 {n}건 선택")

    sample_df = pd.concat(samples).reset_index(drop=True)
    total = len(sample_df)
    print(f"\n총 {total}건 테스트 시작...\n")

    for idx, row in sample_df.iterrows():
        post_type = row["post_type"]
        print(f"[{idx + 1}/{total}] 처리 중... ({post_type})")
        result = extract_keywords_seveneleven(row["title"], row["body"])
        print_result(idx + 1, total, row, post_type, result)


if __name__ == "__main__":
    main()
