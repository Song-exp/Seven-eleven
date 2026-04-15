"""
인스타그램 게시글 키워드 추출 테스트 스크립트
- 게시글 유형별(신상템 알림 / 광고 / 지역 가이드) 샘플을 골라 추출 결과를 나란히 출력합니다.
- 전체 배치 실행 전 프롬프트 품질 검증용입니다.
"""
import os
import sys
import pandas as pd

# keyword_extractor가 같은 디렉토리에 있으므로 경로 추가
sys.path.insert(0, os.path.dirname(__file__))
from keyword_extractor import extract_keywords_instagram, preprocess_instagram_text

# ==========================================
# 설정
# ==========================================
SAMPLE_PER_TYPE = 1   # 유형별 샘플 수 (총 6건 테스트)
RANDOM_SEED = 38
# ==========================================

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
KNEWNEW_DIR = os.path.join(PROJECT_ROOT, "data", "raw", "knewnew")


def load_knewnew_data() -> pd.DataFrame:
    csv_files = [f for f in os.listdir(KNEWNEW_DIR) if f.endswith(".csv")]
    if not csv_files:
        raise FileNotFoundError(f"knewnew CSV 파일을 찾을 수 없습니다: {KNEWNEW_DIR}")
    frames = [pd.read_csv(os.path.join(KNEWNEW_DIR, f)) for f in sorted(csv_files)]
    df = pd.concat(frames, ignore_index=True).drop_duplicates(subset="post_id")
    df["title"] = df["title"].fillna("")
    df["body"] = df["body"].fillna("")
    return df


def classify_post_type(row: pd.Series) -> str:
    title = str(row["title"])
    if "딩동" in title or "신상템" in title:
        return "신상템 알림"
    if any(tag in title for tag in ["#광고", "#AD", "#제작지원"]):
        return "광고/AD"
    return "지역·F&B 가이드"


def print_result(idx: int, row: pd.Series, post_type: str, keywords: list[str]):
    preprocessed = preprocess_instagram_text(row["title"], row["body"])
    print(f"{'='*70}")
    print(f"[{idx}] 유형: {post_type}  |  날짜: {row['date']}  |  likes: {row['likes']}")
    print(f"TITLE : {row['title'][:80]}")
    print(f"BODY  : {row['body'][:150].replace(chr(10), ' ')}...")
    print(f"전처리 : {preprocessed[:120]}...")
    print(f"키워드 : {keywords if keywords else '(추출 실패 — Ollama 연결 확인 필요)'}")
    print()


def main():
    print("데이터 로딩 중...")
    df = load_knewnew_data()
    print(f"전체 게시글: {len(df)}건\n")

    df["post_type"] = df.apply(classify_post_type, axis=1)

    # 유형별 샘플 추출
    samples = []
    for post_type in ["신상템 알림", "광고/AD", "지역·F&B 가이드"]:
        subset = df[df["post_type"] == post_type]
        n = min(SAMPLE_PER_TYPE, len(subset))
        samples.append(subset.sample(n, random_state=RANDOM_SEED))
        print(f"  {post_type}: 전체 {len(subset)}건 중 {n}건 선택")

    sample_df = pd.concat(samples).reset_index(drop=True)
    print(f"\n총 {len(sample_df)}건 테스트 시작...\n")

    for idx, row in sample_df.iterrows():
        post_type = row["post_type"]
        print(f"[{idx+1}/{len(sample_df)}] 처리 중... ({post_type})")
        keywords = extract_keywords_instagram(row["title"], row["body"])
        print_result(idx + 1, row, post_type, keywords)


if __name__ == "__main__":
    main()
