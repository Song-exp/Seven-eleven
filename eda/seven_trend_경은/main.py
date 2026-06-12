"""
세븐일레븐 트렌드 키워드 자동 발굴기 v5
────────────────────────────────────────
STEP 1. keywords.txt 읽기
         → DataLab 쇼핑인사이트에서 복사한 인기검색어 목록
STEP 2. DataLab 검색량 API → 급등 키워드 필터 (+20% 이상)
STEP 3. 네이버 뉴스 API   → 뉴스 교차검증
STEP 4. 결과 저장         → trend_result.csv

사용법:
  1. keywords.txt 에 DataLab 인기검색어 붙여넣기 (이미 완료)
  2. .env 파일에 인증키 입력
  3. python main.py
"""

import os
import re
import time
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta

# ─────────────────────────────────
# 0. 인증키 불러오기
# ─────────────────────────────────
load_dotenv()

CLIENT_ID     = os.getenv("NAVER_CLIENT_ID")
CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET")

if not CLIENT_ID or not CLIENT_SECRET:
    raise ValueError(
        "❌ .env 파일에 인증키를 입력해주세요.\n"
        "   NAVER_CLIENT_ID=...\n"
        "   NAVER_CLIENT_SECRET=..."
    )

HEADERS = {
    "X-Naver-Client-Id"    : CLIENT_ID,
    "X-Naver-Client-Secret": CLIENT_SECRET,
    "Content-Type"         : "application/json",
}

# ─────────────────────────────────
# 설정값
# ─────────────────────────────────
KEYWORDS_FILE = "keywords.txt"   # DataLab에서 복사한 파일
TREND_LIMIT   = 50               # 상위 몇 개까지 검증할지 (많을수록 오래 걸림)
CHANGE_CUTOFF = 10.0             # 검색량 급등 기준 (%) — 과자 카테고리라 낮게 설정
NEWS_CUTOFF   = 3                # 뉴스 기사 수 기준 (건)

TODAY     = datetime.today()
API_END   = TODAY.strftime("%Y-%m-%d")
API_START = (TODAY - timedelta(weeks=8)).strftime("%Y-%m-%d")


# ─────────────────────────────────────────────────
# STEP 1. keywords.txt 파싱
# ─────────────────────────────────────────────────
def load_keywords(filepath, limit=TREND_LIMIT):
    """
    keywords.txt 형식 예시:
      1촉촉한황치즈칩
      2두바이찰떡파이
      ...
    숫자(순위) + 키워드 형태를 파싱해서 리스트로 반환
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(
            f"❌ {filepath} 파일이 없어요.\n"
            "   DataLab에서 복사한 키워드를 keywords.txt에 저장해주세요."
        )

    keywords = []
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            # 앞의 숫자(순위) 제거 → 키워드만 추출
            match = re.match(r"^(\d+)(.+)$", line)
            if match:
                rank    = int(match.group(1))
                keyword = match.group(2).strip()
                keywords.append({"rank": rank, "keyword": keyword})

    print(f"  → {len(keywords)}개 키워드 로드 완료")
    return keywords[:limit]


# ─────────────────────────────────────────────────
# STEP 2. DataLab 검색량 급등 확인
# ─────────────────────────────────────────────────
def get_trend_change(keywords):
    """
    DataLab 쇼핑인사이트 공개 API로 검색량 시계열 조회
    최근 4주 평균 vs 이전 4주 평균 비교 → 증감률 반환
    5개씩 나눠서 요청 (API 1회 제한)
    """
    url    = "https://openapi.naver.com/v1/datalab/shopping/categories"
    result = {}
    names  = [kw["keyword"] for kw in keywords]

    for i in range(0, len(names), 5):
        chunk = names[i:i+5]
        body  = {
            "startDate": API_START,
            "endDate"  : API_END,
            "timeUnit" : "week",
            "category" : [
                {"name": "과자베이커리", "param": ["50000169"]}
            ],
            "keyword"  : [
                {"name": kw, "param": [kw]} for kw in chunk
            ],
        }
        try:
            res = requests.post(url, headers=HEADERS, json=body, timeout=10)
            res.raise_for_status()
            data = res.json()

            for item in data.get("results", []):
                ratios = [d["ratio"] for d in item.get("data", [])]
                if len(ratios) < 8:
                    result[item["title"]] = 0.0
                    continue
                old_avg = sum(ratios[:4]) / 4    # 4~8주 전 평균
                new_avg = sum(ratios[-4:]) / 4   # 최근 4주 평균
                change  = (
                    (new_avg - old_avg) / old_avg * 100
                    if old_avg > 0 else 0.0
                )
                result[item["title"]] = round(change, 1)

        except Exception as e:
            print(f"  ⚠ DataLab 조회 실패 ({chunk}): {e}")

        time.sleep(0.5)

    return result


# ─────────────────────────────────────────────────
# STEP 3. 뉴스 교차검증
# ─────────────────────────────────────────────────
def get_news_count(keyword):
    """
    네이버 뉴스 검색 API
    "키워드 + 편의점" 으로 편의점 맥락 기사만 카운트
    """
    url    = "https://openapi.naver.com/v1/search/news.json"
    params = {
        "query"  : keyword + " 편의점",
        "display": 100,
        "sort"   : "date",
    }
    try:
        res = requests.get(url, headers=HEADERS, params=params, timeout=10)
        res.raise_for_status()
        return res.json().get("total", 0)
    except Exception as e:
        print(f"  ⚠ 뉴스 조회 실패 ({keyword}): {e}")
        return 0


# ─────────────────────────────────────────────────
# STEP 4. 판정
# ─────────────────────────────────────────────────
def judge(rank, change, news_count):
    """
    인기검색어 순위 + 검색량 급등 + 뉴스 기사 수 종합 판정
    """
    if change >= CHANGE_CUTOFF and news_count >= NEWS_CUTOFF:
        return "유효 트렌드"
    elif change >= CHANGE_CUTOFF:
        return "모니터링"
    elif news_count >= NEWS_CUTOFF:
        return "언론 주목"
    else:
        return "제외"


# ─────────────────────────────────────────────────
# 메인 실행
# ─────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 55)
    print("  세븐일레븐 트렌드 키워드 자동 발굴기 v5")
    print(f"  카테고리: 식품 > 과자/베이커리")
    print(f"  기간: {API_START} ~ {API_END}")
    print("=" * 55)

    # ── STEP 1: keywords.txt 읽기 ─────────────────
    print(f"\n[1/3] {KEYWORDS_FILE} 로드 중...")
    keywords = load_keywords(KEYWORDS_FILE, limit=TREND_LIMIT)
    print(f"  → 상위 {len(keywords)}개 키워드 검증 시작")
    for kw in keywords[:5]:
        print(f"     {kw['rank']}위: {kw['keyword']}")
    if len(keywords) > 5:
        print(f"     ... 외 {len(keywords)-5}개")

    # ── STEP 2: DataLab 검색량 급등 확인 ─────────
    print(f"\n[2/3] DataLab 검색량 급등 확인 중...")
    trend_changes = get_trend_change(keywords)

    rising_count = sum(
        1 for kw in keywords
        if trend_changes.get(kw["keyword"], 0) >= CHANGE_CUTOFF
    )
    print(f"  → 검색량 +{CHANGE_CUTOFF}% 이상 키워드: {rising_count}개")

    # ── STEP 3: 뉴스 교차검증 ────────────────────
    print(f"\n[3/3] 뉴스 교차검증 중...")

    rows = []
    for kw in keywords:
        name       = kw["keyword"]
        rank       = kw["rank"]
        change     = trend_changes.get(name, 0.0)
        news_count = get_news_count(name)
        verdict    = judge(rank, change, news_count)

        rows.append({
            "순위"          : rank,
            "키워드"        : name,
            "검색량증감(%)" : f"{change:+.1f}",
            "뉴스기사수"    : news_count,
            "판정"          : verdict,
            "수집일"        : TODAY.strftime("%Y-%m-%d"),
        })

        icon = (
            "✅" if verdict == "유효 트렌드"
            else "⚠" if verdict in ("모니터링", "언론 주목")
            else "❌"
        )
        print(
            f"  {icon} [{rank:3d}위] {name:15s}"
            f"  검색 {change:+.1f}%  뉴스 {news_count:3d}건  →  {verdict}"
        )
        time.sleep(0.3)

    # ── 결과 저장 ─────────────────────────────────
    df = pd.DataFrame(rows)
    order = {"유효 트렌드": 0, "모니터링": 1, "언론 주목": 2, "제외": 3}
    df["_sort"] = df["판정"].map(order)
    df = (
        df.sort_values(["_sort", "순위"])
          .drop(columns="_sort")
          .reset_index(drop=True)
    )

    output_file = "trend_result.csv"
    df.to_csv(output_file, index=False, encoding="utf-8-sig")

    # ── 최종 요약 ─────────────────────────────────
    print("\n" + "=" * 55)
    print(f"  완료!  결과 저장 → {output_file}")
    print("=" * 55)

    valid   = df[df["판정"] == "유효 트렌드"]["키워드"].tolist()
    monitor = df[df["판정"] == "모니터링"]["키워드"].tolist()
    noted   = df[df["판정"] == "언론 주목"]["키워드"].tolist()

    print(f"\n✅ 유효 트렌드 ({len(valid)}개): {', '.join(valid) if valid else '없음'}")
    print(f"⚠  모니터링   ({len(monitor)}개): {', '.join(monitor) if monitor else '없음'}")
    print(f"📰 언론 주목  ({len(noted)}개): {', '.join(noted) if noted else '없음'}")
    print(f"\n엑셀에서 열기: {output_file}")
