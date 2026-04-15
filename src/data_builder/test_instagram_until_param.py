"""
Apify Instagram Scraper `until` 파라미터 동작 여부 테스트
- `until` 파라미터가 상한선 필터로 실제 동작하는지 소량(resultsLimit=30)으로 검증합니다.
- 테스트 통과 조건: [skip] 로그 없이 end_date 이하 게시물만 수집될 경우
"""
import os
from apify_client import ApifyClient
from dotenv import load_dotenv

load_dotenv()

# ==========================================
# 테스트 설정
# ==========================================
TARGET_URL = "https://www.instagram.com/7elevenkorea/"
START_DATE = "2025-05-01"
END_DATE = "2025-05-29"
RESULTS_LIMIT = 30  # 소량 테스트 — 토큰 최소 소모
# ==========================================


def test_until_param():
    APIFY_TOKEN = os.getenv("APIFY_TOKEN", "your_apify_token_here")
    client = ApifyClient(APIFY_TOKEN)

    # Case A: until 파라미터 없이 실행 (기존 방식)
    run_input_without_until = {
        "directUrls": [TARGET_URL],
        "resultsLimit": RESULTS_LIMIT,
        "resultsType": "posts",
        "onlyPostsNewerThan": START_DATE,
        "proxyConfiguration": {"useApifyProxy": True, "apifyProxyGroups": ["DATACENTER"]},
    }

    # Case B: until 파라미터 추가 (신규 시도)
    run_input_with_until = {
        "directUrls": [TARGET_URL],
        "resultsLimit": RESULTS_LIMIT,
        "resultsType": "posts",
        "onlyPostsNewerThan": START_DATE,
        "until": END_DATE,              # 상한선 파라미터 테스트
        "proxyConfiguration": {"useApifyProxy": True, "apifyProxyGroups": ["DATACENTER"]},
    }

    print("=" * 60)
    print(f"[테스트 조건]")
    print(f"  대상 계정 : {TARGET_URL}")
    print(f"  수집 기간 : {START_DATE} ~ {END_DATE}")
    print(f"  resultsLimit : {RESULTS_LIMIT} (소량 테스트)")
    print("=" * 60)

    for label, run_input in [
        ("Case A: until 파라미터 없음 (기존)", run_input_without_until),
        ("Case B: until 파라미터 있음 (신규)", run_input_with_until),
    ]:
        print(f"\n--- {label} ---")
        try:
            run = client.actor("apify/instagram-scraper").call(run_input=run_input)

            dates = []
            skipped = []

            for item in client.dataset(run["defaultDatasetId"]).iterate_items():
                post_date = (item.get("timestamp") or "")[:10]
                if not post_date:
                    continue
                if post_date > END_DATE:
                    skipped.append(post_date)
                    print(f"  [skip] {post_date} — END_DATE({END_DATE}) 초과")
                else:
                    dates.append(post_date)
                    print(f"  [수집] {post_date}")

            print(f"\n  결과 요약:")
            print(f"    수집: {len(dates)}개  |  skip: {len(skipped)}개")
            if dates:
                print(f"    수집 범위: {min(dates)} ~ {max(dates)}")
            if skipped:
                print(f"  => until 파라미터 미동작 — skip 발생 ({len(skipped)}개)")
            else:
                print(f"  => until 파라미터 동작 확인 — skip 없음")

        except Exception as e:
            print(f"  오류 발생: {e}")

    print("\n" + "=" * 60)
    print("[판정 기준]")
    print("  Case A skip 있음 + Case B skip 없음 → until 파라미터 유효, 본 코드에 적용 가능")
    print("  Case A skip 있음 + Case B skip 있음 → until 파라미터 미지원, 기존 방식 유지")
    print("=" * 60)


if __name__ == "__main__":
    test_until_param()
