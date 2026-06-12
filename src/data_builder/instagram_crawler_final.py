import os
import pandas as pd
from apify_client import ApifyClient
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

def run_instagram_crawler(target_url, start_date=None, end_date=None, results_limit=1200):
    """
    Apify Instagram Scraper를 사용하여 특정 기간의 게시물을 수집합니다.

    - 하한선(start_date): onlyPostsNewerThan 파라미터로 API 레벨에서 필터링
    - 상한선(end_date): API 파라미터 미지원 → Python 레벨에서 필터링 후 break 조기 종료
    - resultsLimit: (현재날짜 - start_date) 기간의 예상 게시물 수에 안전 마진을 더해 설정
    """
    # 1. 인증 및 클라이언트 설정
    APIFY_TOKEN = os.getenv("APIFY_TOKEN", "your_apify_token_here")
    client = ApifyClient(APIFY_TOKEN)

    # 프로젝트 경로 설정
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    output_dir = os.path.join(project_root, 'data', 'raw')
    os.makedirs(output_dir, exist_ok=True)

    # 2. 크롤링 입력 파라미터
    # - onlyPostsNewerThan: API 레벨 하한선 필터 (이 날짜 이전 게시물은 수집 안 함)
    # - 상한선(end_date)에 해당하는 API 파라미터는 공식 미지원 → iterate_items 루프에서 처리
    run_input = {
        "directUrls": [target_url],
        "resultsLimit": results_limit,
        "resultsType": "posts",
        "onlyPostsNewerThan": start_date,
        "proxyConfiguration": {
            "useApifyProxy": True,
            "apifyProxyGroups": ["DATACENTER"]
        },
    }

    print(f"[시작] '{target_url}' 수집 시작...")
    print(f"[기간] {start_date} ~ {end_date}")
    print(f"[설정] resultsLimit={results_limit} | {end_date} 이후 게시물은 수집 후 제외, {start_date} 이전 도달 시 조기 종료")

    try:
        # 액터 실행
        run = client.actor("apify/instagram-scraper").call(run_input=run_input)

        print("데이터 수집 완료, 결과 필터링 중...")
        items = []
        skipped_newer = 0

        # Instagram은 최신순으로 반환
        # - end_date 초과: continue (더 오래된 게시물이 아직 남아있으므로 계속 탐색)
        # - start_date 미만: break (이후는 모두 범위 밖이므로 조기 종료)
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            full_timestamp = item.get("timestamp")
            if not full_timestamp:
                continue
            post_date = full_timestamp[:10]

            # 상한선 초과: 건너뜀 (API 미지원이므로 Python 레벨 처리)
            if end_date and post_date > end_date:
                skipped_newer += 1
                print(f"[skip] {post_date} — end_date({end_date}) 이후, 건너뜀 ({skipped_newer}번째)")
                continue

            # 하한선 미만: 조기 종료 (이후 모든 게시물도 범위 밖)
            if start_date and post_date < start_date:
                print(f"[조기 종료] {post_date} 도달 — start_date({start_date}) 이전이므로 중단")
                break
            
            # 텍스트 분리
            full_caption = item.get("caption", "")
            if full_caption:
                lines = full_caption.split('\n', 1)
                title_clean = lines[0].strip()[:50] + "..." if len(lines[0]) > 50 else lines[0].strip()
                body_clean = lines[1].strip() if len(lines) > 1 else ""
            else:
                title_clean = "제목 없음"
                body_clean = ""

            print(f"[{len(items)+1:04d}] 수집 중 | 날짜: {post_date} | 제목: {title_clean}")

            items.append({
                "post_id": item.get("id"),
                "date": post_date,
                "timestamp": full_timestamp,
                "title": title_clean,
                "body": body_clean,
                "likes": item.get("likesCount"),
                "comments": item.get("commentsCount"),
                "url": item.get("url")
            })

        earliest = items[-1]["date"] if items else "없음"
        latest = items[0]["date"] if items else "없음"
        print(f"[필터 결과] 수집: {len(items)}개 | end_date 초과로 제외: {skipped_newer}개")
        print(f"[수집 범위] {earliest} ~ {latest}")

        if not items:
            print("조건에 맞는 데이터가 없습니다. (수집 기간 및 resultsLimit을 확인하세요.)")
            return

        df = pd.DataFrame(items)
        account_name = target_url.strip("/").split("/")[-1]
        filename = f"instagram_{account_name}_{start_date}_to_{end_date}.csv"
        output_path = os.path.join(output_dir, filename)

        df.to_csv(output_path, index=False, encoding='utf-8-sig')

        print(f"저장 완료: {len(df)}개 → {os.path.abspath(output_path)}")

    except Exception as e:
        print(f"❌ 오류 발생: {e}")

if __name__ == "__main__":
    
    # 2025년 전체 수집
    run_instagram_crawler(
        target_url="https://www.instagram.com/gs25_official/",
        start_date="2025-01-01",
        end_date="2025-12-31",
        results_limit=2500
    )
