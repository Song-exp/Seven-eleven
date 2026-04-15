import os
import pandas as pd
from apify_client import ApifyClient
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv(dotenv_path='/Users/hyunoworld/Desktop/Seminar/My_Seven_Eleven/Workspace/API_3.env')

def run_instagram_crawler(target_url, start_date=None, end_date=None, results_limit=1500):
    """
    [토큰 초절약 버전]
    onlyPostsNewerThan/oldestPostDate(하한선)와 newestPostDate(상한선)를 사용하여
    특정 기간 외의 데이터를 유료 결과물에서 제외합니다.
    """
    # 1. 인증 및 클라이언트 설정
    APIFY_TOKEN = os.getenv("APIFY_TOKEN", "your_apify_token_here")
    client = ApifyClient(APIFY_TOKEN)

    # 프로젝트 경로 설정
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    output_dir = os.path.join(project_root, 'data', 'raw')
    os.makedirs(output_dir, exist_ok=True)

    # 2. 크롤링 입력 파라미터 (비용 최적화 극대화)
    # Instagram Scraper는 최신순으로 수집하므로, 하한선/상한선 파라미터를 모두 설정합니다.
    # 참고: Apify actor 버전에 따라 파라미터명이 다를 수 있어 양쪽 명칭을 모두 전달합니다.
    run_input = {
        "directUrls": [target_url],
        "resultsLimit": results_limit,
        "resultsType": "posts",
        "onlyPostsNewerThan": start_date,  # 하한선 (공식 문서 기준 파라미터명)
        "oldestPostDate": start_date,      # 하한선 (actor 버전에 따른 대체 파라미터명)
        "newestPostDate": end_date,        # 상한선 — actor가 지원 시 2026년 게시물 서버 단에서 차단
        "skipPinnedPosts": True,           # 고정 게시물은 날짜가 섞이므로 제외

        "proxyConfiguration": {
            "useApifyProxy": True,
            "apifyProxyGroups": ["RESIDENTIAL"]
        },
        "includeComments": False,
        "includeContextualLocation": False,
        "includeSourceVideoUrl": False,
    }

    print(f"🚀 [최적화 모드] '{target_url}' 수집 시작...")
    print(f"📅 목표 구간: {start_date} ~ {end_date}")
    print(f"⚠️  주의: 최신글(2026년 등)부터 {end_date}까지는 무료 탐색 후 데이터셋에서 제외합니다.")

    try:
        # 액터 실행
        run = client.actor("apify/instagram-scraper").call(run_input=run_input)

        print("📦 데이터 수집 완료, 결과 필터링 중...")
        items = []
        total_received = 0   # Apify로부터 수신한 총 게시물 수
        skipped_future = 0   # end_date 이후 게시물 스킵 수
        consecutive_old = 0  # start_date 이전 게시물이 연속으로 나온 횟수
        CONSECUTIVE_OLD_LIMIT = 5  # 이 횟수 연속 초과 시 완전히 종료 (정렬 오류 방어)

        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            total_received += 1
            full_timestamp = item.get("timestamp")
            if not full_timestamp:
                continue

            # timestamp 형식 진단 (처음 3건만 출력)
            if total_received <= 3:
                print(f"  [진단] timestamp 샘플 #{total_received}: {full_timestamp!r} → 파싱: {full_timestamp[:10]}")

            post_date = full_timestamp[:10]

            # end_date보다 미래 게시물은 건너뜀 (newestPostDate 미지원 actor 버전 대비 클라이언트 필터)
            if end_date and post_date > end_date:
                skipped_future += 1
                continue

            # start_date 이전 게시물: 정렬 오류를 감안해 연속 N회 초과 시에만 종료
            if start_date and post_date < start_date:
                consecutive_old += 1
                if consecutive_old >= CONSECUTIVE_OLD_LIMIT:
                    print(f"⏹️  {post_date} — start_date({start_date}) 이전 게시물 {consecutive_old}건 연속, 수집 종료")
                    print(f"  [요약] Apify 수신: {total_received}건 | 미래 스킵: {skipped_future}건 | 수집: {len(items)}건")
                    break
                continue  # 일시적 정렬 오류일 수 있으므로 바로 break 하지 않고 계속 진행
            else:
                consecutive_old = 0  # 범위 안 게시물이 나오면 카운터 리셋

            # 텍스트 분리
            full_caption = item.get("caption", "")
            if full_caption:
                lines = full_caption.split('\n', 1)
                title_clean = lines[0].strip()[:50] + "..." if len(lines[0]) > 50 else lines[0].strip()
                body_clean = lines[1].strip() if len(lines) > 1 else ""
            else:
                title_clean = "제목 없음"
                body_clean = ""

            # 수집 로그
            print(f"[{len(items)+1:04d}] ✅ 수집 중 | 날짜: {post_date} | 제목: {title_clean}")

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

        print(f"  [요약] Apify 수신: {total_received}건 | 미래 스킵: {skipped_future}건 | 수집: {len(items)}건")

        if not items:
            print("⚠️ 조건에 맞는 데이터가 없습니다.")
            if total_received == 0:
                print("  → Apify에서 반환된 데이터가 0건입니다. API 토큰 및 actor 실행 결과를 확인하세요.")
            elif skipped_future == total_received:
                print(f"  → 수신된 {total_received}건 전부가 end_date({end_date}) 이후 게시물입니다.")
                print(f"     results_limit({results_limit})을 늘리거나 newestPostDate 파라미터 지원 여부를 확인하세요.")
            return

        df = pd.DataFrame(items)
        account_name = target_url.strip("/").split("/")[-1]
        filename = f"instagram_{account_name}_{start_date}_to_{end_date}.csv"
        output_path = os.path.join(output_dir, filename)

        df.to_csv(output_path, index=False, encoding='utf-8-sig')

        print(f"✅ 최종 저장 완료: {len(df)}개")
        print(f"💾 저장 경로: {os.path.abspath(output_path)}")

    except Exception as e:
        print(f"❌ 오류 발생: {e}")

if __name__ == "__main__":
    # [2025년 전체 수집 실행]
    run_instagram_crawler(
        target_url="https://www.instagram.com/eyesmag/",
        start_date="2025-01-01",
        end_date="2025-12-31",
        results_limit=10000  # 2025년 구간 도달을 위한 넉넉한 한도
    )
