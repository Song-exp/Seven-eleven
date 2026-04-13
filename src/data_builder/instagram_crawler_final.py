import os
import pandas as pd
from apify_client import ApifyClient
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

def run_instagram_crawler(target_url, start_date=None, end_date=None, results_limit=1500):
    """
    [토큰 초절약 버전] 
    untilDate와 oldestPostDate를 사용하여 특정 기간 외의 데이터를 유료 결과물에서 제외합니다.
    """
    # 1. 인증 및 클라이언트 설정
    APIFY_TOKEN = os.getenv("APIFY_TOKEN", "your_apify_token_here")
    client = ApifyClient(APIFY_TOKEN)

    # 프로젝트 경로 설정
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    output_dir = os.path.join(project_root, 'data', 'raw')
    os.makedirs(output_dir, exist_ok=True)

    # 2. 크롤링 입력 파라미터 (비용 최적화 극대화)
    # 4/22까지 이미 수집했으므로, 4/21 이전의 데이터만 유료 결과로 카운트하게 설정
    run_input = {
        "directUrls": [target_url],
        "resultsLimit": results_limit,
        "resultType": "posts",
        
        # [비용 절감 핵심 1] 이 날짜 이전의 게시물만 '결과물'로 저장 (2026년~2025년 5월 무시)
        "untilDate": end_date if end_date else None, 
        
        # [비용 절감 핵심 2] 이 날짜에 도달하면 탐색을 즉시 중단
        "oldestPostDate": start_date if start_date else None,
        
        "proxyConfiguration": {
            "useApifyProxy": True,
            "apifyProxyGroups": ["DATACENTER"] 
        },
        "includeComments": False,
        "includeContextualLocation": False,
        "includeSourceVideoUrl": False,
    }

    print(f"🚀 [토큰 절약 모드] '{target_url}' 수집 시작...")
    print(f"📅 수집 구간: {start_date} ~ {end_date} (이외 기간은 무시)")
    print(f"💰 비용 관리: untilDate 적용으로 {end_date} 이후 데이터는 비용 청구 제외")

    try:
        # 액터 실행
        run = client.actor("apify/instagram-scraper").call(run_input=run_input)

        print("📦 데이터 수집 완료, 결과 정리 중...")
        items = []
        
        for i, item in enumerate(client.dataset(run["defaultDatasetId"]).iterate_items()):
            full_timestamp = item.get("timestamp")
            if not full_timestamp: continue
            post_date = full_timestamp[:10]
            
            # 텍스트 분리
            full_caption = item.get("caption", "")
            if full_caption:
                lines = full_caption.split('\n', 1)
                title_clean = lines[0].strip()[:50] + "..." if len(lines[0]) > 50 else lines[0].strip()
                body_clean = lines[1].strip() if len(lines) > 1 else ""
            else:
                title_clean = "제목 없음"
                body_clean = ""

            # 수집 로그 (untilDate 덕분에 수집되는 것만 찍힘)
            print(f"[{i+1:04d}] ✅ 수집 중 | 날짜: {post_date} | 제목: {title_clean}")

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

        if not items:
            print("⚠️ 조건에 맞는 데이터가 없습니다. (수집 기간 및 한도를 확인하세요.)")
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
    # [남은 1월~4월 수집 실행]
    run_instagram_crawler(
        target_url="https://www.instagram.com/knewnew.official/", 
        start_date="2025-01-01", 
        end_date="2025-04-21", 
        results_limit=1500 # 4개월치 넉넉한 한도
    )
