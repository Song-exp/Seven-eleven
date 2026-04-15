import os
import requests
import pandas as pd
import time
from datetime import datetime
from dotenv import load_dotenv
from bs4 import BeautifulSoup

# .env 파일 로드
load_dotenv()

def get_blog_content(url):
    """
    네이버 블로그 링크에서 본문 내용을 추출합니다.
    """
    try:
        if "blog.naver.com" not in url:
            return ""

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
        # 1. URL 정규화: 실제 본문이 있는 PostView 페이지로 변환
        content_url = url
        if "PostView.naver" not in url:
            if "Redirect=Log" in url:
                # 형식: https://blog.naver.com/id?Redirect=Log&logNo=123...
                blog_id = url.split("blog.naver.com/")[1].split("?")[0]
                log_no = url.split("logNo=")[1].split("&")[0] if "&" in url else url.split("logNo=")[1]
                content_url = f"https://blog.naver.com/PostView.naver?blogId={blog_id}&logNo={log_no}"
            elif "blog.naver.com/" in url:
                # 형식: https://blog.naver.com/id/123...
                parts = url.split("blog.naver.com/")[1].split("/")
                if len(parts) >= 2:
                    blog_id = parts[0]
                    log_no = parts[1].split("?")[0] # 쿼리 스트링 제거
                    content_url = f"https://blog.naver.com/PostView.naver?blogId={blog_id}&logNo={log_no}"

        response = requests.get(content_url, headers=headers, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 2. 본문 텍스트 추출 (다양한 에디터 버전 대응)
        # 스마트에디터 ONE
        content_div = soup.select_one('.se-main-container')
        if not content_div:
            # 스마트에디터 2.0
            content_div = soup.select_one('#postViewArea')
        if not content_div:
            # 구형 에디터 등 기타
            content_div = soup.select_one('.se_component_wrap') or soup.select_one('.post_ct')

        if content_div:
            # 이미지 캡션이나 불필요한 태그 제거 (선택 사항)
            return content_div.get_text(separator='\n', strip=True)
            
        return ""
    except Exception as e:
        print(f"   ⚠️ 본문 추출 중 오류 ({url}): {e}")
        return ""

def run_naver_blog_crawler(keywords, start_date=None, results_limit=100, sort='sim'):
    """
    네이버 블로그 검색 API를 사용하여 데이터를 수집합니다.
    
    :param keywords: 검색할 키워드 리스트 (또는 단일 문자열)
    :param start_date: 수집할 데이터의 시작 날짜 (YYYY-MM-DD 형식, 기본값: None)
    :param results_limit: 키워드당 최대 수집 결과 수 (최대 100, 기본값: 100)
    :param sort: 정렬 방식 ('sim': 유사도순, 'date': 날짜순)
    :return: 수집된 데이터프레임
    """
    
    # 1. 인증 정보 설정
    client_id = os.getenv("NAVER_CLIENT_ID")
    client_secret = os.getenv("NAVER_CLIENT_SECRET")
    
    if not client_id or not client_secret:
        print("❌ 네이버 API 인증 정보가 .env에 없습니다.")
        return None

    headers = {
        "X-Naver-Client-Id": client_id,
        "X-Naver-Client-Secret": client_secret
    }
    
    url = "https://openapi.naver.com/v1/search/blog.json"
    
    if isinstance(keywords, str):
        keywords = [keywords]
        
    all_results = []
    
    print(f"🚀 네이버 블로그 수집 시작... (키워드: {len(keywords)}개)")
    
    for kw in keywords:
        kw_results = [] # 키워드별 결과를 따로 담기 위한 리스트
        params = {
            "query": kw,
            "display": min(results_limit, 100),
            "start": 1,
            "sort": sort
        }
        
        try:
            print(f"🔍 키워드 검색 중: '{kw}'")
            response = requests.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            items = data.get('items', [])
            for item in items:
                # 데이터 정제 (HTML 태그 제거 등)
                title = item['title'].replace('<b>', '').replace('</b>', '')
                description = item['description'].replace('<b>', '').replace('</b>', '')
                postdate = item['postdate'] # YYYYMMDD
                link = item['link']
                
                # 날짜 필터링 (start_date가 있는 경우)
                if start_date:
                    formatted_date = datetime.strptime(postdate, "%Y%m%d").strftime("%Y-%m-%d")
                    if formatted_date < start_date:
                        continue
                
                # 본문 크롤링 추가
                print(f"   📄 본문 수집 중: {title[:20]}...")
                content = get_blog_content(link)
                
                result_item = {
                    "keyword": kw,
                    "title": title,
                    "description": description,
                    "content": content,
                    "link": link,
                    "bloggername": item['bloggername'],
                    "postdate": postdate,
                    "crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                kw_results.append(result_item)
                all_results.append(result_item)
                
                # 본문 수집 후 짧은 대기
                time.sleep(0.3)
            
            # 키워드별 결과 저장 (결과가 있을 때만)
            if kw_results:
                kw_df = pd.DataFrame(kw_results)
                # 저장 폴더: tests/네이버블로그크롤링
                output_dir = os.path.dirname(os.path.abspath(__file__))
                os.makedirs(output_dir, exist_ok=True)
                
                # 파일명에 키워드 포함 (공백은 언더바로 대체)
                safe_kw = kw.replace(" ", "_")
                output_file = os.path.join(output_dir, f'naver_blog_data_{safe_kw}.csv')
                kw_df.to_csv(output_file, index=False, encoding='utf-8-sig')
                print(f"💾 키워드 '{kw}' 저장 완료: {output_file}")
                
        except Exception as e:
            print(f"⚠️ '{kw}' 검색 중 오류 발생: {e}")
            
        # API 호출 간격 조절
        time.sleep(0.1)
        
    if not all_results:
        print("📭 수집된 데이터가 없습니다.")
        return None
        
    return pd.DataFrame(all_results)

if __name__ == "__main__":
    # 테스트 실행
    test_keywords = ["세븐일레븐 이나피스퀘어"]
    run_naver_blog_crawler(test_keywords, start_date="2025-01-01", results_limit=50)
