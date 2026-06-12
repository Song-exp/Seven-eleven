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
        
        # URL 정규화
        content_url = url
        if "PostView.naver" not in url:
            if "Redirect=Log" in url:
                blog_id = url.split("blog.naver.com/")[1].split("?")[0]
                log_no = url.split("logNo=")[1].split("&")[0] if "&" in url else url.split("logNo=")[1]
                content_url = f"https://blog.naver.com/PostView.naver?blogId={blog_id}&logNo={log_no}"
            elif "blog.naver.com/" in url:
                parts = url.split("blog.naver.com/")[1].split("/")
                if len(parts) >= 2:
                    blog_id = parts[0]
                    log_no = parts[1].split("?")[0]
                    content_url = f"https://blog.naver.com/PostView.naver?blogId={blog_id}&logNo={log_no}"

        response = requests.get(content_url, headers=headers, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 본문 텍스트 추출
        content_div = soup.select_one('.se-main-container')
        if not content_div:
            content_div = soup.select_one('#postViewArea')
        if not content_div:
            content_div = soup.select_one('.se_component_wrap') or soup.select_one('.post_ct')

        if content_div:
            return content_div.get_text(separator='\n', strip=True)
            
        return ""
    except Exception as e:
        # print(f"   ⚠️ 본문 추출 중 오류 ({url}): {e}")
        return ""

def run_naver_blog_crawler(keywords, start_date=None, results_limit=50, sort='sim'):
    """
    네이버 블로그 검색 API를 사용하여 데이터를 수집합니다.
    """
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
        kw_results = []
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
                title = item['title'].replace('<b>', '').replace('</b>', '')
                description = item['description'].replace('<b>', '').replace('</b>', '')
                postdate = item['postdate']
                link = item['link']
                
                if start_date:
                    formatted_date = datetime.strptime(postdate, "%Y%m%d").strftime("%Y-%m-%d")
                    if formatted_date < start_date:
                        continue
                
                # 본문 크롤링
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
                
                time.sleep(0.1) # 딜레이를 조금 줄임
            
            print(f"✅ '{kw}' 수집 완료: {len(kw_results)}건")
                
        except Exception as e:
            print(f"⚠️ '{kw}' 검색 중 오류 발생: {e}")
            
        time.sleep(0.1)
        
    if not all_results:
        print("📭 수집된 데이터가 없습니다.")
        return None
        
    return pd.DataFrame(all_results)

if __name__ == "__main__":
    # IP 리스트
    raw_ip_list = [
        "KBL", "최강록(흑백요리사)", "추성훈", "주토피아", "케이팝데몬헌터스", 
        "산리오캐릭터즈", "K LEAGUE", "헬로키티", "밸리곰", "라인플렌즈", 
        "이장우", "서울 자가에 대기업 다니는 김 부장 이야기", "이정후", "FIFA 파니니", 
        "하츄핑", "SK하이닉스", "아티제", "홍콩제니쿠키", "테디베어", "귀멸의 칼날", 
        "부창제과", "앙리마티스", "하정우", "온정돈까스(디진다 돈까스)", "롯데리아", 
        "롯데자이언츠", "장민호", "마루짱", "캐치티니핑", "엔하이픈", "춘식이", 
        "케플러", "김잼작가", "디즈니", "좀비딸", "MADEIN", "이펙스", "김진환", 
        "RESCENE(리센느)", "뵈르뵈르", "미키", "청수당(익선동 한옥카페)", 
        "미노리키친(나고야 출신 셰프)", "키키블룸", "성적을 부탁해 티처스2(티처스)", 
        "이스타항공", "페코짱", "트리플에스", "히밥", "푸하하소금빵", "미미미누", 
        "NCTWISH (엔시티 위시)", "장충동왕족발", "이봉원", "템페스트", "세븐틴", 
        "토트넘", "맨시티", "키키쿼카", "박은영(흑백요리사)", "엑스디너리 히어로즈", 
        "SF9", "정희원교수", "안유성(흑백요리사)", "CIX", "랏소베어", "블루밍테일", 
        "디저트39", "에드워드 리(흑백요리사)", "유미의 세포들", "두햄빠! (Doo-ham-pa!)", 
        "위글위글 (Wiggle Wiggle)", "이나피스퀘어 (INAPSQUARE)", "리락쿠마", "오구", 
        "앙리마티스", "헬로맨"
    ]
    
    # 각 키워드 뒤에 ' 콜라보' 추가
    ip_list = [f"{ip} 콜라보" for ip in raw_ip_list]
    
    # 크롤링 실행 (결과 제한을 10으로 설정하여 빠르게 진행, 필요시 조절 가능)
    df_results = run_naver_blog_crawler(ip_list, start_date="2024-01-01", results_limit=10)
    
    if df_results is not None:
        output_file = 'tests/IP크롤링/IP_crawling_results.xlsx'
        # 엑셀 파일로 저장
        df_results.to_excel(output_file, index=False)
        print(f"🎉 전체 크롤링 결과 저장 완료: {output_file}")
