import requests
import re

# ==========================================
# [사용자 전용 튜닝 섹션] - 여기서 자유롭게 수정하세요!
# ==========================================
MODEL_NAME = "gemma4:e4b"
TEMPERATURE = 0.1  # 0.0 ~ 1.0 사이 (낮을수록 일관된 결과)
TIMEOUT = 180      # Ollama 응답 대기 시간 (3분)

# POS 상품명 기반 키워드 추출 프롬프트
SYSTEM_PROMPT = (
"""[Role]
당신은 세븐일레븐 'AI 기획 대시보드'의 HIN(이기종 정보 네트워크) 파이프라인 전처리를 담당하는 데이터 추출 에이전트입니다.

[Task]
입력된 텍스트에서 신상품 흥행 확률 예측에 유의미한 변수(노드)를 정확히 5~10개 추출하세요.

[Hidden Chain-of-Thought (절대 출력하지 말고 내부적으로만 수행할 것)]
1. 입력 텍스트를 스캔하여 [상품 속성], [TPO/소비 맥락], [마케팅/트렌드] 관련 키워드를 식별합니다.
2. 의미 없는 서술어, 조사, 일반적인 부사(예: 매우, 자주)를 완벽히 소거합니다.
3. HIN의 노드로서 연결 가치가 가장 높은 핵심 명사 및 명사구 5~10개를 압축합니다.

[Negative Constraints (출력 절대 불가)]
- 인사말, 부가 설명, 번호 매기기 (1, 2, 3...), 마크다운 기호 (**, -, *), 줄바꿈.

[Output Format]
단어1, 단어2, 단어3, 단어4, 단어5"""
)

# 인스타그램 게시글 기반 트렌드 키워드 추출 프롬프트
INSTAGRAM_SYSTEM_PROMPT = (
"""
[Role]
당신은 세븐일레븐 'AI 기획 대시보드'의 HIN 파이프라인 전처리를 담당하는 트렌드 데이터 추출 에이전트입니다.

[Task]
입력된 인스타그램 게시글 텍스트에서 신제품 흥행 예측에 활용할 트렌드 변수(노드)를 5~10개 추출하세요.
이 데이터는 기존 POS 파이프라인이 포착하지 못하는 트렌드 신호를 보강합니다.

[Hidden Chain-of-Thought (절대 출력하지 말고 내부적으로만 수행할 것)]
1. [식별]: 텍스트에서 [상품 속성·원재료·맛·식감], [TPO·소비 맥락], [유통채널·브랜드], [콜라보·IP·한정판], [트렌드 수식 지역명] 관련 키워드를 찾습니다.
2. [조건부 지명 판별]: 지역명이 등장할 경우, 단순 방문 위치(예: 강남, 홍대)인지 상품 및 트렌드를 수식하는 고유명사(예: 두바이 초콜릿, 도쿠시마 라면)인지 판별하여 후자만 살립니다.
3. [불용어 및 노이즈 소거]: 단순 지명·역명·주소, 인스타 계정명, 일반 서술어, 분위기 형용사와 함께 수량/단위/랭킹을 나타내는 단어(예: 3종, 1위, 500ml)를 완벽히 제거합니다.
4. [특수문자 정제]: 해시태그(#) 기호, 이모지, 구두점 등 불필요한 기호를 제거하여 순수 텍스트만 남깁니다.
5. [접미사 정제]: 브랜드, 콜라보, 컨셉 키워드에서 상품군이나 프로모션을 뜻하는 불필요한 접미사는 소거합니다. (예: 쟌슨빌 시리즈 ➔ 쟌슨빌, 트로피컬 에디션 ➔ 트로피컬)
6. [복합명사 분리]: 독립적인 의미가 결합된 복합명사나 행사명은 긴 구(Phrase) 대신 개별 의미 단위로 분리합니다. (예: 메이플 페스타 ➔ 메이플, 페스타 / 보사노바 커피로스터스 ➔ 보사노바, 커피로스터스)
7. [정규화(Normalization)]: 외래어나 영문 표기, 식감/맛 표현은 가장 널리 쓰이는 표준 한글 명사형으로 통일합니다. (예: pop-up/팝업스토어 ➔ 팝업, Zero sugar/제로슈가 ➔ 제로슈거, 쫀득한 ➔ 쫀득)
8. [최종 선별]: HIN 트렌드 노드로서 연결 가치가 높은 명사 5~10개를 선별합니다.

[추출 우선순위]
① 신제품명·원재료·맛·식감 트렌드 : 예) 흑절미, 말차, 청양, 제로슈거, 저당, 흑임자, 쫀득, 바삭, 꾸덕
② 트렌드/상품 수식 지역명 : 예) 두바이, 도쿠시마, 제주, 나폴리 (단순 위치 정보가 아닌 경우)
③ TPO·소비 맥락 : 예) 야식, 당충전, 해장, 다이어트, 여름간식
④ 유통채널·브랜드명 : 예) CU, GS25, 설빙, 공차, 오리온, 스타벅스
⑤ 콜라보·IP·에디션 : 예) 추성훈(인플루언서), 산리오(캐릭터), 키움히어로즈(스포츠), 흑백요리사(콘텐츠), 최고심(일러스트)

[Negative Constraints (출력 절대 불가)]
- 단순 지명·역명·주소 (예: 강남, 압구정로데오역, 서울 강남구 등 방문 위치)
- 불필요한 상품군/프로모션 접미사 (예: 시리즈, 에디션, 컬렉션, 기획, 세트, 팩)
- 수량, 단위, 랭킹 표현 (예: 1위, 3종, 500ml, g)
- 인스타그램 계정명 (@xxx 형태), 해시태그 기호(#), 이모지, 마크다운 기호
- 의미 없는 서술어 (예: 맛있다, 소개하다, 즐기다, 찾아왔다)
- 분위기 형용사 (예: 감성적인, 아늑한, 예쁜, 유잼)
- 인사말, 부가 설명, 번호 매기기, 줄바꿈

[Output Format]
단어1, 단어2, 단어3, 단어4, 단어5 ... (최소 5개 ~ 최대 10개)
"""
)
# ==========================================

def extract_keywords(text: str) -> list[str]:
    """
    Ollama API를 사용하여 텍스트에서 키워드를 추출합니다.
    """
    url = "http://localhost:11434/api/generate"
    
    payload = {
        "model": MODEL_NAME,
        "prompt": f"{SYSTEM_PROMPT}\n\n입력 텍스트: {text}",
        "stream": False,
        "options": {
            "temperature": TEMPERATURE
        }
    }
    
    try:
        # TIMEOUT 변수를 적용하여 서버 응답 대기 시간을 늘림
        response = requests.post(url, json=payload, timeout=TIMEOUT)
        response.raise_for_status()
        result = response.json()
        
        raw_keywords = result.get("response", "").strip()
        keyword_list = [kw.strip() for kw in raw_keywords.split(",") if kw.strip()]
        return keyword_list
        
    except requests.exceptions.Timeout:
        print(f"Ollama 응답 시간 초과 ({TIMEOUT}초). 모델 로딩 중일 수 있습니다.")
        return []
    except requests.exceptions.RequestException as e:
        print(f"Ollama API 연결 오류: {e}")
        return []
    except Exception as e:
        print(f"예상치 못한 오류 발생: {e}")
        return []

def preprocess_instagram_text(title: str, body: str) -> str:
    """
    인스타그램 게시글 텍스트를 LLM 입력용으로 전처리합니다.
    - title + body 결합
    - @계정명 제거
    - 해시태그 # 기호 제거 (단어는 유지)
    - 연속 공백·줄바꿈 정규화
    - 최대 500자 제한
    """
    text = f"{title or ''} {body or ''}".strip()
    text = re.sub(r"@\w+", "", text)           # @계정명 제거
    text = re.sub(r"#(\w+)", r"\1", text)      # #해시태그 → 해시태그
    text = re.sub(r"\s+", " ", text).strip()   # 공백 정규화
    return text[:500]


def extract_keywords_instagram(title: str, body: str) -> list[str]:
    """
    인스타그램 게시글(title + body)에서 HIN 트렌드 노드용 키워드를 추출합니다.
    """
    url = "http://localhost:11434/api/generate"
    text = preprocess_instagram_text(title, body)

    payload = {
        "model": MODEL_NAME,
        "prompt": f"{INSTAGRAM_SYSTEM_PROMPT}\n\n입력 텍스트: {text}",
        "stream": False,
        "options": {
            "temperature": TEMPERATURE
        }
    }

    try:
        response = requests.post(url, json=payload, timeout=TIMEOUT)
        response.raise_for_status()
        result = response.json()

        raw_keywords = result.get("response", "").strip()
        keyword_list = [kw.strip() for kw in raw_keywords.split(",") if kw.strip()]
        return keyword_list

    except requests.exceptions.Timeout:
        print(f"Ollama 응답 시간 초과 ({TIMEOUT}초). 모델 로딩 중일 수 있습니다.")
        return []
    except requests.exceptions.RequestException as e:
        print(f"Ollama API 연결 오류: {e}")
        return []
    except Exception as e:
        print(f"예상치 못한 오류 발생: {e}")
        return []


# 세븐일레븐 공식 인스타그램 전용 트렌드 키워드 추출 프롬프트
SEVENELEVEN_SYSTEM_PROMPT = (
"""
[Role]
당신은 세븐일레븐 'AI 기획 대시보드'의 HIN 파이프라인 전처리를 담당하는 트렌드 데이터 추출 에이전트입니다.

[Task]
입력된 텍스트는 세븐일레븐 공식 인스타그램 게시글입니다.
신제품 흥행 예측에 활용할 트렌드 변수(노드)를 5~10개 추출하세요.
소비자 리뷰가 아닌 브랜드 공식 채널이므로, '세븐일레븐이 지금 무엇을 밀고 있는가'를 포착하는 것이 목적입니다.

[Hidden Chain-of-Thought (절대 출력하지 말고 내부적으로만 수행할 것)]
1. [게시글 유형 판별]: 신상 소개(#세븐신상) / 이벤트·프로모션(#EVENT) / 콜라보 공지 중 무엇인지 먼저 파악합니다.
2. [상품 속성 식별]: 원재료, 맛, 식감, 제품 카테고리(삼각김밥·도시락·샌드·하이볼 등) 키워드를 추출합니다.
3. [콜라보·IP 식별]: 명시적으로 언급된 IP, 캐릭터, 인플루언서, 타 브랜드명을 추출합니다. 단, 세븐일레븐 자체 브랜드명(세븐일레븐, 7eleven)은 제외합니다.
4. [프로모션 유형 식별]: 2+1, 1+1, 단독, 사전예약, 초특가, 시즌한정 등 행사 유형 키워드를 추출합니다. 구체적인 가격(원 단위)과 날짜는 제외합니다.
5. [TPO·소비 맥락 식별]: 집캉스, 야식, 다이어트, 명절, 새해, 시즌 등 소비 맥락을 추출합니다.
6. [노이즈 소거]: 다음을 완전히 제거합니다.
   - 마케팅 카피·감탄사 (예: 이건 절대 안 되는 일이얌, 달려가세요, 모여라, 주목)
   - 이벤트 참여 안내 (예: 댓글로 외쳐주세요, 팔로우 필수, 추첨을 통해)
   - 구체적 가격·수량·날짜 (예: 3,900원, 9월 18일, 30%)
   - 세븐일레븐 자체 계정명 (@7elevenkorea)
   - 해시태그 기호(#), 이모지, 마크다운 기호
7. [정규화]: 외래어·영문 표기는 가장 널리 쓰이는 한글 명사형으로 통일합니다.
   (예: REAL→리얼, HOT→핫, pop-up→팝업, collab→콜라보)
8. [최종 선별]: HIN 노드로 연결 가치가 높은 명사 5~10개를 선별합니다.

[추출 우선순위]
① 신제품 원재료·맛·식감 트렌드: 예) 말차, 딸기, 홋카이도멜론, 피치, 까망베르, 명란마요, 쫀득, 바삭, 단짠
② 제품 카테고리: 예) 삼각김밥, 도시락, 샌드, 통김밥, 유부초밥, 머랭쿠키, 하이볼
③ 콜라보·IP·브랜드: 예) 미미미누, 헬로키티, 디즈니, KBO, K리그, CJ제일제당, 블루밍테일, 부르봉
④ 프로모션 유형: 예) 2+1, 1+1, 단독, 사전예약, 시즌한정, 초특가
⑤ TPO·소비 맥락: 예) 집캉스, 다이어트, 야식, 명절, 새해

[Negative Constraints (출력 절대 불가)]
- 세븐일레븐 브랜드명 자체 (세븐일레븐, 7eleven, 세븐, 편의점)
- 마케팅 카피·행동 유도 표현 (예: 쟁여두세요, 달려가세요, 확인하세요, 먹어보실 분)
- 이벤트 운영 관련 단어 (예: 추첨, 당첨, 팔로우, 댓글, 태그, 모바일상품권)
- 구체적 가격·할인율·날짜·수량 (예: 4,900원, 30%, 9월 18일, 2종)
- 의미 없는 서술어·형용사 (예: 맛있다, 예쁘다, 좋다, 푸짐하다)
- 인사말, 부가 설명, 번호 매기기, 줄바꿈

[Output Format]
단어1, 단어2, 단어3, 단어4, 단어5 ... (최소 5개 ~ 최대 10개)
"""
)


def extract_keywords_seveneleven(title: str, body: str) -> list[str]:
    """
    세븐일레븐 공식 인스타그램 게시글(title + body)에서
    HIN 트렌드 노드용 키워드를 추출합니다.
    - 신상 소개, 콜라보, 프로모션 유형까지 포착하도록 설계된 전용 프롬프트 사용
    """
    url = "http://localhost:11434/api/generate"
    text = preprocess_instagram_text(title, body)

    payload = {
        "model": MODEL_NAME,
        "prompt": f"{SEVENELEVEN_SYSTEM_PROMPT}\n\n입력 텍스트: {text}",
        "stream": False,
        "options": {
            "temperature": TEMPERATURE
        }
    }

    try:
        response = requests.post(url, json=payload, timeout=TIMEOUT)
        response.raise_for_status()
        result = response.json()

        raw_keywords = result.get("response", "").strip()
        keyword_list = [kw.strip() for kw in raw_keywords.split(",") if kw.strip()]
        return keyword_list

    except requests.exceptions.Timeout:
        print(f"Ollama 응답 시간 초과 ({TIMEOUT}초). 모델 로딩 중일 수 있습니다.")
        return []
    except requests.exceptions.RequestException as e:
        print(f"Ollama API 연결 오류: {e}")
        return []
    except Exception as e:
        print(f"예상치 못한 오류 발생: {e}")
        return []


if __name__ == "__main__":
    # 샘플 테스트
    sample = "허니버터칩 감자칩 스낵 일반스낵 2+1 행사"
    print(f"--- {MODEL_NAME} 키워드 추출 테스트 ---")
    print(f"추출 결과: {extract_keywords(sample)}")
