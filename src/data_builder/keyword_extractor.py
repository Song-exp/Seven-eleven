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


# 세븐일레븐 공식 인스타그램 전용 구조화 추출 프롬프트 (v2)
# - 상품 메타데이터(명칭·가격·용량) + 트렌드 노드를 JSON으로 분리 출력
# - metadata를 배열로 처리하여 다중 상품 게시글 대응
SEVENELEVEN_SYSTEM_PROMPT = (
"""
[Role]
당신은 세븐일레븐 'AI 기획 대시보드'의 HIN 파이프라인 전처리를 담당하는 데이터 추출 에이전트입니다.

[Task]
입력된 세븐일레븐 공식 인스타그램 게시글에서
① 정확한 상품 메타데이터(상품명, 가격, 용량)와
② 신제품 흥행 예측에 활용할 트렌드 변수(노드)를 추출하여 JSON 형식으로 출력하세요.
소비자 리뷰가 아닌 브랜드 공식 채널이므로, '세븐일레븐이 지금 무엇을 밀고 있는가'를 포착하는 것이 목적입니다.

[Hidden Chain-of-Thought (절대 출력하지 말고 내부적으로만 수행할 것)]
1. [게시글 유형 판별]: 신상 소개(#세븐신상) / 이벤트·프로모션(#EVENT) / 콜라보 공지 중 무엇인지 먼저 파악합니다.
2. [메타데이터 확보]: 텍스트에 명시된 상품명, 가격(원), 용량/중량(g·ml)을 있는 그대로 추출합니다.
   - 가격은 텍스트에 표기된 값을 그대로 사용합니다. 표기가 없으면 null로 처리합니다. (추정 금지)
   - 상품이 여러 개인 경우 각각 별도 객체로 나열합니다.
   - 용량/중량이 없으면 null로 처리합니다.
3. [맛·식감·원재료 식별]: 말차, 딸기, 명란마요, 쫀득, 바삭 등 신제품 트렌드를 나타내는 표현을 추출합니다.
4. [제품 카테고리 식별]: 삼각김밥, 도시락, 샌드, 하이볼 등 제품 유형을 추출합니다.
   (3·4번 결과는 flavor_and_category 필드에 함께 담습니다.)
5. [콜라보·IP 식별]: 명시적으로 언급된 IP, 캐릭터, 인플루언서, 타 브랜드명을 추출합니다.
   단, 세븐일레븐 자체 브랜드명(세븐일레븐, 7eleven, 세븐)은 제외합니다.
6. [프로모션 유형 식별]: 2+1, 1+1, 단독, 사전예약, 초특가, 시즌한정 등 행사 유형을 추출합니다.
   표기 방식이 다르면 규격화합니다 (원플원 → 1+1, 투플러스원 → 2+1).
7. [TPO·소비 맥락 식별]: 집캉스, 야식, 다이어트, 명절, 새해, 여름간식 등 소비 맥락을 추출합니다.
8. [노이즈 소거]: 다음을 완전히 제거합니다.
   - 마케팅 카피·감탄사 (예: 달려가세요, 주목, 쟁여두세요)
   - 이벤트 참여 안내 (예: 댓글로 외쳐주세요, 팔로우 필수, 추첨을 통해)
   - 행사 날짜·기간 (예: 9월 18일~10월 31일) — 단, 가격과 g/ml 단위는 보존
   - 세븐일레븐 자체 계정명 (@7elevenkorea), 해시태그 기호(#), 이모지
9. [정규화]: 외래어·영문 표기는 가장 널리 쓰이는 한글 명사형으로 통일합니다.
   (예: REAL→리얼, HOT→핫, pop-up→팝업, collab→콜라보)
10. [최종 선별]: 각 필드별 최대 개수(아래 참고)를 초과하지 않도록 연결 가치가 높은 것만 선별합니다.

[추출 우선순위]
⓪ [필수] 상품 메타데이터: 상품명, 가격(원), 용량/중량(g·ml)
① 맛·식감·원재료: 예) 말차, 딸기, 홋카이도멜론, 피치, 까망베르, 명란마요, 쫀득, 바삭, 단짠
② 제품 카테고리: 예) 삼각김밥, 도시락, 샌드, 통김밥, 유부초밥, 머랭쿠키, 하이볼
③ 콜라보·IP·브랜드: 예) 미미미누, 헬로키티, 디즈니, KBO, K리그, CJ제일제당, 부르봉
④ 프로모션 유형: 예) 2+1, 1+1, 단독, 사전예약, 시즌한정, 초특가
⑤ TPO·소비 맥락: 예) 집캉스, 다이어트, 야식, 명절, 새해

[Negative Constraints (출력 절대 불가)]
- 세븐일레븐 브랜드명 자체 (세븐일레븐, 7eleven, 세븐, 편의점)
- 마케팅 카피·행동 유도 표현 (예: 쟁여두세요, 달려가세요, 확인하세요, 먹어보실 분)
- 이벤트 운영 관련 단어 (예: 추첨, 당첨, 팔로우, 댓글, 태그, 모바일상품권)
- 행사 날짜·기간 (예: 9월 18일까지, 주말 한정) — 가격과 용량은 보존할 것
- 의미 없는 서술어·형용사 (예: 맛있다, 예쁘다, 좋다, 푸짐하다)
- 불필요한 마케팅 접미사 (예: 시리즈, 에디션, 기획, 세트, 패키지, 팩)
- 인사말, 부가 설명, 번호 매기기, 줄바꿈

[Output Format (Strict JSON — 다른 텍스트 절대 출력 금지)]
{
  "metadata": [
    {"name": "상품명", "price": 가격숫자또는null, "capacity": "용량문자열또는null"}
  ],
  "flavor_and_category": ["최대 6개"],
  "collab_and_brand": ["최대 5개"],
  "promotion_type": ["최대 3개"],
  "tpo_context": ["최대 3개"]
}
"""
)


def extract_keywords_seveneleven(title: str, body: str) -> dict:
    """
    세븐일레븐 공식 인스타그램 게시글(title + body)에서
    상품 메타데이터 + 트렌드 노드를 구조화된 dict로 추출합니다.

    반환 형식:
    {
        "metadata": [{"name": str, "price": int|None, "capacity": str|None}, ...],
        "flavor_and_category": [str, ...],
        "collab_and_brand": [str, ...],
        "promotion_type": [str, ...],
        "tpo_context": [str, ...]
    }
    파싱 실패 시 빈 dict {} 반환.
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
        raw = response.json().get("response", "").strip()

        # JSON 블록만 추출 (모델이 앞뒤에 텍스트를 붙이는 경우 대비)
        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start == -1 or end == 0:
            print("JSON 파싱 실패: 응답에서 JSON 블록을 찾을 수 없습니다.")
            return {}

        import json as _json
        result = _json.loads(raw[start:end])
        return result

    except requests.exceptions.Timeout:
        print(f"Ollama 응답 시간 초과 ({TIMEOUT}초). 모델 로딩 중일 수 있습니다.")
        return {}
    except requests.exceptions.RequestException as e:
        print(f"Ollama API 연결 오류: {e}")
        return {}
    except Exception as e:
        print(f"예상치 못한 오류 발생: {e}")
        return {}


# CU 공식 인스타그램 전용 구조화 추출 프롬프트
# - SEVENELEVEN_SYSTEM_PROMPT와 동일한 구조, CU 브랜드 관련 부분만 교체
CU_SYSTEM_PROMPT = (
"""
[Role]
당신은 CU 'AI 기획 대시보드'의 HIN 파이프라인 전처리를 담당하는 데이터 추출 에이전트입니다.

[Task]
입력된 CU 공식 인스타그램 게시글에서
① 정확한 상품 메타데이터(상품명, 가격, 용량)와
② 신제품 흥행 예측에 활용할 트렌드 변수(노드)를 추출하여 JSON 형식으로 출력하세요.
소비자 리뷰가 아닌 브랜드 공식 채널이므로, 'CU가 지금 무엇을 밀고 있는가'를 포착하는 것이 목적입니다.

[Hidden Chain-of-Thought (절대 출력하지 말고 내부적으로만 수행할 것)]
1. [게시글 유형 판별]: 신상 소개 / 이벤트·프로모션 / 콜라보 공지 중 무엇인지 먼저 파악합니다.
2. [메타데이터 확보]: 텍스트에 명시된 상품명, 가격(원), 용량/중량(g·ml)을 있는 그대로 추출합니다.
   - 가격은 텍스트에 표기된 값을 그대로 사용합니다. 표기가 없으면 null로 처리합니다. (추정 금지)
   - 상품이 여러 개인 경우 각각 별도 객체로 나열합니다.
   - 용량/중량이 없으면 null로 처리합니다.
3. [맛·식감·원재료 식별]: 말차, 딸기, 명란마요, 쫀득, 바삭 등 신제품 트렌드를 나타내는 표현을 추출합니다.
4. [제품 카테고리 식별]: 삼각김밥, 도시락, 샌드, 하이볼 등 제품 유형을 추출합니다.
   (3·4번 결과는 flavor_and_category 필드에 함께 담습니다.)
5. [콜라보·IP 식별]: 명시적으로 언급된 IP, 캐릭터, 인플루언서, 타 브랜드명을 추출합니다.
   단, CU 자체 브랜드명(CU, BGF리테일)은 제외합니다.
6. [프로모션 유형 식별]: 2+1, 1+1, 단독, 사전예약, 초특가, 시즌한정 등 행사 유형을 추출합니다.
   표기 방식이 다르면 규격화합니다 (원플원 → 1+1, 투플러스원 → 2+1).
7. [TPO·소비 맥락 식별]: 집캉스, 야식, 다이어트, 명절, 새해, 여름간식 등 소비 맥락을 추출합니다.
8. [노이즈 소거]: 다음을 완전히 제거합니다.
   - 마케팅 카피·감탄사 (예: 달려가세요, 주목, 쟁여두세요)
   - 이벤트 참여 안내 (예: 댓글로 외쳐주세요, 팔로우 필수, 추첨을 통해)
   - 행사 날짜·기간 (예: 9월 18일~10월 31일) — 단, 가격과 g/ml 단위는 보존
   - CU 자체 계정명 (@cu_convenience), 해시태그 기호(#), 이모지
9. [정규화]: 외래어·영문 표기는 가장 널리 쓰이는 한글 명사형으로 통일합니다.
   (예: REAL→리얼, HOT→핫, pop-up→팝업, collab→콜라보)
10. [최종 선별]: 각 필드별 최대 개수(아래 참고)를 초과하지 않도록 연결 가치가 높은 것만 선별합니다.

[추출 우선순위]
⓪ [필수] 상품 메타데이터: 상품명, 가격(원), 용량/중량(g·ml)
① 맛·식감·원재료: 예) 말차, 딸기, 홋카이도멜론, 피치, 까망베르, 명란마요, 쫀득, 바삭, 단짠
② 제품 카테고리: 예) 삼각김밥, 도시락, 샌드, 통김밥, 유부초밥, 머랭쿠키, 하이볼
③ 콜라보·IP·브랜드: 예) 미미미누, 헬로키티, 디즈니, KBO, K리그, CJ제일제당, 부르봉
④ 프로모션 유형: 예) 2+1, 1+1, 단독, 사전예약, 시즌한정, 초특가
⑤ TPO·소비 맥락: 예) 집캉스, 다이어트, 야식, 명절, 새해

[Negative Constraints (출력 절대 불가)]
- CU 브랜드명 자체 (CU, BGF리테일, 편의점)
- 마케팅 카피·행동 유도 표현 (예: 쟁여두세요, 달려가세요, 확인하세요, 먹어보실 분)
- 이벤트 운영 관련 단어 (예: 추첨, 당첨, 팔로우, 댓글, 태그, 모바일상품권)
- 행사 날짜·기간 (예: 9월 18일까지, 주말 한정) — 가격과 용량은 보존할 것
- 의미 없는 서술어·형용사 (예: 맛있다, 예쁘다, 좋다, 푸짐하다)
- 불필요한 마케팅 접미사 (예: 시리즈, 에디션, 기획, 세트, 패키지, 팩)
- 인사말, 부가 설명, 번호 매기기, 줄바꿈

[Output Format (Strict JSON — 다른 텍스트 절대 출력 금지)]
{
  "metadata": [
    {"name": "상품명", "price": 가격숫자또는null, "capacity": "용량문자열또는null"}
  ],
  "flavor_and_category": ["최대 6개"],
  "collab_and_brand": ["최대 5개"],
  "promotion_type": ["최대 3개"],
  "tpo_context": ["최대 3개"]
}
"""
)


def extract_keywords_cu(title: str, body: str) -> dict:
    """
    CU 공식 인스타그램 게시글(title + body)에서
    상품 메타데이터 + 트렌드 노드를 구조화된 dict로 추출합니다.

    반환 형식:
    {
        "metadata": [{"name": str, "price": int|None, "capacity": str|None}, ...],
        "flavor_and_category": [str, ...],
        "collab_and_brand": [str, ...],
        "promotion_type": [str, ...],
        "tpo_context": [str, ...]
    }
    파싱 실패 시 빈 dict {} 반환.
    """
    url = "http://localhost:11434/api/generate"
    text = preprocess_instagram_text(title, body)

    payload = {
        "model": MODEL_NAME,
        "prompt": f"{CU_SYSTEM_PROMPT}\n\n입력 텍스트: {text}",
        "stream": False,
        "options": {
            "temperature": TEMPERATURE
        }
    }

    try:
        response = requests.post(url, json=payload, timeout=TIMEOUT)
        response.raise_for_status()
        raw = response.json().get("response", "").strip()

        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start == -1 or end == 0:
            print("JSON 파싱 실패: 응답에서 JSON 블록을 찾을 수 없습니다.")
            return {}

        import json as _json
        result = _json.loads(raw[start:end])
        return result

    except requests.exceptions.Timeout:
        print(f"Ollama 응답 시간 초과 ({TIMEOUT}초). 모델 로딩 중일 수 있습니다.")
        return {}
    except requests.exceptions.RequestException as e:
        print(f"Ollama API 연결 오류: {e}")
        return {}
    except Exception as e:
        print(f"예상치 못한 오류 발생: {e}")
        return {}


if __name__ == "__main__":
    # 샘플 테스트
    sample = "허니버터칩 감자칩 스낵 일반스낵 2+1 행사"
    print(f"--- {MODEL_NAME} 키워드 추출 테스트 ---")
    print(f"추출 결과: {extract_keywords(sample)}")
