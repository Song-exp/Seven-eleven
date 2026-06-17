import requests
import json

# ==========================================
# [사용자 전용 튜닝 섹션] - 여기서 자유롭게 수정하세요!
# ==========================================
MODEL_NAME = "gemma4:e4b"
TEMPERATURE = 0.1  # 0.0 ~ 1.0 사이 (낮을수록 일관된 결과)
TIMEOUT = 180      # Ollama 응답 대기 시간 (3분)

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

if __name__ == "__main__":
    # 샘플 테스트
    sample = "허니버터칩 감자칩 스낵 일반스낵 2+1 행사"
    print(f"--- {MODEL_NAME} 키워드 추출 테스트 ---")
    print(f"추출 결과: {extract_keywords(sample)}")
