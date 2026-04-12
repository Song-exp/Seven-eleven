import requests
import json

# ==========================================
# [사용자 전용 튜닝 섹션] - 여기서 자유롭게 수정하세요!
# ==========================================
MODEL_NAME = "gemma4:26b"
TEMPERATURE = 0.1  # 0.0 ~ 1.0 사이 (추론 정밀도 조절)
TIMEOUT = 600      # 26B 모델은 로딩 및 연산이 오래 걸리므로 10분으로 설정

SYSTEM_PROMPT = (
    """당신은 세븐일레븐 신상품 기획(NPD) 데이터 파이프라인의 '상품 속성 추출(Attribute Extraction) AI 에이전트'입니다.
제공된 [제품명]을 분석하여 아래의 [지정된 속성 카테고리] 내에서 가장 적합한 속성들을 추론해 추출하세요. 
이 데이터는 HIN(이기종 정보 네트워크)의 속성 및 TPO 노드에 직접 적재되어 AI 기획 대시보드의 학습 데이터로 사용되므로, 지정된 단어 외의 출력은 시스템 치명적 오류를 발생시킵니다.

[지정된 속성 카테고리]
- 맛: 매운맛, 단맛, 짠맛, 쓴맛, 신맛, 담백함, 고소함
- 온도: 뜨거움, 차가움, 상온
- 식감: 바삭함, 부드러움, 쫄깃함, 촉촉함, 크리미함
- 취식 편의성 및 조리: 바로취식(RTE), 전자레인지, 에어프라이어, 끓는물, 해동필요
- 건강 및 라이프스타일: 고단백, 저칼로리, 제로슈거, 제로칼로리, 글루텐프리, 비건, 해당없음
- 소비 목적 및 TPO: 식사대용, 안주용, 당충전, 해장용, 다이어트용, 파티용
- 용량 및 포장 형태: 1인용(소포장), 대용량(패밀리), 한입거리(바이트), 번들형

[제약 조건]
1. 반드시 [지정된 속성 카테고리]에 있는 정확한 단어만 사용할 것 (유의어 사용 절대 금지).
2. 제품 특성에 맞는 속성만 추출할 것 (정보가 부족하여 명확한 추론이 불가한 카테고리는 억지로 채우지 말고 생략).
3. 출력은 반드시 쉼표(,)로만 구분된 단어 리스트 형태여야 함.
4. 서론, 결론, 부연 설명, 마침표(.), 따옴표, 카테고리명(예: '맛:', 'TPO:')은 절대 포함하지 말 것.

[Few-Shot 예시]
Input: 백종원 찐 매콤 제육한판 도시락
Output: 매운맛,상온,부드러움,전자레인지,해당없음,식사대용,1인용(소포장)

Input: 코카콜라 제로 500ml
Output: 단맛,차가움,바로취식(RTE),제로슈거,제로칼로리,당충전,1인용(소포장)

Input: 질러 직화육포
Output: 짠맛,고소함,상온,쫄깃함,바로취식(RTE),고단백,안주용,1인용(소포장)

Input: 점보 공간춘 쟁반짬짜면
Output: 매운맛,짠맛,뜨거움,쫄깃함,끓는물,해당없음,식사대용,파티용,대용량(패밀리)"""
)
# ==========================================

def infer_attributes(product_name: str) -> list[str]:
    """
    Ollama API를 사용하여 제품 속성을 추론합니다.
    """
    url = "http://localhost:11434/api/generate"
    
    payload = {
        "model": MODEL_NAME,
        "prompt": f"{SYSTEM_PROMPT}\n\n제품명: {product_name}",
        "stream": False,
        "options": {
            "temperature": TEMPERATURE
        }
    }
    
    try:
        # TIMEOUT 변수를 적용하여 대기 시간을 충분히 확보
        response = requests.post(url, json=payload, timeout=TIMEOUT)
        response.raise_for_status()
        result = response.json()
        
        raw_attributes = result.get("response", "").strip()
        attribute_list = [attr.strip() for attr in raw_attributes.split(",") if attr.strip()]
        return attribute_list
        
    except requests.exceptions.Timeout:
        print(f"Ollama 응답 시간 초과 ({TIMEOUT}초). 26B 모델 로딩 시간이 부족할 수 있습니다.")
        return []
    except requests.exceptions.RequestException as e:
        print(f"Ollama API 연결 오류: {e}")
        return []
    except Exception as e:
        print(f"예상치 못한 오류 발생: {e}")
        return []

if __name__ == "__main__":
    # 샘플 테스트
    sample = "마라탕"
    print(f"--- {MODEL_NAME} 속성 추론 테스트 ---")
    print(f"추론 결과: {infer_attributes(sample)}")
