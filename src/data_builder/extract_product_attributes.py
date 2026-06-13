import os
import sys
import json
import time
import requests
import pandas as pd
from tqdm import tqdm

# 기존 로직을 재사용하기 위해 경로 추가
sys.path.append(os.getcwd())
try:
    from src.data_builder.ip_attribute_extractor import (
        collect_enriched_text, 
        MODEL_NAME, 
        TEMPERATURE, 
        TIMEOUT_LLM
    )
except ImportError:
    # 기본값 설정 (모듈 로드 실패 대비)
    MODEL_NAME = "gemma4:e4b"
    TEMPERATURE = 0.1
    TIMEOUT_LLM = 120

# ─── 1. 설정 및 경로 ──────────────────────────────────────────────────

CSV_INPUT_PATH = "data/processed/IP_속성추출/찐최종뉴뉴_community_1hop_mapping_final.csv"
PRODUCT_OUTPUT_JSON = "data/processed/IP_속성추출/제품_속성.json"

# ─── 2. 상품 전용 프롬프트 (Few-Shot 예시 포함) ──────────────────────────

PRODUCT_SYSTEM_PROMPT = """
당신은 편의점 F&B 상품 기획 및 관능 평가(Sensory Evaluation) 데이터 전문가입니다.
분석 대상인 [ {product_keyword} ]을(를) 분석하여, 소비자가 기대하는 물리적/관능적 속성을 JSON 형식으로만 응답하세요.

[Few-Shot 예시]
Input: 초콜릿
Output: {{"product_category": "스낵", "flavor": ["달콤함", "쌉싸름함", "진한풍미"], "texture": ["부드러움", "사르르"], "ingredients": ["카카오", "설탕"], "tpo": ["당충전", "디저트"]}}

Input: 치즈김밥
Output: {{"product_category": "도시락/간편식", "flavor": ["고소함", "짭짤함", "담백함"], "texture": ["촉촉함", "부드러움"], "ingredients": ["쌀", "치즈", "김"], "tpo": ["식사대용", "간단한한끼"]}}

Input: 딸기아이스크림
Output: {{"product_category": "아이스크림", "flavor": ["상큼함", "달콤함", "새콤달콤"], "texture": ["차가움", "부드러움"], "ingredients": ["딸기", "우유", "크림"], "tpo": ["디저트", "여름간식"]}}

[출력 포맷 고정 및 추론 규칙]
1. 반드시 아래 포맷의 순수 JSON 형식으로만 응답하세요. 마크다운 기호(```json 등)나 기타 설명은 "절대" 금지합니다.
2. 모든 키워드는 반드시 '명사' 또는 '명사형'으로 통일하세요. (예: 바삭하다 -> 바삭함, 시다 -> 신맛)
3. [고유어 및 복합어 보존 규칙 - 매우 중요] 
   - '새콤달콤', '단짠단짠', '겉바속촉', '꼬숩맛', '쌉쌀한맛' 등 널리 쓰이는 복합 관능어나 고유 명사(원재료명, 브랜드명)는 그 자체로 고유한 의미를 가집니다. 
   - 절대 이 단어들을 임의로 쪼개지 마세요. (예: '새콤달콤'을 '새콤', '달콤'으로 분리 금지 / '시즌한정'을 '시즌', '한정'으로 분리 금지)
4. 상품 카테고리(`product_category`)는 다음 대분류 중에서만 1개를 선택하세요: (베이커리, 스낵, 음료, 유제품, 도시락/간편식, 안주, 아이스크림, 기타)
5. 만약 검색 데이터가 부족하거나 신조어인 경우, 상품명에 포함된 단어의 일반적인 식음료 속성을 바탕으로 논리적으로 추론하세요.

{{
  "product_category": "대분류 1개 선택",
  "flavor": ["맛 관련 명사형 키워드 2~3개"],
  "texture": ["식감 관련 명사형 키워드 2개"],
  "ingredients": ["핵심 원재료 명사형 2~3개"],
  "tpo": ["추천 취식 상황 명사형 2개 (예: 야식, 피크닉, 당충전)"]
}}
"""
# ─── 3. 추출 함수 ─────────────────────────────────────────────────────

def extract_product_attributes(product_name, text=""):
    """LLM을 통한 상품 속성 추론"""
    url = "http://localhost:11434/api/generate"
    prompt = f"{PRODUCT_SYSTEM_PROMPT}\n\n[상품 키워드]: {product_name}\n[참고 정보]: {text}"
    
    payload = {
        "model": MODEL_NAME,
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "options": {"temperature": TEMPERATURE}
    }
    
    try:
        response = requests.post(url, json=payload, timeout=TIMEOUT_LLM)
        response.raise_for_status()
        return json.loads(response.json().get("response", "{}"))
    except Exception as e:
        print(f"  [!] '{product_name}' 추출 중 오류: {e}")
        return None

# ─── 4. 실행 메인 ──────────────────────────────────────────────────────

def main():
    print("🚀 상품 키워드 기반 속성 추출 프로세스 시작...")
    
    # 1. 데이터 로드 및 유니크 키워드 추출
    if not os.path.exists(CSV_INPUT_PATH):
        print(f"[-] 파일을 찾을 수 없습니다: {CSV_INPUT_PATH}")
        return

    try:
        df = pd.read_csv(CSV_INPUT_PATH, encoding='utf-8-sig')
    except:
        df = pd.read_csv(CSV_INPUT_PATH, encoding='cp949')

    if '키워드_분류결과_제품명' not in df.columns:
        print("[-] '키워드_분류결과_제품명' 컬럼이 없습니다.")
        return

    all_rows = df['키워드_분류결과_제품명'].dropna().astype(str)
    
    unique_keywords = set()
    for row in all_rows:
        items = [i.strip() for i in row.split(",") if i.strip()]
        unique_keywords.update(items)
    
    target_list = sorted(list(unique_keywords))
    print(f"[*] 총 {len(target_list)}개의 유니크 상품 키워드 확보")

    # 2. 체크포인트 로드
    results = {}
    if os.path.exists(PRODUCT_OUTPUT_JSON):
        with open(PRODUCT_OUTPUT_JSON, "r", encoding="utf-8") as f:
            try:
                old_data = json.load(f)
                results = {item['keyword']: item for item in old_data}
            except: pass

    todo = [k for k in target_list if k not in results]
    print(f"[*] 새로 추출할 키워드: {len(todo)}개")

    if not todo:
        print("✅ 모든 키워드가 이미 처리되었습니다.")
        return

    # 3. 추출 실행
    for i, kw in enumerate(tqdm(todo, desc="Product Attribute Extraction")):
        # 상품 속성 추출
        attrs = extract_product_attributes(kw)
        
        if attrs:
            results[kw] = {
                "keyword": kw,
                **attrs
            }
            
            # 20건마다 중간 저장
            if (i + 1) % 20 == 0:
                with open(PRODUCT_OUTPUT_JSON, "w", encoding="utf-8") as f:
                    json.dump(list(results.values()), f, ensure_ascii=False, indent=2)
        
        # Ollama 과부하 방지
        time.sleep(0.2)

    # 4. 최종 저장
    with open(PRODUCT_OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(list(results.values()), f, ensure_ascii=False, indent=2)
    
    print(f"\n✨ 모든 작업 완료! 결과 저장: {PRODUCT_OUTPUT_JSON}")

if __name__ == "__main__":
    main()
