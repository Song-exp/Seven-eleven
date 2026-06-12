"""
Pipeline 4 Final Fill: p_attrs 리스트 직접 추출
- 전략: LLM이 [행사정보, 맛, 식감, 카테고리]를 포함한 단일 리스트만 응답하도록 유도
"""

import pandas as pd
import os
import time
import requests
import sys
from dotenv import load_dotenv

load_dotenv()
sys.stdout.reconfigure(encoding="utf-8")

PROJECT_ROOT = r'C:\Users\송정현\Documents\Projects\박재홍교수님세미나\Projects\20기\7eleven_npd_framework'
RECRAWL_LIST_PATH = os.path.join(PROJECT_ROOT, "eda", "recrawl_targets_list.xlsx")
OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL_NAME = "gemma4:e4b"

# ─── 리스트 직접 응답용 프롬프트 ──────────────────────────────────────────

LIST_ONLY_PROMPT = """
[Role]
당신은 편의점 상품 데이터 파이프라인의 핵심인 '데이터 정밀 구조화 에이전트'입니다.

[Task]
입력된 비정형 게시글 텍스트에서 [타겟 상품: {p_name}]의 정보를 찾아, 아래 [추출 우선순위]와 [제약 조건]을 엄격히 적용하여 단 하나의 1차원 리스트(List) 형태로만 출력하세요. 
이 작업의 핵심 목적은 '제품의 본질적 속성(맛, 식감, 원재료)'을 최대한 풍부하게 포착하는 것입니다.

[Hidden Chain-of-Thought (절대 출력하지 말고 내부적으로만 수행할 것)]
1. [문맥 필터링]: 텍스트에서 타겟 상품 `{p_name}`과 관련된 서술만 분리합니다.
2. [1순위 - 핵심 속성 심층 탐색]: 해당 상품의 '맛(달콤, 고소 등)', '식감/텍스처(바삭, 쫀득, 부드러움 등)', '주요 원재료(우유, 초코, 흑임자 등)'를 묘사하는 모든 핵심 단어를 찾아 표준 명사/형용사형으로 추출합니다.
3. [2순위 - 카테고리 분류]: 스낵, 음료, 디저트, 간편식 등 상품의 정체성을 나타내는 대표 카테고리 명사를 1개 도출합니다.
4. [3순위 - 부가 정보(행사) 탐색]: 1+1, 2+1, 할인 등 부가적인 행사 정보를 찾습니다. (없으면 "없음"으로 처리)
5. [리스트 조합 및 정렬]: 파이프라인 일관성을 위해 리스트의 첫 번째는 [카테고리], 그다음은 [핵심 속성 키워드들]을 풍부하게 나열하고, 리스트의 가장 마지막 요소로 [행사정보]를 배치합니다.

[추출 우선순위 및 정렬 기준]
- 인덱스 0 (필수): 카테고리 (예: "스낵", "디저트")
- 중간 인덱스 (최우선/다중): 제품 속성 키워드 (예: "달콤", "바삭", "초코", "꾸덕")
- 마지막 인덱스 (필수): 행사정보 (예: "1+1", "없음")

[Negative Constraints (시스템 오류 방지 - 위반 시 파이프라인 마비)]
- 리스트를 마크다운 코드 블록(예: ```python, ```)으로 감싸지 마세요. 순수 텍스트 리스트만 출력해야 합니다.
- "결과:", "추출된 리스트입니다" 등의 부연 설명이나 인사말을 절대 포함하지 마세요.
- 다중 리스트(List of Lists)나 딕셔너리(Dictionary) 형태를 사용하지 마세요. 오직 단일 1차원 리스트여야 합니다.

[Output Format (Strict List - 괄호와 따옴표만 사용할 것)]
['카테고리', '속성키워드1', '속성키워드2', '속성키워드3', ..., '행사정보']
"""

def extract_list_directly(p_name, body):
    """LLM으로부터 리스트 형태의 응답을 직접 받음"""
    payload = {
        "model": MODEL_NAME,
        "prompt": f"{LIST_ONLY_PROMPT.format(p_name=p_name)}\n\n입력 텍스트: {body}",
        "stream": False,
        "options": {"temperature": 0.1}
    }
    try:
        response = requests.post(OLLAMA_URL, json=payload, timeout=60)
        response.raise_for_status()
        res_text = response.json().get("response", "").strip()
        
        # 리스트 형태만 남기기 (혹시 모를 앞뒤 공백 제거)
        start = res_text.find("[")
        end = res_text.rfind("]") + 1
        if start != -1 and end != 0:
            return res_text[start:end]
    except:
        return None
    return None

# ─── 실행 로직 ──────────────────────────────────────────────────────────

def run_final_fill():
    if not os.path.exists(RECRAWL_LIST_PATH): return

    df = pd.read_excel(RECRAWL_LIST_PATH)
    print(f"🚀 p_attrs 리스트 직접 업데이트 시작...")

    for idx, row in df.iterrows():
        p_name, body = str(row['p_name']), str(row.get('body', ''))
        if not body or body == 'nan': continue
            
        print(f"  🔍 [{idx+1}/{len(df)}] {p_name} 분석 중...")
        res_list_str = extract_list_directly(p_name, body)
        
        if res_list_str:
            df.at[idx, 'p_attrs'] = res_list_str
            print(f"    ✅ 결과: {res_list_str}")
        
        time.sleep(0.05)

    output_path = RECRAWL_LIST_PATH.replace(".xlsx", "_fixed.xlsx")
    df.to_excel(output_path, index=False)
    print(f"\n✨ 완료! 결과: {output_path}")

if __name__ == "__main__":
    run_final_fill()
