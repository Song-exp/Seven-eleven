"""
Pipeline 5: 최종 속성 추론 (Final Attribute Inference)
- 대상: data/processed/편의점_instagram/merged_instagram_products_final.xlsx
- 동작: 통합된 전체 상품 리스트를 순회하며 LLM(Ollama)으로 속성 키워드를 정밀 추출합니다.
- 특징: 브랜드별 전용 프롬프트를 자동 매칭하여 속성 품질을 극대화합니다.
"""

import pandas as pd
import os
import sys
import json
import time
import ast
from tqdm import tqdm

# 1. 환경 설정
sys.path.insert(0, "src/data_builder")
try:
    from keyword_extractor import (
        extract_keywords_cu,
        extract_keywords_gs25,
        extract_keywords_seveneleven,
    )
except ImportError:
    print("❌ src/data_builder/keyword_extractor.py를 찾을 수 없습니다.")
    sys.exit(1)

PROJECT_ROOT = r'C:\Users\송정현\Documents\Projects\박재홍교수님세미나\Projects\20기\7eleven_npd_framework'
INPUT_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "편의점_instagram", "merged_instagram_products_final.xlsx")
OUTPUT_PATH = INPUT_PATH # 덮어쓰기 (또는 별도 경로 설정 가능)

ATTR_MIN_THRESHOLD = 3 # 이 개수 미만인 경우만 재추출 수행

# ─── 보조 함수 ────────────────────────────────────────────────────────────

def _merge_fields(obj: dict) -> list[str]:
    """LLM 응답 JSON 객체에서 모든 속성 필드를 하나의 리스트로 합침"""
    if not obj: return []
    merged = []
    # keyword_extractor의 JSON 구조에 맞춤
    for field in ("flavor_and_category", "collab_and_brand", "promotion_type", "tpo_context"):
        values = obj.get(field, [])
        if isinstance(values, list):
            for v in values:
                if v: merged.append(str(v).strip())
        elif isinstance(values, str) and values:
            merged.append(values.strip())
            
    # metadata 내부에도 속성이 있을 수 있음 (v2 대응)
    metadata = obj.get("metadata", [])
    if isinstance(metadata, list) and len(metadata) > 0:
        first_item = metadata[0]
        for field in ("flavor_and_category", "collab_and_brand", "promotion_type", "tpo_context"):
            vals = first_item.get(field, [])
            if isinstance(vals, list):
                for v in vals: merged.append(str(v).strip())
    
    return list(dict.fromkeys(merged)) # 중복 제거

def get_extractor(brand):
    """브랜드명에 따른 추출 함수 매칭"""
    b = str(brand).upper()
    if 'CU' in b: return extract_keywords_cu
    if 'GS' in b: return extract_keywords_gs25
    if '세븐' in b or 'SEVEN' in b: return extract_keywords_seveneleven
    return extract_keywords_gs25 # 기본값

# ─── 실행 로직 ────────────────────────────────────────────────────────────

def run_inference():
    if not os.path.exists(INPUT_PATH):
        print(f"❌ 통합 파일을 찾을 수 없습니다: {INPUT_PATH}")
        return

    df = pd.read_excel(INPUT_PATH)
    print(f"🚀 총 {len(df)}개 상품에 대한 속성 추론 시작...")

    # p_attrs를 리스트 객체로 변환 (기존 데이터가 문자열 형태일 수 있음)
    def safe_parse_list(val):
        if not val or pd.isna(val) or val == '[]': return []
        try: return ast.literal_eval(str(val))
        except: return []

    df['p_attrs'] = df['p_attrs'].apply(safe_parse_list)

    updated_count = 0
    
    # tqdm으로 진행률 표시
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Inference Progress"):
        current_attrs = row['p_attrs']
        
        # 속성이 없거나 너무 적은 경우만 LLM 호출
        if len(current_attrs) < ATTR_MIN_THRESHOLD:
            brand_name = row.get('brand', 'GS25')
            extractor = get_extractor(brand_name)
            
            p_name = str(row['p_name'])
            body = str(row.get('body', ''))
            
            try:
                # LLM 호출
                result_dict = extractor(p_name, body)
                new_attrs = _merge_fields(result_dict)
                
                if new_attrs:
                    # 기존 속성과 합치기 (중복 제거)
                    combined = list(dict.fromkeys(current_attrs + new_attrs))
                    df.at[idx, 'p_attrs'] = combined
                    updated_count += 1
                
                # Ollama 과부하 방지 및 안정성
                time.sleep(0.1)
                
            except Exception as e:
                # 에러 발생 시 건너뜀 (다음 상품 계속 진행)
                continue
        
        # 중간 저장 (10건마다)
        if (idx + 1) % 10 == 0:
            df.to_excel(OUTPUT_PATH, index=False)

    # 최종 저장
    # 리스트를 다시 문자열로 변환하여 저장
    df['p_attrs'] = df['p_attrs'].apply(lambda x: json.dumps(x, ensure_ascii=False))
    df.to_excel(OUTPUT_PATH, index=False)
    
    print(f"\n✨ 속성 추론 완료!")
    print(f"✅ 업데이트된 상품 수: {updated_count}개")
    print(f"📂 결과 저장 경로: {OUTPUT_PATH}")

if __name__ == "__main__":
    print("--- Ollama 기반 최종 속성 추론 파이프라인 ---")
    run_inference()
