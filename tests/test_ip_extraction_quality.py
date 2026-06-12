
import sys
import os
import json
import time

# 프로젝트 루트를 path에 추가하여 모듈 임포트 가능하게 함
sys.path.append(os.getcwd())

from src.data_builder.ip_attribute_extractor import (
    collect_enriched_text,
    extract_ip_attributes,
    MODEL_NAME
)

def test_extraction_quality():
    """대표 IP 3종을 대상으로 핵심 키워드 추출 품질 테스트"""
    test_cases = [
        {"name": "짱구는 못말려", "category": "캐릭터", "expected_themes": ["초코비", "엉뚱함", "흰둥이"]},
        {"name": "블랙핑크", "category": "K팝", "expected_themes": ["힙함", "패션", "YG"]},
        {"name": "흑백요리사", "category": "콘텐츠", "expected_themes": ["요리", "넷플릭스", "백종원"]}
    ]

    print(f"\n🚀 [IP 속성 추출 품질 테스트] 사용 모델: {MODEL_NAME}")
    print("-" * 70)

    for case in test_cases:
        name = case["name"]
        print(f"\n🔍 테스트 대상: {name} ({case['category']})")
        
        # 1. 텍스트 수집 테스트
        text, source = collect_enriched_text(name)
        print(f"   - 데이터 출처: {source} (길이: {len(text)}자)")
        
        # 2. LLM 추출 테스트
        start_time = time.time()
        attrs = extract_ip_attributes(name, text)
        duration = time.time() - start_time
        
        # 3. 결과 출력
        if attrs:
            print(f"   - 추출 성공! (소요시간: {duration:.2f}초)")
            print(f"   - ip_type: {attrs.get('ip_type', 'N/A')}")
            print(f"   - target_age: {attrs.get('target_age', [])}")
            
            keywords = attrs.get('signature_keywords', [])
            print(f"   - 핵심 키워드: {', '.join(keywords)}")
            
            # 예상 테마 포함 여부 확인 (느슨한 검사)
            matches = [t for t in case["expected_themes"] if any(t in kw for kw in keywords)]
            print(f"   - 검증: 예상 테마 {len(case['expected_themes'])}개 중 {len(matches)}개 유사 키워드 발견")
        else:
            print("   ❌ 추출 실패 (LLM 응답 또는 파싱 오류)")
        
        print("-" * 50)
        time.sleep(1) # Ollama 부하 조절

if __name__ == "__main__":
    test_extraction_quality()
