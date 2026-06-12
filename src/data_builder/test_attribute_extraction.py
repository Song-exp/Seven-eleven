import os
import sys
import json

# 프로젝트 루트 경로 추가
sys.path.append(os.getcwd())

# 추출 함수 임포트
try:
    from src.data_builder.extract_expanded_ip_batch import extract_ip_attributes_expanded
    from src.data_builder.extract_product_attributes import extract_product_attributes
    from src.data_builder.ip_attribute_extractor import collect_enriched_text
except ImportError as e:
    print(f"❌ 임포트 에러: {e}")
    sys.exit(1)

def test_extraction_logic():
    print("="*60)
    print("🧪 [속성 추출 로직 통합 테스트 모드]")
    print("="*60)

    # --- Test 1: IP 속성 추출 테스트 ---
    print("\n[Test 1] IP 속성 추출 테스트 (검색 + 층위별 프롬프트)")
    print("설명: 외부 텍스트를 수집하고, 1단계 카테고리(게임/아이돌 등) 힌트를 주어 추론합니다.")
    
    ip_samples = [
        ("블루 아카이브", "게임_IP"),
        ("세븐틴", "K팝_아이돌")
    ]

    for ip_name, category in ip_samples:
        print(f"\n▶ 대상 IP: {ip_name} ({category})")
        print("  - 텍스트 수집 중 (나무위키/네이버)...")
        wiki_text, source = collect_enriched_text(ip_name)
        print(f"  - 수집 완료 (Source: {source}, Length: {len(wiki_text) if wiki_text else 0})")
        
        print("  - LLM 속성 추론 중...")
        result = extract_ip_attributes_expanded(ip_name, wiki_text, category)
        
        if result:
            print(f"  - [성공]: {json.dumps(result, indent=2, ensure_ascii=False)}")
        else:
            print("  - [실패]: LLM 응답이 없거나 형식이 올바르지 않습니다.")

    # --- Test 2: 상품 속성 추출 테스트 ---
    print("\n" + "="*60)
    print("[Test 2] 상품 속성 추출 테스트 (Few-Shot 기반 추론)")
    print("설명: 블로그 데이터 기반 Few-Shot 예시를 참고하여 맛/식감/TPO를 추론합니다.")
    
    product_samples = [
        "마라탕면",
        "연세우유 생크림빵"
    ]

    for p_name in product_samples:
        print(f"\n▶ 대상 상품: {p_name}")
        print("  - LLM 속성 추론 중...")
        result = extract_product_attributes(p_name)
        
        if result:
            print(f"  - [성공]: {json.dumps(result, indent=2, ensure_ascii=False)}")
        else:
            print("  - [실패]: LLM 응답이 없거나 형식이 올바르지 않습니다.")

    print("\n" + "="*60)
    print("✅ 테스트 완료! 결과가 만족스러우면 'run_all_attribute_extraction.py'를 실행하세요.")
    print("="*60)

if __name__ == "__main__":
    test_extraction_logic()
