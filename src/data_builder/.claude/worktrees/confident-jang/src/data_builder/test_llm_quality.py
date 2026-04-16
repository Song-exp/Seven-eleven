import polars as pl
import os
import sys

# 모듈 임포트를 위해 경로 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from keyword_extractor import extract_keywords
from attribute_inferrer import infer_attributes

def run_test_suite():
    print("="*60)
    print(" [Gemma 4] 제품 속성 및 키워드 추출 품질 테스트")
    print("="*60)

    # 1. 샘플 데이터 테스트
    # 작업 디렉토리에 따라 경로 조정
    base_path = "7eleven_npd_framework" if os.path.exists("7eleven_npd_framework") else "."
    input_path = os.path.join(base_path, "data/processed/PRODUCT_FULL_CONTEXT.parquet")
    
    if os.path.exists(input_path):
        print(f"\n[1] 샘플 데이터 기반 테스트 (랜덤 3개)")
        df = pl.read_parquet(input_path)
        if len(df) > 0:
            sample_df = df.sample(min(3, len(df)))
            
            for row in sample_df.iter_rows(named=True):
                name = row["상품명"]
                context = row["full_text_description"]
                
                print(f"\n▶ 제품명: {name}")
                print(f"  - 입력 컨텍스트: {context}")
                
                # 키워드 추출 (gemma4:e4b)
                print("  - 키워드 추출 중 (gemma4:e4b)...")
                keywords = extract_keywords(context)
                print(f"  - [추출 키워드]: {', '.join(keywords) if keywords else '실패'}")
                
                # 속성 추론 (gemma4:26b)
                print("  - 속성 추론 중 (gemma4:26b)...")
                attributes = infer_attributes(name)
                print(f"  - [추론 속성]: {', '.join(attributes) if attributes else '실패'}")
                print("-" * 40)
    else:
        print(f"\n[!] {input_path} 파일이 없어 샘플 테스트를 건너뜁니다.")

    # 2. 자유 입력 테스트
    print("\n[2] 자유 입력 테스트 (종료하려면 'exit' 입력)")
    while True:
        user_input = input("\n분석할 제품명이나 설명을 입력하세요: ").strip()
        if not user_input or user_input.lower() == 'exit':
            print("테스트를 종료합니다.")
            break
        
        print(f"  - [{user_input}] 분석 중...")
        keywords = extract_keywords(user_input)
        attributes = infer_attributes(user_input)
        
        print(f"  - 결과 키워드: {', '.join(keywords) if keywords else '추출 실패'}")
        print(f"  - 결과 속성: {', '.join(attributes) if attributes else '추론 실패'}")

if __name__ == "__main__":
    run_test_suite()
