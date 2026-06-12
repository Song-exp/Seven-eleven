"""
AI 키워드 추출 품질 테스트 (실제 모델 호출)
- 긴 블로그 본문에 대해 읽기 제한(cutoff)에 따른 답변 차이 비교
"""
import os
import sys
import pandas as pd
import time

# 경로 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src", "data_builder")))
from keyword_extractor import extract_keywords_blog

def run_quality_test():
    print("=" * 60)
    print("AI 추출 품질 테스트 (Real Model Call)")
    print("=" * 60)

    # 1. 데이터 로드 및 긴 샘플 추출
    df = pd.read_csv('data/processed/blog_merged.csv')
    df['len'] = df['본문내용'].fillna('').str.len()
    
    # 3,000자 근처의 글 하나 선택
    sample = df[df['len'] >= 3000].iloc[0]
    p_name = sample['검색어'].replace('세븐일레븐 ', '')
    full_body = sample['본문내용']
    
    print(f"대상 상품: {p_name}")
    print(f"전체 본문 길이: {len(full_body)}자")
    print("-" * 60)

    # 테스트 함수: 특정 cutoff로 추출 실행
    def test_with_cutoff(name, body, cutoff_val):
        print(f"\n[테스트] 읽기 제한: {cutoff_val}자 설정")
        start_time = time.time()
        
        # keyword_extractor의 extract_keywords_blog를 직접 호출하되 cutoff 인자 전달
        # (기존 함수가 cutoff를 인자로 받으므로 이를 활용)
        result = extract_keywords_blog(name, body, cutoff=cutoff_val)
        
        duration = time.time() - start_time
        print(f"소요 시간: {duration:.2f}초")
        
        if result:
            print(f"▶ Review Keywords: {result.get('review_keywords')}")
            print(f"▶ HIN Keywords: {result.get('hin_keywords')}")
        else:
            print("▶ 결과 추출 실패")
        return result

    # Case 1: 짧게 읽었을 때 (기존 방식 혹은 그 이하)
    res_short = test_with_cutoff(p_name, full_body, 500)

    # Case 2: 중간 (이전 설정 2,000자)
    res_mid = test_with_cutoff(p_name, full_body, 2000)

    # Case 3: 현재 설정 (3,000자)
    res_long = test_with_cutoff(p_name, full_body, 3000)

    print("\n" + "=" * 60)
    print("품질 분석 결과 요약")
    print("-" * 60)
    print(f"500자 읽기 키워드 수: {len(res_short.get('review_keywords', []))}")
    print(f"2000자 읽기 키워드 수: {len(res_mid.get('review_keywords', []))}")
    print(f"3000자 읽기 키워드 수: {len(res_long.get('review_keywords', []))}")
    print("\n* 팁: 글자가 길어질수록 글 뒤쪽에 숨겨진 상세한 맛 표현이나")
    print("  비교 후기(재구매 의사 등)가 더 많이 포함되었는지 확인해보세요.")
    print("=" * 60)

if __name__ == "__main__":
    run_quality_test()
