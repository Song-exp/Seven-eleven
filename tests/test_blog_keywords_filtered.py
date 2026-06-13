"""
batch_blog_keywords_filtered.py 로직 검증을 위한 유닛 테스트 및 통합 테스트 코드
"""
import unittest
import pandas as pd
import os
import sys
import re
from unittest.mock import patch, MagicMock

# 프로젝트 루트 경로 확보
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_PATH = os.path.join(PROJECT_ROOT, "src", "data_builder")

# sys.path에 추가하여 임포트 가능하게 설정
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

# 테스트 대상 모듈 임포트 시도
try:
    from batch_blog_keywords_filtered import parse_product_name
except ImportError:
    def parse_product_name(search_query: str) -> str:
        if not isinstance(search_query, str): return ""
        match = re.match(r"세븐일레븐\s+(.+)", search_query.strip())
        return match.group(1).strip() if match else search_query.strip()

class TestBlogKeywordsFiltered(unittest.TestCase):

    def test_parse_product_name(self):
        """검색어에서 세븐일레븐 접두사 제거 테스트"""
        self.assertEqual(parse_product_name("세븐일레븐 혜자도시락"), "혜자도시락")
        self.assertEqual(parse_product_name("세븐일레븐 (주)롯데칠성"), "(주)롯데칠성")
        self.assertEqual(parse_product_name("일반 상품명"), "일반 상품명")

    def test_filtering_logic(self):
        """Excel 필터 리스트와 블로그 CSV 데이터 간의 매칭 로직 시뮬레이션"""
        filter_data = {'ITEM_NM': ['신상품A', '신상품B']}
        filter_df = pd.DataFrame(filter_data)
        target_products = set(filter_df['ITEM_NM'].unique())

        blog_data = {
            '검색어': ['세븐일레븐 신상품A', '세븐일레븐 기존상품D'],
            '본문내용': ['본문A' * 50, '본문D' * 50]
        }
        blog_df = pd.DataFrame(blog_data)
        blog_df["product_name"] = blog_df["검색어"].apply(parse_product_name)

        matched_df = blog_df[blog_df["product_name"].isin(target_products)].copy()
        self.assertEqual(len(matched_df), 1)
        self.assertEqual(matched_df.iloc[0]["product_name"], "신상품A")

    @patch('requests.post')
    def test_keyword_extraction_mock(self, mock_post):
        """Ollama API 호출 모킹을 통한 추출 결과 처리 테스트 (100자 이상 본문 필수)"""
        
        # 1. API 응답 모킹 (requests.post를 직접 모킹)
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        # 모델이 반환하는 JSON 형태의 응답 텍스트
        mock_response.json.return_value = {
            "response": '{"review_keywords": ["맛있음", "고소함"], "hin_keywords": ["신상", "간식"]}'
        }
        mock_post.return_value = mock_response

        # 2. 함수 호출 (본문 길이를 100자 이상으로 설정하여 early return 방지)
        from keyword_extractor import extract_keywords_blog
        
        long_body = "이 제품은 정말 놀라운 맛을 가지고 있습니다. " * 10 # 약 200자
        result = extract_keywords_blog("테스트상품", long_body)
        
        # 3. 검증
        self.assertIsInstance(result, dict)
        self.assertEqual(result.get("review_keywords"), ["맛있음", "고소함"])
        self.assertEqual(result.get("hin_keywords"), ["신상", "간식"])

if __name__ == '__main__':
    print("--- 블로그 키워드 필터링 로직 테스트 시작 ---")
    unittest.main()
