import os
import sys
import unittest
import ast

# 프로젝트 루트를 경로에 추가
PROJECT_ROOT = r'C:\Users\송정현\Documents\Projects\박재홍교수님세미나\Projects\20기\7eleven_npd_framework'
sys.path.append(PROJECT_ROOT)

from preprocessing.pipeline4_final_fill import extract_list_directly

class TestPipeline4FillObservation(unittest.TestCase):
    def setUp(self):
        self.sample_cases = [
            {
                "category": "디저트/베이커리",
                "p_name": "연세우유 생크림빵",
                "body": "연세우유 생크림빵 신상! 한 입 베어물면 동물성 크림의 고소함과 묵직한 달콤함이 확 느껴져요. 빵 피는 쫀득하고 속은 꽉 차서 진짜 꾸덕하네요. 2+1 행사까지 하니 무조건 쟁여야 함."
            },
            {
                "category": "스낵/안주",
                "p_name": "먹태청양마요광어칩",
                "body": "광어 살이 들어갔다더니 진짜 바삭하고 고소해요. 알싸한 청양고추 향이 매콤하게 올라와서 맥주 안주로 딱입니다. 짭짤한 시즈닝이 중독성 있네요."
            },
            {
                "category": "음료/건강",
                "p_name": "새싹보리 제로",
                "body": "구수한 보리 맛이 진한데 제로 칼로리라 깔끔해요. 시원하게 마시면 갈증 해소에 최고. 끝맛이 쌉쌀하지 않고 부드럽네요."
            },
            {
                "category": "간편식/도시락",
                "p_name": "혜자로운 집밥 제육볶음",
                "body": "고기가 두툼하고 불향이 가득해서 든든해요. 매콤달콤한 양념이 밥도둑입니다. 구성이 알차고 양이 푸짐해서 가성비 최고네요."
            }
        ]

    def test_observe_attributes(self):
        print("\n" + "="*60)
        print("🔍 [속성 추출 관찰 테스트] LLM이 어떤 특징을 포착하는가?")
        print("="*60)
        
        for case in self.sample_cases:
            p_name = case["p_name"]
            body = case["body"]
            
            print(f"\n📌 [대상 상품]: {p_name} ({case['category']})")
            print(f"📝 [입력 본문]: {body[:80]}...")
            
            result_str = extract_list_directly(p_name, body)
            
            if not result_str:
                print("  ❌ 추출 실패")
                continue
            
            try:
                res_list = ast.literal_eval(result_str)
                
                category = res_list[0]
                promotion = res_list[-1]
                attributes = res_list[1:-1] # 중간에 있는 모든 속성들
                
                print(f"  ▶ [카테고리 판별]: {category}")
                print(f"  ▶ [추출된 순수 속성들]: {', '.join(attributes)}")
                print(f"  ▶ [행사정보 판별]: {promotion}")
                
                # 속성 개수 및 질적 관찰
                if len(attributes) >= 3:
                    print(f"  ✅ 관찰 결과: {len(attributes)}개의 풍부한 속성 포착 성공")
                else:
                    print(f"  ⚠️  관찰 결과: 속성 추출이 다소 빈약함 ({len(attributes)}개)")
                
            except Exception as e:
                print(f"  ❌ 결과 파싱 오류: {e} (Raw: {result_str})")

if __name__ == "__main__":
    unittest.main()
