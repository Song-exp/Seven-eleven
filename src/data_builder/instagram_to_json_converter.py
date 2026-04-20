import pandas as pd
import json
import os

# 입력 파일 및 출력 파일 설정
input_path = '/Users/yumi/Desktop/instagram_7elevenkorea_2025-01-01_to_2025-12-31.csv'
output_path = '/Users/yumi/Desktop/instagram_7elevenkorea_converted.json'

def convert_to_user_style():
    # 1. CSV 파일 읽기
    df = pd.read_csv(input_path)
    
    final_result = {}
    
    for idx, row in df.iterrows():
        # hin_pipeline_json 데이터 파싱
        try:
            raw_json = row.get('hin_pipeline_json')
            if pd.isna(raw_json):
                continue
                
            data = json.loads(raw_json)
            
            # 속성 리스트 통합 (중복 제거)
            all_attributes = []
            all_attributes.extend(data.get('flavor_and_category', []))
            all_attributes.extend(data.get('collab_and_brand', []))
            all_attributes.extend(data.get('promotion_type', []))
            all_attributes.extend(data.get('tpo_context', []))
            
            # None 제거 및 중복 제거
            all_attributes = list(dict.fromkeys([v for v in all_attributes if v]))
            
            # metadata에 있는 상품별로 키 생성
            metadata_list = data.get('metadata', [])
            for item in metadata_list:
                name = item.get('name', 'Unknown')
                price = item.get('price', 0)
                capacity = item.get('capacity', 'null')
                
                # 요청하신 형식: {["상품명", 2500, 300ml]: [...]}
                # JSON 키는 문자열이어야 하므로 리스트의 문자열 표현을 사용합니다.
                key = f"['{name}', {price}, '{capacity}']"
                
                # 이미 키가 있다면 속성 합치기 (중복 제거)
                if key in final_result:
                    final_result[key] = list(dict.fromkeys(final_result[key] + all_attributes))
                else:
                    final_result[key] = all_attributes
                    
        except Exception as e:
            print(f"Row {idx} 변환 중 오류: {e}")
            continue

    # 2. 결과 저장
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final_result, f, ensure_ascii=False, indent=2)
    
    print(f"변환 완료! 결과 파일: {output_path}")
    print(f"총 추출된 상품 수: {len(final_result)}")

if __name__ == "__main__":
    convert_to_user_style()
