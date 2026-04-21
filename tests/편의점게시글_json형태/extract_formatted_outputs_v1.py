import pandas as pd
import os
import json

def get_formatted_outputs(file_path):
    if not os.path.exists(file_path):
        print(f"❌ 파일을 찾을 수 없습니다: {file_path}")
        return []

    try:
        df = pd.read_excel(file_path)
        if 'formatted_output' in df.columns:
            raw_data = df['formatted_output'].dropna().tolist()
            final_results = []
            seen_products = set()  # 중복 체크를 위한 집합
            
            for item_str in raw_data:
                if not isinstance(item_str, str):
                    continue
                try:
                    parsed = json.loads(item_str)
                    if isinstance(parsed, dict):
                        for key, value in parsed.items():
                            # 상품 정보(key)가 이미 존재하면 건너뜀 (중복 제거)
                            if key not in seen_products:
                                seen_products.add(key)
                                final_results.append({key: value})
                    elif isinstance(parsed, list):
                        for item in parsed:
                            if isinstance(item, dict):
                                for key, value in item.items():
                                    if key not in seen_products:
                                        seen_products.add(key)
                                        final_results.append({key: value})
                except json.JSONDecodeError:
                    continue
            
            print(f"✅ 중복 제거 완료: {len(raw_data)}개 행 -> {len(final_results)}개 유니크 상품")
            return final_results
        return []
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        return []

if __name__ == "__main__":
    input_file = '/Users/yumi/Desktop/7eleven_multiple_products_final.xlsx'
    output_dir = 'tests/편의점게시글_json형태'
    output_path = os.path.join(output_dir, 'extracted_formatted_outputs.json')
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    print(f"🔄 '{os.path.basename(input_file)}' 데이터 정리 및 중복 제거 중...")
    results = get_formatted_outputs(input_file)
    
    if results:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("[\n")
            for i, item in enumerate(results):
                line = json.dumps(item, ensure_ascii=False)
                f.write(f"  {line}")
                if i < len(results) - 1:
                    f.write(",")
                f.write("\n")
            f.write("]\n")
            
        print(f"✨ 완료! 중복이 제거된 상품 리스트가 '{output_path}'에 저장되었습니다.")
    else:
        print("❌ 추출된 데이터가 없습니다.")
