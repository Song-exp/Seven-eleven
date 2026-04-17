import os
import json
import pandas as pd
import sys
import re

def clean_price(price_val):
    """가격 데이터에서 숫자만 남기고 정제합니다."""
    if price_val is None or pd.isna(price_val):
        return 0
    if isinstance(price_val, (int, float)):
        return int(price_val)
    price_str = str(price_val)
    cleaned = re.sub(r'[^0-9]', '', price_str)
    return int(cleaned) if cleaned else 0

def extract_product_indices(raw_json):
    """
    hin_pipeline_json에서 상품 식별을 위한 '인덱스(Key)' 부분만 리스트로 추출합니다.
    출력 예시: [["생초코파이", 3000, "null"], ["메론빵", 2500, "120g"]]
    """
    if not isinstance(raw_json, str) or not raw_json.strip():
        return ''
    
    try:
        data = json.loads(raw_json)
        metadata_list = data.get('metadata', [])
        
        if not metadata_list:
            return ''
            
        if isinstance(metadata_list, dict):
            metadata_list = [metadata_list]
            
        product_indices = []
        for item in metadata_list:
            if not isinstance(item, dict):
                continue
            
            name = str(item.get('name', 'Unknown')).strip()
            price = clean_price(item.get('price', 0))
            capacity = item.get('capacity')
            
            # 용량 처리
            if capacity is None or str(capacity).lower() in ['none', 'null', 'nan']:
                capacity_str = "null"
            else:
                capacity_str = str(capacity).strip()
            
            # 인덱스(Key) 구성: ["상품명", 가격, "용량"]
            product_indices.append([name, price, capacity_str])
            
        # 엑셀 셀에 담기 위해 JSON 문자열로 변환
        return json.dumps(product_indices, ensure_ascii=False)
        
    except Exception:
        return ''

def process_file_to_excel(input_path, output_path):
    """원본 파일을 읽어 '상품 인덱스' 열을 추가하고 엑셀로 저장합니다."""
    print(f"[*] 작업 시작: {input_path}")
    
    if not os.path.exists(input_path):
        print(f"[-] 파일을 찾을 수 없습니다: {input_path}")
        return

    try:
        if input_path.lower().endswith('.csv'):
            df = pd.read_csv(input_path)
        else:
            df = pd.read_excel(input_path)

        if 'hin_pipeline_json' not in df.columns:
            print("[-] 오류: 'hin_pipeline_json' 컬럼이 없습니다.")
            return

        print("[*] 상품 인덱스(Key) 추출 중...")
        # 'product_index'라는 이름으로 새로운 열 생성 (hin_pipeline_json 바로 뒤에 배치)
        df['product_index'] = df['hin_pipeline_json'].apply(extract_product_indices)

        cols = list(df.columns)
        hin_idx = cols.index('hin_pipeline_json')
        other_cols = [c for c in cols if c != 'product_index']
        new_cols = other_cols[:hin_idx+1] + ['product_index'] + other_cols[hin_idx+1:]
        df = df[new_cols]

        print(f"[*] 결과 저장 중: {output_path}")
        df.to_excel(output_path, index=False, engine='openpyxl')
        print(f"[+] 저장 완료! 결과 확인: {output_path}")

    except Exception as e:
        print(f"[-] 오류 발생: {e}")

if __name__ == "__main__":
    try:
        import openpyxl
    except ImportError:
        print("[!] openpyxl 라이브러리가 필요합니다. 'pip install openpyxl'을 실행하세요.")
        sys.exit(1)

    desktop = os.path.join(os.path.expanduser("~"), "Desktop")
    input_file = os.path.join(desktop, "instagram_7elevenkorea_2025-01-01_to_2025-12-31.csv")
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # 파일명도 'index_only'임을 알 수 있게 변경했습니다.
    output_file = os.path.join(current_dir, "instagram_7elevenkorea_index_only.xlsx")

    process_file_to_excel(input_file, output_file)
