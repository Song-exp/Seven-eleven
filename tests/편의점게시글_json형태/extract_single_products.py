
import os
import json
import pandas as pd
import re

def clean_price(price_val):
    """가격 데이터 정제: 숫자만 남깁니다."""
    if pd.isna(price_val) or price_val is None:
        return 0
    if isinstance(price_val, (int, float)):
        return int(price_val)
    cleaned = re.sub(r'[^0-9]', '', str(price_val))
    return int(cleaned) if cleaned else 0

def process_single_product(row):
    """
    hin_pipeline_json 데이터를 분석하여 단일 상품인 경우 요청된 형식으로 변환합니다.
    형식: {["상품명", 가격, "용량"]: [속성 리스트]}
    """
    raw_json = row.get('hin_pipeline_json')
    if not isinstance(raw_json, str) or not raw_json.strip():
        return None
    
    try:
        data = json.loads(raw_json)
        metadata = data.get('metadata', [])
        
        # 1. 단일 상품군만 필터링 (metadata 리스트의 길이가 1인 경우)
        if len(metadata) != 1:
            return None
        
        item = metadata[0]
        name = str(item.get('name', 'Unknown')).replace('/', '').strip()
        price = clean_price(item.get('price', 0))
        capacity = item.get('capacity')
        # 용량이 없으면 null로 표시, '/' 제거
        capacity_val = str(capacity).replace('/', '').strip() if capacity and not pd.isna(capacity) else "null"
        
        # 2. 모든 속성 리스트 통합 및 '/' 제거
        all_attributes = []
        for key in ['flavor_and_category', 'collab_and_brand', 'promotion_type', 'tpo_context']:
            vals = data.get(key, [])
            if isinstance(vals, list):
                all_attributes.extend([str(v).replace('/', '').strip() for v in vals if v])
            elif isinstance(vals, str) and vals.strip():
                all_attributes.append(vals.replace('/', '').strip())
        
        # 중복 제거 (순서 유지)
        all_attributes = list(dict.fromkeys(all_attributes))
        
        # 3. 요청된 형식의 문자열 생성
        # 키 형식: ["상품명", 가격, "용량"]
        # 값 형식: [속성 리스트]
        product_key = f'["{name}", {price}, "{capacity_val}"]'
        formatted_result = {product_key: all_attributes}
        
        return json.dumps(formatted_result, ensure_ascii=False)
        
    except Exception:
        return None

def main():
    # --- 설정 영역 ---
    # 바탕화면에 있는 가공된 CSV 파일을 직접 지정합니다.
    user_home = os.path.expanduser("~")
    input_path = os.path.join(user_home, "Desktop", "instagram_7elevenkorea_2025-01-01_to_2025-12-31.csv")
    output_path = os.path.join(user_home, "Desktop", "7eleven_single_products_final.xlsx")
    # ----------------

    print(f"[*] 데이터 읽기 시작: {input_path}")
    
    if not os.path.exists(input_path):
        print(f"[-] 파일을 찾을 수 없습니다: {input_path}")
        return

    try:
        # 파일 읽기 (CSV)
        df = pd.read_csv(input_path, encoding='utf-8-sig', low_memory=False)

        if 'hin_pipeline_json' not in df.columns:
            print(f"[-] 오류: 'hin_pipeline_json' 컬럼이 데이터에 없습니다.")
            return

        print("[*] 단일 상품 필터링 및 데이터 변환 중...")
        # 변환 적용 (단일 상품이 아니면 None 반환)
        df['formatted_output'] = df.apply(process_single_product, axis=1)
        
        # None(단일 상품이 아닌 것) 제외하고 필터링
        filtered_df = df[df['formatted_output'].notna()].copy()
        
        if filtered_df.empty:
            print("[-] 결과: 조건에 맞는 단일 상품 데이터를 찾지 못했습니다.")
            return

        # 열 순서 정리: 원본 데이터 뒤에 변환된 데이터 배치
        cols = list(filtered_df.columns)
        hin_idx = cols.index('hin_pipeline_json')
        new_cols = cols[:hin_idx+1] + ['formatted_output'] + [c for c in cols[hin_idx+1:] if c != 'formatted_output']
        filtered_df = filtered_df[new_cols]

        print(f"[*] {len(filtered_df)}개의 단일 상품 데이터를 저장 중: {output_path}")
        filtered_df.to_excel(output_path, index=False, engine='openpyxl')
        
        print(f"[+] 완료! 파일이 생성되었습니다.")
        print(f"    - 저장 위치: {output_path}")

    except Exception as e:
        print(f"[-] 오류 발생: {e}")

if __name__ == "__main__":
    main()
