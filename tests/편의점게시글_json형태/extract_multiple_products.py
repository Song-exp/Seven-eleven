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

def process_all_products(row):
    """
    hin_pipeline_json 데이터를 분석하여 여러 상품 정보(2개 이상)를 추출하여 변환합니다.
    속성이 게시물 전체(data) 또는 개별 상품(item)에 있는 경우를 모두 통합합니다.
    형식: {['상품명', 가격, '용량']: [속성 리스트], ...}
    """
    raw_json = row.get('hin_pipeline_json')
    if not isinstance(raw_json, str) or not raw_json.strip():
        return None
    
    try:
        data = json.loads(raw_json)
        metadata = data.get('metadata', [])
        
        # 상품이 2개 이상인 경우만 필터링
        if not metadata or len(metadata) < 2:
            return None
        
        # 1. 게시물 공통 속성 추출
        common_attributes = []
        for key in ['flavor_and_category', 'collab_and_brand', 'promotion_type', 'tpo_context']:
            vals = data.get(key, [])
            if isinstance(vals, list):
                common_attributes.extend([str(v).replace('/', '').strip() for v in vals if v])
            elif isinstance(vals, str) and vals.strip():
                common_attributes.append(vals.replace('/', '').strip())
        
        product_results = {}
        
        # 2. 모든 상품(metadata 내의 모든 항목) 처리
        for item in metadata:
            name = str(item.get('name', 'Unknown')).replace('/', '').strip()
            price = clean_price(item.get('price', 0))
            capacity = item.get('capacity')
            capacity_val = str(capacity).replace('/', '').strip() if capacity and not pd.isna(capacity) else "null"
            
            # 개별 상품 내부의 속성 리스트 추출
            item_attributes = []
            for key in ['flavor_and_category', 'collab_and_brand', 'promotion_type', 'tpo_context']:
                vals = item.get(key, [])
                if isinstance(vals, list):
                    item_attributes.extend([str(v).replace('/', '').strip() for v in vals if v])
                elif isinstance(vals, str) and vals.strip():
                    item_attributes.append(vals.replace('/', '').strip())
            
            # 공통 속성과 개별 속성 통합 및 중복 제거
            combined_attributes = list(dict.fromkeys(item_attributes + common_attributes))
            
            # 백슬래시(\) 방지를 위해 키에 싱글 쿼트(') 사용
            # 형식: ['상품명', 가격, '용량']
            product_key = f"['{name}', {price}, '{capacity_val}']"
            product_results[product_key] = combined_attributes
        
        # 결과 반환 (JSON 문자열)
        return json.dumps(product_results, ensure_ascii=False)
            
    except Exception:
        return None

def main():
    # --- 설정 영역 ---
    user_home = os.path.expanduser("~")
    input_path = os.path.join(user_home, "Desktop", "instagram_7elevenkorea_2025-01-01_to_2025-12-31.csv")
    output_path = os.path.join(user_home, "Desktop", "7eleven_multiple_products_final.xlsx")
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

        print("[*] 모든 상품 정보 추출 및 데이터 변환 중...")
        # 변환 적용
        df['formatted_output'] = df.apply(process_all_products, axis=1)
        
        # 결과가 있는 데이터만 필터링
        filtered_df = df[df['formatted_output'].notna()].copy()
        
        if filtered_df.empty:
            print("[-] 결과: 추출된 상품 데이터가 없습니다.")
            return

        # 열 순서 정리
        cols = list(filtered_df.columns)
        if 'hin_pipeline_json' in cols:
            hin_idx = cols.index('hin_pipeline_json')
            new_cols = cols[:hin_idx+1] + ['formatted_output'] + [c for c in cols[hin_idx+1:] if c != 'formatted_output']
            filtered_df = filtered_df[new_cols]

        print(f"[*] 총 {len(filtered_df)}개의 게시글 데이터를 저장 중: {output_path}")
        filtered_df.to_excel(output_path, index=False, engine='openpyxl')
        
        print(f"[+] 완료! 파일이 생성되었습니다: {output_path}")

    except Exception as e:
        print(f"[-] 오류 발생: {e}")

if __name__ == "__main__":
    main()
