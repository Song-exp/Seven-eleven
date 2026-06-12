import json
import os
import pandas as pd

# 설정
BASE_DIR = "data/processed/IP_속성추출"
IP_JSON = os.path.join(BASE_DIR, "IP_속성.json")
PROD_JSON = os.path.join(BASE_DIR, "제품_속성.json")
OUTPUT_CSV = os.path.join(BASE_DIR, "통합_속성_리스트.csv")

def list_to_str(val):
    """리스트 데이터를 쉼표로 구분된 문자열로 변환"""
    if isinstance(val, list):
        return ", ".join(map(str, val))
    return val

def export_combined():
    print("📊 속성 데이터 통합 및 CSV 변환 시작...")
    
    combined_data = []

    # 1. IP 데이터 로드
    if os.path.exists(IP_JSON):
        with open(IP_JSON, "r", encoding="utf-8") as f:
            try:
                ip_data = json.load(f)
                for item in ip_data:
                    # 복사본 생성하여 원본 JSON 데이터 보호
                    row = item.copy()
                    row['data_source'] = "IP" # 구분 컬럼
                    # 리스트 변환 (엑셀 가독성용)
                    row['target_age'] = list_to_str(row.get('target_age', []))
                    row['signature_keywords'] = list_to_str(row.get('signature_keywords', []))
                    combined_data.append(row)
                print(f"✅ IP 데이터 로드 완료: {len(ip_data)}건")
            except Exception as e:
                print(f"⚠️ IP JSON 로드 실패: {e}")

    # 2. 제품 데이터 로드
    if os.path.exists(PROD_JSON):
        with open(PROD_JSON, "r", encoding="utf-8") as f:
            try:
                prod_data = json.load(f)
                for item in prod_data:
                    row = item.copy()
                    row['data_source'] = "Product" # 구분 컬럼
                    # 리스트 변환
                    row['flavor'] = list_to_str(row.get('flavor', []))
                    row['texture'] = list_to_str(row.get('texture', []))
                    row['ingredients'] = list_to_str(row.get('ingredients', []))
                    row['tpo'] = list_to_str(row.get('tpo', []))
                    combined_data.append(row)
                print(f"✅ 제품 데이터 로드 완료: {len(prod_data)}건")
            except Exception as e:
                print(f"⚠️ 제품 JSON 로드 실패: {e}")

    if not combined_data:
        print("❌ 통합할 데이터가 없습니다. JSON 파일이 생성되었는지 확인하세요.")
        return

    # 3. 데이터프레임 생성 및 컬럼 정렬
    df = pd.DataFrame(combined_data)
    
    # 보기 편하게 컬럼 순서 재배치
    preferred_order = [
        'data_source', 'keyword', 'ip_type', 'product_category', 
        'flavor', 'texture', 'target_age', 'signature_keywords', 
        'ingredients', 'tpo'
    ]
    
    # 실제 존재하는 컬럼만 순서대로 배치, 그 외 컬럼은 뒤로
    existing_cols = [c for c in preferred_order if c in df.columns]
    other_cols = [c for c in df.columns if c not in preferred_order]
    df = df[existing_cols + other_cols]

    # 4. 저장 (한글 깨짐 방지를 위해 utf-8-sig 사용)
    df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
    print(f"\n✨ 통합 완료! 총 {len(df)}개 항목이 저장되었습니다.")
    print(f"📂 저장 위치: {OUTPUT_CSV}")

if __name__ == "__main__":
    export_combined()
