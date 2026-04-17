import pandas as pd
import json
import os
import sys

def format_hin_json(raw_json):
    \"\"\"
    hin_pipeline_json 데이터를 분석하여 사용자가 요청한 형식으로 변환합니다.
    형식: {['상품명', 가격, '용량']: [맛, 카테고리, 콜라보, 프로모션, TPO 등]}
    \"\"\"
    if not isinstance(raw_json, str) or not raw_json.strip():
        return ''
    
    try:
        data = json.loads(raw_json)
        
        # 1. 모든 속성(Attributes)을 하나의 리스트로 통합
        all_attributes = []
        all_attributes.extend(data.get('flavor_and_category', []))
        all_attributes.extend(data.get('collab_and_brand', []))
        all_attributes.extend(data.get('promotion_type', []))
        all_attributes.extend(data.get('tpo_context', []))
        
        # 중복 제거 및 빈 값 제거
        all_attributes = list(dict.fromkeys([v for v in all_attributes if v]))
        
        # 2. metadata에 있는 각 상품별로 키 생성
        row_summary = {}
        for item in data.get('metadata', []):
            name = item.get('name', 'Unknown')
            price = item.get('price', 0)
            capacity = item.get('capacity', 'None')
            
            # 키 형식: ['상품명', 가격, '용량']
            key = f"['{name}', {price}, '{capacity}']"
            row_summary[key] = all_attributes
            
        # JSON 문자열로 반환 (엑셀 셀에 담기 위함)
        return json.dumps(row_summary, ensure_ascii=False)
        
    except (json.JSONDecodeError, TypeError, KeyError):
        return ''

def main():
    # --- 설정 영역 ---
    # 바탕화면 경로 자동 인식 (macOS 기준)
    desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
    
    input_csv = os.path.join(desktop_path, "instagram_7elevenkorea_2025-01-01_to_2025-12-31.csv")
    output_excel = os.path.join(desktop_path, "instagram_7elevenkorea_with_summary.xlsx")
    # ----------------

    print(f"[*] 파일 읽기 시작: {input_csv}")
    
    if not os.path.exists(input_csv):
        print(f"[-] 오류: 파일을 찾을 수 없습니다. 경로를 확인해주세요: {input_csv}")
        return

    try:
        # 1. CSV 로드
        df = pd.read_csv(input_csv)
        
        if 'hin_pipeline_json' not in df.columns:
            print("[-] 오류: 'hin_pipeline_json' 컬럼이 파일에 존재하지 않습니다.")
            return

        print("[*] 데이터 변환 중...")
        # 2. 데이터 변환 및 새 컬럼 추가
        df['formatted_summary'] = df['hin_pipeline_json'].apply(format_hin_json)

        # 3. 열 순서 조정 (hin_pipeline_json 바로 뒤에 배치)
        cols = list(df.columns)
        hin_idx = cols.index('hin_pipeline_json')
        # 'formatted_summary'는 현재 맨 마지막에 있으므로 위치 재배치
        new_cols = cols[:hin_idx+1] + ['formatted_summary'] + [c for c in cols[hin_idx+1:] if c != 'formatted_summary']
        df = df[new_cols]

        # 4. 엑셀로 저장
        print(f"[*] 엑셀 저장 중: {output_excel}")
        df.to_excel(output_excel, index=False, engine='openpyxl')
        
        print(f"[+] 완료! 총 {len(df)}개 행이 처리되었습니다.")
        print(f"[+] 결과 확인: {output_excel}")

    except Exception as e:
        print(f"[-] 실행 중 오류 발생: {e}")

if __name__ == "__main__":
    # 라이브러리 체크
    try:
        import openpyxl
    except ImportError:
        print("[!] openpyxl 라이브러리가 필요합니다. 'pip install openpyxl'을 실행하세요.")
        sys.exit(1)
        
    main()
