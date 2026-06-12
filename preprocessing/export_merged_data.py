"""
Export Merged Data: 정제된 인스타그램 데이터 통합 및 추출
- 6개 원본 파일을 읽어 개별 상품 단위로 펼친 후 하나의 엑셀로 통합 저장합니다.
- 최종 결과물 경로: data/processed/편의점_instagram/merged_instagram_products_final.xlsx
"""

import pandas as pd
import os
import ast
import re
import json
import sys

# 1. 환경 설정
sys.stdout.reconfigure(encoding="utf-8")
PROJECT_ROOT = r'C:\Users\송정현\Documents\Projects\박재홍교수님세미나\Projects\20기\7eleven_npd_framework'
BASE_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "편의점", "편의점_인스타")

FILES_MAP = {
    "CU_단일":   os.path.join(BASE_DATA_DIR, "단일", "CU단일.xlsx"),
    "세븐_단일": os.path.join(BASE_DATA_DIR, "단일", "세븐_단일.xlsx"),
    "GS25_단일": os.path.join(BASE_DATA_DIR, "단일", "GS25_단일.xlsx"),
    "CU_다중":   os.path.join(BASE_DATA_DIR, "다중", "CU다중.xlsx"),
    "세븐_다중": os.path.join(BASE_DATA_DIR, "다중", "세븐_다중.xlsx"),
    "GS25_다중": os.path.join(BASE_DATA_DIR, "다중", "GS25_다중.xlsx"),
}

# ─── 통합 파서 로직 ────────────────────────────────────────────────────────

def parse_formatted_universal(s: str):
    if not isinstance(s, str) or not s.strip():
        return []

    # 1. 전처리: null/None 통일 및 개행 제거 (ast.literal_eval 안정성)
    s = s.strip()
    s_fixed = re.sub(r'\bnull\b', 'None', s)
    s_fixed = re.sub(r'\'null\'', 'None', s_fixed)
    s_fixed = re.sub(r'\"null\"', 'None', s_fixed)

    # 특수 케이스: 문자열 내부의 줄바꿈은 공백으로 치환
    s_fixed = s_fixed.replace('\n', ' ').replace('\r', ' ')

    # 특수 케이스: I'm 같은 단어 내 홑따옴표 탈출 시도 (필요시)
    if "'" in s_fixed:
        s_fixed = re.sub(r"([a-zA-Z가-힣])'([a-zA-Z가-힣])", r"\1\'\2", s_fixed)

    results = []

    # Strategy A: 전체를 하나의 파이썬 객체(Dict or List)로 인식 시도
    try:
        data = ast.literal_eval(s_fixed)
        if isinstance(data, dict):
            for k, v in data.items():
                k_list = k
                if isinstance(k, str):
                    try: k_list = ast.literal_eval(k)
                    except: pass
                if isinstance(k_list, (list, tuple)) and len(k_list) >= 3:
                    results.append({
                        'name': str(k_list[0]), 
                        'price': k_list[1], 
                        'cap': k_list[2], 
                        'attrs': v if isinstance(v, list) else []
                    })
            if results: return results
        elif isinstance(data, (list, tuple)) and len(data) >= 3:
            return [{'name': str(data[0]), 'price': data[1], 'cap': data[2], 'attrs': []}]
    except:
        pass

    # Strategy B: 대괄호 쌍을 찾아 '키 : 값' 구조로 강제 분리 (Regex 한계 극복)
    def find_balanced_blocks(text):
        objs = []
        stack = 0
        start_idx = -1
        for i, char in enumerate(text):
            if char == '[':
                if stack == 0: start_idx = i
                stack += 1
            elif char == ']':
                stack -= 1
                if stack == 0 and start_idx != -1:
                    objs.append((text[start_idx:i+1], start_idx, i+1))
        return objs

    blocks = find_balanced_blocks(s_fixed)
    for i in range(len(blocks)-1):
        k_str, _, k_end = blocks[i]
        v_str, v_start, _ = blocks[i+1]

        # 두 블록 사이의 텍스트 확인 (콜론이 있으면 키-값 쌍으로 간주)
        between = s_fixed[k_end:v_start].strip()
        if ':' in between:
            try:
                k_data = ast.literal_eval(k_str)
                v_data = ast.literal_eval(v_str)
                if isinstance(k_data, (list, tuple)) and len(k_data) >= 3:
                    results.append({
                        'name': str(k_data[0]), 
                        'price': k_data[1], 
                        'cap': k_data[2], 
                        'attrs': v_data if isinstance(v_data, list) else []
                    })
            except:
                continue

    return results

# ─── 메인 실행 로직 ────────────────────────────────────────────────────────

def export_merged():
    all_rows = []
    print("🚀 인스타그램 데이터 통합 시작...")

    for key, path in FILES_MAP.items():
        if not os.path.exists(path):
            print(f"⚠️  파일 없음: {key}")
            continue
        
        try:
            # '확정' 시트만 읽도록 변경
            df_src = pd.read_excel(path, sheet_name='확정')
            print(f"📦 [{key}] '확정' 시트 읽는 중 ({len(df_src)}행)...")
        except ValueError:
            print(f"⚠️  '확정' 시트 없음 (건너뜀): {key}")
            continue

        for _, row in df_src.iterrows():
            products = parse_formatted_universal(str(row.get('formatted_output', '')))
            
            for p in products:
                new_row = {
                    'p_name': p['name'],
                    'p_price': p['price'],
                    'p_cap': p['cap'],
                    'p_attrs': str(p['attrs']), # 리스트를 문자열로 저장 (엑셀용)
                    'brand': key.split('_')[0],
                    'type': key.split('_')[1],
                    'date': row.get('date', ''),
                    'body': row.get('body', ''),
                    'url': row.get('url', ''),
                    'source_file': key
                }
                all_rows.append(new_row)

    if all_rows:
        df_final = pd.DataFrame(all_rows)
        output_path = os.path.join(PROJECT_ROOT, "data", "processed", "편의점_instagram", "merged_instagram_products_final.xlsx")
        df_final.to_excel(output_path, index=False)
        print(f"\n✨ 통합 완료! 총 {len(df_final)}개의 개별 상품 데이터가 추출되었습니다.")
        print(f"📂 저장 경로: {output_path}")
    else:
        print("❌ 통합할 데이터가 없습니다.")
        

if __name__ == "__main__":
    export_merged()
