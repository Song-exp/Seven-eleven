"""
Apply Manual Corrections: 수동 정리 리스트 내용을 원본 파일에 반영
- 대상: eda/recrawl_targets_list.xlsx (사용자가 수정한 최종본)
- 동작: 수정한 가격/용량을 원본 엑셀에 덮어쓰고 플래그 해제
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
CORRECTION_LIST_PATH = os.path.join(PROJECT_ROOT, "eda", "recrawl_targets_list.xlsx")
BASE_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "processed", "편의점_instagram")

FILES_MAP = {
    "CU_단일":   os.path.join(BASE_DATA_DIR, "단일", "CU단일.xlsx"),
    "세븐_단일": os.path.join(BASE_DATA_DIR, "단일", "세븐단일.xlsx"),
    "GS25_단일": os.path.join(BASE_DATA_DIR, "단일", "GS25_단일.xlsx"),
    "CU_다중":   os.path.join(BASE_DATA_DIR, "다중", "CU다중.xlsx"),
    "세븐_다중": os.path.join(BASE_DATA_DIR, "다중", "세븐다중.xlsx"),
    "GS25_다중": os.path.join(BASE_DATA_DIR, "다중", "GS25_다중.xlsx"),
}

# ─── 헬퍼 함수 (Pipeline 3/4 로직 계승) ──────────────────────────────────

def parse_formatted_universal(s: str):
    if not isinstance(s, str): return []
    s_fixed = re.sub(r'\bnull\b', 'None', s)
    s_fixed = re.sub(r'\'null\'', 'None', s_fixed)
    s_fixed = re.sub(r'\"null\"', 'None', s_fixed)
    pairs = re.findall(r'(\[[^\]]+\])\s*:\s*(\[[^\]]*\])', s_fixed)
    results = []
    for k_str, v_str in pairs:
        try:
            k_data = ast.literal_eval(k_str.strip().lstrip('"').rstrip('"'))
            v_data = ast.literal_eval(v_str)
            if isinstance(k_data, list) and len(k_data) >= 3:
                results.append({'name': k_data[0], 'price': k_data[1], 'cap': k_data[2], 'attrs': v_data})
        except: continue
    if not results:
        try:
            data = ast.literal_eval(s_fixed)
            if isinstance(data, list) and len(data) >= 3:
                results.append({'name': data[0], 'price': data[1], 'cap': data[2], 'attrs': []})
        except: pass
    return results

def rebuild_formatted_universal(products, original_str):
    segments = [f"{json.dumps([p['name'], p['price'], p['cap']], ensure_ascii=False)}: {json.dumps(p['attrs'], ensure_ascii=False)}" for p in products]
    res = ", ".join(segments)
    return "{" + res + "}" if str(original_str).strip().startswith('{') else res

# ─── 실행 로직 ──────────────────────────────────────────────────────────

def apply_corrections():
    if not os.path.exists(CORRECTION_LIST_PATH):
        print(f"❌ 수정 리스트 파일을 찾을 수 없습니다: {CORRECTION_LIST_PATH}")
        return

    df_corr = pd.read_excel(CORRECTION_LIST_PATH)
    print(f"🚀 총 {len(df_corr)}건의 수동 수정사항 반영 시작...")

    # 소스 파일별로 그룹화하여 일괄 처리
    for source_file, group in df_corr.groupby('source_file'):
        path = FILES_MAP.get(source_file)
        if not path or not os.path.exists(path):
            print(f"⚠️  원본 파일 없음: {source_file}")
            continue

        print(f"\n📦 [{source_file}] 반영 중...")
        df_src = pd.read_excel(path)
        
        # 플래그 컬럼 타입 보정
        for flag in ["재추출_가격용량", "재추출_단위"]:
            if flag in df_src.columns:
                df_src[flag] = df_src[flag].astype(str).replace('nan', '')

        apply_count = 0
        for _, row in group.iterrows():
            p_name = row['p_name']
            new_price = row['p_price']
            new_cap = row['p_cap']
            
            # 원본에서 해당 상품명 포함 행 찾기
            mask = df_src['formatted_output'].astype(str).str.contains(re.escape(str(p_name)))
            target_indices = df_src.index[mask].tolist()
            
            for idx in target_indices:
                products = parse_formatted_universal(str(df_src.at[idx, 'formatted_output']))
                changed = False
                for p in products:
                    if p['name'] == p_name:
                        p['price'] = new_price
                        p['cap'] = str(new_cap)
                        changed = True
                
                if changed:
                    df_src.at[idx, 'formatted_output'] = rebuild_formatted_universal(products, df_src.at[idx, 'formatted_output'])
                    # 수동 수정이 완료되었으므로 모든 재추출 플래그 해제
                    for flag in ["재추출_가격용량", "재추출_단위"]:
                        if flag in df_src.columns:
                            df_src.at[idx, flag] = ""
                    apply_count += 1

        df_src.to_excel(path, index=False)
        print(f"✅ [{source_file}] {apply_count}개 항목 업데이트 및 플래그 해제 완료.")

if __name__ == "__main__":
    apply_corrections()
    print("\n✨ 모든 수동 수정사항이 원본 파일에 병합되었습니다.")
