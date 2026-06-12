import pandas as pd
import os
import ast
import re

# 1. 경로 설정
BASE = "data/processed/편의점_instagram"
FILES = {
    "CU단일":   f"{BASE}/단일/CU단일.xlsx",
    "세븐단일": f"{BASE}/단일/세븐단일.xlsx",
    "GS25단일": f"{BASE}/단일/GS25_단일.xlsx",
    "CU다중":   f"{BASE}/다중/CU다중.xlsx",
    "세븐다중": f"{BASE}/다중/세븐다중.xlsx",
    "GS25다중": f"{BASE}/다중/GS25_다중.xlsx",
}

# 2. 필터링 키워드
NON_FOOD_KWS = [
    '카드', '굿즈', '티켓', '피규어', '인형', '키링', '세트', '교통카드', '복권',
    '다이어리', '플래너', '스티커', '포카', '앨범', '응원봉', '캘린더', '포스터', '마그넷',
    '마스크', '생리대', '팬티라이너', '양말', '택배', '박스', '에디션', '장난감'
]

# 식품 관련 예외 키워드 (박스/세트 등이 포함되어도 살려야 하는 경우)
FOOD_EXCEPTIONS = ['도시락', '커피', '버거', '치킨', '초콜릿', '과자', '캔디', '젤리']

def parse_product_name(s):
    """formatted_output에서 상품명만 추출"""
    if not isinstance(s, str): return ""
    s = s.strip()
    if s.startswith('{') and s.endswith('}'): s = s[1:-1].strip()
    
    # 간단한 파싱 (구분자 기준)
    for sep in [']": [', ']: [', ']\\\\": [', ']\\\\\\\\\\\": [']:
        if sep in s:
            try:
                idx = s.index(sep)
                k_str = s[:idx + 1].strip().lstrip('"').rstrip('"')
                k_list = ast.literal_eval(k_str)
                return str(k_list[0]) if isinstance(k_list, list) and len(k_list) > 0 else ""
            except: continue
    
    # 단일 리스트 형태
    try:
        data = ast.literal_eval(s)
        if isinstance(data, list) and len(data) > 0: return str(data[0])
    except: pass
    
    return ""

def clean_files():
    for key, path in FILES.items():
        if not os.path.exists(path):
            print(f"⚠️ {key}: 파일 없음 ({path})")
            continue
            
        df = pd.read_excel(path)
        initial_count = len(df)
        
        # [A] 수동 제외 처리
        # 컬럼명에 '제외'가 들어간 모든 컬럼 확인
        exclude_cols = [c for c in df.columns if '제외' in c]
        for col in exclude_cols:
            # 값이 존재하면(NaN이 아니면) 제외 대상으로 간주
            df = df[df[col].isna()].copy()
        
        manual_removed = initial_count - len(df)
        
        # [B] 키워드 기반 자동 필터링
        def is_non_food(row):
            p_name = parse_product_name(row.get('formatted_output', ''))
            body = str(row.get('body', ''))
            full_text = f"{p_name} {body}".lower()
            
            for kw in NON_FOOD_KWS:
                if kw in full_text:
                    # 식품 예외 확인
                    if any(ex in p_name for ex in FOOD_EXCEPTIONS):
                        continue
                    return True
            return False
            
        if len(df) > 0:
            df['is_non_food'] = df.apply(is_non_food, axis=1)
            df = df[~df['is_non_food']].copy()
            df.drop(columns=['is_non_food'], inplace=True)
            
        auto_removed = initial_count - manual_removed - len(df)
        
        # 결과 저장
        df.to_excel(path, index=False)
        print(f"✅ {key:<8}: {initial_count:>5} -> {len(df):>5} (수동제외: {manual_removed}, 키워드제외: {auto_removed})")

if __name__ == "__main__":
    print("🚀 인스타그램 데이터 Cleaning 파이프라인 시작...")
    clean_files()
    print("\n✨ Cleaning 완료.")
