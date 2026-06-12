"""
Pipeline 3: 단위 규격화 및 재추출
- '6캔', '5개입' 등의 단위를 '1개당 가격'으로 변환
- '355ml캔' 등 이미 ml/g 정보가 있는 용량은 크롤링 없이 단위만 정제
- 실제 ml, g 정보를 네이버 블로그 검색으로 재보충
"""

import ast
import json
import os
import re
import sys
import time
from collections import Counter

import pandas as pd
from dotenv import load_dotenv

load_dotenv()
sys.stdout.reconfigure(encoding="utf-8")

BASE = "data/processed/편의점_instagram"
FILES = {
    "CU단일":   (f"{BASE}/단일/CU단일.xlsx",   "CU"),
    "세븐단일": (f"{BASE}/단일/세븐단일.xlsx", "세븐일레븐"),
    "GS25단일": (f"{BASE}/단일/GS25_단일.xlsx", "GS25"),
    "CU다중":   (f"{BASE}/다중/CU다중.xlsx",   "CU"),
    "세븐다중": (f"{BASE}/다중/세븐다중.xlsx", "세븐일레븐"),
    "GS25다중": (f"{BASE}/다중/GS25_다중.xlsx", "GS25"),
}

# ─── 정규식 패턴 ────────────────────────────────────────────────────────────

_PRICE_RE    = re.compile(r"(\d{1,3}(?:,\d{3})*|\d{3,5})\s*원")
_EMBEDDED_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(ml|g|kg|l)", re.IGNORECASE)
_ML_G_RE     = re.compile(r"(\d+(?:\.\d+)?)\s*(ml|ℓ|l|g|kg)", re.IGNORECASE)
_COUNT_RE    = re.compile(
    r"(\d+)\s*(?:캔|개|입|봉|팩|병|조각|매|알|구|종|입팩|봉입|개입|장|인분|일|t)",
    re.IGNORECASE,
)

# ─── 유틸리티 함수 ──────────────────────────────────────────────────────────

def search_naver_blog(query: str, display: int = 10) -> list[dict]:
    import requests
    cid = os.getenv("NAVER_CLIENT_ID", "")
    csec = os.getenv("NAVER_CLIENT_SECRET", "")
    if not cid or not csec:
        return []
    try:
        resp = requests.get(
            "https://openapi.naver.com/v1/search/blog.json",
            headers={"X-Naver-Client-Id": cid, "X-Naver-Client-Secret": csec},
            params={"query": query, "display": display, "sort": "sim"},
            timeout=10,
        )
        return resp.json().get("items", [])
    except:
        return []


def parse_formatted(s):
    try:
        if not isinstance(s, str):
            return None, None, None, None
        s = s.strip()
        if s.startswith("{") and s.endswith("}"):
            s = s[1:-1].strip()
        
        idx = -1
        split_len = 0
        if ']\": [' in s:
            idx = s.find(']\": [')
            split_len = 4
        elif ']": [' in s:
            idx = s.find(']": [')
            split_len = 4
        elif "]: [" in s:
            idx = s.find("]: [")
            split_len = 2
        
        if idx == -1: return None, None, None, None
        
        k_str = s[:idx+1].strip()
        v_str = s[idx+split_len:].strip()
        
        if k_str.startswith('\"'): k_str = k_str[1:]
        if k_str.endswith('\"'): k_str = k_str[:-1]
        if v_str.startswith(':'): v_str = v_str[1:].strip()
        
        def py_fix(txt):
            import re
            txt = re.sub(r'\bnull\b', 'None', txt)
            return txt.replace('\\\"', '"')
            
        key_data = ast.literal_eval(py_fix(k_str))
        attrs = ast.literal_eval(py_fix(v_str))
        return key_data[0], key_data[1], key_data[2], attrs
    except:
        return None, None, None, None
        return None, None, None, None


def rebuild_formatted(original, name, price, cap, attrs):
    s = str(original).strip()
    a = json.dumps(attrs, ensure_ascii=False)
    if s.startswith('{"['):
        return f'{{"["{name}", {price}, "{cap}"]": {a}}}'
    elif s.startswith('{['):
        return f'{{["{name}", {price}, "{cap}"]: {a}}}'
    else:
        return f"['{name}', {price}, '{cap}']: {a}"


# ─── 핵심 로직: 정규화 및 크롤링 ────────────────────────────────────────────

def normalize_and_crawl(name, brand, price, cap):
    """
    1. embedded ml/g 추출 (355ml캔 → 355ml, 크롤링 스킵)
    2. 개수(Count) 파악 → 단가 계산
    3. 부족한 용량 정보 크롤링
    """
    cap_str = str(cap)

    # [0] 이미 ml/g 수치가 포함된 경우 (355ml캔, 200g팩 등) → 크롤링 스킵
    emb = _EMBEDDED_RE.search(cap_str)
    if emb:
        new_cap = f"{emb.group(1)}{emb.group(2).lower()}"
        count_match = _COUNT_RE.search(cap_str)
        count = int(count_match.group(1)) if count_match else 1
        new_price = int(price / count) if count > 1 and price > 2000 else price
        return new_price, new_cap

    # [1] 개수 파악 (6캔 → 6, 5개입 → 5)
    count = 1
    count_match = _COUNT_RE.search(cap_str)
    if count_match:
        count = int(count_match.group(1))

    # [2] 단가 계산
    new_price = price
    if count > 1 and price > 2000:
        new_price = int(price / count)
        print(f"    -> 단가 계산: {price} / {count} = {new_price}원")

    # [3] 네이버 검색으로 ml/g 용량 확보 (fallback 3단계)
    queries = [
        f"{name} 용량 ml g",        # 1차: 용량 키워드 포함
        f"편의점 {name}",            # 2차: 브랜드명 → 편의점 일반화
        name,                        # 3차: 상품명 단독
    ]

    found_prices = []
    found_caps = []

    for query in queries:
        items = search_naver_blog(query, display=20)
        time.sleep(0.3)
        for item in items:
            text = item.get("title", "") + " " + item.get("description", "")
            for m in _PRICE_RE.finditer(text):
                v = int(m.group(1).replace(",", ""))
                if 500 <= v <= 20000:
                    found_prices.append(v)
            for m in _ML_G_RE.finditer(text):
                found_caps.append(f"{m.group(1)}{m.group(2).lower()}")
        ml_g_caps = [c for c in found_caps if any(u in c for u in ('ml', 'g', 'l', 'kg'))]
        if ml_g_caps:
            break  # ml/g 찾으면 다음 쿼리 시도 안 함

    # [4] 값 결정
    final_price = new_price if new_price > 0 else (
        Counter(found_prices).most_common(1)[0][0] if found_prices else 0
    )
    ml_g_caps = [c for c in found_caps if any(u in c for u in ('ml', 'g', 'l', 'kg'))]
    final_cap = Counter(ml_g_caps).most_common(1)[0][0] if ml_g_caps else cap

    return final_price, final_cap


def run():
    for file_key, (path, brand) in FILES.items():
        df = pd.read_excel(path)
        if '재추출_가격용량' not in df.columns:
            continue

        target_idx = df.index[df['재추출_가격용량'] == 'Y'].tolist()
        if not target_idx:
            continue

        print(f"\n[{file_key}] {len(target_idx)}개 상품 규격화 시작...")

        updated = 0
        for i in target_idx:
            row = df.loc[i]
            name, price, cap, attrs = parse_formatted(row['formatted_output'])
            if name is None:
                continue

            print(f"  검사: [{name}] ({price}원 / {cap})")

            new_price, new_cap = normalize_and_crawl(name, brand, price, cap)

            df.at[i, 'formatted_output'] = rebuild_formatted(
                row['formatted_output'], name, new_price, new_cap, attrs
            )

            if any(u in str(new_cap).lower() for u in ('ml', 'g', 'l', 'kg')):
                df.at[i, '재추출_가격용량'] = ""
                print(f"    -> 완료: {new_price}원 / {new_cap}")
                updated += 1
            else:
                print(f"    -> 미완료: {new_price}원 / {new_cap}")

        df.to_excel(path, index=False)
        print(f"[{file_key}] {updated}개 상품 업데이트 완료.")


if __name__ == "__main__":
    run()
