import pandas as pd
import os
import ast
import re
import time
import json
import sys
from collections import Counter
from dotenv import load_dotenv

# 1. 환경 설정 및 로드
load_dotenv()
sys.stdout.reconfigure(encoding="utf-8")

PATH = "data/processed/편의점_instagram/다중/CU다중.xlsx"
BRAND = "CU"
FLAG_COL = "재추출_가격용량"

# 편의점 가격대 필터
PRICE_MIN, PRICE_MAX = 500, 20_000

# ─── Naver API 검색 도구 ──────────────────────────────────────────────────

def _naver_headers():
    cid = os.getenv("NAVER_CLIENT_ID", "")
    csec = os.getenv("NAVER_CLIENT_SECRET", "")
    if not cid or not csec:
        raise RuntimeError("NAVER_CLIENT_ID/SECRET이 .env에 없습니다.")
    return {"X-Naver-Client-Id": cid, "X-Naver-Client-Secret": csec}

def search_naver_integrated(query: str, display: int = 10) -> list[dict]:
    """블로그, 뉴스, 카페를 통합하여 검색 결과 반환"""
    search_types = ["blog", "news", "cafearticle"]
    combined_items = []
    import requests
    for stype in search_types:
        try:
            url = f"https://openapi.naver.com/v1/search/{stype}.json"
            resp = requests.get(
                url,
                headers=_naver_headers(),
                params={"query": query, "display": display, "sort": "sim"},
                timeout=10
            )
            if resp.status_code == 200:
                combined_items.extend(resp.json().get("items", []))
        except: continue
        time.sleep(0.1)
    return combined_items

# ─── 정보 추출 도구 ──────────────────────────────────────────────────────

_PRICE_RE = re.compile(r"(\d{1,3}(?:,\d{3})*|\d{3,5})\s*원")
_CAP_RE   = re.compile(r"(\d+(?:\.\d+)?)\s*(ml|mL|ℓ|L|g|G|kg|개|봉|팩|병|캔|매|인분|조각)", re.IGNORECASE)

def extract_prices(text: str) -> list[int]:
    results = []
    for m in _PRICE_RE.finditer(text):
        val = int(m.group(1).replace(",", ""))
        if PRICE_MIN <= val <= PRICE_MAX: results.append(val)
    return results

def extract_capacities(text: str) -> list[str]:
    return [f"{m.group(1)}{m.group(2).lower()}" for m in _CAP_RE.finditer(text)]

def best_value(lst: list):
    return Counter(lst).most_common(1)[0][0] if lst else None

# ─── CU Multi 전용 파서 및 빌더 ──────────────────────────────────────────

def parse_cu_multi_blocks(s: str):
    """
    formatted_output 문자열에서 [key]: [val] 쌍들을 찾아 리스트로 반환
    구조: [ {'name':..., 'price':..., 'cap':..., 'attrs':...}, ... ]
    """
    if not isinstance(s, str): return []
    
    # null -> None 변환 및 불필요한 따옴표 정리
    s_fixed = re.sub(r'\bnull\b', 'None', s)
    s_fixed = re.sub(r'\'null\'', 'None', s_fixed)
    s_fixed = re.sub(r'\"null\"', 'None', s_fixed)

    # 모든 [...] 블록 찾기
    blocks = re.findall(r'\[[^\]]+\]', s_fixed)
    
    products = []
    # 보통 [상품정보]: [속성] 쌍으로 존재
    for i in range(0, len(blocks) - 1, 2):
        try:
            info = ast.literal_eval(blocks[i])
            attrs = ast.literal_eval(blocks[i+1])
            if isinstance(info, list) and len(info) >= 3:
                products.append({
                    'name': info[0],
                    'price': info[1],
                    'cap': info[2],
                    'attrs': attrs,
                    'orig_idx': i # 복원용
                })
        except: continue
    return products

def rebuild_cu_multi(products):
    """수정된 상품 리스트를 다시 문자열로 결합"""
    segments = []
    for p in products:
        key_str = json.dumps([p['name'], p['price'], p['cap']], ensure_ascii=False)
        val_str = json.dumps(p['attrs'], ensure_ascii=False)
        segments.append(f"{key_str}: {val_str}")
    return ", ".join(segments)

# ─── 실행 로직 ──────────────────────────────────────────────────────────

def run():
    if not os.path.exists(PATH):
        print("파일을 찾을 수 없습니다.")
        return

    df = pd.read_excel(PATH)
    if FLAG_COL not in df.columns:
        print(f"[{FLAG_COL}] 컬럼이 없습니다.")
        return

    targets = df[df[FLAG_COL] == "Y"].index.tolist()
    print(f"🚀 CU 다중 전용 처리 시작 (대상: {len(targets)}행)")

    for idx in targets:
        row = df.loc[idx]
        products = parse_cu_multi_blocks(str(row['formatted_output']))
        
        if not products:
            print(f"  [Skip] 파싱 실패 (Index: {idx})")
            continue

        changed = False
        for p in products:
            # 가격이나 용량이 없으면 검색
            is_p_null = p['price'] is None or p['price'] == 0
            is_c_null = p['cap'] is None or str(p['cap']).lower() in ('null', 'none', '')

            if is_p_null or is_c_null:
                print(f"  🔍 통합 검색: [{p['name']}] (P:{p['price']}, C:{p['cap']})")
                query = f"{BRAND} {p['name']}"
                items = search_naver_integrated(query)
                
                prices, caps = [], []
                for item in items:
                    text = (item.get('title','') + " " + item.get('description','')).replace('<b>','').replace('</b>','')
                    prices.extend(extract_prices(text))
                    caps.extend(extract_capacities(text))
                
                if is_p_null:
                    new_p = best_value(prices)
                    if new_p: p['price'], changed = new_p, True
                
                if is_c_null:
                    new_c = best_value(caps)
                    if new_c: p['cap'], changed = new_c, True
                
                time.sleep(0.2)

        if changed:
            df.at[idx, 'formatted_output'] = rebuild_cu_multi(products)
            # 정보가 다 채워졌는지 확인 후 플래그 해제
            all_filled = all(
                (p['price'] is not None and p['price'] != 0) and 
                (p['cap'] is not None and str(p['cap']).lower() not in ('null', 'none', ''))
                for p in products
            )
            if all_filled:
                df.at[idx, FLAG_COL] = ""
            
            # 중간 저장 (안정성)
            if targets.index(idx) % 10 == 0:
                df.to_excel(PATH, index=False)
                print(f"  [저장] {targets.index(idx)}건 완료...")

    df.to_excel(PATH, index=False)
    print(f"\n✨ CU 다중 처리 완료. (최종 남은 Y: {len(df[df[FLAG_COL]=='Y'])})")

if __name__ == "__main__":
    run()
