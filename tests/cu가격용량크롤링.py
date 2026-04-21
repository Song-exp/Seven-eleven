"""
CU 편의점 상품 가격/용량 전수 수집 스크립트 (네이버 검색만 사용)

설치:  pip install pandas openpyxl requests tqdm
실행:  python crawl_all.py
"""

import json
import re
import time
from collections import Counter
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm


# ══════════════════════════════════════════════════════════
# ★ 설정
# 여기 수정해!!!! 네이버개발자센터에서따와!!
# ══════════════════════════════════════════════════════════
NAVER_CLIENT_ID     = "네이버개발자센터에서따와"
NAVER_CLIENT_SECRET = "네이버개발자센터에서따와"

INPUT_PATH  = "CU_single_products_exploded.xlsx"
OUTPUT_PATH = "CU_single_products_crawled.xlsx"
CACHE_PATH  = "search_cache.json"

SEARCH_DELAY = 0.13   # 초당 약 7회. 429 오류 나면 0.2로 늘리세요
# ══════════════════════════════════════════════════════════


# ──────────────────────────────────────────────────────────
# formatted_output 파싱 / 재조립
# ──────────────────────────────────────────────────────────
FO_RE = re.compile(r"^\['(.+?)',\s*(.+?),\s*'(.+?)'\]:\s*(\[.*\])$")

def parse_fo(cell: str):
    m = FO_RE.match(str(cell).strip())
    if not m:
        return None
    return m.group(1), m.group(2), m.group(3), m.group(4)
    # 순서: 제품명, 가격(문자열), 용량(문자열), 태그(원문)

def build_fo(name, price, cap, tags) -> str:
    return f"['{name}', {price}, '{cap}']: {tags}"


# ──────────────────────────────────────────────────────────
# 네이버 검색 API
# ──────────────────────────────────────────────────────────
def naver_search(query: str, display: int = 5) -> list:
    url = "https://openapi.naver.com/v1/search/webkr.json"
    headers = {
        "X-Naver-Client-Id":     NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
    }
    for _ in range(2):
        try:
            r = requests.get(url, headers=headers,
                             params={"query": query, "display": display},
                             timeout=6)
            if r.status_code == 200:
                return r.json().get("items", [])
            if r.status_code == 429:
                tqdm.write("  [429] 잠시 대기...")
                time.sleep(5)
        except Exception as e:
            tqdm.write(f"  [오류] {e}")
    return []

def clean(html: str) -> str:
    return re.sub(r"<[^>]+>", "", html)

def texts(items: list) -> list:
    """검색 결과에서 제목+설명 텍스트 리스트 추출."""
    result = []
    for item in items:
        result.append(clean(item.get("title", "")))
        result.append(clean(item.get("description", "")))
    return result


# ──────────────────────────────────────────────────────────
# 용량 추출
# ──────────────────────────────────────────────────────────
_CAP_PATS = [
    re.compile(r"(\d+(?:\.\d+)?(?:g|ml|mL|kg|L))\s*(?:칼로리|\d+[Kk]cal)", re.I),
    re.compile(r"1회분\s*[\(（](\d+(?:\.\d+)?(?:g|ml|mL))[\)）]",            re.I),
    re.compile(r"[\(（](\d+(?:\.\d+)?(?:g|ml|mL|kg))[\)）]",                 re.I),
    re.compile(r"\b(\d+(?:\.\d+)?(?:g|ml|mL))\b",                            re.I),
]

def extract_cap(text: str):
    for pat in _CAP_PATS:
        m = pat.search(text)
        if m:
            cap = m.group(1)
            num = float(re.search(r"\d+(?:\.\d+)?", cap).group())
            if 5 <= num <= 3000:
                return cap
    return None

def search_cap(name: str):
    """
    네이버에서 두 가지 쿼리로 용량 검색.
    ① "CU {제품명} 용량"
    ② "CU {제품명} g"
    """
    for query in [f"CU {name} 용량", f"CU {name} g"]:
        for text in texts(naver_search(query)):
            cap = extract_cap(text)
            if cap:
                return cap
        time.sleep(SEARCH_DELAY)
    return None


# ──────────────────────────────────────────────────────────
# 가격 추출
# ──────────────────────────────────────────────────────────
_PRICE_PATS = [
    re.compile(r"(\d{1,2},\d{3})원"),
    re.compile(r"\b(\d{4,5})원"),
]

def extract_price(text: str):
    for pat in _PRICE_PATS:
        for raw in pat.findall(text):
            price = int(raw.replace(",", ""))
            if 300 <= price <= 50000:
                return price
    return None

def search_price(name: str):
    """
    네이버에서 두 가지 쿼리로 가격 검색.
    ① "CU {제품명} 가격"
    ② "CU {제품명} 원"
    여러 후보 중 최빈값 반환.
    """
    candidates = []
    for query in [f"CU {name} 가격", f"CU {name} 원"]:
        for text in texts(naver_search(query)):
            p = extract_price(text)
            if p:
                candidates.append(p)
        time.sleep(SEARCH_DELAY)
    if candidates:
        return Counter(candidates).most_common(1)[0][0]
    return None


# ──────────────────────────────────────────────────────────
# 캐시
# ──────────────────────────────────────────────────────────
def load_cache() -> dict:
    if Path(CACHE_PATH).exists():
        with open(CACHE_PATH, encoding="utf-8") as f:
            data = json.load(f)
        print(f"캐시 로드: 가격 {len(data.get('price',{}))}개 / 용량 {len(data.get('cap',{}))}개\n")
        return data
    return {"price": {}, "cap": {}}

def save_cache(cache: dict):
    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


# ──────────────────────────────────────────────────────────
# 메인
# ──────────────────────────────────────────────────────────
def main():
    print(f"로딩: {INPUT_PATH}")
    df = pd.read_excel(INPUT_PATH)
    print(f"총 {len(df)}행\n")

    cache = load_cache()
    price_cache = cache["price"]
    cap_cache   = cache["cap"]

    # 전체 고유 제품명 수집 (null 여부 무관)
    all_names = set()
    for cell in df["formatted_output"]:
        parsed = parse_fo(str(cell))
        if parsed:
            all_names.add(parsed[0])

    need_cap   = sorted(n for n in all_names if n not in cap_cache)
    need_price = sorted(n for n in all_names if n not in price_cache)

    print(f"고유 제품명: {len(all_names)}개")
    print(f"신규 검색 — 가격: {len(need_price)}개 / 용량: {len(need_cap)}개")
    print(f"예상 API 호출: 최대 {(len(need_price) + len(need_cap)) * 2}회\n")

    # ── 용량 검색 ─────────────────────────────────────
    if need_cap:
        print("=== 용량 검색 ===")
        for name in tqdm(need_cap, desc="용량"):
            cap = search_cap(name)
            cap_cache[name] = cap
            if cap:
                tqdm.write(f"  v {name[:40]:<40} {cap}")
        save_cache({"price": price_cache, "cap": cap_cache})
        print()

    # ── 가격 검색 ─────────────────────────────────────
    if need_price:
        print("=== 가격 검색 ===")
        for name in tqdm(need_price, desc="가격"):
            price = search_price(name)
            price_cache[name] = price
            if price:
                tqdm.write(f"  v {name[:40]:<40} {price:,}원")
        save_cache({"price": price_cache, "cap": cap_cache})
        print()

    # ── 엑셀 반영 ─────────────────────────────────────
    print("=== 엑셀 업데이트 ===")
    updated_price = updated_cap = skipped = 0
    new_cells = []

    for cell in df["formatted_output"]:
        parsed = parse_fo(str(cell))
        if not parsed:
            new_cells.append(cell)
            skipped += 1
            continue

        name, old_price, old_cap, tags = parsed

        new_price = str(price_cache[name]) if price_cache.get(name) else old_price
        new_cap   = cap_cache[name]        if cap_cache.get(name)   else old_cap

        if new_price != old_price: updated_price += 1
        if new_cap   != old_cap:   updated_cap   += 1

        new_cells.append(build_fo(name, new_price, new_cap, tags))

    df["formatted_output"] = new_cells
    df.to_excel(OUTPUT_PATH, index=False)

    # 최종 null 잔여
    final_null_p = final_null_c = 0
    for cell in df["formatted_output"]:
        parsed = parse_fo(str(cell))
        if not parsed:
            continue
        _, price, cap, _ = parsed
        if str(price).strip() in ("0", "null", "None", ""):
            final_null_p += 1
        if str(cap).strip() in ("null", "None", ""):
            final_null_c += 1

    print(f"""
완료!
  가격 변경: {updated_price}건
  용량 변경: {updated_cap}건
  파싱 불가: {skipped}건 (원본 유지)
  최종 가격 null 잔여: {final_null_p}건
  최종 용량 null 잔여: {final_null_c}건
  결과 파일: {OUTPUT_PATH}
  캐시 파일: {CACHE_PATH}  <- 재실행 시 자동 재사용
""")


if __name__ == "__main__":
    main()
