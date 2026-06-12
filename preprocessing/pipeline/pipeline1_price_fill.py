"""
Pipeline 1: 재추출_가격용량 대상 제품에 대해
Naver Blog 검색으로 가격·용량 정보를 수집하여 formatted_output을 업데이트합니다.

필수 .env 설정:
    NAVER_CLIENT_ID     = 네이버 검색 API Client ID
    NAVER_CLIENT_SECRET = 네이버 검색 API Client Secret
"""

import ast
import json
import os
import re
import sys
import time
from collections import Counter
from html.parser import HTMLParser

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

# 편의점 가격대 필터 (원)
PRICE_MIN, PRICE_MAX = 500, 20_000


# ─── HTML 텍스트 추출 (BeautifulSoup 없이) ───────────────────────────────────

class _StripHTML(HTMLParser):
    def __init__(self):
        super().__init__()
        self._parts = []
        self._skip = False

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style", "head"):
            self._skip = True

    def handle_endtag(self, tag):
        if tag in ("script", "style", "head"):
            self._skip = False

    def handle_data(self, data):
        if not self._skip and data.strip():
            self._parts.append(data.strip())

    def get_text(self):
        return " ".join(self._parts)


def html_to_text(html: str) -> str:
    p = _StripHTML()
    try:
        p.feed(html)
        return p.get_text()
    except Exception:
        return re.sub(r"<[^>]+>", " ", html)


# ─── Naver API 검색 ──────────────────────────────────────────────────────────

def _naver_headers():
    cid = os.getenv("NAVER_CLIENT_ID", "")
    csec = os.getenv("NAVER_CLIENT_SECRET", "")
    if not cid or not csec:
        raise RuntimeError(
            "NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 가 .env에 없습니다."
        )
    return {"X-Naver-Client-Id": cid, "X-Naver-Client-Secret": csec}


def search_naver_blog(query: str, display: int = 10) -> list[dict]:
    """Naver 블로그 검색 API 호출 → items 리스트 반환"""
    try:
        import requests

        resp = requests.get(
            "https://openapi.naver.com/v1/search/blog.json",
            headers=_naver_headers(),
            params={"query": query, "display": display, "sort": "sim"},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json().get("items", [])
    except Exception as e:
        print(f"  [검색 오류] {query}: {e}")
        return []


def fetch_blog_body(url: str) -> str:
    """블로그 URL → 본문 텍스트 (실패 시 빈 문자열)"""
    try:
        import requests

        # URL 정규화: PostView 형식으로
        if "PostView.naver" not in url and "blog.naver.com" in url:
            parts = url.split("blog.naver.com/")[1].split("/")
            if len(parts) >= 2:
                blog_id = parts[0]
                log_no = parts[1].split("?")[0]
                url = f"https://blog.naver.com/PostView.naver?blogId={blog_id}&logNo={log_no}"

        resp = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=8,
        )
        resp.raise_for_status()
        text = html_to_text(resp.text)
        return text
    except Exception:
        return ""


# ─── 가격·용량 정규식 추출 ────────────────────────────────────────────────────

_PRICE_RE = re.compile(r"(\d{1,3}(?:,\d{3})*|\d{3,5})\s*원")
_CAP_RE   = re.compile(
    r"(\d+(?:\.\d+)?)\s*(ml|mL|ℓ|L|g|G|kg|개|봉|팩|병|캔|매|인분|조각)",
    re.IGNORECASE,
)


def extract_prices(text: str) -> list[int]:
    candidates = []
    for m in _PRICE_RE.finditer(text):
        raw = m.group(1).replace(",", "")
        val = int(raw)
        if PRICE_MIN <= val <= PRICE_MAX:
            candidates.append(val)
    return candidates


def extract_capacities(text: str) -> list[str]:
    candidates = []
    for m in _CAP_RE.finditer(text):
        candidates.append(f"{m.group(1)}{m.group(2).lower()}")
    return candidates


def best_value(lst: list):
    """최빈값 반환, 없으면 None"""
    if not lst:
        return None
    return Counter(lst).most_common(1)[0][0]


# ─── formatted_output 파싱·재구성 ────────────────────────────────────────────

def parse_formatted(s: str):
    """(name, price, capacity, attrs) 반환. 실패 시 4개 None"""
    if not isinstance(s, str):
        return None, None, None, None
    s = s.strip()
    if s.startswith("{") and s.endswith("}"):
        s = s[1:-1].strip()

    if s.startswith('"[') and ']": [' in s:
        idx = s.index(']": [')
        key_str, val_str = s[1:idx + 1], s[idx + 4:]
    elif "]: [" in s:
        idx = s.index("]: [")
        key_str, val_str = s[:idx + 1], s[idx + 3:]
    else:
        return None, None, None, None

    try:
        k = ast.literal_eval(key_str)
        name, price, capacity = k[0], k[1], k[2]
    except Exception:
        return None, None, None, None

    try:
        attrs = ast.literal_eval(val_str)
    except Exception:
        attrs = []

    return name, price, capacity, attrs


def rebuild_formatted(original: str, name, price, capacity, attrs) -> str:
    """원본 포맷을 유지하면서 새 값으로 재구성"""
    s = str(original).strip()
    attrs_json = json.dumps(attrs, ensure_ascii=False)

    if s.startswith('{"['):          # 세븐단일: {"["name", p, "c"]": [attrs]}
        return f'{{["\\"{name}\\", {price}, \\"{capacity}\\""]": {attrs_json}}}'
    elif s.startswith('{['):         # CU단일:   {["name", p, "c"]: [attrs]}
        return f'{{["{name}", {price}, "{capacity}"]: {attrs_json}}}'
    else:                            # 다중 (CU/세븐): ['name', p, 'c']: [attrs]
        return f"['{name}', {price}, '{capacity}']: {attrs_json}"


def _is_null_price(p) -> bool:
    return p is None or p == 0


def _is_null_cap(c) -> bool:
    return c is None or str(c).strip().lower() in ("null", "none", "")


# ─── 제품 1개 처리 ────────────────────────────────────────────────────────────

def fill_one_product(
    product_name: str,
    brand: str,
    current_price,
    current_cap,
) -> tuple:
    """
    Naver 블로그 검색으로 price, capacity 를 채운다.
    반환: (found_price, found_capacity)  — 찾지 못하면 current 값 유지
    """
    query = f"{brand} {product_name}"
    items = search_naver_blog(query, display=10)
    time.sleep(0.3)

    all_prices = []
    all_caps   = []

    for item in items:
        # description(스니펫) 우선 확인
        desc = re.sub(r"<[^>]+>", "", item.get("description", ""))
        all_prices.extend(extract_prices(desc))
        all_caps.extend(extract_capacities(desc))

        # 스니펫에서 못 찾으면 본문 크롤링
        if not all_prices or not all_caps:
            body = fetch_blog_body(item.get("link", ""))
            all_prices.extend(extract_prices(body))
            all_caps.extend(extract_capacities(body))
            time.sleep(0.2)

        if all_prices and all_caps:
            break  # 둘 다 찾으면 조기 종료

    found_price = best_value(all_prices) if _is_null_price(current_price) else current_price
    found_cap   = best_value(all_caps)   if _is_null_cap(current_cap)   else current_cap
    if found_cap:
        found_cap = str(found_cap)

    return found_price, found_cap


# ─── 메인 파이프라인 ──────────────────────────────────────────────────────────

def run(dry_run: bool = False):
    for file_key, (path, brand) in FILES.items():
        df = pd.read_excel(path)
        flag_col = "재추출_가격용량"
        if flag_col not in df.columns:
            print(f"[{file_key}] 플래그 컬럼 없음, 건너뜀")
            continue

        target_idx = df.index[df[flag_col] == "Y"].tolist()
        print(f"\n[{file_key}] 대상: {len(target_idx)}행")

        updated = 0
        for i in target_idx:
            row = df.loc[i]
            name, price, cap, attrs = parse_formatted(row["formatted_output"])
            if name is None:
                continue

            # 이미 둘 다 있으면 플래그만 해제
            if not _is_null_price(price) and not _is_null_cap(cap):
                df.at[i, flag_col] = ""
                continue

            print(f"  검색: [{name}] price={price}, cap={cap}")

            if not dry_run:
                new_price, new_cap = fill_one_product(name, brand, price, cap)
            else:
                new_price, new_cap = price, cap

            new_price = new_price if new_price is not None else price
            new_cap   = str(new_cap) if new_cap is not None else str(cap)

            df.at[i, "formatted_output"] = rebuild_formatted(
                row["formatted_output"], name, new_price, new_cap, attrs
            )

            # 둘 다 채워졌으면 플래그 해제
            if not _is_null_price(new_price) and not _is_null_cap(new_cap):
                df.at[i, flag_col] = ""
            updated += 1

        if not dry_run:
            df.to_excel(path, index=False)
        
        remaining_y = (df[flag_col] == "Y").sum()
        print(f"  처리 완료: {updated}행 시도 (남은 'Y': {remaining_y}행) / 저장{'(dry_run)' if dry_run else ''}")


if __name__ == "__main__":
    run(dry_run=False)
