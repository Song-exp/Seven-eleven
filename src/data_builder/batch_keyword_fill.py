# -*- coding: utf-8 -*-
"""
batch_keyword_fill.py — under-tagged 제품(키워드 ≤2개) 키워드 보충

소스:
  1. 네이버 쇼핑 API  → 상품 타이틀 + 카테고리 텍스트 (vocab 매칭)
  2. 네이버 블로그 크롤링 → 관련성 필터 → LLM 키워드 추출
  3. 이름 기반 vocab 매칭 → 블로그·쇼핑 모두 0개일 때 최후 fallback

와인 특화:
  - 제품명에서 이탈리아어/프랑스어/포도품종으로 와인 색상 감지
  - 검색어를 '{색상}와인 편의점 추천 맛'으로 변환
  - 관련성 필터를 제품명 매칭 대신 '와인' 용어 포함 여부로 완화

입력 (자동):
  data/processed/hin/product_nodes.parquet
  data/processed/hin/keyword_nodes.parquet
  data/processed/pos_product_features.parquet
  .env (NAVER_CLIENT_ID, NAVER_CLIENT_SECRET)

출력:
  data/processed/keyword_fill_edges.parquet   (ITEM_CD | keyword | source)
  data/processed/keyword_fill_checkpoint.csv  (체크포인트)
"""

import ast
import os
import re
import sys
import time

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "tests", "네이버블로그크롤링"))

from keyword_extractor import extract_keywords_blog
from keyword_pipeline import run_pipeline
from naver_blog_crawler import run_naver_blog_crawler
from preprocess_blog_merge import compute_quality_features, is_high_quality, merge_bodies

load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

# ─── 경로 ──────────────────────────────────────────────────────────────────
NODES_PATH  = os.path.join(PROJECT_ROOT, "data", "processed", "hin", "product_nodes.parquet")
KW_PATH     = os.path.join(PROJECT_ROOT, "data", "processed", "hin", "keyword_nodes.parquet")
POS_PATH    = os.path.join(PROJECT_ROOT, "data", "processed", "pos_product_features.parquet")
CKPT_CSV    = os.path.join(PROJECT_ROOT, "data", "processed", "keyword_fill_checkpoint.csv")
OUTPUT_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "keyword_fill_edges.parquet")

# ─── 파라미터 ──────────────────────────────────────────────────────────────
KW_COUNT_THRESHOLD = 2       # 이 이하인 제품이 대상
SHOP_RESULTS       = 10      # 쇼핑 검색 결과 수
BLOG_RESULTS       = 25      # 블로그 검색 결과 수
BLOG_START_DATE    = "2025-01-01"
CHECKPOINT_EVERY   = 20
MIN_KW_TO_SKIP     = 8       # 쇼핑에서 이미 이상 확보하면 블로그 skip

MIN_KW_LEN      = 3    # vocab 키워드 최소 글자 수
MIN_KW_FREQ     = 1    # 텍스트 내 최소 등장 횟수
MAX_KW_PER_PROD = 30   # 제품당 최대 키워드 수 캡

SHOP_NOISE_KWS = {
    "편의점", "세븐일레븐", "GS25", "CU편의점", "이마트", "모바일", "콜라보",
    "간식", "야식", "디저트", "할인", "쿠폰", "증정", "기획", "행사",
}

MDDV_SEARCH_SUFFIX: dict = {
    "양주":        "위스키",
    "노벨티":      "아이스크림",
    "가공미반류":  "즉석밥",
    "푸드간편식":  "간편식",
    "냉장간편식":  "간편식",
    "즉석조리":    "즉석식품",
    "구움과자":    "과자",
    "냉장베이커리": "베이커리",
    "스낵안주":    "안주",
    "냉장안주":    "안주",
    "기능성드링크": "드링크",
    "프로틴음료":  "단백질음료",
    "프로틴/시리얼": "단백질",
    "가공우유":    "우유",
}

# ─── 와인 특화 설정 ───────────────────────────────────────────────────────
WINE_MDDV = "와인"

# 이탈리아어/프랑스어/포도품종 → 한국어 와인 색상
WINE_COLOR_MAP = {
    "로쏘":      "레드와인",
    "루즈":      "로제와인",
    "비안코":    "화이트와인",
    "블랑":      "화이트와인",
    "레드":      "레드와인",
    "화이트":    "화이트와인",
    "로제":      "로제와인",
    "피노누아":  "레드와인",
    "까버네":    "레드와인",
    "카르미네르": "레드와인",
    "템프라니요": "레드와인",
    "조진판델":  "레드와인",
    "멜로":      "레드와인",
    "말벡":      "레드와인",
    "소비뇽":    "화이트와인",
    "샤르도네":  "화이트와인",
    "리슬링":    "화이트와인",
    "모스카토":  "화이트와인",
    "세미스윗":  "화이트와인",
    "누아":      "레드와인",
}

_WINE_RELEVANCE_TERMS = {"와인", "레드와인", "화이트와인", "로제와인", "포도주", "와이너리", "포도"}

_SPEC_RE  = re.compile(r"\d+(\.\d+)?\s*[gGmlML개입봉팩P]")
_BRAND_RE = re.compile(r"^[^)]+\)")
_CAT_CODE = re.compile(r"^\d{4}\s*:")


# ─── 유틸 ──────────────────────────────────────────────────────────────────

def norm_id(x):
    try:
        return str(int(float(x)))
    except Exception:
        return str(x)


def clean_name(name: str) -> str:
    """브랜드 접두사·규격 제거."""
    s = _BRAND_RE.sub("", str(name)).strip()
    s = _SPEC_RE.sub("", s).strip()
    s = re.sub(r"[★☆▶▷!&\(\)\[\].·]", "", s).strip()
    return s


def _strip_spec(name: str) -> str:
    return _SPEC_RE.sub("", name).strip()


def detect_wine_color(name: str) -> str | None:
    """제품명에서 와인 색상 감지."""
    for pattern, color in WINE_COLOR_MAP.items():
        if pattern in name:
            return color
    return None


def is_relevant_to_product(text: str, product_name: str) -> bool:
    """일반 제품 관련성 필터: 제품명 핵심어 포함 여부.

    복합어는 슬라이딩 윈도우(3~4자)로 분해해 매칭.
    """
    core = _strip_spec(product_name)
    words = re.findall(r"[가-힣]{2,}", core)
    tokens: set = set()
    for w in words:
        tokens.add(w)
        for length in range(3, min(5, len(w) + 1)):
            for start in range(len(w) - length + 1):
                tokens.add(w[start:start + length])
    return any(t in text for t in tokens)


def is_relevant_wine(text: str, product_name: str = None) -> bool:
    """와인 관련성 필터: 제품명 대신 와인 용어 포함 여부로 완화."""
    return any(t in text for t in _WINE_RELEVANCE_TERMS)


def kw_len(v) -> int:
    if v is None:
        return 0
    if isinstance(v, float) and np.isnan(v):
        return 0
    if isinstance(v, (list, np.ndarray)):
        return len(v)
    if isinstance(v, str):
        try:
            return len(ast.literal_eval(v))
        except Exception:
            return 0
    return 0


def load_vocab() -> tuple:
    """keyword_nodes에서 vocab 반환. (list: 쇼핑 매칭용, set: 이름기반 매칭용)"""
    kw_nodes = pd.read_parquet(KW_PATH)
    vocab_list = [
        k for k in kw_nodes["keyword"].tolist()
        if not _CAT_CODE.match(str(k)) and len(str(k)) >= MIN_KW_LEN
    ]
    vocab_set = set(vocab_list)
    return vocab_list, vocab_set


def match_vocab(text: str, vocab: list) -> list:
    """쇼핑 API 텍스트에서 vocab 키워드 매칭."""
    if not text:
        return []
    matched = [
        kw for kw in vocab
        if kw and kw not in SHOP_NOISE_KWS and text.count(kw) >= MIN_KW_FREQ
    ]
    return matched[:MAX_KW_PER_PROD]


def name_based_vocab_match(product_name: str, vocab_set: set) -> list:
    """제품명 토큰을 vocab에 직접 매칭 (쇼핑·블로그 모두 0개일 때 최후 fallback).

    한글 토큰을 추출 후 전체 → 슬라이딩 윈도우(긴 것 우선) 순으로 vocab 매칭.
    """
    tokens = re.findall(r"[가-힣]{2,}", product_name)
    matched = []
    seen = set()
    for tok in tokens:
        # 전체 토큰 먼저
        if tok in vocab_set and tok not in seen and tok not in SHOP_NOISE_KWS:
            matched.append(tok)
            seen.add(tok)
        # 슬라이딩 윈도우 (긴 것 우선)
        for length in range(len(tok) - 1, MIN_KW_LEN - 1, -1):
            for start in range(len(tok) - length + 1):
                sub = tok[start:start + length]
                if sub in vocab_set and sub not in seen and sub not in SHOP_NOISE_KWS:
                    matched.append(sub)
                    seen.add(sub)
    return matched


# ─── 소스 1: 네이버 쇼핑 API ───────────────────────────────────────────────

def search_shopping(query: str) -> str:
    """쇼핑 검색 결과 타이틀 + 카테고리 텍스트 반환."""
    client_id     = os.getenv("NAVER_CLIENT_ID")
    client_secret = os.getenv("NAVER_CLIENT_SECRET")
    if not client_id or not client_secret:
        return ""

    url     = "https://openapi.naver.com/v1/search/shop.json"
    headers = {"X-Naver-Client-Id": client_id, "X-Naver-Client-Secret": client_secret}
    params  = {"query": query, "display": SHOP_RESULTS, "sort": "sim"}

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        items = resp.json().get("items", [])
    except Exception as e:
        print(f"      [쇼핑 API 오류] {e}")
        return ""

    parts = []
    for item in items:
        title = re.sub(r"<[^>]+>", "", item.get("title", ""))
        parts.append(title)
        for cat_key in ("category1", "category2", "category3", "category4"):
            cat = item.get(cat_key, "")
            if cat:
                parts.append(cat)
        if item.get("brand"):
            parts.append(item["brand"])
        if item.get("maker"):
            parts.append(item["maker"])

    return " ".join(parts)


# ─── 소스 2: 네이버 블로그 (관련성 필터 + LLM 추출) ─────────────────────────

def _crawl_one_query(query: str) -> list:
    """단일 쿼리로 블로그 크롤링 후 본문 리스트 반환."""
    try:
        raw = run_naver_blog_crawler(
            [query],
            results_limit=BLOG_RESULTS,
            sort="sim",
            start_date=BLOG_START_DATE,
            save_csv=False,
        )
    except Exception as e:
        print(f"      [블로그 크롤 오류] {e}")
        return []
    if raw is None or raw.empty:
        return []
    body_col = "content" if "content" in raw.columns else "본문내용"
    return raw[body_col].fillna("").tolist()


def crawl_blog_relevant(
    query: str,
    product_name: str,
    fallback_query: str = None,
    relevance_fn=None,
) -> tuple:
    """블로그 크롤링 후 품질·관련성 2단계 필터를 통과한 게시물 본문 반환.

    relevance_fn: (text) → bool. None이면 is_relevant_to_product(text, product_name) 사용.

    Returns:
        merged_body (str), quality_count (int), relevant_count (int)
    """
    if relevance_fn is None:
        def relevance_fn(text):
            return is_relevant_to_product(text, product_name)

    bodies_raw = _crawl_one_query(query)

    quality_passed = [
        str(t) for t in bodies_raw
        if is_high_quality(compute_quality_features(str(t or "")))
    ]

    relevant = [t for t in quality_passed if relevance_fn(t)]

    if not relevant and fallback_query and fallback_query != query:
        print(f"      [fallback] '{fallback_query}' 재시도")
        bodies_fb = _crawl_one_query(fallback_query)
        quality_fb = [
            str(t) for t in bodies_fb
            if is_high_quality(compute_quality_features(str(t or "")))
        ]
        relevant_fb = [t for t in quality_fb if relevance_fn(t)]
        quality_passed = quality_passed + quality_fb
        relevant = relevant + relevant_fb

    merged = merge_bodies(pd.Series(relevant))
    return merged, len(quality_passed), len(relevant)


# ─── 대상 계산 ──────────────────────────────────────────────────────────────

def compute_targets() -> pd.DataFrame:
    nodes = pd.read_parquet(NODES_PATH)
    nodes["kw_count"] = nodes["키워드_final"].apply(kw_len)
    under = nodes[nodes["kw_count"] <= KW_COUNT_THRESHOLD].copy()
    under["query_name"] = under["ITEM_NM"].apply(clean_name)

    try:
        pos = pd.read_parquet(POS_PATH)[["ITEM_CD", "ITEM_MDDV_NM"]]
        pos["ITEM_CD"] = pos["ITEM_CD"].apply(norm_id)
        under["_norm"] = under["ITEM_CD"].apply(norm_id)
        under = under.merge(pos.rename(columns={"ITEM_CD": "_norm"}), on="_norm", how="left")
        under = under.drop(columns=["_norm"])
    except Exception as e:
        print(f"  [경고] 중분류 로드 실패: {e}")
        under["ITEM_MDDV_NM"] = ""

    def make_query(row):
        name = row["query_name"]
        mddv = str(row.get("ITEM_MDDV_NM") or "").strip()

        # 와인 특화: 색상 감지 후 범용 검색어
        if mddv == WINE_MDDV:
            color = detect_wine_color(row["ITEM_NM"])
            if color:
                return f"{color} 편의점 추천 맛"
            return "편의점 와인 추천 맛"

        suffix = MDDV_SEARCH_SUFFIX.get(mddv, mddv)
        if suffix and suffix not in name:
            return f"{name} {suffix}"
        return name

    under["search_query"] = under.apply(make_query, axis=1)
    cols = ["ITEM_CD", "ITEM_NM", "편의점명", "kw_count", "query_name", "search_query", "ITEM_MDDV_NM"]
    return under[cols].reset_index(drop=True)


# ─── 메인 ──────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("키워드 보충 파이프라인 (Shopping + Blog LLM + 이름기반 fallback)")
    print(f"  대상: 키워드 ≤{KW_COUNT_THRESHOLD}개 / 쇼핑 top{SHOP_RESULTS} / 블로그 top{BLOG_RESULTS}")
    print("=" * 60)

    vocab_list, vocab_set = load_vocab()
    print(f"\n[0] vocab 로드: {len(vocab_list)}개 키워드")

    if os.path.exists(CKPT_CSV):
        print(f"\n[1] 체크포인트 로드: {CKPT_CSV}")
        df = pd.read_csv(CKPT_CSV, encoding="utf-8-sig")
        df["_processed"] = df["_processed"].astype(bool)
        for col, default in [
            ("blog_review_kws", ""),
            ("blog_relevant", 0),
            ("ITEM_MDDV_NM", ""),
        ]:
            if col not in df.columns:
                df[col] = default
        # 미처리 항목: 새 로직으로 쿼리·중분류 재계산
        if (~df["_processed"]).any():
            new_q = compute_targets().set_index("ITEM_CD")[["query_name", "search_query", "ITEM_MDDV_NM"]]
            for idx in df.index[~df["_processed"]]:
                icd = str(df.at[idx, "ITEM_CD"])
                if icd in new_q.index:
                    df.at[idx, "query_name"]   = new_q.at[icd, "query_name"]
                    df.at[idx, "search_query"] = new_q.at[icd, "search_query"]
                    df.at[idx, "ITEM_MDDV_NM"] = new_q.at[icd, "ITEM_MDDV_NM"]
            print(f"  미처리 {(~df['_processed']).sum()}개 쿼리 재계산 완료")
    else:
        print("\n[1] 대상 계산 중...")
        df = compute_targets()
        print(f"  대상: {len(df)}개")
        print(df["편의점명"].value_counts().to_string())
        df["shop_kws"]        = ""
        df["blog_kws"]        = ""
        df["blog_review_kws"] = ""
        df["blog_relevant"]   = 0
        df["fill_kws"]        = ""
        df["_processed"]      = False

    total   = len(df)
    done    = int(df["_processed"].sum())
    pending = df.index[~df["_processed"]].tolist()
    print(f"\n  총 {total}개 / 완료: {done}개 / 남은: {len(pending)}개")

    if not pending:
        print("  모두 완료됨.")
    else:
        print("\n[2] 크롤링 + 키워드 추출 시작...\n")
        processed_since_save = 0

        for i, idx in enumerate(pending, start=1):
            row        = df.loc[idx]
            name       = row["ITEM_NM"]
            query_name = row["query_name"]
            query      = row["search_query"]
            store      = row["편의점명"]
            mddv       = str(row.get("ITEM_MDDV_NM") or "").strip()
            is_wine    = (mddv == WINE_MDDV)

            print(f"  [{done + i}/{total}] {name} [{store}] [{mddv}]", flush=True)
            print(f"      검색어: {query}")

            # ── 쇼핑 검색 (vocab 매칭)
            shop_text = search_shopping(query)
            shop_kws  = match_vocab(shop_text, vocab_list)
            df.at[idx, "shop_kws"] = str(shop_kws)
            print(f"      쇼핑: {len(shop_kws)}개 매칭")
            time.sleep(0.2)

            # ── 블로그 (관련성 필터 → LLM 추출)
            blog_kws        = []
            blog_review_kws = []
            blog_relevant   = 0

            if len(shop_kws) < MIN_KW_TO_SKIP:
                # 와인이면 완화된 관련성 필터, 아니면 제품명 기반
                rel_fn    = is_relevant_wine if is_wine else None
                fallback  = query_name if query_name != query else None
                merged_body, quality_cnt, blog_relevant = crawl_blog_relevant(
                    query, query_name,
                    fallback_query=fallback,
                    relevance_fn=rel_fn,
                )
                print(f"      블로그: 품질통과 {quality_cnt}건 / 관련성 통과 {blog_relevant}건")

                if merged_body:
                    result = extract_keywords_blog(query_name, merged_body, cutoff=5000, num_ctx=8192)
                    if result:
                        raw_hin         = result.get("hin_keywords") or []
                        raw_rv          = result.get("review_keywords") or []
                        blog_kws        = run_pipeline(raw_hin, product_name=query_name)
                        blog_review_kws = raw_rv
                        print(f"      LLM: hin={blog_kws[:3]} / review={blog_review_kws[:3]}")
                    else:
                        print("      LLM: 추출 실패")
                else:
                    print("      블로그: 관련 게시물 없음")

                time.sleep(0.3)

            df.at[idx, "blog_kws"]        = str(blog_kws)
            df.at[idx, "blog_review_kws"] = str(blog_review_kws)
            df.at[idx, "blog_relevant"]   = blog_relevant

            # ── 이름 기반 vocab 매칭 (쇼핑·블로그 모두 0개일 때 최후 fallback)
            name_kws = []
            if len(shop_kws) == 0 and len(blog_kws) == 0:
                if is_wine:
                    # 와인: 색상 감지 → vocab에 있으면 직접 추가
                    color = detect_wine_color(name)
                    if color and color in vocab_set:
                        name_kws = [color]
                        print(f"      이름기반(와인색상): {name_kws}")
                if not name_kws:
                    name_kws = name_based_vocab_match(query_name, vocab_set)
                    if name_kws:
                        print(f"      이름기반: {name_kws[:5]}")

            # ── 합치기 (dedup, 순서 유지)
            combined = list(dict.fromkeys(shop_kws + blog_kws + name_kws))
            df.at[idx, "fill_kws"] = str(combined)

            llm_failed = (blog_relevant > 0) and (len(blog_kws) == 0)
            df.at[idx, "_processed"] = not llm_failed
            if llm_failed:
                print("      [재시도 대기] LLM 실패 - 다음 실행에서 재시도")

            print(f"      최종: {len(combined)}개 → {combined[:5]}")

            processed_since_save += 1
            if processed_since_save >= CHECKPOINT_EVERY:
                df.to_csv(CKPT_CSV, index=False, encoding="utf-8-sig")
                print(f"\n  [체크포인트] {done + i}건 완료\n")
                processed_since_save = 0

        if processed_since_save > 0:
            df.to_csv(CKPT_CSV, index=False, encoding="utf-8-sig")

    # ─── 엣지 파일 생성 ────────────────────────────────────────────────────
    print("\n[3] 엣지 파일 생성 중...")

    def parse_kws(v):
        if isinstance(v, list):
            return v
        try:
            return ast.literal_eval(str(v))
        except Exception:
            return []

    records = []
    for _, row in df.iterrows():
        shop_kws = parse_kws(row.get("shop_kws", "[]"))
        blog_kws = parse_kws(row.get("blog_kws", "[]"))
        fill_kws = parse_kws(row.get("fill_kws", "[]"))
        shop_set = set(shop_kws)
        blog_set = set(blog_kws)
        for kw in shop_kws:
            records.append({"ITEM_CD": row["ITEM_CD"], "keyword": kw, "source": "shopping_fill"})
        for kw in blog_kws:
            if kw not in shop_set:
                records.append({"ITEM_CD": row["ITEM_CD"], "keyword": kw, "source": "blog_llm_fill"})
        # name_fill: fill_kws 중 shop/blog에 없는 것
        for kw in fill_kws:
            if kw not in shop_set and kw not in blog_set:
                records.append({"ITEM_CD": row["ITEM_CD"], "keyword": kw, "source": "name_fill"})

    edges = pd.DataFrame(records).drop_duplicates(["ITEM_CD", "keyword"])
    edges.to_parquet(OUTPUT_PATH, index=False, engine="pyarrow")

    filled_prods = edges["ITEM_CD"].nunique()
    print(f"\n=== 저장 완료: {OUTPUT_PATH} ===")
    print(f"  엣지 {len(edges)}개 / 제품 {filled_prods}개 커버")
    print(f"\n다음 단계: refine_fill_keywords.py → 04_hin_graph_builder.ipynb")


if __name__ == "__main__":
    main()
