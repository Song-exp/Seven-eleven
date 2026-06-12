"""
블로그 키워드 추출 테스트
- blog_merged.csv 에서 다양한 본문 길이 구간별 샘플을 뽑아
  extract_keywords_blog() 품질과 소요 시간을 확인합니다.
- 실행: python src/data_builder/test_blog_keywords.py
"""
import os
import sys
import time
import re
import requests

import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from keyword_extractor import (
    extract_keywords_blog,
    BLOG_REVIEW_SYSTEM_PROMPT,
    MODEL_NAME,
    TEMPERATURE,
    TIMEOUT,
)

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
MERGED_CSV = os.path.join(PROJECT_ROOT, "data", "processed", "blog_merged.csv")

# 길이 구간별 샘플 수
LENGTH_BINS = [
    (0,    1_000,  1, "극단짧음"),
    (1_000, 3_000, 2, "짧음"),
    (3_000, 6_000, 2, "중간"),
    (6_000, 10_000, 2, "긺"),
    (10_000, 99_999_999, 1, "초과(10k 잘림)"),
]


def pick_samples(df: pd.DataFrame) -> list[dict]:
    df = df.copy()
    df["본문길이"] = df["본문내용"].fillna("").str.len()
    samples = []
    for lo, hi, n, label in LENGTH_BINS:
        bucket = df[(df["본문길이"] >= lo) & (df["본문길이"] < hi)]
        for _, row in bucket.sample(min(n, len(bucket)), random_state=42).iterrows():
            samples.append({
                "label": label,
                "검색어": row["검색어"],
                "product_name": row["product_name"],
                "본문길이": row["본문길이"],
                "본문내용": row["본문내용"],
            })
    return samples


def evaluate(sample: dict) -> dict:
    start = time.perf_counter()
    result = extract_keywords_blog(sample["product_name"], sample["본문내용"])
    elapsed = time.perf_counter() - start

    review_kws = result.get("review_keywords") or []
    hin_kws = result.get("hin_keywords") or []

    return {
        "elapsed": elapsed,
        "review_count": len(review_kws),
        "hin_count": len(hin_kws),
        "review_keywords": review_kws,
        "hin_keywords": hin_kws,
        "success": bool(result),
    }


def debug_raw_response(product_name: str, body: str) -> None:
    """raw 응답을 그대로 출력해서 JSON 파싱 실패 원인 파악."""
    text = re.sub(r"\s+", " ", (body or "").strip())[:2000]
    prompt = (
        f"{BLOG_REVIEW_SYSTEM_PROMPT}\n\n"
        f"[분석 대상 제품명]: {product_name}\n\n"
        f"[블로그 본문]: {text}"
    )
    payload = {
        "model": MODEL_NAME,
        "prompt": prompt,
        "stream": False,
        "think": False,
        "options": {"temperature": TEMPERATURE, "num_ctx": 4096},
    }
    resp = requests.post("http://localhost:11434/api/generate", json=payload, timeout=TIMEOUT)
    resp_json = resp.json()
    print(f"\n[HTTP 상태코드]: {resp.status_code}")
    print(f"[응답 JSON 키목록]: {list(resp_json.keys())}")
    print(f"[done_reason]: {resp_json.get('done_reason')}")
    print(f"[eval_count(출력토큰)]: {resp_json.get('eval_count')}")
    print(f"[prompt_eval_count(입력토큰)]: {resp_json.get('prompt_eval_count')}")
    raw = resp_json.get("response", "").strip()
    print(f"\n[RAW 응답 전체 ({len(raw)}자)]")
    print(raw[:500] if raw else "(비어있음)")
    print()


def main():
    print("=" * 70)
    print("블로그 키워드 추출 품질 & 속도 테스트")
    print("=" * 70)

    print(f"\n데이터 로드: {MERGED_CSV}")
    df = pd.read_csv(MERGED_CSV, encoding="utf-8-sig")
    df["본문내용"] = df["본문내용"].fillna("")
    print(f"총 {len(df)}개 검색어")

    samples = pick_samples(df)
    print(f"샘플 {len(samples)}건 선정\n")

    # --- RAW 응답 디버그: 첫 번째 샘플 1건만 찍어보기 ---
    print("=" * 70)
    print("[ DEBUG ] 첫 번째 샘플 raw 응답 확인")
    print("=" * 70)
    debug_sample = next((s for s in samples if s["본문길이"] > 500), samples[0])
    print(f"검색어: {debug_sample['검색어']} / 본문길이: {debug_sample['본문길이']:,}자")
    debug_raw_response(debug_sample["product_name"], debug_sample["본문내용"])
    print("=" * 70)
    print()

    results = []
    for i, s in enumerate(samples, 1):
        print(f"[{i}/{len(samples)}] {s['label']} | {s['검색어']} ({s['본문길이']:,}자)")
        print(f"  product_name: {s['product_name']}")

        r = evaluate(s)
        results.append({**s, **r})

        status = "[OK]" if r["success"] else "[FAIL]"
        print(f"  {status} 소요시간: {r['elapsed']:.1f}초")
        print(f"  review_keywords ({r['review_count']}개): {', '.join(r['review_keywords'])}")
        print(f"  hin_keywords    ({r['hin_count']}개): {', '.join(r['hin_keywords'])}")
        print()

    # 요약
    print("=" * 70)
    print("요약")
    print("=" * 70)
    success_n = sum(1 for r in results if r["success"])
    times = [r["elapsed"] for r in results]
    print(f"성공률: {success_n}/{len(results)}")
    print(f"평균 소요시간: {sum(times)/len(times):.1f}초")
    print(f"최소/최대: {min(times):.1f}초 / {max(times):.1f}초")
    print()
    print(f"{'구간':<12} {'검색어':<35} {'본문길이':>8}  {'시간':>6}  review  hin")
    print("-" * 80)
    for r in results:
        print(
            f"{r['label']:<12} {r['검색어'][:33]:<35} {r['본문길이']:>8,}"
            f"  {r['elapsed']:>5.1f}s  {r['review_count']:>4}개  {r['hin_count']:>3}개"
        )


if __name__ == "__main__":
    main()
