"""
trend_cache_builder.py
───────────────────────────────────────────────────────────────────────
시연용 트렌드 사전 캐시 빌더.

Ollama(Gemma 2) 또는 외부 LLM API를 사용하여 지정된 트렌드 키워드의
속성 매핑 결과를 미리 생성하고 data/trend_cache.json에 저장한다.

서버 시작 시 이 파일을 로딩하면 해당 트렌드에 대해
LLM 호출 없이 즉시 응답할 수 있다.

사용법:
  python trend_cache_builder.py
  python trend_cache_builder.py --trends "뇨끼 열풍" "헬시플레저" --top_n 5
"""

import argparse
import json
import os
import sys

# ── 경로 설정 ──────────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(__file__))

from llm_connector import infer_trend_attributes, get_current_mode
from network_builder import load_graph, get_all_attributes

DATA_DIR       = os.path.join(os.path.dirname(__file__), "data")
CACHE_PATH     = os.path.join(DATA_DIR, "trend_cache.json")
os.makedirs(DATA_DIR, exist_ok=True)

# ── 기본 캐싱 트렌드 목록 (추후 키워드 지정 후 업데이트) ───────────────
DEFAULT_TRENDS: list[str] = [
    # 추후 지정 예정
    # "뇨끼 열풍",
    # "헬시플레저",
    # "두바이 초콜릿",
]


def build_cache(trends: list[str], top_n: int = 5, overwrite: bool = False) -> None:
    """
    트렌드 목록에 대해 LLM 추론을 실행하고 캐시 파일을 생성/업데이트.

    Args:
        trends:    캐싱할 트렌드 키워드 목록
        top_n:     각 트렌드당 추출할 속성 수
        overwrite: True이면 기존 캐시 전체 덮어쓰기, False이면 미존재 항목만 추가
    """
    if not trends:
        print("[trend_cache_builder] 캐싱할 트렌드가 지정되지 않았습니다.")
        print("  DEFAULT_TRENDS 목록을 채우거나 --trends 인수를 사용하세요.")
        return

    # 그래프에서 전체 속성 노드 목록 로딩
    print("[trend_cache_builder] 그래프 로딩 중...")
    result = load_graph()
    G = result[0] if isinstance(result, tuple) else result
    all_attrs = get_all_attributes(G)

    if not all_attrs:
        print("[trend_cache_builder] 속성 노드가 없습니다. 네트워크 데이터를 먼저 확인하세요.")
        return

    print(f"  전체 속성 노드 수: {len(all_attrs)}")
    print(f"  LLM 모드: {get_current_mode()}")

    # 기존 캐시 로딩
    if not overwrite and os.path.exists(CACHE_PATH):
        with open(CACHE_PATH, encoding="utf-8") as f:
            cache = json.load(f)
        existing = {item["trend"] for item in cache.get("cached_trends", [])}
        print(f"  기존 캐시: {len(existing)}개 트렌드")
    else:
        cache = {"cached_trends": []}
        existing = set()

    # 신규 트렌드만 추론
    new_count = 0
    for trend in trends:
        if trend in existing and not overwrite:
            print(f"  [SKIP] '{trend}' — 이미 캐시됨")
            continue

        print(f"  [추론] '{trend}' ...")
        attrs = infer_trend_attributes(trend, all_attrs, top_n=top_n)

        if not attrs:
            print(f"  [경고] '{trend}' 추론 결과 없음, 건너뜁니다.")
            continue

        # 기존 항목 제거 후 추가 (overwrite 시)
        cache["cached_trends"] = [
            item for item in cache["cached_trends"] if item["trend"] != trend
        ]
        cache["cached_trends"].append({
            "trend": trend,
            "attrs": attrs,
        })
        new_count += 1
        print(f"    → {[a['attribute'] for a in attrs]}")

    # 저장
    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

    print(f"\n[완료] {new_count}개 트렌드 추가 → {CACHE_PATH}")
    print(f"  전체 캐시: {len(cache['cached_trends'])}개 트렌드")


def load_cache() -> dict:
    """캐시 파일 로딩. 파일 없으면 빈 캐시 반환."""
    if not os.path.exists(CACHE_PATH):
        return {"cached_trends": []}
    with open(CACHE_PATH, encoding="utf-8") as f:
        return json.load(f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="트렌드 속성 사전 캐시 빌더")
    parser.add_argument(
        "--trends", nargs="*", default=None,
        help="캐싱할 트렌드 키워드 목록 (미지정 시 DEFAULT_TRENDS 사용)"
    )
    parser.add_argument("--top_n",    type=int,  default=5,     help="속성 추출 수 (기본 5)")
    parser.add_argument("--overwrite", action="store_true",     help="기존 캐시 전체 덮어쓰기")
    args = parser.parse_args()

    trends = args.trends if args.trends is not None else DEFAULT_TRENDS
    build_cache(trends, top_n=args.top_n, overwrite=args.overwrite)
