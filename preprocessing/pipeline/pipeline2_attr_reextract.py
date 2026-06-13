"""
Pipeline 2: 재추출_속성 대상 제품에 대해
src/data_builder/keyword_extractor.py 의 Ollama 기반 추출 함수로
속성 키워드를 재추출하여 formatted_output을 업데이트합니다.

사전 조건:
    Ollama 로컬 서버가 실행 중이어야 합니다 (http://localhost:11434)
"""

import ast
import json
import sys

import pandas as pd

sys.path.insert(0, "src/data_builder")
from keyword_extractor import (
    extract_keywords_cu,
    extract_keywords_cu_v2,
    extract_keywords_gs25,
    extract_keywords_gs25_v2,
    extract_keywords_seveneleven,
    extract_keywords_seveneleven_v2,
)

sys.stdout.reconfigure(encoding="utf-8")

BASE = "data/processed/편의점_instagram"

# (경로, 브랜드, 단일/다중)
FILES = {
    "CU단일":   (f"{BASE}/단일/CU단일.xlsx",    "CU",   "단일"),
    "세븐단일": (f"{BASE}/단일/세븐단일.xlsx",  "세븐", "단일"),
    "GS25단일": (f"{BASE}/단일/GS25_단일.xlsx", "GS25", "단일"),
    "CU다중":   (f"{BASE}/다중/CU다중.xlsx",    "CU",   "다중"),
    "세븐다중": (f"{BASE}/다중/세븐다중.xlsx",  "세븐", "다중"),
    "GS25다중": (f"{BASE}/다중/GS25_다중.xlsx", "GS25", "다중"),
}

# 브랜드 × 단일/다중 → extractor 함수
EXTRACTORS = {
    ("CU",   "단일"): extract_keywords_cu,
    ("세븐", "단일"): extract_keywords_seveneleven,
    ("GS25", "단일"): extract_keywords_gs25,
    ("CU",   "다중"): extract_keywords_cu_v2,
    ("세븐", "다중"): extract_keywords_seveneleven_v2,
    ("GS25", "다중"): extract_keywords_gs25_v2,
}

ATTR_MIN_COUNT = 4   # 이 이상이어야 플래그 해제


# ─── 속성 합산 ────────────────────────────────────────────────────────────────

def _merge_fields(obj: dict) -> list[str]:
    """flavor_and_category / collab_and_brand / promotion_type / tpo_context 합산"""
    merged = []
    for field in ("flavor_and_category", "collab_and_brand", "promotion_type", "tpo_context"):
        for v in obj.get(field, []):
            if v:
                merged.append(str(v).strip())
    return list(dict.fromkeys(merged))  # 순서 유지 중복 제거


def extract_attrs_single(result: dict) -> list[str]:
    """단일(v1): 최상위 4개 필드에서 속성 합산"""
    return _merge_fields(result)


def extract_attrs_multi(result: dict, product_name: str) -> list[str]:
    """다중(v2): metadata 배열에서 제품명 매칭 후 해당 제품 속성 추출"""
    metadata = result.get("metadata", [])
    if not metadata:
        return []

    # 1순위: 정확 일치
    for item in metadata:
        if item.get("name", "") == product_name:
            return _merge_fields(item)

    # 2순위: 부분 포함
    for item in metadata:
        name = item.get("name", "")
        if product_name in name or name in product_name:
            return _merge_fields(item)

    # 3순위: 폴백 (단독 상품 게시글로 추정)
    return _merge_fields(metadata[0])


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
    """원본 포맷을 유지하면서 attrs만 교체"""
    s = str(original).strip()
    a = json.dumps(attrs, ensure_ascii=False)

    if s.startswith('{"['):    # 세븐단일: {"["name", p, "c"]": [attrs]}
        return f'{{"["{name}", {price}, "{capacity}"]": {a}}}'
    elif s.startswith('{['):   # CU단일 / GS25단일: {["name", p, "c"]: [attrs]}
        return f'{{["{name}", {price}, "{capacity}"]: {a}}}'
    else:                      # 다중 공통: ['name', p, 'c']: [attrs]
        return f"['{name}', {price}, '{capacity}']: {a}"


# ─── 메인 파이프라인 ──────────────────────────────────────────────────────────

def run(dry_run: bool = False):
    for file_key, (path, brand, kind) in FILES.items():
        df = pd.read_excel(path)
        flag_col = "재추출_속성"
        if flag_col not in df.columns:
            print(f"[{file_key}] 플래그 컬럼 없음, 건너뜀")
            continue

        extractor = EXTRACTORS[(brand, kind)]
        target_idx = df.index[df[flag_col] == "Y"].tolist()
        print(f"\n[{file_key}] 대상: {len(target_idx)}행  |  {extractor.__name__}")

        tried = success = skipped = 0

        for i in target_idx:
            row = df.loc[i]
            name, price, cap, _ = parse_formatted(row["formatted_output"])
            if name is None:
                skipped += 1
                continue

            title = str(row.get("title", "") or "")
            body  = str(row.get("body",  "") or "")

            if dry_run:
                print(f"  (dry) [{name}]")
                tried += 1
                continue

            try:
                result = extractor(title, body)
                if not result:
                    print(f"  [빈 결과] {name}")
                    skipped += 1
                    continue

                new_attrs = (
                    extract_attrs_single(result) if kind == "단일"
                    else extract_attrs_multi(result, name)
                )

                if not new_attrs:
                    print(f"  [속성 없음] {name}")
                    skipped += 1
                    continue

                df.at[i, "formatted_output"] = rebuild_formatted(
                    row["formatted_output"], name, price, cap, new_attrs
                )

                if len(new_attrs) >= ATTR_MIN_COUNT:
                    df.at[i, flag_col] = ""
                    success += 1
                # 4개 미만이면 플래그 유지 (재실행 시 재시도)

                print(f"  [{name}] {len(new_attrs)}개: {new_attrs}")
                tried += 1

            except Exception as e:
                print(f"  [오류] {name}: {e}")
                skipped += 1

        if not dry_run:
            df.to_excel(path, index=False)

        print(
            f"  → 시도 {tried} / 성공(4개↑) {success} / 건너뜀 {skipped}"
            + (" (dry_run)" if dry_run else " 저장완료")
        )


if __name__ == "__main__":
    run(dry_run=False)
