"""키워드 추출 서비스 — 현호 keyword_api_server.py의 변환 로직을 FastAPI(api.py)에서 재사용하도록 이식.

별도 http.server(:8010) 폐기 → api.py가 /extract-one·/extract-batch 로 단일호스트 서빙.
파이프라인(Kiwi)은 지연 로드 싱글톤. extract 결과를 대시보드 시트2용 row 스키마로 변환.
"""
from __future__ import annotations

from functools import lru_cache
from typing import Any

from .pipeline import MDKeywordPipeline


@lru_cache(maxsize=1)
def pipeline() -> MDKeywordPipeline:
    return MDKeywordPipeline()


def _norm_id(value: Any) -> str:
    try:
        return str(int(float(value)))
    except (TypeError, ValueError):
        return str(value or "").strip()


def _method_label(method: str) -> str:
    return {
        "event_normalization": "행사 표현 정리",
        "substring_original": "원문 직접 발견",
        "substring_no_space": "공백 제거 후 발견",
        "token_exact": "단어 단위 일치",
        "short_keyword_exact": "짧은 키워드 발견",
        "safe_contains": "맛/향 표현 정리",
        "curated_mapping": "사전 변환 기준 매칭",
        "sensory_normalization": "감각 표현 정리",
        "attribute_normalization": "상품 형태 표현 정리",
        "fuzzy": "오타 보정",
        "compound_token": "복합 단어 분리",
    }.get(method, method)


def _display_detail(method: str, detail: str, is_existing: bool = True) -> str:
    """파이프라인 원본 근거를 화면용 고정 표현으로 바꾼다."""
    if method == "candidate" and "식감/질감" in detail:
        return "식감/질감 표현을 키워드형으로 정리"
    if method == "candidate" and "상품 형태" in detail:
        return "상품 형태 표현을 키워드형으로 정리"
    if not is_existing and method == "token_exact":
        return "매핑 기준 키워드와 단어 단위 일치"
    if not is_existing and method == "curated_mapping":
        return "변환표 기준 신규 키워드로 정리"
    return {
        "event_normalization": "행사/시즌 표현을 등록 키워드로 정리",
        "substring_original": "상품 설명문에 그대로 등장",
        "substring_no_space": "공백 제거 시 등록 키워드와 같음",
        "token_exact": "등록 키워드와 단어 단위 일치",
        "short_keyword_exact": "짧은 등록 키워드가 독립 표현으로 등장",
        "safe_contains": "맛/향 표현에서 핵심 키워드 발견",
        "curated_mapping": "변환표 기준 키워드 변환",
        "sensory_normalization": "식감/질감 표현을 키워드형으로 정리",
        "attribute_normalization": "상품 형태 표현을 키워드형으로 정리",
        "fuzzy": "등록 키워드 기준 오타 보정",
        "compound_token": "복합 단어 안에서 등록 키워드 발견",
        "candidate": "미등록 신규 표현",
    }.get(method, detail)


def _keyword_row_from_evidence(ev: dict[str, Any], network_set: set[str]) -> dict[str, Any]:
    keyword = str(ev.get("network_keyword", ""))
    method = str(ev.get("match_method", ""))
    is_existing = keyword in network_set
    return {
        "keyword": keyword,
        "keywordType": "기존 키워드" if is_existing else "신규 키워드",
        "selected": True,
        "inputTerm": str(ev.get("input_term", "")),
        "method": method,
        "methodLabel": _method_label(method),
        "detail": _display_detail(method, str(ev.get("detail", "")), is_existing),
    }


def _keyword_row_from_candidate(ev: dict[str, Any]) -> dict[str, Any]:
    return {
        "keyword": str(ev.get("candidate_keyword", "")),
        "keywordType": "신규 키워드",
        "selected": True,
        "inputTerm": str(ev.get("source_term", "")),
        "method": "candidate",
        "methodLabel": "신규 키워드",
        "detail": _display_detail("candidate", str(ev.get("reason", ""))),
    }


def build_review_row(item_cd: Any, item_nm: Any, description: Any) -> dict[str, Any]:
    pipe = pipeline()
    result = pipe.extract(str(description or ""))
    network_set = pipe.network_keyword_set

    keywords: list[dict[str, Any]] = []
    seen: set[str] = set()
    for ev in result.get("match_evidence", []):
        row = _keyword_row_from_evidence(ev, network_set)
        if row["keyword"] and row["keyword"] not in seen:
            seen.add(row["keyword"])
            keywords.append(row)
    for ev in result.get("candidate_details", []):
        row = _keyword_row_from_candidate(ev)
        if row["keyword"] and row["keyword"] not in seen:
            seen.add(row["keyword"])
            keywords.append(row)

    return {
        "id": "",
        "itemCode": _norm_id(item_cd),
        "itemName": str(item_nm or "").strip(),
        "sourceText": str(description or "").strip(),
        "memo": "",
        "keywords": keywords,
        "raw": result,
    }


def _pick(row: dict[str, Any], names: list[str]) -> Any:
    lower_map = {str(k).lower(): k for k in row}
    for name in names:
        if name in row:
            return row[name]
        key = lower_map.get(name.lower())
        if key is not None:
            return row[key]
    return ""


def build_review_row_from_csv_row(row: dict[str, Any], fallback_code: int) -> dict[str, Any]:
    item_cd = _pick(row, ["상품코드", "ITEM_CD", "item_cd", "Item_CD", "코드"]) or fallback_code
    item_nm = _pick(row, ["상품명", "ITEM_NM", "item_nm", "Item_NM", "제품명"])
    description = _pick(
        row,
        ["상품설명", "상품 설명문", "설명문", "원문설명", "NP_INFO", "np_info", "description_text", "DESCRIPTION"],
    )
    if not str(description or "").strip():
        description = " ".join(
            str(part).strip()
            for part in (
                _pick(row, ["INTRO_BGR_CN", "intro_bgr_cn"]),
                _pick(row, ["ITEM_CRTR_CN", "item_crtr_cn"]),
            )
            if str(part or "").strip()
        )
    return build_review_row(item_cd, item_nm, description)
