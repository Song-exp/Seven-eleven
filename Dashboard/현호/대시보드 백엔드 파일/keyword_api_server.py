"""시트2 네트워크 업데이트용 로컬 API.

브라우저 화면은 Python/Kiwi를 직접 실행할 수 없으므로, 이 서버가
keyword_extraction_pipeline.py를 그대로 호출해 결과를 JSON으로 돌려준다.
"""
from __future__ import annotations

import json
from functools import lru_cache
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse

from keyword_extraction_pipeline import MDKeywordPipeline


HOST = "127.0.0.1"
PORT = 8010


@lru_cache(maxsize=1)
def pipeline() -> MDKeywordPipeline:
    return MDKeywordPipeline()


def _norm_id(value: Any) -> str:
    try:
        return str(int(float(value)))
    except (TypeError, ValueError):
        return str(value or "").strip()


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


def build_review_row(item_cd: Any, item_nm: Any, description: Any) -> dict[str, Any]:
    pipe = pipeline()
    result = pipe.extract(str(description or ""))
    network_set = pipe.network_keyword_set

    keywords: list[dict[str, Any]] = []
    seen: set[str] = set()

    for ev in result.get("match_evidence", []):
        row = _keyword_row_from_evidence(ev, network_set)
        keyword = row["keyword"]
        if keyword and keyword not in seen:
            seen.add(keyword)
            keywords.append(row)

    for ev in result.get("candidate_details", []):
        row = _keyword_row_from_candidate(ev)
        keyword = row["keyword"]
        if keyword and keyword not in seen:
            seen.add(keyword)
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


class Handler(BaseHTTPRequestHandler):
    server_version = "SevenElevenSheet2API/1.0"

    def do_OPTIONS(self) -> None:
        self._send_empty(204)

    def do_GET(self) -> None:
        path = urlparse(self.path).path
        if path == "/health":
            data = {"ok": True, "message": "시트2 키워드 API가 실행 중입니다."}
            self._send_json(data)
            return
        self._send_json({"ok": False, "error": "지원하지 않는 경로입니다."}, 404)

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        try:
            payload = self._read_json()
            if path == "/extract-one":
                row = build_review_row(
                    payload.get("itemCode") or payload.get("상품코드"),
                    payload.get("itemName") or payload.get("상품명"),
                    payload.get("sourceText") or payload.get("description") or payload.get("상품설명"),
                )
                self._send_json({"ok": True, "row": row})
                return

            if path == "/extract-batch":
                rows = payload.get("rows") or []
                out = [
                    build_review_row_from_csv_row(row, idx + 1)
                    for idx, row in enumerate(rows)
                    if isinstance(row, dict)
                ]
                out = [row for row in out if row["sourceText"]]
                self._send_json({"ok": True, "rows": out})
                return

            self._send_json({"ok": False, "error": "지원하지 않는 경로입니다."}, 404)
        except Exception as exc:
            self._send_json({"ok": False, "error": str(exc)}, 500)

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length).decode("utf-8") if length else "{}"
        return json.loads(raw or "{}")

    def _send_empty(self, status: int) -> None:
        self.send_response(status)
        self._send_headers()
        self.end_headers()

    def _send_json(self, data: dict[str, Any], status: int = 200) -> None:
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self._send_headers()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def log_message(self, fmt: str, *args: Any) -> None:
        print("[%s] %s" % (self.log_date_time_string(), fmt % args))


def main() -> None:
    pipeline()
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"시트2 키워드 API 실행 중: http://{HOST}:{PORT}")
    print("종료하려면 Ctrl+C를 누르세요.")
    server.serve_forever()


if __name__ == "__main__":
    main()
