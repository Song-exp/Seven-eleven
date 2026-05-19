"""
llm_connector.py
───────────────────────────────────────────────────────────────────────
Gemma 2 속성 추론 모듈 — 이중 LLM 모드 지원.

[모드 선택] 환경변수 LLM_MODE로 제어:
  local  (기본값) : 로컬 Ollama (http://localhost:11434) 직접 호출
  remote          : 외부 FastAPI Gemma 서버 호출 (URL + API Key 필요)

[환경변수 목록]
  LLM_MODE        = "local" | "remote"          (기본: local)
  OLLAMA_URL      = http://localhost:11434       (로컬 모드 전용)
  OLLAMA_MODEL    = gemma2                       (로컬 모드 전용)
  REMOTE_LLM_URL  = https://xxxx.ngrok-free.app (원격 모드 전용)
  REMOTE_LLM_KEY  = Bearer 토큰                 (원격 모드 전용)

[.env 파일 예시]
  LLM_MODE=local
  # LLM_MODE=remote
  # REMOTE_LLM_URL=https://xxxx.ngrok-free.app
  # REMOTE_LLM_KEY=MAPISODE_SECRET_2026
"""

import json
import os
import requests
from typing import Optional

# ── 환경변수 로딩 (.env 지원) ───────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass   # python-dotenv 미설치 시 무시

LLM_MODE: str       = os.environ.get("LLM_MODE", "local").lower()
OLLAMA_URL: str     = os.environ.get("OLLAMA_URL", "http://localhost:11434/api/generate")
OLLAMA_MODEL: str   = os.environ.get("OLLAMA_MODEL", "gemma2")
REMOTE_LLM_URL: str = os.environ.get("REMOTE_LLM_URL", "")
REMOTE_LLM_KEY: str = os.environ.get("REMOTE_LLM_KEY", "")

TIMEOUT_SEC: int = 120   # Gemma 추론 최대 대기 시간

PROMPT_TEMPLATE = """당신은 편의점 상품 분석 전문가입니다.
다음은 편의점 상품 네트워크의 속성 노드 목록입니다:
{all_attributes}

트렌드 키워드 "{trend_text}"와 가장 관련성이 높은 속성을 위 목록에서 정확히 {top_n}개 선정하세요.

반드시 아래 JSON 배열 형식으로만 응답하세요. 다른 설명 텍스트는 포함하지 마세요:
[
  {{"attribute": "속성명1", "score": 0.95}},
  {{"attribute": "속성명2", "score": 0.88}}
]

score는 0.0~1.0 사이의 관련도 점수입니다. 반드시 위 목록에 있는 속성명만 사용하세요."""


# ═══════════════════════════════════════════════════════════════════════
# 공개 API
# ═══════════════════════════════════════════════════════════════════════

def infer_trend_attributes(
    trend_text: str,
    all_attributes: list[str],
    top_n: int = 5,
) -> list[dict]:
    """
    트렌드 텍스트와 관련된 속성 노드를 LLM으로 추론.

    Args:
        trend_text:     사용자 입력 트렌드 (예: "뇨끼 열풍")
        all_attributes: 네트워크 전체 속성 노드 label 목록
        top_n:          반환할 속성 수

    Returns:
        [{"attribute": str, "score": float}, ...]  (score 내림차순)
    """
    print(f"[llm_connector] 모드={LLM_MODE} | 트렌드='{trend_text}' | top_n={top_n}")

    prompt = PROMPT_TEMPLATE.format(
        all_attributes=", ".join(all_attributes),
        trend_text=trend_text,
        top_n=top_n,
    )

    if LLM_MODE == "remote":
        raw = _call_remote(prompt)
    else:
        raw = _call_local(prompt)

    if raw is None:
        print("[llm_connector] LLM 호출 실패 → 폴백 사용")
        return _fallback_infer(trend_text, all_attributes, top_n)

    return _parse_and_validate(raw, set(all_attributes), top_n)


def get_current_mode() -> dict:
    """현재 LLM 연결 모드 정보 반환 (디버깅/헬스체크용)."""
    return {
        "mode":     LLM_MODE,
        "endpoint": OLLAMA_URL if LLM_MODE == "local" else REMOTE_LLM_URL,
        "model":    OLLAMA_MODEL if LLM_MODE == "local" else "remote",
    }


# ═══════════════════════════════════════════════════════════════════════
# 로컬 Ollama 호출
# ═══════════════════════════════════════════════════════════════════════

def _call_local(prompt: str) -> Optional[str]:
    """Ollama REST API에 직접 요청."""
    payload = {
        "model":  OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
    }
    try:
        resp = requests.post(OLLAMA_URL, json=payload, timeout=TIMEOUT_SEC)
        resp.raise_for_status()
        return resp.json().get("response", "")
    except requests.ConnectionError:
        print(f"[llm_connector] Ollama 연결 실패 ({OLLAMA_URL}). Ollama가 실행 중인지 확인하세요.")
        return None
    except Exception as e:
        print(f"[llm_connector] 로컬 호출 에러: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════
# 외부 API 호출 (시연용 서버)
# ═══════════════════════════════════════════════════════════════════════

def _call_remote(prompt: str) -> Optional[str]:
    """
    외부 FastAPI Gemma 서버에 요청.
    서버는 아래 인터페이스를 따른다:
      POST {REMOTE_LLM_URL}/generate
      Authorization: Bearer {REMOTE_LLM_KEY}
      Body: {"prompt": str}
      Response: {"response": str}
    """
    if not REMOTE_LLM_URL:
        print("[llm_connector] REMOTE_LLM_URL 미설정. .env 또는 환경변수를 확인하세요.")
        return None

    url     = REMOTE_LLM_URL.rstrip("/") + "/generate"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {REMOTE_LLM_KEY}",
    }
    payload = {"prompt": prompt}

    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=TIMEOUT_SEC)
        if resp.status_code == 401:
            print("[llm_connector] 인증 실패 (401). REMOTE_LLM_KEY를 확인하세요.")
            return None
        resp.raise_for_status()
        return resp.json().get("response", "")
    except requests.ConnectionError:
        print(f"[llm_connector] 원격 서버 연결 실패 ({url}).")
        return None
    except Exception as e:
        print(f"[llm_connector] 원격 호출 에러: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════
# 응답 파싱 및 검증
# ═══════════════════════════════════════════════════════════════════════

def _parse_and_validate(raw: str, attr_set: set, top_n: int) -> list[dict]:
    """LLM 응답 문자열을 파싱하고 네트워크 속성과 교차검증."""
    # JSON 블록 추출 (```json ... ``` 또는 [ ... ] 형태 모두 처리)
    raw = raw.strip()
    if "```" in raw:
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()

    # [ ... ] 시작 부분만 추출
    start = raw.find("[")
    end   = raw.rfind("]")
    if start != -1 and end != -1:
        raw = raw[start:end+1]

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        print(f"[llm_connector] JSON 파싱 실패: {raw[:200]}")
        return []

    if not isinstance(parsed, list):
        parsed = [parsed]

    valid = []
    for item in parsed:
        attr  = item.get("attribute", "").strip()
        score = float(item.get("score", 0.5))
        if attr in attr_set:
            valid.append({"attribute": attr, "score": round(score, 4)})

    valid.sort(key=lambda x: -x["score"])
    return valid[:top_n]


# ═══════════════════════════════════════════════════════════════════════
# 폴백: 키워드 부분 매칭
# ═══════════════════════════════════════════════════════════════════════

def _fallback_infer(trend_text: str, all_attributes: list[str], top_n: int) -> list[dict]:
    """
    LLM 연결 실패 시 폴백.
    트렌드 텍스트와 글자가 겹치는 속성을 단순 매칭으로 반환.
    시연 중 LLM이 응답하지 않을 때 최소한의 결과를 보장.
    """
    keywords = [ch for ch in trend_text if ch.strip() and ch not in " ,·×"]
    matched = []
    for attr in all_attributes:
        overlap = sum(1 for kw in keywords if kw in attr)
        if overlap > 0:
            matched.append({"attribute": attr, "score": round(overlap / len(keywords), 4)})

    matched.sort(key=lambda x: -x["score"])
    return matched[:top_n]


# ── 단독 실행 시 연결 테스트 ──────────────────────────────────────────
if __name__ == "__main__":
    print("[연결 모드]", get_current_mode())
    test_attrs = ["고소함", "달콤함", "쫀득한 식감", "바삭함", "제로슈가", "이탈리안", "프리미엄"]
    result = infer_trend_attributes("뇨끼 열풍", test_attrs, top_n=3)
    print("[추론 결과]", result)
