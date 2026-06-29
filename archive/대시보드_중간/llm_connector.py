"""
llm_connector.py
LLM 추상화 레이어.

[현재 활성] 로컬 Ollama (gemma4:e4b)
[비활성/주석] DeepSeek API / 외부 Gemma ngrok 서버

환경변수 (.env):
  LLM_MODE     = local | remote
  OLLAMA_URL   = http://localhost:11434/api/generate
  OLLAMA_MODEL = gemma4:e4b

  # remote 모드 시 (ngrok 노출 후)
  REMOTE_LLM_URL = https://xxxx.ngrok-free.app
  REMOTE_LLM_KEY = MAPISODE_SECRET_2026
"""

import json
import os
import re

import requests
from dotenv import load_dotenv

load_dotenv()

# ══════════════════════════════════════════════════════════════════════
# 설정
# ══════════════════════════════════════════════════════════════════════
LLM_MODE       = os.environ.get("LLM_MODE", "local")
OLLAMA_URL     = os.environ.get("OLLAMA_URL", "http://localhost:11434/api/generate")
OLLAMA_MODEL   = os.environ.get("OLLAMA_MODEL", "gemma4:e4b")
REMOTE_LLM_URL = os.environ.get("REMOTE_LLM_URL", "")
REMOTE_LLM_KEY = os.environ.get("REMOTE_LLM_KEY", "")
TIMEOUT_SEC    = int(os.environ.get("TIMEOUT_SEC", "120"))


# ══════════════════════════════════════════════════════════════════════
# 공개 인터페이스
# ══════════════════════════════════════════════════════════════════════

def get_current_mode() -> str:
    return LLM_MODE


def infer_trend_attributes(trend: str, all_attrs: list[str], top_n: int = 5) -> list[dict]:
    """
    트렌드 키워드를 받아 관련 속성 목록을 반환.

    Returns:
        [{"attribute": "쫀득한 식감", "score": 0.92}, ...]
        실패 시 빈 리스트
    """
    attr_list = ", ".join(f'"{a}"' for a in all_attrs)
    prompt = (
        f'트렌드: "{trend}"\n'
        f"사용 가능한 속성 목록: [{attr_list}]\n\n"
        f"위 속성 목록 중 이 트렌드와 가장 관련성 높은 속성 {top_n}개를 골라 "
        f"아래 형식의 JSON 배열로만 응답하세요. 다른 텍스트는 절대 포함하지 마세요:\n"
        f'[{{"attribute": "속성명", "score": 0.0~1.0}}, ...]'
    )

    if LLM_MODE == "remote":
        raw = _call_remote(prompt)
    else:
        raw = _call_local(prompt)

    return _parse_attr_json(raw, top_n)


# ══════════════════════════════════════════════════════════════════════
# LLM 호출
# ══════════════════════════════════════════════════════════════════════

def _call_local(prompt: str) -> str:
    """로컬 Ollama 호출."""
    resp = requests.post(
        OLLAMA_URL,
        json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
        timeout=TIMEOUT_SEC,
    )
    resp.raise_for_status()
    return resp.json().get("response", "")


def _call_remote(prompt: str) -> str:
    """ngrok으로 노출된 외부 Ollama/Gemma 서버 호출."""
    resp = requests.post(
        f"{REMOTE_LLM_URL}/generate",
        json={"prompt": prompt},
        headers={"Authorization": f"Bearer {REMOTE_LLM_KEY}"},
        timeout=TIMEOUT_SEC,
    )
    resp.raise_for_status()
    return resp.json().get("response", "")


# ══════════════════════════════════════════════════════════════════════
# 공통 유틸
# ══════════════════════════════════════════════════════════════════════

def _parse_attr_json(raw: str, top_n: int) -> list[dict]:
    """LLM 응답에서 JSON 배열을 추출하여 파싱."""
    match = re.search(r'\[.*?\]', raw, re.DOTALL)
    if not match:
        return []
    try:
        items = json.loads(match.group())
        return [
            {"attribute": str(i["attribute"]), "score": float(i.get("score", 0.5))}
            for i in items
            if "attribute" in i
        ][:top_n]
    except Exception:
        return []
