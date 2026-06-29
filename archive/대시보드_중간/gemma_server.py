"""
gemma_server.py
───────────────────────────────────────────────────────────────────────
데스크탑용 Gemma API 서버 — 노트북의 FastAPI 백엔드(server.py)로부터
LLM 추론 요청을 받아 로컬 Ollama를 호출하고 결과를 반환한다.

실행:
  uvicorn gemma_server:app --host 0.0.0.0 --port 9000

엔드포인트:
  GET  /health          — 서버·Ollama 상태 확인
  POST /generate        — 프롬프트 → Gemma 추론 결과 반환

인증:
  Authorization: Bearer {GEMMA_SERVER_KEY}
  환경변수 GEMMA_SERVER_KEY 미설정 시 인증 우회 (개발용)

환경변수 (.env 또는 OS):
  OLLAMA_URL        = http://localhost:11434/api/generate
  OLLAMA_MODEL      = gemma4:e4b
  GEMMA_SERVER_KEY  = (인증 키, 미설정 시 DEV_KEY)
  TIMEOUT_SEC       = 120
"""

import os
import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# ── 설정 ───────────────────────────────────────────────────────────────
OLLAMA_URL       = os.environ.get("OLLAMA_URL",       "http://localhost:11434/api/generate")
OLLAMA_MODEL     = os.environ.get("OLLAMA_MODEL",     "gemma4:e4b")
GEMMA_SERVER_KEY = os.environ.get("GEMMA_SERVER_KEY", "DEV_KEY")
TIMEOUT_SEC      = int(os.environ.get("TIMEOUT_SEC",  "120"))

# ── 앱 초기화 ──────────────────────────────────────────────────────────
app = FastAPI(title="Gemma API Server", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── 인증 미들웨어 ──────────────────────────────────────────────────────
@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    """DEV_KEY가 아닌 경우 Bearer 토큰 검증."""
    if GEMMA_SERVER_KEY != "DEV_KEY":
        if request.url.path not in ("/health", "/"):
            auth = request.headers.get("Authorization", "")
            if auth != f"Bearer {GEMMA_SERVER_KEY}":
                return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    return await call_next(request)


# ── 요청 모델 ──────────────────────────────────────────────────────────
class GenerateRequest(BaseModel):
    prompt: str


# ── 엔드포인트 ─────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {"status": "ok", "message": "Gemma API Server"}


@app.get("/health")
def health():
    """Ollama 연결 상태 포함 헬스체크."""
    ollama_ok = False
    try:
        # Ollama API 태그 엔드포인트로 연결 확인 (generate보다 빠름)
        base_url = OLLAMA_URL.replace("/api/generate", "")
        resp = requests.get(f"{base_url}/api/tags", timeout=5)
        ollama_ok = resp.status_code == 200
    except Exception:
        pass

    return {
        "status": "ok",
        "ollama": "connected" if ollama_ok else "unreachable",
        "model": OLLAMA_MODEL,
        "endpoint": OLLAMA_URL,
    }


@app.post("/generate")
def generate(req: GenerateRequest):
    """
    프롬프트를 Ollama Gemma 모델에 전달하고 응답을 반환.

    Request Body:
        {"prompt": str}

    Response:
        {"response": str}
    """
    if not req.prompt.strip():
        raise HTTPException(status_code=400, detail="prompt가 비어 있습니다.")

    payload = {
        "model":  OLLAMA_MODEL,
        "prompt": req.prompt,
        "stream": False,
    }

    try:
        resp = requests.post(OLLAMA_URL, json=payload, timeout=TIMEOUT_SEC)
        resp.raise_for_status()
        response_text = resp.json().get("response", "")
        return {"response": response_text}

    except requests.ConnectionError:
        raise HTTPException(
            status_code=503,
            detail=f"Ollama 연결 실패 ({OLLAMA_URL}). Ollama가 실행 중인지 확인하세요."
        )
    except requests.Timeout:
        raise HTTPException(
            status_code=504,
            detail=f"Ollama 응답 시간 초과 ({TIMEOUT_SEC}s). 모델 로딩 중일 수 있습니다."
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ollama 호출 오류: {str(e)}")


# ── 단독 실행 ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    print(f"[gemma_server] 모델: {OLLAMA_MODEL}")
    print(f"[gemma_server] Ollama URL: {OLLAMA_URL}")
    print(f"[gemma_server] 인증 모드: {'DEV (우회)' if GEMMA_SERVER_KEY == 'DEV_KEY' else '운영 (Bearer 검증)'}")
    uvicorn.run("gemma_server:app", host="0.0.0.0", port=9000, reload=False)
