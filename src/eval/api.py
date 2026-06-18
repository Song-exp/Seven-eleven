"""대시보드 라이브 백엔드 (FastAPI) — serve.py 함수를 HTTP 엔드포인트로 노출.

개편: 단일 화면 — 트렌드 추론(/infer) + 선택 속성(최대 3)의 1-hop 네트워크(/network).
미등록 트렌드 추론은 serve 내부에서 Gemma(Ollama) 호출.

실행: python -m uvicorn src.eval.api:app --port 8000
"""
import os
import threading
from typing import List, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel

from src.eval import serve

_DASHBOARD_DIR = os.path.join(os.getcwd(), "Dashboard")

app = FastAPI(title="7-Eleven NPD Dashboard API")


@app.on_event("startup")
async def _warmup():
    """백엔드 시작 시 Gemma 콜드 로드를 백그라운드에서 미리 실행."""
    def _do():
        try:
            serve._ollama("안녕", temperature=0.0, timeout=300)
            print("[warmup] Gemma 로드 완료")
        except Exception as e:
            print(f"[warmup] 실패: {e}")
    threading.Thread(target=_do, daemon=True).start()
    print("[warmup] Gemma 백그라운드 로드 시작…")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)


class InferReq(BaseModel):
    trend: str


class NetReq(BaseModel):
    trend: str = ""
    attrs: Optional[List[str]] = None     # 선택된 출발 속성 (최대 3); 없으면 추론 상위


@app.get("/")
def dashboard():
    return FileResponse(os.path.join(_DASHBOARD_DIR, "dashboard.html"))

@app.get("/config.js")
def config_js():
    return FileResponse(os.path.join(_DASHBOARD_DIR, "config.js"), media_type="application/javascript")

@app.get("/health")
def health():
    return {"status": "ok", "serving_exp": serve.SERVING_EXP}


@app.get("/infer")
def infer_get():
    from fastapi import Response
    return Response(content='{"detail":"POST로 요청하세요: {trend: string}"}',
                    media_type="application/json", status_code=405)


@app.post("/infer")
def infer(req: InferReq):
    """검색어 → 네트워크 출발점 후보 키워드. 기지 트렌드 조회 → 임의 입력 Gemma+매칭."""
    return {"trend": req.trend, "attrs": serve.infer_attrs(req.trend)}


@app.post("/network")
def network(req: NetReq):
    """선택된 출발 속성(최대 3)별 1-hop 네트워크 + 교집합 병합 종합 네트워크 + 설명."""
    attrs = req.attrs if req.attrs else serve.infer_attrs(req.trend)[:3]
    net = serve.attr_network(attrs, trend=req.trend or "")
    return {"net": net, "explain": serve.explain_attr_network(net)}
