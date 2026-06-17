"""대시보드 라이브 백엔드 (FastAPI) — serve.py 함수를 HTTP 엔드포인트로 노출.

개편: 단일 화면 — 트렌드 추론(/infer) + 선택 속성(최대 3)의 1-hop 네트워크(/network).
미등록 트렌드 추론은 serve 내부에서 Gemma(Ollama) 호출.

실행: python -m uvicorn src.eval.api:app --port 8000
"""
from typing import List, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from src.eval import serve

app = FastAPI(title="7-Eleven NPD Dashboard API")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)


class InferReq(BaseModel):
    trend: str


class NetReq(BaseModel):
    trend: str = ""
    attrs: Optional[List[str]] = None     # 선택된 출발 속성 (최대 3); 없으면 추론 상위


@app.get("/")
def health():
    return {"status": "ok", "serving_exp": serve.SERVING_EXP}


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
