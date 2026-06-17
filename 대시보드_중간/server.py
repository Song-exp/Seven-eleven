"""
server.py
───────────────────────────────────────────────────────────────────────
7-Eleven NPD Dashboard — FastAPI 백엔드 서버.

실행:
  uvicorn server:app --host 0.0.0.0 --port 8000 --reload

환경변수 (.env 또는 OS):
  DASHBOARD_SECRET_KEY  = 대시보드 인증 키 (기본: DEV_KEY)
  LLM_MODE              = local | remote
  OLLAMA_URL, REMOTE_LLM_URL, REMOTE_LLM_KEY  (llm_connector.py 참조)
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel

from network_builder import (
    load_graph, get_meta, get_all_attributes,
    check_trend_node, inject_trend_node,
)
from graph_engine import propose_by_trend, get_bust_diagnosis
from llm_connector import infer_trend_attributes, get_current_mode
from trend_cache_builder import load_cache

# ── 설정 ───────────────────────────────────────────────────────────────
SECRET_KEY = os.environ.get("DASHBOARD_SECRET_KEY", "DEV_KEY")
DATA_DIR   = os.path.join(os.path.dirname(__file__), "data")

# ── 앱 초기화 ──────────────────────────────────────────────────────────
app = FastAPI(title="7-Eleven NPD Dashboard API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── 서버 시작 시 그래프 로딩 ───────────────────────────────────────────
print("[server] 그래프 로딩 중...")
_graph_result = load_graph()
G = _graph_result[0] if isinstance(_graph_result, tuple) else _graph_result

# 사전 캐시 트렌드 → 그래프에 영구 주입
_cache = load_cache()
for _item in _cache.get("cached_trends", []):
    inject_trend_node(G, _item["trend"], _item["attrs"])
print(f"[server] 캐시 트렌드 {len(_cache.get('cached_trends', []))}개 로딩 완료")
print(f"[server] {get_meta(G)}")
print(f"[server] LLM 모드: {get_current_mode()}")


# ══════════════════════════════════════════════════════════════════════
# 인증 미들웨어
# ══════════════════════════════════════════════════════════════════════

@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    """
    /api/* 엔드포인트에 대해 Authorization 헤더 검증.
    DEV_KEY 사용 시 개발 환경에서 인증 우회.
    """
    if request.url.path.startswith("/api"):
        if SECRET_KEY != "DEV_KEY":   # 운영 환경에서만 검증
            auth = request.headers.get("Authorization", "")
            if auth != f"Bearer {SECRET_KEY}":
                return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    return await call_next(request)


# ══════════════════════════════════════════════════════════════════════
# 요청 모델
# ══════════════════════════════════════════════════════════════════════

class TrendCheckRequest(BaseModel):
    trend: str

class TrendInferRequest(BaseModel):
    trend: str
    top_n: int = 5

class ProposeRequest(BaseModel):
    category:  str
    trend:     str
    hop_limit: int = 3
    top_k:     int = 3


# ══════════════════════════════════════════════════════════════════════
# 엔드포인트
# ══════════════════════════════════════════════════════════════════════

@app.get("/")
def root():
    return FileResponse(os.path.join(DATA_DIR, "..", "dashboard.html"))

@app.get("/config.js")
def get_config():
    return FileResponse(os.path.join(DATA_DIR, "..", "config.js"))


@app.get("/api/network/meta")
def api_network_meta():
    """그래프 메타 정보 반환."""
    return get_meta(G)


@app.get("/api/attributes")
def api_attributes():
    """전체 속성 노드 목록 반환 (LLM 프롬프트용)."""
    return {"attributes": get_all_attributes(G)}


@app.post("/api/trend/check")
def api_trend_check(req: TrendCheckRequest):
    """
    트렌드가 그래프에 존재하는지 확인.
    캐시 트렌드는 서버 시작 시 이미 주입되어 있으므로 즉시 hit.
    """
    node_id = check_trend_node(G, req.trend)
    if node_id:
        # 해당 트렌드 노드의 연결 속성 반환
        neighbors = list(G.neighbors(node_id))
        attrs = [
            {"attribute": G.nodes[n]["label"], "score": G[node_id][n].get("weight", 1.0)}
            for n in neighbors
            if G.nodes[n].get("type") == "attribute"
        ]
        attrs.sort(key=lambda x: -x["score"])
        return {"exists": True, "node_id": node_id, "attrs": attrs}
    return {"exists": False, "node_id": None, "attrs": []}


@app.post("/api/trend/infer")
def api_trend_infer(req: TrendInferRequest):
    """
    트렌드 속성 LLM 추론 → 그래프에 임시 주입 → 속성 반환.
    이미 존재하면 LLM 호출 없이 기존 연결 속성 반환.
    """
    # 이미 존재하면 재추론 없이 반환
    node_id = check_trend_node(G, req.trend)
    if node_id:
        neighbors = list(G.neighbors(node_id))
        attrs = [
            {"attribute": G.nodes[n]["label"], "score": G[node_id][n].get("weight", 1.0)}
            for n in neighbors
            if G.nodes[n].get("type") == "attribute"
        ]
        attrs.sort(key=lambda x: -x["score"])
        return {"trend": req.trend, "attrs": attrs, "source": "cache"}

    # LLM 추론
    all_attrs = get_all_attributes(G)
    inferred  = infer_trend_attributes(req.trend, all_attrs, top_n=req.top_n)

    if not inferred:
        raise HTTPException(status_code=500, detail="LLM 추론 결과 없음")

    # 그래프에 임시 주입
    inject_trend_node(G, req.trend, inferred)

    return {"trend": req.trend, "attrs": inferred, "source": "llm"}


@app.post("/api/propose")
def api_propose(req: ProposeRequest):
    """
    카테고리 × 트렌드 → 신제품 속성 조합 반환.
    트렌드 노드가 없으면 자동으로 LLM 추론 후 진행.
    """
    # 트렌드 노드 확인 (없으면 자동 추론)
    node_id = check_trend_node(G, req.trend)
    if not node_id:
        all_attrs = get_all_attributes(G)
        inferred  = infer_trend_attributes(req.trend, all_attrs, top_n=5)
        if not inferred:
            raise HTTPException(status_code=500, detail="트렌드 속성 추론 실패")
        node_id = inject_trend_node(G, req.trend, inferred)

    proposals = propose_by_trend(
        G,
        trend_node_id=node_id,
        category=req.category,
        hop_limit=req.hop_limit,
        top_k=req.top_k,
    )

    if not proposals:
        return {
            "trend": req.trend, "category": req.category,
            "proposals": [], "message": "해당 카테고리와 트렌드 조합의 경로를 찾을 수 없습니다."
        }

    return {"trend": req.trend, "category": req.category, "proposals": proposals}


@app.get("/api/bust")
def api_bust():
    """부진 상품 목록 반환 (is_survived=False인 product 노드)."""
    bust_items = [
        {
            "item_cd":  n,
            "item_name": G.nodes[n].get("label", n),
            "category":  G.nodes[n].get("category", ""),
            "price":     G.nodes[n].get("price"),
        }
        for n in G.nodes
        if G.nodes[n].get("type") == "product"
        and G.nodes[n].get("survived") is False
    ]
    return {"items": bust_items, "count": len(bust_items)}


@app.get("/api/bust/{item_cd}")
def api_bust_detail(item_cd: str):
    """부진 상품 상세 진단 반환."""
    diagnosis = get_bust_diagnosis(G, item_cd)
    if not diagnosis:
        raise HTTPException(status_code=404, detail=f"상품 '{item_cd}'을 찾을 수 없습니다.")
    return diagnosis


@app.get("/api/health")
def api_health():
    """헬스체크 + LLM 모드 확인."""
    return {
        "status":   "ok",
        "graph":    get_meta(G),
        "llm_mode": get_current_mode(),
        "cache_trends": [
            item["trend"] for item in _cache.get("cached_trends", [])
        ],
    }
