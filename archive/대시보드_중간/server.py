"""
server.py
7-Eleven NPD Dashboard 메인 FastAPI 백엔드.

실행:
  uvicorn server:app --port 8000 --reload
"""

import os
import pandas as pd
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

from network_builder import (
    load_graph, get_all_attributes, get_meta,
    inject_trend_node, check_trend_node,
)
from graph_engine import propose_by_trend, get_bust_diagnosis, derive_from_product
from llm_connector import infer_trend_attributes, get_current_mode
from trend_cache_builder import load_cache

# ── 경로 설정 ──────────────────────────────────────────────────────────
_BASE        = os.path.dirname(__file__)
DATA_DIR     = os.path.join(_BASE, "data")
PRODUCT_PARQUET = os.path.join(DATA_DIR, "final_product_keywords.parquet")
PARETO_XLSX  = os.path.join(_BASE, "..", "eda", "파레토기반_카테고리확정.xlsx")

# ══════════════════════════════════════════════════════════════════════
# 앱 시작 시 1회 로딩 (그래프 + 상품 데이터)
# ══════════════════════════════════════════════════════════════════════
G        = None
prod_df  = None
CATEGORIES: list[str] = []   # 카테고리 드롭다운 목록 (교집합 68개)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global G, prod_df, CATEGORIES

    # ── 그래프 로딩 ──────────────────────────────────────────────────
    result  = load_graph()
    G       = result[0] if isinstance(result, tuple) else result
    prod_df = result[1] if isinstance(result, tuple) and len(result) > 1 else None

    # 모드 B(network.json) 로드 시 prod_df가 없으므로 parquet 별도 로딩
    if prod_df is None and os.path.exists(PRODUCT_PARQUET):
        prod_df = pd.read_parquet(PRODUCT_PARQUET)

    print(f"[server] 그래프 로딩 완료: {G.number_of_nodes()}노드 / {G.number_of_edges()}엣지")

    # ── 카테고리 목록 빌드 (파레토 xlsx ∩ 네트워크 parquet) ──────────
    net_cats = set(prod_df["중분류명"].dropna().unique()) if prod_df is not None else set()

    if os.path.exists(PARETO_XLSX):
        pareto_df  = pd.read_excel(PARETO_XLSX, sheet_name=0)
        pareto_cats = set(pareto_df["중분류명"].dropna().unique())
        CATEGORIES  = sorted(pareto_cats & net_cats)   # 교집합만 노출
        print(f"[server] 카테고리 로딩: 파레토 {len(pareto_cats)}개 ∩ 네트워크 {len(net_cats)}개 = {len(CATEGORIES)}개")
    else:
        CATEGORIES = sorted(net_cats)
        print(f"[server] 파레토 xlsx 없음 → 네트워크 카테고리 {len(CATEGORIES)}개 사용")

    yield


app = FastAPI(title="7-Eleven NPD API", version="1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ══════════════════════════════════════════════════════════════════════
# 요청 모델
# ══════════════════════════════════════════════════════════════════════

class TrendInferRequest(BaseModel):
    trend: str
    top_n: int = 5

class ProposeRequest(BaseModel):
    category:     str
    trend:        str
    item_cd:      str | None = None
    pinned_attrs: list[str] = []   # 선택한 키워드 노드 label
    pinned_ips:   list[str] = []   # 선택한 IP 노드 label
    hop_limit:    int = 3
    top_k:        int = 3

class DeriveRequest(BaseModel):
    item_cd: str
    top_k:   int = 4


# ══════════════════════════════════════════════════════════════════════
# 엔드포인트
# ══════════════════════════════════════════════════════════════════════

# ── 1. 헬스체크 ────────────────────────────────────────────────────────
@app.get("/api/health")
def health():
    # [활성] DeepSeek 모드
    return {
        "status":   "ok",
        "llm_mode": get_current_mode(),   # "deepseek"
    }

    # [비활성] 기존 Gemma 모드 (Ollama local / ngrok remote)
    # import os
    # return {
    #     "status":   "ok",
    #     "llm_mode": os.environ.get("LLM_MODE", "local"),
    #     "endpoint": os.environ.get("REMOTE_LLM_URL", "localhost"),
    # }


# ── 2. 카테고리 목록 ────────────────────────────────────────────────────
@app.get("/api/categories")
def categories():
    """파레토 확정 카테고리 ∩ 네트워크 보유 카테고리 목록 반환."""
    return {"categories": CATEGORIES}


# ── 3. 카테고리별 상품 목록 ─────────────────────────────────────────────
@app.get("/api/products")
def products(category: str = Query(..., description="중분류명")):
    """
    선택된 카테고리 내 상품 목록 반환.
    프론트 상품명 드롭다운 + 네트워크 노드 조회에 사용.
    """
    if prod_df is None:
        raise HTTPException(status_code=503, detail="상품 데이터가 로딩되지 않았습니다.")

    filtered = prod_df[prod_df["중분류명"] == category]
    if filtered.empty:
        return {"category": category, "products": []}

    def _safe(v):
        """NaN / numpy 스칼라 → Python 기본형으로 변환."""
        if v is None:
            return None
        try:
            if pd.isna(v):
                return None
        except (TypeError, ValueError):
            pass
        return v

    items = []
    for _, row in filtered.iterrows():
        kw = row.get("최종키워드")
        items.append({
            "item_cd":  str(row["상품코드"]),
            "item_nm":  row["상품명"],
            "survived": str(row.get("생존여부", "")).strip() == "생존",
            "price":    _safe(row.get("인스타가격")),
            "keywords": list(kw) if isinstance(kw, (list, tuple)) or hasattr(kw, "__iter__") and not isinstance(kw, str) and _safe(kw) is not None else [],
            "brand":    _safe(row.get("브랜드")),
        })

    return {"category": category, "products": items}


# ── 4. 트렌드 속성 추론 ─────────────────────────────────────────────────
@app.post("/api/trend/infer")
def trend_infer(req: TrendInferRequest):
    """
    트렌드 키워드 → LLM 속성 추론 → 그래프 트렌드 노드 주입.

    우선순위:
      1) 캐시(trend_cache.json) 히트
      2) 그래프 내 기존 트렌드 노드
      3) DeepSeek API 추론  ← [활성]
      (기존: Gemma Ollama / ngrok 추론)  ← [비활성]
    """
    # 1) 캐시 조회
    cache = load_cache()
    for item in cache.get("cached_trends", []):
        if item["trend"] == req.trend:
            return {"source": "cache", "attrs": item["attrs"]}

    # 2) 그래프 내 기존 트렌드 노드 조회
    existing_node = check_trend_node(G, req.trend)
    if existing_node:
        neighbors = [
            {
                "attribute": G.nodes[n]["label"],
                "score":     round(G[existing_node][n].get("weight", 1.0) / 10, 2),
            }
            for n in G.neighbors(existing_node)
            if G.nodes[n].get("type") == "attribute"
        ]
        if neighbors:
            return {
                "source": "graph",
                "attrs":  sorted(neighbors, key=lambda x: -x["score"])[:req.top_n],
            }

    # 3) [활성] DeepSeek API 추론
    all_attrs = get_all_attributes(G)
    attrs     = infer_trend_attributes(req.trend, all_attrs, top_n=req.top_n)

    # [비활성] 기존 Gemma 추론 (llm_connector의 _call_local / _call_remote 사용)
    # all_attrs = get_all_attributes(G)
    # attrs     = infer_trend_attributes(req.trend, all_attrs, top_n=req.top_n)
    # → 함수 시그니처는 동일하나, llm_connector 내부가 Ollama를 호출하던 버전

    if not attrs:
        raise HTTPException(
            status_code=502,
            detail="LLM 추론 결과가 없습니다. DEEPSEEK_API_KEY를 확인하세요.",
            # 기존 Gemma 버전 메시지:
            # detail="LLM 추론 실패. Ollama 실행 상태 또는 ngrok URL을 확인하세요.",
        )

    inject_trend_node(G, req.trend, attrs)
    return {"source": "llm", "attrs": attrs}


# ── 5. 신제품 조합 제안 ─────────────────────────────────────────────────
@app.post("/api/propose")
def propose(req: ProposeRequest):
    """
    카테고리 × 트렌드 → 그래프 BFS → 신제품 속성 조합 TOP-K 제안.
    /api/trend/infer를 먼저 호출하여 트렌드 노드를 주입한 후 사용.
    """
    trend_node_id = check_trend_node(G, req.trend)
    if not trend_node_id:
        raise HTTPException(
            status_code=400,
            detail=f"트렌드 '{req.trend}'를 먼저 /api/trend/infer로 추론하세요.",
        )

    proposals = propose_by_trend(
        G,
        trend_node_id,
        category=req.category,
        item_cd=req.item_cd,
        pinned_attrs=req.pinned_attrs,
        pinned_ips=req.pinned_ips,
        hop_limit=req.hop_limit,
        top_k=req.top_k,
    )

    if not proposals:
        return {
            "proposals": [],
            "message":   f"카테고리 '{req.category}'에서 조합을 찾지 못했습니다.",
        }

    return {"proposals": proposals}


# ── 6. 부진 상품 목록 ───────────────────────────────────────────────────
@app.get("/api/bust")
def bust_list():
    """생존여부 != '생존' 인 상품 목록 반환."""
    items = [
        {
            "item_cd":   n,
            "item_name": G.nodes[n].get("label", n),
            "category":  G.nodes[n].get("category", ""),
            "price":     G.nodes[n].get("price"),
        }
        for n in G.nodes
        if G.nodes[n].get("type") == "product"
        and G.nodes[n].get("survived") is False
    ]
    return {"items": items}


# ── 7. 부진 상품 개별 진단 ──────────────────────────────────────────────
@app.get("/api/bust/{item_cd}")
def bust_detail(item_cd: str):
    """부진 상품의 회피 속성 및 약한 IP 연결 진단."""
    result = get_bust_diagnosis(G, item_cd)
    if not result:
        raise HTTPException(status_code=404, detail=f"'{item_cd}' 노드를 찾을 수 없습니다.")
    return result


# ── 8. 네트워크 메타 정보 ───────────────────────────────────────────────
@app.get("/api/network/meta")
def network_meta():
    """그래프 노드 수, 엣지 수, 타입별 분포 반환."""
    return get_meta(G)


# ── 9. 카테고리 내 키워드/IP 노드 목록 ────────────────────────────────
@app.get("/api/category-nodes")
def category_nodes(category: str = Query(..., description="중분류명")):
    """카테고리에 연결된 속성·IP 노드 목록 반환 (신제품 조합 노드 선택기용)."""
    attr_set: set[str] = set()
    ip_set:   set[str] = set()
    for n in G.nodes:
        if G.nodes[n].get("type") == "product" and G.nodes[n].get("category") == category:
            for nb in G.neighbors(n):
                t   = G.nodes[nb].get("type")
                lbl = G.nodes[nb].get("label", "")
                if t == "attribute" and lbl:
                    attr_set.add(lbl)
                elif t == "ip" and lbl:
                    ip_set.add(lbl)
    return {"attributes": sorted(attr_set), "ips": sorted(ip_set)}


# ── 10. 히트 상품 목록 (survived=True) ───────────────────────────────
@app.get("/api/hit-products")
def hit_products():
    """생존 상품 목록 반환 (Tab 2 파생 아이디어용)."""
    items = [
        {
            "item_cd":  n,
            "item_nm":  G.nodes[n].get("label", n),
            "category": G.nodes[n].get("category", ""),
        }
        for n in G.nodes
        if G.nodes[n].get("type") == "product"
        and G.nodes[n].get("survived") is True
    ]
    items.sort(key=lambda x: x["category"])
    return {"items": items, "count": len(items)}


# ── 11. 히트 상품 파생 아이디어 ──────────────────────────────────────
@app.post("/api/derive")
def derive(req: DeriveRequest):
    """히트 상품 기반 파생 신제품 아이디어 반환."""
    result = derive_from_product(G, req.item_cd, req.top_k)
    if not result:
        raise HTTPException(status_code=404, detail=f"상품 '{req.item_cd}'을 찾을 수 없습니다.")
    return result


# ══════════════════════════════════════════════════════════════════════
# 단독 실행
# ══════════════════════════════════════════════════════════════════════
# 정적 파일 서빙 — API 라우트 정의 후 마지막에 마운트
app.mount("/", StaticFiles(directory=_BASE, html=True), name="static")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
