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
            serve.llm_warmup()      # 로컬 Ollama만 콜드로드 (DeepSeek API면 no-op)
            print(f"[warmup] LLM 준비 완료 (provider={serve.LLM_PROVIDER})")
        except Exception as e:
            print(f"[warmup] LLM 실패: {e}")
        try:
            # combo 엔진 + 라이브 분류(Δprob 배치)를 미리 계산·캐시 → /combo 첫 요청이 안 막힘
            from src.eval import combo_serve
            from src.eval.md.classify import classify_keywords_live
            eng, _ = combo_serve._engine()
            classify_keywords_live(eng)
            print("[warmup] combo 엔진 + 키워드 분류 준비 완료")
        except Exception as e:
            print(f"[warmup] combo/분류 준비 실패: {e}")
    threading.Thread(target=_do, daemon=True).start()
    print(f"[warmup] 백그라운드 준비 시작… (provider={serve.LLM_PROVIDER})")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)


class InferReq(BaseModel):
    trend: str


class NetReq(BaseModel):
    trend: str = ""
    attrs: Optional[List[str]] = None     # 선택된 출발 속성 (최대 3); 없으면 추론 상위


class ComboReq(BaseModel):
    seed: str
    max_hops: int = 4


class ComboPairReq(BaseModel):
    a: str
    b: str


class ComboRecReq(BaseModel):
    seed: str
    pool: List[str]
    top: int = 10


class ExtractOneReq(BaseModel):       # 시트2: 단건 설명문 → 키워드
    itemCode: Optional[str] = ""
    itemName: Optional[str] = ""
    sourceText: str = ""


class ExtractBatchReq(BaseModel):     # 시트2: CSV 배치
    rows: List[dict] = []


@app.get("/")
def dashboard():
    return FileResponse(os.path.join(_DASHBOARD_DIR, "dashboard.html"))

@app.get("/config.js")
def config_js():
    return FileResponse(os.path.join(_DASHBOARD_DIR, "config.js"), media_type="application/javascript")

@app.get("/combo_data.js")
def combo_data_js():
    return FileResponse(os.path.join(_DASHBOARD_DIR, "combo_data.js"), media_type="application/javascript")

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


# ── 조합 서브네트워크 (동적) — combo_serve(MDEngine 싱글톤) 지연 로드 ──────────
#    오프라인 combo_data.js와 동일 구조 반환 → 프론트는 캐시 미스 시 이 엔드포인트로 fallback.
#    첫 호출만 모델 로드(수 초), 이후 배치 가속으로 시드당 ~1-2s, 같은 시드 재요청은 캐시 즉시.
@app.post("/combo")
def combo(req: ComboReq):
    """시드 서브네트워크 페이로드 (rail·nodes·edges·recommend·synergy)."""
    from src.eval import combo_serve
    return combo_serve.combo_network(req.seed, req.max_hops)


@app.post("/combo/pair")
def combo_pair(req: ComboPairReq):
    """두 노드 보완/대체 인과 판정 (synergy=margin(b|a)−margin(b|∅))."""
    from src.eval import combo_serve
    return combo_serve.combo_pair(req.a, req.b)


@app.post("/combo/recommend")
def combo_recommend(req: ComboRecReq):
    """서브네트 내 seed에 붙일 best 노드 (headroom Δ)."""
    from src.eval import combo_serve
    return {"recommend": combo_serve.combo_recommend(req.seed, req.pool, req.top)}


# ── 시트2: 네트워크 업데이트 (상품 설명문 → 키워드 추출, Kiwi) ──────────
#    현호 keyword_api_server(:8010) 폐기 → 단일호스트. 파이프라인 지연 로드(첫 호출 1회).
@app.post("/extract-one")
def extract_one(req: ExtractOneReq):
    """단건: 설명문 → 기존/신규 키워드 분리 + 근거."""
    from src.eval.keyword_extract import service
    return {"ok": True, "row": service.build_review_row(req.itemCode, req.itemName, req.sourceText)}


@app.post("/extract-batch")
def extract_batch(req: ExtractBatchReq):
    """CSV 배치: rows[] → 상품별 추출 결과."""
    from src.eval.keyword_extract import service
    out = [service.build_review_row_from_csv_row(r, i + 1)
           for i, r in enumerate(req.rows) if isinstance(r, dict)]
    out = [r for r in out if r["sourceText"]]
    return {"ok": True, "rows": out}
