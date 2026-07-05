"""대시보드 서빙 코어 (single source of truth).

학습된 best 모델의 export 산출물(parquet)만으로 offline 서빙한다 — torch/GPU 불필요.
config.js 배치(`scripts/export_dashboard.py`)와 라이브 API(`src/eval/api.py`)가
모두 이 모듈의 함수를 호출 → 캐시와 라이브 결과가 항상 일치.

서빙 모델은 pluggable: SERVING_EXP 한 줄만 바꾸면 best로 교체.
Graph RAG 생성(제품명·근거·진단)은 S5에서 gemma_generate로 추가 — 현재는 템플릿 출력.
"""
from __future__ import annotations

import json
import os
from collections import defaultdict
from functools import lru_cache
from typing import Dict, List, Optional, Tuple

import pandas as pd


def _load_local_env(path: str = ".env") -> None:
    """의존성 없는 최소 .env 로더 — 프로젝트 루트 .env를 os.environ에 적재(이미 설정된 키는 안 덮음).

    DEEPSEEK_API_KEY·LLM_PROVIDER 등을 코드에 하드코딩하지 않고 .env(gitignore됨)에서 읽기 위함.
    """
    try:
        if not os.path.exists(path):
            return
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k:
                    os.environ.setdefault(k, v)
    except Exception:
        pass


_load_local_env()

# ── 서빙 모델 선택 (best 교체 지점) ──────────────────────────────
SERVING_EXP = "v2_sweepA"   # 최종 채택 (HINGNNv2 멀티태스크+basket_comp, leak-free, 2026-06-21). test PR-AUC 0.608 > exp47 0.570, 과적합 gap 0.115 < 0.224. THR=0.7757. 산출물: experiments.v2_export_serving. 이전 exp47_no_copurchase / exp41(누수)은 docs/v2_serving_transition.md 참조
RESULTS_ROOT = "experiments/results"
HIN_DIR = "data/processed/hin"
POS_PATH = "data/processed/pos_product_features.parquet"
TREND_PATH = "data/processed/trend_keywords.parquet"


def norm_id(x) -> str:
    try:
        return str(int(float(x)))
    except (ValueError, TypeError):
        return str(x)


# ── 데이터 로드 (1회 캐시) ───────────────────────────────────────
@lru_cache(maxsize=1)
def _data() -> Dict[str, object]:
    rdir = os.path.join(RESULTS_ROOT, SERVING_EXP)
    scores = pd.read_parquet(os.path.join(rdir, "learned_product_scores.parquet"))
    wedges = pd.read_parquet(os.path.join(rdir, "weighted_product_keyword_edges.parquet"))
    pnodes = pd.read_parquet(os.path.join(HIN_DIR, "product_nodes_final.parquet"))
    pnodes["_id"] = pnodes["ITEM_CD"].map(norm_id)
    scores["_id"] = scores["ITEM_CD"].map(norm_id)

    # POS 카테고리/매출 맵 (세븐일레븐 한정) — 카테고리 = 대분류(ITEM_LRDV_NM)
    # 대분류로 묶으면 제품 풀이 커서 K_cat(서브네트워크 속성 우주)이 풍부 → 조합 자연스러움
    pos = pd.read_parquet(POS_PATH, columns=["ITEM_CD", "ITEM_LRDV_NM", "sales_30d_amt"])
    pos["_id"] = pos["ITEM_CD"].map(norm_id)
    pos_cat = dict(zip(pos["_id"], pos["ITEM_LRDV_NM"]))
    pos_sales = dict(zip(pos["_id"], pos["sales_30d_amt"]))

    # 미래 호환: product_nodes에 대분류 컬럼이 생기면 그것을 우선
    has_cat_col = "대분류" in pnodes.columns
    cat_col = dict(zip(pnodes["_id"], pnodes["대분류"])) if has_cat_col else {}

    def category_of(_id: str) -> str:
        if has_cat_col and pd.notna(cat_col.get(_id)):
            return str(cat_col[_id])
        c = pos_cat.get(_id)
        return str(c) if pd.notna(c) else "미분류"

    scores["category"] = scores["_id"].map(category_of)
    scores["sales_amt"] = scores["_id"].map(pos_sales)
    cvt = dict(zip(pnodes["_id"], pnodes["편의점명"]))
    scores["편의점명"] = scores["_id"].map(cvt)

    # K-P-K 순회용 인접 (offline) — wedges: product(name)·keyword·attention·success
    prod2kw: Dict[str, List[Tuple[str, float]]] = defaultdict(list)
    kw2prod: Dict[str, List[Tuple[str, float]]] = defaultdict(list)
    success: Dict[str, float] = {}
    for prod, kw, att, sp in zip(wedges["product"], wedges["keyword"],
                                 wedges["attention"], wedges["product_success_prob"]):
        prod2kw[prod].append((kw, float(att)))
        kw2prod[kw].append((prod, float(att)))
        success[prod] = float(sp)

    # 트렌드 → 속성 (그래프 어휘 정합본)
    trend = pd.read_parquet(TREND_PATH)
    trend_attrs = {str(t): list(a) for t, a in
                   zip(trend["트렌드_키워드"], trend["추출_속성_final"])}

    graph_kw = set(kw2prod.keys())
    # ── 확정 키워드 훅 (keyword_final.csv) — include 필터 + tag 색 ──
    #    파일 없으면 전체 통과(하위호환). 생성: src.eval.md.inspector.export_keyword_final
    kw_include, kw_tag = _load_keyword_final()
    if kw_include is not None:
        graph_kw = graph_kw & kw_include
    deg = {k: len(v) for k, v in kw2prod.items()}      # 키워드별 제품 수 (빈도 보정용)

    # product_nodes 성공여부 라벨 (성공/실패) 맵
    success_label: Dict[str, str] = dict(zip(pnodes["ITEM_NM"], pnodes["성공여부"]))

    # ── 이기종 체인 순회용 통합 인접 (gate × attention 융합) ──────────────
    # 노드 식별: (type, name). type ∈ {keyword, product, ip}
    # 엣지 가중 base = relation_gate[rel] × edge_attention. 착지 노드가 product 면
    # 순회 시 × success 추가(성공 경로 지향). hetero 파일 부재 시 PK만으로 동작.
    gates = _load_gates(rdir)
    hadj: Dict[tuple, List[Tuple[str, str, float]]] = defaultdict(list)
    g_pk = gates.get("product__has_kw__keyword", 1.0)
    for kw, lst in kw2prod.items():                    # keyword → product
        for prod, a in lst:
            hadj[("keyword", kw)].append(("product", prod, g_pk * a))
    for prod, lst in prod2kw.items():                  # product → keyword
        for kw, a in lst:
            hadj[("product", prod)].append(("keyword", kw, g_pk * a))
    _load_hetero_into_adj(rdir, hadj, gates)

    # 제품명(ITEM_NM) → 대분류 — walk_network 카테고리 서브네트워크 한정용
    prod_cat = dict(zip(scores["ITEM_NM"].astype(str), scores["category"].astype(str)))

    # K_cat: 대분류 → 그 대분류 제품들의 속성 키워드 집합 (그라운딩 우주)
    # P는 경로 멤버가 아니라 "허용 키워드"를 정의하는 필터 역할
    cat_keywords: Dict[str, set] = defaultdict(set)
    for prod, lst in prod2kw.items():
        c = prod_cat.get(prod)
        if c and c != "미분류":
            for kw, _ in lst:
                cat_keywords[c].add(kw)

    # IP → 키워드 역참조 (infer_attrs IP fallback용)
    ip2kw: Dict[str, List[str]] = defaultdict(list)
    for (ntype, nname), neighbors in hadj.items():
        if ntype == "ip":
            for ttype, tname, _ in neighbors:
                if ttype == "keyword":
                    ip2kw[nname].append(tname)

    # ── 제품 메타 (브리핑 패널용): 프로모션·인스타 지표·성공소스 — ITEM_NM 키 ──
    promo_cols = [c for c in pnodes.columns if c.startswith("promo_")]

    def _promo_label(c: str) -> str:
        s = c[len("promo_"):]
        mm = re.match(r"\d{4}_(.+)", s)         # 'promo_0101_번들증정' → '번들증정'
        return mm.group(1) if mm else s          # 'promo_1+1' → '1+1'

    promo_labels = [_promo_label(c) for c in promo_cols]
    pv = pnodes[promo_cols].to_numpy() if promo_cols else None
    nm_list = pnodes["ITEM_NM"].astype(str).tolist()
    src_list = pnodes["성공_소스"].tolist() if "성공_소스" in pnodes.columns else [None] * len(nm_list)
    men_list = pnodes["인스타_언급횟수"].tolist() if "인스타_언급횟수" in pnodes.columns else [0] * len(nm_list)
    m30_list = pnodes["insta_mention_30d"].tolist() if "insta_mention_30d" in pnodes.columns else [0] * len(nm_list)
    prod_meta: Dict[str, dict] = {}
    for i, nm in enumerate(nm_list):
        promos = [promo_labels[j] for j in range(len(promo_cols)) if pv is not None and pv[i, j] == 1]
        prod_meta[nm] = {
            "promo": promos,
            "insta_mentions": int(men_list[i]) if pd.notna(men_list[i]) else 0,
            "insta_30d": int(m30_list[i]) if pd.notna(m30_list[i]) else 0,
            "success_src": (str(src_list[i]) if pd.notna(src_list[i]) else None),
        }

    # mine(지뢰) 키워드 — 그래프엔 남기되(빨강 회피 뱃지) 추천(K-P-K)에서는 제외
    mine = {k for k, t in kw_tag.items() if t == "mine"}

    return dict(scores=scores, prod2kw=prod2kw, kw2prod=kw2prod, success=success,
                trend_attrs=trend_attrs, graph_kw=graph_kw, category_of=category_of,
                deg=deg, success_label=success_label, hadj=hadj, gates=gates,
                prod_cat=prod_cat, cat_keywords=cat_keywords, ip2kw=dict(ip2kw),
                prod_meta=prod_meta, kw_tag=kw_tag, mine=mine)


def _load_keyword_final() -> Tuple[Optional[set], Dict[str, str]]:
    """data/processed/hin/keyword_final.csv → (include 키워드 집합, 키워드→tag).

    파일 부재 시 (None, {}) = 전체 통과(하위호환). include 컬럼 Y/N, tag=killer/mine/hub/neutral.
    생성·갱신: `python -m src.eval.md.export_keyword_final` 또는 키워드 확정 노트북.
    """
    fp = os.path.join(HIN_DIR, "keyword_final.csv")
    if not os.path.exists(fp):
        return None, {}
    df = pd.read_csv(fp, encoding="utf-8-sig")
    inc = df[df["include"].astype(str).str.upper().isin(["Y", "YES", "TRUE", "1"])]
    include_set = set(inc["keyword"].astype(str))
    tag_map = {str(k): str(t) for k, t in zip(inc["keyword"], inc["tag"]) if str(t) != "neutral"}
    return include_set, tag_map


def _load_gates(rdir: str) -> Dict[str, float]:
    """relation_importance.json 마지막 층 게이트 (last_attention 과 동일 층)."""
    path = os.path.join(rdir, "relation_importance.json")
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        layers = json.load(f)
    return layers[-1] if layers else {}


def _load_hetero_into_adj(rdir: str, hadj, gates) -> None:
    """이기종 엣지 parquet(ip↔keyword / product↔ip / trend kw↔kw)을 통합 인접에 양방향 추가."""
    specs = [
        ("weighted_ip_keyword_edges.parquet", "ip", "keyword", "ip", "keyword",
         "ip__has_kw__keyword"),
        ("weighted_product_ip_edges.parquet", "product", "ip", "product", "ip",
         "product__has_ip__ip"),
        ("weighted_trend_keyword_edges.parquet", "keyword", "keyword",
         "src_keyword", "tgt_keyword", "keyword__trend_to__keyword"),
    ]
    for fname, s_type, t_type, scol, tcol, relkey in specs:
        path = os.path.join(rdir, fname)
        if not os.path.exists(path):
            continue
        df = pd.read_parquet(path)
        g = gates.get(relkey, 1.0)
        for s, t, a in zip(df[scol], df[tcol], df["attention"]):
            if s_type == t_type and s == t:           # 트렌드 self-loop 제외
                continue
            w = g * float(a)
            hadj[(s_type, str(s))].append((t_type, str(t), w))
            hadj[(t_type, str(t))].append((s_type, str(s), w))


# ── Gemma (Ollama) RAG 생성 ──────────────────────────────────────
import re
import requests

def _ollama_base() -> str:
    """Ollama 베이스 URL. 우선순위: env OLLAMA_BASE_URL → WSL 내부 → Windows.

    Windows 네이티브 Ollama(11434)는 한글경로 CLIP 로드 실패 → WSL Ollama 사용.
    WSL에서 `OLLAMA_MODELS=<win경로 심볼릭링크> OLLAMA_HOST=0.0.0.0:11435 ollama serve`
    로 띄우면 WSL2 localhost 포워딩으로 Windows가 localhost:11435 로 도달 (IP 변동 무관).
    """
    import os
    import platform
    if os.environ.get("OLLAMA_BASE_URL"):
        return os.environ["OLLAMA_BASE_URL"].rstrip("/")
    rel = platform.uname().release.lower()
    if "microsoft" in rel or os.path.exists("/proc/sys/fs/binfmt_misc/WSLInterop"):
        return "http://localhost:11434"          # WSL 내부 실행
    return "http://localhost:11435"              # Windows → WSL Ollama (한글경로 우회)


OLLAMA_URL = _ollama_base() + "/api/generate"
GEMMA_MODEL = "gemma4:12b"

_P_EXPAND = """[SYSTEM]
당신은 세븐일레븐 상품 네트워크의 '검색어→속성 매핑 AI'다.
입력된 [검색어]가 트렌드 키워드든 인물·IP·브랜드든 상관없이,
그와 어울리는 편의점 식품 속성(맛·식감·재료·분위기·컨셉)을 한국어 단어로
8~12개 폭넓게 확장하라. 네트워크 매칭용 후보이므로 풍부하게.
[제약]
0. 검색어가 외래어 한글 음차면 원어(한자·영문)를 복원해 실제 음식을 특정하라. 발음만 비슷한 엉뚱한 향토요리로 오인 금지(예: '양쯔깐루'는 양쯔강 절임육이 아니라 杨枝甘露 망고·자몽·코코넛 디저트).
1. 속성 단어만 (입력어 자체·인물명·브랜드명 제외).
2. 쉼표 구분, 설명·따옴표·마침표 금지.
[예시]
입력: 디진다 돈까스 → 출력: 돈까스,매콤,매운맛,자극적,바삭,튀김,육즙,감칠맛,불맛,도파민,야식
입력: 할매니얼 → 출력: 전통,뉴트로,인절미,콩고물,흑임자,쑥,팥,구수,옛날,한과
입력: 유재석 → 출력: 친근,든든,국민,편안,가족,부드럽,대중적,따뜻,정겨운,풍성
입력: 추성훈 → 출력: 단백질,에너지,강인,든든,볼륨,건강,남성적,자극적,짭짤,파이터
입력: BTS → 출력: 청량,달콤,팬덤,아이돌,화려,트렌디,에너지,상큼,글로벌,젊음
입력: 티니핑 → 출력: 달콤,귀여운,핑크,어린이,딸기,캐릭터,사탕,젤리,말랑,화사
[USER]
검색어: {query}"""

_P_SELECT = """[SYSTEM]
당신은 세븐일레븐 상품 네트워크의 '검색어→속성 선택 AI'다.
[중요 맥락] 검색어는 한국 편의점·카페에서 화제가 된 '식품 트렌드'(디저트·음료·간식·맛)의 표기이며, 외래어 한글 음차일 수 있다.
[정체 파악 — 출력 전 내부적으로 먼저]
- 검색어가 음차/외래어면 원어(한자·영문·현지어) 표기를 복원해 '실제로 어떤 음식/맛인지'를 특정하라.
- 한글 발음만 비슷한 엉뚱한 향토요리로 오인하지 마라 (예: '양쯔깐루'를 양쯔강 절임육·쓰촨 매운요리로 착각 금지 → 실제는 杨枝甘露, 망고·자몽·코코넛밀크·사고로 만든 홍콩식 망고 디저트다).
- 정체가 분명하면 그 실제 핵심 재료·맛으로 채우고, 불확실하면 추측으로 채우지 말고 자신 있는 속성만 적게 출력하라.
[선택 규칙] 위에서 특정한 '실제 정체'의 속성을, 반드시 아래 [허용 어휘] 목록에 그대로 있는 단어로만 골라라.
1. 반드시 [허용 어휘]에 그대로 존재하는 단어만 출력. 목록에 없는 단어·변형·신조어·동의어 절대 금지.
2. **그 정체성을 이루는 핵심 재료**를 1~3개 맨 앞에 먼저 골라라.
   - 어휘에 **구체 재료**가 있으면 상위 카테고리보다 그것을 우선(예: '피스타치오'가 어휘에 있으면 '견과류'보다 '피스타치오').
   - 어휘에 없는 외래 재료는 가장 가까운 재료로 치환(예: 우베=보라 얌→타로·고구마, 포멜로→자몽).
   - 특정 디저트·트렌드는 그 **시그니처 재료**를 포함(예: 두바이초콜릿→피스타치오·카다이프·초코, 몽블랑→밤).
3. 정체가 분명하면 그 다음으로 맛·식감·분위기·컨셉 속성을 채워 총 8~15개. 검색어 자체·인물명·브랜드명은 제외.
4. 관련도 높은 순서로, 쉼표로만 구분. 설명·따옴표·마침표·번호 금지.
[허용 어휘]
{vocab}
[USER]
검색어: {query}"""

_P_NAME = """[SYSTEM]
당신은 세븐일레븐 신제품 네이밍 AI다. 학습된 상품 네트워크가 추천한 [핵심 속성]과
[참고 히트제품]을 근거로, 편의점 매대에 어울리는 신제품명 1개를 만들어라.
[제약]
1. 제공된 속성/카테고리만 반영. 없는 재료·브랜드 지어내기 금지.
2. 14자 이내. 제품명만 출력(따옴표·설명·마침표 금지).
3. 카테고리명을 끝에 자연스럽게 포함.
[예시]
카테고리=삼각김밥 / 속성=매콤,돈까스,자극적 / 참고=직화불고기삼각 → 출력: 매콤돈까스삼각김밥
카테고리=도시락 / 속성=불,직화,도파민 / 참고=제육볶음도시락 → 출력: 도파민직화불도시락
[USER]
카테고리: {category}
핵심 속성: {key_attrs}
참고 히트제품: {via}"""

_P_RATIONALE = """[SYSTEM]
당신은 NPD 기획 근거 설명 AI다. 학습된 네트워크가 산출한 [추천 조합]·[참고 히트제품]·
[성공 신호]만을 근거로, 이 신제품 조합의 성공 가능성을 1~2문장으로 설명하라.
제공되지 않은 수치·사실을 절대 지어내지 마라.
[제약] 1~2문장, 기획 보고체("~로 판단됨","~기대됨"). 임의 수치 날조 금지.
[USER]
신제품: {name}
추천 키워드 조합: {key_attrs}
참고 히트제품: {via} (예측 성공확률 {via_prob})
카테고리: {category}"""

_P_DIAG = """[SYSTEM]
당신은 NPD 부진 진단 AI다. 학습된 네트워크에서 이 제품의 [약한 연결 속성]과
[낮은 성공확률]을 근거로, 부진 원인(회피 속성)과 리뉴얼 처방을 제시하라.
제공된 속성 밖을 지어내지 마라.
[출력 형식 — 정확히 이 형식]
회피: 속성1, 속성2, 속성3
처방: 처방1 | 처방2 | 처방3
[USER]
부진 제품: {name}
약한(저기여) 속성: {weak_attrs}
카테고리: {category} / 예측 성공확률: {prob}"""


def _ollama(prompt: str, temperature: float = 0.3, timeout: int = 240) -> str:
    """Ollama gemma 호출. 실패 시 빈 문자열 → 호출부가 템플릿 fallback.

    keep_alive 30m: 콜드 로드(9.6GB, /mnt/c 9p ≈ 83초)를 RAM에 유지해 후속 호출 가속.
    """
    try:
        r = requests.post(OLLAMA_URL, json={
            "model": GEMMA_MODEL, "prompt": prompt, "stream": False,
            "keep_alive": "30m",
            "options": {"temperature": temperature}}, timeout=timeout)
        r.raise_for_status()
        return r.json().get("response", "").strip()
    except Exception:
        return ""


# ── LLM 프로바이더 스위치 (로컬 Ollama ↔ DeepSeek API) ──────────────────
#   LLM_PROVIDER=deepseek → DeepSeek(OpenAI 호환 /chat/completions), 그 외 → 로컬 Ollama(_ollama).
#   프롬프트는 동일 — DeepSeek은 기존 프롬프트 전체를 단일 user 메시지로 전달([SYSTEM]/[USER] 텍스트 그대로).
#   반환 계약도 동일: 실패 시 빈 문자열 → 호출부가 템플릿 fallback.
LLM_PROVIDER = os.environ.get("LLM_PROVIDER", "ollama").lower()
DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "")
DEEPSEEK_BASE = os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com").rstrip("/")
DEEPSEEK_MODEL = os.environ.get("DEEPSEEK_MODEL", "deepseek-chat")


def _deepseek(prompt: str, temperature: float = 0.3, timeout: int = 240) -> str:
    """DeepSeek chat completions 호출. 키 없거나 실패 시 빈 문자열 (Ollama와 동일 계약)."""
    if not DEEPSEEK_API_KEY:
        return ""
    try:
        r = requests.post(
            DEEPSEEK_BASE + "/chat/completions",
            headers={"Authorization": "Bearer " + DEEPSEEK_API_KEY,
                     "Content-Type": "application/json"},
            json={"model": DEEPSEEK_MODEL, "stream": False, "temperature": temperature,
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=timeout)
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"].strip()
    except Exception:
        return ""


def _llm(prompt: str, temperature: float = 0.3, timeout: int = 240) -> str:
    """LLM 디스패처 — LLM_PROVIDER에 따라 DeepSeek API 또는 로컬 Ollama로 라우팅."""
    if LLM_PROVIDER == "deepseek":
        return _deepseek(prompt, temperature=temperature, timeout=timeout)
    return _ollama(prompt, temperature=temperature, timeout=timeout)


def llm_warmup() -> None:
    """프로바이더별 워밍업 — 로컬 Ollama만 콜드로드 의미 있음(DeepSeek API는 불필요)."""
    if LLM_PROVIDER == "ollama":
        _ollama("안녕", temperature=0.0, timeout=300)


def _tokens(s: str) -> List[str]:
    return [t for t in re.split(r"[\s,/·]+", s) if len(t) >= 2]


def _match_to_graph(terms: List[str], graph_kw: set) -> List[str]:
    """확장어/토큰을 네트워크 키워드(k2i)로 매칭 — exact→substring. 시작점 보장."""
    out: List[str] = []
    seen: set = set()
    kw_list = list(graph_kw)
    for term in terms:
        term = term.strip()
        if len(term) < 1:
            continue
        if term in graph_kw:
            if term not in seen:
                seen.add(term); out.append(term)
            continue
        best = None
        for kw in kw_list:                       # substring 양방향 (len>=2)
            if len(term) >= 2 and (term in kw or kw in term):
                if best is None or abs(len(kw) - len(term)) < abs(len(best) - len(term)):
                    best = kw
        if best and best not in seen:
            seen.add(best); out.append(best)
    return out


_KIWI = None
_KIWI_FAILED = False


def _get_kiwi():
    """Kiwi 싱글톤 지연 로드(첫 호출 1회). 비ASCII(한글) 경로 폴백은 keyword_extract._load_kiwi 재사용."""
    global _KIWI, _KIWI_FAILED
    if _KIWI is not None or _KIWI_FAILED:
        return _KIWI
    try:
        from src.eval.keyword_extract.pipeline import _load_kiwi
        _KIWI = _load_kiwi()
    except Exception:
        _KIWI_FAILED = True
        _KIWI = None
    return _KIWI


def _kiwi_nouns(query: str) -> List[str]:
    """검색어 형태소 분석 → 명사(NN*) 조각 추출. 복합어 분리(예: 호박인절미 → 호박·인절미).

    Kiwi 미가용·오류 시 빈 리스트(상위에서 토큰·LLM 매칭으로 폴백). graph_kw 매칭 전 후보 확장용.
    """
    k = _get_kiwi()
    if not k:
        return []
    try:
        return [t.form for t in k.tokenize(query)
                if t.tag.startswith("NN") and len(t.form) >= 2]
    except Exception:
        return []


def gemma_expand(query: str) -> List[str]:
    # timeout 300s: 첫 호출 콜드 로드(9.6GB, /mnt/c 9p)가 실측 ~190초까지 → 첫 추론 실패 방지
    out = _llm(_P_EXPAND.format(query=query), temperature=0.2, timeout=300)
    return [t.strip() for t in out.replace("\n", " ").split(",") if t.strip()]


@lru_cache(maxsize=1)
def _graph_vocab_str() -> str:
    """그래프 키워드 전체를 LLM 허용 어휘 목록 문자열로 1회 구성(캐시). 프롬프트 prefix로 재사용."""
    return ", ".join(sorted(_data()["graph_kw"]))


def vocab_select(query: str, timeout: int = 120) -> List[str]:
    """어휘 제약 LLM 선택 — 그래프 어휘 목록 안에서만 고르게 + exact 멤버십 후필터.

    substring 억지매칭(스파이시→파이)·어미 누락(부드러운)을 없애고 의미 정확도 확보.
    출력은 100% graph_kw. 목록 밖·변형은 후필터로 제거.
    """
    gk = _data()["graph_kw"]
    out = _llm(_P_SELECT.format(vocab=_graph_vocab_str(), query=query),
               temperature=0.2, timeout=timeout)
    seen: set = set()
    res: List[str] = []
    for t in out.replace("\n", ",").split(","):
        t = t.strip().strip('"').strip("'")
        if t in gk and t not in seen:
            seen.add(t); res.append(t)
    return res


def insight_filter(concept: List[str], keywords: List[str], timeout: int = 60) -> List[str]:
    """AI 인사이트 후보 중 컨셉에 '명백히 안 어울리는' 키워드만 골라 제거 대상(drop)으로 반환.

    같은 카테고리 브랜드·IP·먹거리·맛/식감 키워드는 유지, 전혀 다른 카테고리(치킨·돈까스·라면·핫도그 등)
    또는 무관한 IP만 제거. LLM 실패/빈 입력 시 빈 리스트(=필터 안 함, 하위호환).
    """
    cand = [k for k in (keywords or []) if k]
    if not cand or not concept:
        return []
    prompt = (
        "신상품 컨셉: " + ", ".join(concept) + "\n"
        "아래 후보 중 이 컨셉의 신상품 기획에 '명백히 안 어울리는' 것만 제거 대상으로 골라줘.\n"
        "[유지] 맛/식감/재료/분위기/시즌 키워드, 컨셉과 같은 카테고리의 브랜드·IP·먹거리.\n"
        "[제거] 컨셉과 전혀 다른 카테고리 음식/브랜드(예: 디저트 컨셉인데 치킨·돈까스·라면·핫도그), 컨셉과 무관한 IP.\n"
        "후보: " + ", ".join(cand) + "\n"
        "제거할 키워드만 쉼표로만 출력. 없으면 '없음'."
    )
    try:
        out = _llm(prompt, temperature=0.0, timeout=timeout)
    except Exception:
        return []
    cset, seen, drop = set(cand), set(), []
    for t in out.replace("\n", ",").split(","):
        t = t.strip().strip('"').strip("'")
        if t in cset and t not in seen:        # 환각 방지: 입력 후보에 있는 것만
            seen.add(t); drop.append(t)
    return drop




# ── 1. 검색어 → 네트워크 키워드 (entry point) ────────────────────
def infer_attrs(query: str) -> List[str]:
    """검색어를 네트워크에 존재하는 키워드(시작점)로 매핑.

    1) 기지 트렌드 → config 조회  2) 임의 입력 → Gemma 확장 후 그래프 매칭.
    출력은 항상 k2i(graph_kw) 키워드 — K-P-K 순회 시작점.
    """
    d = _data()
    gk = d["graph_kw"]

    def _finish(res):
        # 검색어 자체가 그래프 키워드면 맨 앞에 포함(시작점 보장). 그래프에 없으면 추가 안 함
        # → '우베'처럼 그래프 미존재 검색어가 속성 칩으로 새는 것 방지.
        return ([query] + res) if (query in gk and query not in res) else res

    if query in d["trend_attrs"]:
        m = [a for a in d["trend_attrs"][query] if a in gk]
        if m:
            return _finish(m)
    # ★ 트렌드 '부분일치'를 더 이상 즉시 return 하지 않음 — '호박인절미'가 트렌드 '인절미'에 걸려
    #   단락되며 분절(단호박·인절미)에 도달 못 하던 버그 수정. 분절·LLM을 먼저, 트렌드는 폴백으로만.
    det = _match_to_graph(_kiwi_nouns(query) + _tokens(query), gk)   # ① 분절(복합어 분리) + 원토큰
    det += [kw for kw in gk if len(kw) >= 2 and kw in query and kw not in det]  # Kiwi 미분절 복합어 보강(흑임자라떼 → 라떼)
    det = [kw for kw in det if not any(kw != o and kw in o for o in det)]       # 더 긴 매칭의 조각 제거('떡볶이'의 '볶이')
    try:
        selected = vocab_select(query)                              # ② 어휘제약 LLM 선택(의미)
    except Exception:
        selected = []                                               # LLM 오류 시에도 분절 매칭 유지
    merged = list(dict.fromkeys([*det, *selected]))                 # 분절(단호박·인절미) 우선 + LLM 보완
    if merged:
        return _finish(merged)
    # 폴백 — 분절·LLM 모두 실패 시에만 트렌드 부분일치 속성 사용
    trend_hits = []
    for t, attrs in d["trend_attrs"].items():
        if query in t or t in query:
            trend_hits += [a for a in attrs if a in gk]
    if trend_hits:
        return _finish(list(dict.fromkeys(trend_hits)))
    # fallback — LLM·분절 모두 실패(API 오류 등) 시 Gemma 확장 + 문자열매칭 (여전히 graph_kw만 반환)
    expanded = gemma_expand(query)
    return _finish(_match_to_graph(expanded + _tokens(query), gk))


# ── 2. 히트 / 부진 상품 (탭2·탭3) ────────────────────────────────
def _fmt_sales(amt) -> str | None:
    if amt is None or pd.isna(amt) or amt <= 0:
        return None
    return f"{amt / 10000:,.0f}만원"


def top_products(category: str, n: int = 6) -> List[Dict]:
    d = _data()
    s = d["scores"]
    sub = s[s["category"] == category].sort_values("pred_success_prob", ascending=False)
    if sub.empty:
        return []
    out = []
    for rank, (_, r) in enumerate(sub.head(n).iterrows()):
        pct = (rank + 1) / len(sub) * 100
        name = str(r["ITEM_NM"])
        kws_sorted = sorted(d["prod2kw"].get(name, []), key=lambda x: -x[1])
        kws = [k for k, _ in kws_sorted if k not in GENERIC_STOPWORDS][:8]
        out.append({"name": name, "sales": _fmt_sales(r["sales_amt"]),
                    "pct": f"상위 {pct:.0f}%", "prob": round(float(r["pred_success_prob"]), 4),
                    "kws": kws})
    return out


def bust_products(category: str, n: int = 5) -> List[Dict]:
    d = _data()
    s = d["scores"]
    sub = s[s["category"] == category].sort_values("pred_success_prob", ascending=True)
    out = []
    for _, r in sub.head(n).iterrows():
        name = str(r["ITEM_NM"])
        kws_sorted = sorted(d["prod2kw"].get(name, []), key=lambda x: -x[1])
        kws = [k for k, _ in kws_sorted if k not in GENERIC_STOPWORDS][:8]
        out.append({"name": name, "sales": _fmt_sales(r["sales_amt"]),
                    "issue": "낮은 성공확률 (모델 예측)",   # S5에서 RAG 진단 텍스트로 대체
                    "prob": round(float(r["pred_success_prob"]), 4), "kws": kws})
    return out


# ── 3. K-P-K 추천 (학습 가중치 순회) ─────────────────────────────
# 초일반 메타 키워드 — 조합/제품명에서 제외 (변별력 없음, 맛·식감어는 유지)
GENERIC_STOPWORDS = {"간식", "야식", "식사", "디저트", "간편", "음식", "음료", "과자", "밥", "먹거리"}


def recommend_keywords(seed_attrs: List[str], top_k: int = 15,
                       min_support: int = 3, freq_correct: bool = True,
                       category: str = "") -> List[Tuple[str, float]]:
    """K-P-K: score(kt|ks) = Σ_j att(j,ks)·p_success(j)·att(j,kt).

    att은 키워드 기준 정규화 → rare-keyword 편향(질적분석 06에서 확인: 보정 전 노이즈).
    freq_correct(×deg)로 정규화 편향을 상쇄(≈ 성공가중 동시출현), min_support로 극희귀 제외.
    category 지정 시 경유 제품 j를 해당 대분류로 한정 → 추천 키워드가 카테고리 속성으로 그라운딩
    (미분류·타 대분류 제품 통한 누수 차단; 예: 과자에 '돈까스'·'고기' 유입 방지).
    """
    d = _data()
    deg = d["deg"]
    prod_cat = d["prod_cat"]
    seeds = [s for s in seed_attrs if s in d["graph_kw"]]
    if not seeds:
        return []
    seedset = set(seeds)
    mine = d.get("mine", ())                       # 지뢰는 추천 후보 제외
    score: Dict[str, float] = defaultdict(float)
    for ks in seeds:
        for prod, att_in in d["kw2prod"].get(ks, []):
            if category and prod_cat.get(prod) != category:   # 경유 제품 카테고리 한정
                continue
            pj = d["success"].get(prod, 0.0)
            for kt, att_out in d["prod2kw"].get(prod, []):
                if kt in seedset:
                    continue
                score[kt] += att_in * pj * att_out
    out = []
    for kt, s in score.items():
        if deg.get(kt, 0) < min_support or kt in GENERIC_STOPWORDS or kt in mine:
            continue
        out.append((kt, s * deg[kt] if freq_correct else s))
    return sorted(out, key=lambda x: x[1], reverse=True)[:top_k]


def recommend_bundle(seed_attrs: List[str], max_size: int = 6,
                     stop_ratio: float = 0.3, min_support: int = 3) -> List[str]:
    """① 시드 중심 일관 묶음 (집합 성장).

    현재 묶음 '전체'와 가장 강하게 연결된 키워드를 하나씩 추가 → 드리프트 없는 코히런트 조합.
    다음 후보 점수가 첫 추가의 stop_ratio 미만이면 정지 (가변 길이).
    """
    d = _data()
    deg = d["deg"]
    cur = list(dict.fromkeys(s for s in seed_attrs if s in d["graph_kw"]))[:2]
    if not cur:
        return []
    mine = d.get("mine", ())                       # 지뢰는 추천 후보 제외
    first = None
    while len(cur) < max_size:
        curset = set(cur)
        cand: Dict[str, float] = defaultdict(float)
        for ks in cur:
            for prod, att_in in d["kw2prod"].get(ks, []):
                pj = d["success"].get(prod, 0.0)
                for kt, att_out in d["prod2kw"].get(prod, []):
                    if kt in curset or kt in GENERIC_STOPWORDS or kt in mine:
                        continue
                    cand[kt] += att_in * pj * att_out
        scored = [(kt, s * deg[kt]) for kt, s in cand.items() if deg.get(kt, 0) >= min_support]
        if not scored:
            break
        best, bs = max(scored, key=lambda x: x[1])
        if first is None:
            first = bs
        elif bs < stop_ratio * first:
            break
        cur.append(best)
    return cur


def _kpk_next(k: str, exclude: set, d, deg, topn: int = 15, min_support: int = 3,
              mode: str = "lift"):
    """키워드 k에서 K-P-K 1스텝 후보.

    mode='cooc': att·성공·att × deg → 일반 속성 (번들·일관용).
    mode='lift': 성공가중 동시출현 / deg(kt) → 특이 속성 (탐색·의외 발굴용).
    """
    att: Dict[str, float] = defaultdict(float)
    cs: Dict[str, float] = defaultdict(float)
    mine = d.get("mine", ())                       # 지뢰 키워드는 추천 후보에서 제외
    for prod, att_in in d["kw2prod"].get(k, []):
        pj = d["success"].get(prod, 0.0)
        for kt, att_out in d["prod2kw"].get(prod, []):
            if kt in exclude or kt in GENERIC_STOPWORDS or kt in mine:
                continue
            att[kt] += att_in * pj * att_out
            cs[kt] += pj
    out = []
    for kt in att:
        dk = deg.get(kt, 0)
        if dk < min_support:
            continue
        out.append((kt, att[kt] * dk if mode == "cooc" else cs[kt] / dk))
    out.sort(key=lambda x: -x[1])
    return out[:topn]


def recommend_paths(seed_attrs: List[str], max_steps: int = 3, beam: int = 30,
                    n_paths: int = 3, avoid: List[str] = None,
                    mode: str = "lift", min_support: int = 3) -> List[Tuple[List[str], float]]:
    """② 탐색 꿀조합 (빔 walk). 시드에서 K-P-K 엣지를 따라 max_steps홉 체인 생성.

    mode='lift'(기본): 특이성 스코어 → 의외 조합 발굴 (마라→마라탕·샹궈).
    mode='cooc': 일반 속성 → 안정 묶음과 유사.
    avoid(=안정 묶음)와 dedup → 안정과 다른 새 조합만. max_steps·mode 는 walk-EDA에서 튜닝.
    """
    d = _data()
    deg = d["deg"]
    seeds = [s for s in seed_attrs if s in d["graph_kw"]]
    if not seeds:
        return []
    avoid_set = set(avoid or [])
    beams = [([seeds[0]], 0.0)]
    finals: List[Tuple[List[str], float]] = []
    for _ in range(max_steps):
        nb = []
        for path, sc in beams:
            for kt, w in _kpk_next(path[-1], set(path), d, deg, mode=mode, min_support=min_support):
                nb.append((path + [kt], sc + w))
        if not nb:
            break
        nb.sort(key=lambda x: -x[1])
        beams = nb[:beam]
        finals = beams
    # novelty(>=1 신규 키워드) + 집합 dedup
    seen, res = set(), []
    for path, sc in sorted(finals, key=lambda x: -x[1]):
        if not [k for k in path[1:] if k not in avoid_set]:
            continue
        key = frozenset(path)
        if key in seen:
            continue
        seen.add(key)
        res.append((path, sc))
        if len(res) >= n_paths:
            break
    return res


_REL_LABEL = {       # (from_type, to_type) → 사람이 읽는 관계 설명
    ("keyword", "product"): "이 속성을 가진 성공제품",
    ("product", "keyword"): "그 제품의 대표 속성",
    ("keyword", "ip"): "이 속성과 강하게 연결된 IP",
    ("ip", "keyword"): "그 IP의 대표 속성",
    ("product", "ip"): "그 제품이 활용한 IP",
    ("ip", "product"): "그 IP를 쓴 성공제품",
    ("keyword", "keyword"): "함께 뜨는 트렌드 속성",
}
_TYPE_KO = {"keyword": "속성", "product": "제품", "ip": "IP"}


def _short_name(name: str, n: int = 14) -> str:
    name = str(name)
    if ")" in name:                                  # 브랜드 접두 제거
        tail = name.split(")")[-1].strip()
        if tail:
            name = tail
    return name[:n] + "…" if len(name) > n else name


def walk_chain(seed_attrs: List[str], max_steps: int = 6, min_support: int = 3) -> dict:
    """줄줄이 사탕 (이기종) — 시드 속성에서 노드 타입 무관 최대 가중 엣지를 따라가는 그리디 체인.

    통합 인접(키워드·제품·IP) 위에서 매 스텝 가중치 최대인 미방문 이웃 1개로 이동.
    엣지 가중 = relation_gate(α_r) × attention. 착지 노드가 제품이면 × 성공확률(성공 경로 지향).
    체인 예: 마라 →(co/sim)→ 라면제품 →(has_ip)→ 진라면IP →(ip-kw)→ 얼큰 …
    반환: nodes(label/type/success), steps(from/from_type/to/to_type/rel/weight/success), seed.
    """
    d = _data()
    hadj = d["hadj"]
    success = d["success"]
    seeds = [s for s in seed_attrs if s in d["graph_kw"]]
    if not seeds:
        return {"nodes": [], "steps": [], "seed": seed_attrs[0] if seed_attrs else ""}

    cur = ("keyword", seeds[0])
    visited = {cur}
    nodes = [{"label": seeds[0], "type": "keyword"}]
    steps = []
    for _ in range(max_steps):
        best = None  # (weight, ntype, nid, base_w, succ)
        for ntype, nid, base_w in hadj.get(cur, []):
            nkey = (ntype, nid)
            if nkey in visited:
                continue
            if ntype == "keyword" and (nid in GENERIC_STOPWORDS
                                       or d["deg"].get(nid, 0) < min_support):
                continue
            succ = success.get(nid, 0.0) if ntype == "product" else None
            w = base_w * (succ if ntype == "product" else 1.0)
            if best is None or w > best[0]:
                best = (w, ntype, nid, base_w, succ)
        if best is None:
            break
        w, ntype, nid, base_w, succ = best
        label = _short_name(nid) if ntype == "product" else str(nid)
        node = {"label": label, "type": ntype}
        if ntype == "product":
            node["success"] = round(succ, 3)
            node["full"] = str(nid)
        nodes.append(node)
        steps.append({
            "from": cur[1] if cur[0] != "product" else _short_name(cur[1]),
            "from_type": cur[0], "to": label, "to_type": ntype,
            "rel": _REL_LABEL.get((cur[0], ntype), "연결"),
            "weight": round(float(w), 5),
            "success": round(succ, 3) if succ is not None else None,
        })
        visited.add((ntype, nid))
        cur = (ntype, nid)
    return {"nodes": nodes, "steps": steps, "seed": seeds[0]}


def explain_chain(chain: dict) -> str:
    """이기종 체인 설명 — 각 화살표를 관계 타입·가중치로 서술."""
    steps = chain.get("steps", [])
    seed = chain.get("seed", "")
    if not steps:
        return f"'{seed}'에서 이어지는 강한 경로가 없습니다 (시드가 네트워크에 약하게 연결됨)."
    lines = [f"'{seed}'에서 네트워크 **가중치(관계중요도 × 어텐션)가 가장 높은 경로**를 "
             f"노드 타입 구분 없이 따라간 체인입니다."]
    for i, s in enumerate(steps, 1):
        succ = f", 성공확률 {int(s['success'] * 100)}%" if s.get("success") is not None else ""
        lines.append(
            f"{i}. **{s['from']}** →〔{s['rel']}〕**{s['to']}**"
            f" ({_TYPE_KO.get(s['to_type'], s['to_type'])}{succ}, 가중 {s['weight']:.4f})")
    path = [chain["nodes"][0]["label"]] + [s["to"] for s in steps]
    lines.append(
        f"→ 경로: **{' → '.join(path)}**. "
        f"시작 속성 '{seed}'에서 가중치가 가장 큰 방향(=성공 기여가 가장 큰 연결)을 매 단계 "
        f"따라가며, 성공제품·IP·트렌드 속성을 거쳐 가장 유망한 속성 조합으로 수렴합니다.")
    return "\n".join(lines)


def _greedy_walk_from(start: str, d, max_steps: int, min_support: int,
                      category: str = "", allowed_kw: set = None):
    """단일 시작 키워드에서 타입 무관 그리디 워크. (from_id, to_id, to_node, weight, rel) 리스트 반환.
    노드 id: f'{type}\\x01{nid}' (제품은 full명). to_node = {label,type,success?,full?}.
    category 지정 시 제품을 해당 대분류로 한정 + allowed_kw(=K_cat) 안의 키워드로만 착지(그라운딩).
    IP는 허용 키워드를 통해서만 도달하므로 자연 그라운딩 → 별도 제한 없음."""
    hadj, success, prod_cat = d["hadj"], d["success"], d["prod_cat"]
    cur = ("keyword", start)
    visited = {cur}
    steps = []
    for _ in range(max_steps):
        best = None  # (w, ntype, nid, succ)
        for ntype, nid, base_w in hadj.get(cur, []):
            if (ntype, nid) in visited:
                continue
            if ntype == "keyword":
                if nid in GENERIC_STOPWORDS or d["deg"].get(nid, 0) < min_support:
                    continue
                if allowed_kw is not None and nid not in allowed_kw:
                    continue                          # K_cat 그라운딩 — 카테고리 속성만
            if ntype == "product" and category and prod_cat.get(nid) != category:
                continue                              # 대분류 서브네트워크 한정
            succ = success.get(nid, 0.0) if ntype == "product" else None
            w = base_w * (succ if ntype == "product" else 1.0)
            if best is None or w > best[0]:
                best = (w, ntype, nid, succ)
        if best is None:
            break
        w, ntype, nid, succ = best
        label = _short_name(nid) if ntype == "product" else str(nid)
        node = {"label": label, "type": ntype}
        if ntype == "product":
            node["success"] = round(succ, 3)
            node["full"] = str(nid)
        src_id = f"{cur[0]}\x01{cur[1]}"
        tgt_id = f"{ntype}\x01{nid}"
        steps.append((src_id, tgt_id, node, float(w),
                      _REL_LABEL.get((cur[0], ntype), "연결")))
        visited.add((ntype, nid))
        cur = (ntype, nid)
    return steps


# ── 출발점별 1-hop 네트워크 (개편된 대시보드 코어) ──────────────────────
# 백본 노드별 1-hop 가지 기본 상한. '고소'처럼 한 키워드에 제품이 다수 연결되면
# 네트워크가 폭발하므로, branch 미지정 시 가중치 상위 N개만 노출.
TOP_BRANCH_1HOP = 10


def _keyword_net(start: str, d, max_steps: int = 3, branch: Optional[int] = None) -> dict:
    """단일 출발 키워드의 네트워크 = 가중치 최대 체인(백본) + 각 백본 노드의 1-hop 가지.

    노드: {id, label, type, success?, full?, layer, branch(bool), parent?}
    백본은 layer 0,1,2…; 가지(leaf)는 parent 백본 노드 아래(branch=True, parent=백본 id).
    """
    success, deg, hadj = d["success"], d["deg"], d["hadj"]
    pmeta = d.get("prod_meta", {})
    nodes: Dict[str, dict] = {}
    edges: Dict[tuple, dict] = {}

    def reg(key, label, ntype, layer, branch_=False, parent=None, succ=None, full=None):
        if key not in nodes:
            nd = {"id": key, "label": label, "type": ntype, "layer": layer, "branch": branch_}
            if ntype == "keyword":
                tg = d.get("kw_tag", {}).get(label)
                if tg:
                    nd["tag"] = tg          # killer/mine/hub → 대시보드 색·뱃지
            if parent:
                nd["parent"] = parent
            if succ is not None:
                nd["success"] = round(succ, 3)
            if full is not None:
                nd["full"] = full
            if ntype == "product" and full is not None and str(full) in pmeta:
                nd.update(pmeta[str(full)])      # promo·insta_mentions·insta_30d·success_src
            nodes[key] = nd
        else:                                            # 이미 백본이면 가지로 덮지 않음
            nodes[key]["layer"] = min(nodes[key]["layer"], layer)
            if not branch_:
                nodes[key]["branch"] = False
        return key

    def edge(a, b, w, rel, branch_=False):
        ek = (a, b)
        if ek not in edges or w > edges[ek]["weight"]:
            edges[ek] = {"src": a, "tgt": b, "weight": round(float(w), 5),
                         "rel": rel, "branch": branch_}

    s_key = f"keyword\x01{start}"
    reg(s_key, start, "keyword", 0)
    backbone = [("keyword", start, s_key, 0)]            # (type, id, key, layer)
    cur = ("keyword", start)
    layer = 0
    for src_id, tgt_id, node, w, rel in _greedy_walk_from(start, d, max_steps, 3):
        layer += 1
        reg(tgt_id, node["label"], node["type"], layer,
            succ=node.get("success"), full=node.get("full"))
        edge(src_id, tgt_id, w, rel)
        nid = tgt_id.split("\x01", 1)[1]
        backbone.append((node["type"], nid, tgt_id, layer))
        cur = (node["type"], nid)

    # 각 백본 노드의 1-hop 가지 (top-branch, 가중치 내림차순)
    backbone_set = {(t, i) for (t, i, _, _) in backbone}
    used = set(backbone_set)
    for (bt, bid, bkey, blayer) in backbone:
        neighbors_sorted = sorted(
            hadj.get((bt, bid), []),
            key=lambda x: x[2] * (success.get(x[1], 0.0) if x[0] == "product" else 1.0),
            reverse=True,
        )
        cnt = 0
        cap = branch if branch is not None else TOP_BRANCH_1HOP
        for ntype, nid, base_w in neighbors_sorted:
            if cnt >= cap:
                break
            if (ntype, nid) in used:
                continue
            if ntype == "keyword" and (nid in GENERIC_STOPWORDS or deg.get(nid, 0) < 3):
                continue
            w = base_w * (success.get(nid, 0.0) if ntype == "product" else 1.0)
            nkey = f"{ntype}\x01{nid}"
            label = _short_name(nid) if ntype == "product" else str(nid)
            reg(nkey, label, ntype, blayer + 1, branch_=True, parent=bkey,
                succ=(success.get(nid, 0.0) if ntype == "product" else None),
                full=(str(nid) if ntype == "product" else None))
            edge(bkey, nkey, w, _REL_LABEL.get((bt, ntype), "연결"), branch_=True)
            used.add((ntype, nid))
            cnt += 1
    return {"start": start, "nodes": list(nodes.values()), "edges": list(edges.values())}


def attr_network(seed_keywords: List[str], trend: str = "",
                 max_steps: int = 3, branch: Optional[int] = None) -> dict:
    """선택된 출발 속성(최대 3개)별 1-hop 네트워크 + 교집합 병합 종합 네트워크.

    반환: {trend, seeds, keyword_nets:[{start,nodes,edges}], merged:{nodes,edges,has_overlap}}
    병합 = 키워드별 net 합집합(공유 노드는 한 점). has_overlap = 2개 이상 net 공유 노드 존재.
    """
    d = _data()
    seeds = [s for s in seed_keywords if s in d["graph_kw"]][:3]
    if not seeds:
        return {"trend": trend, "seeds": [], "keyword_nets": [], "merged": {}}

    knets = [_keyword_net(s, d, max_steps, branch) for s in seeds]

    # 병합: 노드 id 등장 net 수 집계 → 공유 노드 표시
    appear: Dict[str, int] = defaultdict(int)
    for kn in knets:
        for nid in {n["id"] for n in kn["nodes"]}:
            appear[nid] += 1
    m_nodes: Dict[str, dict] = {}
    m_edges: Dict[tuple, dict] = {}
    for kn in knets:
        for n in kn["nodes"]:
            if n["id"] not in m_nodes:
                m_nodes[n["id"]] = dict(n, shared=(appear[n["id"]] >= 2), deg=appear[n["id"]])
            else:
                m_nodes[n["id"]]["layer"] = min(m_nodes[n["id"]]["layer"], n["layer"])
                if not n.get("branch"):
                    m_nodes[n["id"]]["branch"] = False
        for e in kn["edges"]:
            ek = (e["src"], e["tgt"])
            if ek not in m_edges or e["weight"] > m_edges[ek]["weight"]:
                m_edges[ek] = e
    has_overlap = any(c >= 2 for c in appear.values())
    return {
        "trend": trend, "seeds": seeds,
        "keyword_nets": knets,
        "merged": {"nodes": list(m_nodes.values()), "edges": list(m_edges.values()),
                   "has_overlap": has_overlap},
    }


def explain_attr_network(net: dict) -> str:
    """출발점별 1-hop 네트워크 + 종합 설명 (트렌드 → 출발 속성 → 1-hop 시너지/제품/IP)."""
    trend = net.get("trend", "") or "이 트렌드"
    seeds = net.get("seeds", [])
    if not seeds:
        return f"'{trend}'에서 네트워크 출발점을 찾지 못했습니다."
    merged = net.get("merged", {})
    nodes = merged.get("nodes", [])
    shared = [n["label"] for n in nodes if n.get("shared")]
    ips = [n["label"] for n in nodes if n["type"] == "ip"][:5]

    lines = [f"**'{trend}'**의 출발 속성 **{', '.join(seeds)}** 각각에서 1-hop 네트워크를 "
             f"펼쳤습니다 (노드 {len(nodes)})."]
    if merged.get("has_overlap") and shared:
        lines.append(f"- 출발 속성들이 **{', '.join(shared[:6])}**를 공유 → 하나의 **종합 네트워크**로 연결됩니다 "
                     f"(= 함께 밀어줄 핵심 축).")
    else:
        lines.append("- 출발 속성 간 공유 노드가 없어 각 네트워크가 **독립적**입니다.")
    if ips:
        lines.append(f"- 연결된 IP: **{', '.join(ips)}**.")
    return "\n".join(lines)


def walk_network(seed_attrs: List[str], category: str = "", trend: str = "",
                 max_steps: int = 4, max_seeds: int = 12, min_support: int = 3) -> dict:
    """다중 시작점 병합 네트워크 — 상위 N개 시작 키워드에서 각각 그리디 워크 후 공유 노드로 합침.

    category 지정 시 **K_cat 그라운딩**: 제품을 해당 대분류로 한정하고, 착지 키워드를
    그 대분류 제품들의 속성(K_cat)으로 제한 → 표류 없이 카테고리에 머묾.
    제품(P)은 경로에 강제로 끼우지 않음 — 관련될 때만 자연 등장 (P는 허용 키워드를 정하는 필터).
    레이어드 그래프: 시작점=layer 0, 매 홉마다 layer+1. 같은 노드는 최소 layer로 병합.
    반환: {seeds, category, nodes:[{id,label,type,success?,layer}], edges:[{src,tgt,weight,rel,success?}]}
    """
    d = _data()
    hadj, success, prod_cat = d["hadj"], d["success"], d["prod_cat"]
    allowed_kw = d["cat_keywords"].get(category) if category else None  # K_cat
    seeds = [s for s in seed_attrs if s in d["graph_kw"]]
    if not seeds:
        return {"seeds": [], "category": category, "trend": trend, "nodes": [], "edges": []}

    # 시작점 랭킹: 첫 스텝 최대 가중치 큰 순 → 상위 N (K_cat 그라운딩 반영)
    def _first_w(s):
        best = 0.0
        for ntype, nid, base_w in hadj.get(("keyword", s), []):
            if ntype == "keyword":
                if nid in GENERIC_STOPWORDS or d["deg"].get(nid, 0) < min_support:
                    continue
                if allowed_kw is not None and nid not in allowed_kw:
                    continue
            if ntype == "product" and category and prod_cat.get(nid) != category:
                continue
            w = base_w * (success.get(nid, 0.0) if ntype == "product" else 1.0)
            best = max(best, w)
        return best

    # 카테고리 지정 시 시작점도 K_cat(대분류 속성)으로 한정 → 네트워크에 타 대분류 키워드 유입 차단
    # (없으면 fallback). _first_w 높은 순 정렬, max_seeds(기본 12)까지 = 사실상 전체 추론 속성.
    seeds_use = ([s for s in seeds if s in allowed_kw] or seeds) if allowed_kw is not None else seeds
    chosen = sorted(seeds_use, key=_first_w, reverse=True)[:max_seeds]

    nodes: Dict[str, dict] = {}
    edges: Dict[tuple, dict] = {}
    for s in chosen:                                  # 시작점 노드 (layer 0)
        sid = f"keyword\x01{s}"
        nodes.setdefault(sid, {"id": sid, "label": s, "type": "keyword", "layer": 0})
    for s in chosen:
        hop = 0
        for src_id, tgt_id, node, w, rel in _greedy_walk_from(
                s, d, max_steps, min_support, category, allowed_kw):
            hop += 1
            if tgt_id not in nodes:
                nd = {"id": tgt_id, "label": node["label"], "type": node["type"], "layer": hop}
                if "success" in node:
                    nd["success"] = node["success"]
                if "full" in node:
                    nd["full"] = node["full"]
                nodes[tgt_id] = nd
            else:
                nodes[tgt_id]["layer"] = min(nodes[tgt_id]["layer"], hop)
            ek = (src_id, tgt_id)
            if ek not in edges or w > edges[ek]["weight"]:
                edges[ek] = {"src": src_id, "tgt": tgt_id, "weight": round(w, 5),
                             "rel": rel, "success": node.get("success")}
    return {"seeds": chosen, "category": category, "trend": trend,
            "nodes": list(nodes.values()), "edges": list(edges.values())}


def explain_network(net: dict) -> str:
    """연결 스토리라인 — 트렌드 → 네트워크 속성 → (경유 성공제품) → 제품 키워드 → 제안 제품.

    한 줄기로 이어짐: "트렌드 A를 반영할 속성 B,C,D → 이 속성을 가진 성공제품 P를 경유 →
    제품 키워드(시너지) E,F → 따라서 '{제안 제품명}'을 제안합니다."
    """
    seeds = net.get("seeds", [])
    nodes = net.get("nodes", [])
    edges = net.get("edges", [])
    category = net.get("category", "")
    trend = net.get("trend", "") or "이 트렌드"
    scope = f"「{category}」 제품군 안에서 " if category else ""
    if not seeds or not edges:
        return (f"{scope}'{trend}'와 연결되는 네트워크 속성을 찾지 못했습니다 "
                f"(이 제품군과 학습상 약하게 연결됨).")

    seed_set = set(seeds)
    indeg: Dict[str, int] = defaultdict(int)
    for e in edges:
        indeg[e["tgt"]] += 1
    # 시너지(=제품 경유로 도달한) 키워드: 시작점 제외, 수렴도 높은 순
    synergy = [n["label"] for n in sorted(nodes, key=lambda x: -indeg.get(x["id"], 0))
               if n["type"] == "keyword" and n["label"] not in seed_set][:6]
    ips = [n["label"] for n in nodes if n["type"] == "ip"][:4]
    prods = [n for n in nodes if n["type"] == "product" and n.get("success") is not None]
    top_prod = max(prods, key=lambda n: n["success"]) if prods else None

    # 제안 제품명 — 카드가 지정한 이름 우선, 없으면 트렌드+시너지 키워드+대분류 (중복 회피)
    proposal_name = net.get("proposal_name")
    if not proposal_name:
        name_kw = next((k for k in synergy + seeds if k != trend), "")
        proposal_name = _template_name(trend, name_kw, category)

    # 한 줄기 서술: 트렌드 → 네트워크 속성 → (성공제품 경유) → 제품 키워드 → 제안
    lines = [f"{scope}**'{trend}'**를 반영할 네트워크 속성은 **{', '.join(seeds[:5])}**입니다."]
    if top_prod and top_prod["success"] >= 0.3 and synergy:
        lines.append(f"이 속성을 가진 성공제품 **{top_prod['label']}**(성공확률 "
                     f"{int(top_prod['success'] * 100)}%)을 경유해, 시너지 키워드 "
                     f"**{', '.join(synergy)}**로 이어집니다.")
    elif synergy:
        lines.append(f"이 속성들과 시너지가 좋은 키워드는 **{', '.join(synergy)}**입니다.")
    if ips:
        lines.append(f"연결된 **IP**는 **{', '.join(ips)}**입니다.")
    josa = "이" if (proposal_name and (ord(proposal_name[-1]) - 0xAC00) % 28) else "가"
    lines.append(f"→ 즉 **트렌드 '{trend}' → 네트워크 속성 → 제품 키워드**가 이어지며, "
                 f"이 맥락을 묶은 신제품 **'{proposal_name}'**{josa} 성공 가능성이 가장 높은 방향입니다.")
    return "\n".join(lines)


def _top_via(seed_attrs: List[str]) -> str:
    """시드 속성을 가장 많이 공유하고 성공확률 높은 경유(기존) 제품."""
    d = _data()
    seeds = set(s for s in seed_attrs if s in d["graph_kw"])
    best, best_key = None, (-1, -1.0)
    cand: Dict[str, int] = defaultdict(int)
    for ks in seeds:
        for prod, _ in d["kw2prod"].get(ks, []):
            cand[prod] += 1
    for prod, shared in cand.items():
        key = (shared, d["success"].get(prod, 0.0))
        if key > best_key:
            best_key, best = key, prod
    return best or "—"


def similar_success_products(
    keyword_bundle: List[str],
    category: str = "",
    n: int = 3,
) -> List[Dict]:
    """keyword_bundle과 키워드 구성이 유사한 성공 제품 반환.

    성공 필터: product_nodes.성공여부 == "성공" 라벨 기준 (예측확률 분위 아님).
    복합 점수 = Σ_{k∈bundle∩prod} att(prod,k) × pred_success_prob
    category : 동일 카테고리 우선, 없으면 전체 카테고리 fallback
    """
    d = _data()
    bundle_set = set(k for k in keyword_bundle if k in d["graph_kw"])
    if not bundle_set:
        return []

    scores_df = d["scores"]
    success_label = d["success_label"]   # ITEM_NM → "성공"/"실패"

    # 제품별 어텐션 가중 겹침 합산 (성공 라벨 제품만)
    sim: Dict[str, float] = defaultdict(float)
    hit_kws: Dict[str, List[str]] = defaultdict(list)
    for kw in bundle_set:
        for prod, att in d["kw2prod"].get(kw, []):
            if success_label.get(prod) != "성공":   # 라벨 기준 필터
                continue
            sim[prod] += att
            if kw not in hit_kws[prod]:
                hit_kws[prod].append(kw)

    if not sim:
        return []

    # 복합 점수(유사도 × 성공확률) 내림차순 정렬
    ranked = sorted(sim.items(),
                    key=lambda x: -(x[1] * d["success"].get(x[0], 0.0)))

    # 동일 카테고리 우선, 없으면 전체 카테고리 fallback
    results: List[Dict] = []
    for same_cat_only in ([True, False] if category else [False]):
        for prod, _ in ranked:
            if len(results) >= n:
                break
            row = scores_df[scores_df["ITEM_NM"] == prod]
            if row.empty:
                continue
            r = row.iloc[0]
            cat = str(r.get("category", ""))
            if same_cat_only and cat != category:
                continue
            coverage = len(hit_kws[prod]) / max(len(bundle_set), 1)
            results.append({
                "name": str(r["ITEM_NM"]),
                "category": cat,
                "prob": round(float(r["pred_success_prob"]), 3),
                "coverage": round(coverage, 2),
                "matched_kws": hit_kws[prod],
                "sales": _fmt_sales(r.get("sales_amt")),
            })
        if results:
            break
    return results


_P_NAME_BATCH = """편의점 {category} 신제품 네이밍 AI. 트렌드 '{trend}'를 반영한 신제품명을
아래 각 [키워드 조합]마다 한 줄에 하나씩 만들어라.
규칙: 트렌드 '{trend}'와 키워드를 반영, 14자 이내, 끝에 '{category}' 포함, 이름만 출력(번호·설명·따옴표 금지).
{combos}"""


def gemma_names_batch(category: str, combo_lists: List[List[str]], trend: str = "") -> List[str]:
    """여러 조합의 제품명을 1회 호출로 생성 (긴 prefill 반복 방지 → 속도↑)."""
    combos = "\n".join(f"{i+1}) {','.join(c)}" for i, c in enumerate(combo_lists))
    out = _llm(_P_NAME_BATCH.format(category=category, trend=trend or "트렌드", combos=combos),
               temperature=0.4)
    return [re.sub(r"^\s*\d+[).\s]*", "", ln).strip().strip('"').strip("'")
            for ln in out.splitlines() if ln.strip()]


def _template_name(trend: str, key_kw: str, category: str) -> str:
    """비-RAG 제품명 — 트렌드명 + 핵심 시너지 키워드 + 카테고리 (네트워크 설명 반영).
    트렌드==키워드 중복 토큰은 제거."""
    t = (trend or "").strip()
    parts = []
    if t and ")" not in t and len(t) <= 10:        # 실제 트렌드명일 때만 접두 (제품명 길이 회피)
        parts.append(t)
    if key_kw:
        parts.append(key_kw)
    if category:
        parts.append(category)
    seen: set = set()                              # 중복 토큰 제거 (트렌드==키워드==카테고리 등)
    out = [p for p in parts if p and not (p in seen or seen.add(p))]
    return " ".join(out).strip()


def recommend_proposals(category: str, attrs: List[str], trend: str = "",
                        k: int = 3, use_rag: bool = True) -> List[Dict]:
    """추천 키워드를 조합 카드 k개로 조립. 조합·네트워크는 순회(즉시), use_rag면 제품명만 Gemma(배치)."""
    rec = recommend_keywords(attrs, top_k=max(6 * k, 12), category=category)
    if not rec:
        return []
    # seed2 = 트렌드 핵심 속성 중 카테고리 속성(K_cat) 우선 → 카드/네트워크 그라운딩
    kcat = _data()["cat_keywords"].get(category, set()) if category else set()
    seed2 = [a for a in attrs if not kcat or a in kcat][:2] or attrs[:2]
    proposals, combo_lists = [], []
    for i in range(min(k, max(1, len(rec) // 2))):
        kws = rec[i * 2:i * 2 + 2]
        if not kws:
            break
        key_kw = [kw for kw, _ in kws]
        uniq = [kw for kw, _ in rec[i * 2 + 2:i * 2 + 4]]
        via = _top_via(seed2 + key_kw)        # 카드별: 이 조합(시드+추천)을 매개한 성공 제품
        raw = sum(s for _, s in kws)
        score_label = round(min(9.9, 5.0 + raw / (rec[0][1] + 1e-9) * 4.5), 1)
        cls = "high" if score_label >= 8.0 else "mid" if score_label >= 6.5 else "low"
        bundle = seed2 + key_kw + uniq
        combo_lists.append(bundle)
        # 키워드 군집 기준 유사 성공 제품 탐색
        similar = similar_success_products(bundle, category=category, n=3)
        proposals.append({
            "id": i, "name": _template_name(trend, key_kw[0], category), "category": category,
            "score": cls, "scoreLabel": str(score_label),
            "attrs": seed2 + key_kw, "unique": uniq,
            "via": f"기존 {via}", "hops": 2, "rationale": "",
            "similar": similar,
            "network": {"center": {"label": "", "type": "product"},
                        "trend": {"label": trend or "트렌드", "type": "trend"},
                        "attrs": bundle},
        })
    # 제품명 LLM 배치 생성 (1회 호출로 전체)
    if use_rag and proposals:
        names = gemma_names_batch(category, combo_lists, trend=trend)
        for p, nm in zip(proposals, names):
            if nm:
                p["name"] = nm
    # 카드별 네트워크 + 연결 설명 — 각 카드는 자기 조합을 출발점으로 한 고유 네트워크
    for p in proposals:
        p["network"]["center"]["label"] = p["name"]
        net = walk_network(p["attrs"] + p["unique"], category=category, trend=trend)
        net["proposal_name"] = p["name"]           # 설명의 제안명 = 이 카드 제품명
        p["net"] = net
        p["explain"] = explain_network(net)
    return proposals


# ── 4. 부진 진단 ─────────────────────────────────────────────────
def _parse_diag(text: str) -> Dict:
    """'회피: a,b,c\\n처방: x|y|z' 파싱."""
    avoid, rx = [], []
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("회피"):
            avoid = [a.strip() for a in line.split(":", 1)[-1].split(",") if a.strip()]
        elif line.startswith("처방"):
            rx = [r.strip() for r in line.split(":", 1)[-1].split("|") if r.strip()]
    return {"avoid": avoid, "rx": rx}


def diagnose(item_name: str, use_rag: bool = True) -> Dict:
    """부진 제품의 약한(낮은 att) 키워드 → avoid. use_rag면 Gemma가 진단·처방 생성(그래프 근거)."""
    d = _data()
    kws = sorted(d["prod2kw"].get(item_name, []), key=lambda x: x[1])
    weak = [k for k, _ in kws[:4]]
    row = d["scores"][d["scores"]["ITEM_NM"] == item_name]
    cat = row["category"].iloc[0] if len(row) else "미분류"
    prob = round(float(row["pred_success_prob"].iloc[0]), 3) if len(row) else 0.0

    if use_rag and weak:
        txt = _llm(_P_DIAG.format(name=item_name, weak_attrs=",".join(weak),
                                  category=cat, prob=prob), temperature=0.3, timeout=60)
        p = _parse_diag(txt)
        if p["avoid"] and p["rx"]:
            return {"avoid": p["avoid"], "rx": p["rx"], "text": txt}
    return {"avoid": weak[:3] or ["트렌드 연결 부재"],
            "rx": ["트렌드 속성 재연결", "고성공 속성 보강", "가격 최적화"], "text": ""}


def categories() -> List[str]:
    s = _data()["scores"]
    vc = s["category"].value_counts()
    return [c for c in vc.index if c != "미분류"][:20]


if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding="utf-8")
    print("서빙 모델:", SERVING_EXP)
    print("카테고리(상위):", categories()[:8])
    print("\n[히트 — 삼각김밥]")
    for p in top_products("삼각김밥", 3):
        print("  ", p)
    print("[부진 — 삼각김밥]")
    for p in bust_products("삼각김밥", 3):
        print("  ", p)
    print("\n[트렌드 속성 추론 — '마라']:", infer_attrs("마라")[:8])
    print("[K-P-K 추천 — 시드 '마라']:", recommend_keywords(["마라"], 8))
    print("\n[조합 제안 — 삼각김밥 × 마라]")
    for p in recommend_proposals("삼각김밥", infer_attrs("마라") or ["마라"], "마라", k=3):
        print(f"  {p['name']} (가중치 {p['scoreLabel']}, {p['score']}) attrs={p['attrs']} via={p['via']}")
