"""대시보드 속성-칩 캐시 생성 — config.js.

대시보드는 라이브 전용으로 전환됨(오프라인 어텐션 렌더러 제거). 네트워크 생성은
라이브 /combo(시너지 기반)가 담당하므로, 여기서는 검색 즉시 속성 칩을 띄우기 위한
경량 캐시만 출력한다.
config.js 에 담는 것:
  - trendAttrs : 검색어 → 속성 칩 (라이브 /infer 호출 절약용 캐시; 미등록 검색어만 /infer)
  (구 networks·keywordEvidence = 오프라인 데이터는 더 이상 출력 안 함)

실행: python -m scripts.export_dashboard
"""
import json
import os
import sys

sys.path.insert(0, os.getcwd())
from src.eval import serve  # noqa: E402

OUT = "Dashboard/config.js"
COMBO_JS = "Dashboard/combo_data.js"


def _combo_seed_attrs(gk: set) -> dict:
    """콤보 시드(combo_data.js)를 일반 트렌드처럼 편입하기 위한 속성 목록.

    8개 콤보 시드(마라·로제·… )는 trend_keywords에 없어 오프라인에서 콤보 캐시로만
    서빙됐다. 대시보드를 '속성 선택 → 각 속성이 시작점 → 1-hop 메타패스 병합'으로
    통일하려면, 이 시드들도 일반 networks 트렌드로 노출돼야 한다.

    시작점 후보 = rail(처방 순서) + recommend + 키워드성 노드, graph_kw 정합본만.
    (rail/recommend/노드 라벨은 모두 graph_kw → _keyword_net으로 1-hop 생성 가능)
    """
    if not os.path.exists(COMBO_JS):
        return {}
    s = open(COMBO_JS, encoding="utf-8").read()
    i, j = s.find("{"), s.rfind("}")
    C = json.loads(s[i:j + 1])
    kw_types = {"rail", "trend", "basket", "anti", "ip2"}
    out = {}
    for seed, c in C.get("data", {}).items():
        rail = list(c.get("rail", []))
        rec = [r.get("keyword") for r in c.get("recommend", []) if r.get("keyword")]
        nodes = [n["label"] for n in c.get("nodes", []) if n.get("type") in kw_types]
        order = list(dict.fromkeys(rail + rec + nodes))   # 순서 보존 dedup
        aligned = [a for a in order if a in gk]
        if aligned:
            out[seed] = aligned
    return out


def build_data() -> dict:
    # 대시보드 라이브 전용 — 속성 추론(/infer)·네트워크 생성(/combo) 모두 라이브.
    # 정적 캐시(trendAttrs/networks/keywordEvidence)는 더 이상 출력하지 않는다(전부 라이브).
    return {
        "_meta": {"serving_exp": serve.SERVING_EXP, "note": "live-only (no static cache)"},
    }


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    data = build_data()
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        f.write("// 자동 생성 (scripts/export_dashboard.py) — 라이브 전용\n")
        f.write("// trendAttrs(검색어→속성 칩 캐시)만 출력. 네트워크 생성은 라이브 /combo.\n")
        f.write("window.DASHBOARD_DATA = ")
        json.dump(data, f, ensure_ascii=False, indent=1)
        f.write(";\n")
    print(f"saved: {OUT}")
    print("meta:", data["_meta"])


if __name__ == "__main__":
    main()
