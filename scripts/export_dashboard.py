"""대시보드 정적 캐시 생성 — config.js (서버 없이 오프라인 동작).

단일 화면 — 트렌드 → 추론 속성(버튼, 최대 3 선택) → 1-hop 네트워크.
config.js 에 담는 것:
  - trendAttrs : 기지 트렌드 → 정합 속성 (추론 버튼 즉시 표시; 미등록 트렌드만 /infer Gemma fallback)
  - networks   : 선택 가능한 각 속성의 1-hop 네트워크 (오프라인 /network 대체; 병합은 클라이언트 JS)

이 파일만 있으면 백엔드 서버 없이 dashboard.html 단독으로 동작.

실행: python -m scripts.export_dashboard
"""
import json
import os
import sys

sys.path.insert(0, os.getcwd())
from src.eval import serve  # noqa: E402

OUT = "Dashboard/config.js"


def build_data() -> dict:
    d = serve._data()
    gk = d["graph_kw"]
    # 기지 트렌드 → 그래프 정합 속성 (추론 버튼 즉시 표시; 미등록은 /infer)
    trend_attrs = {}
    selectable = set()
    for t, attrs in d["trend_attrs"].items():
        aligned = [a for a in attrs if a in gk]
        if aligned:
            trend_attrs[t] = aligned
            selectable.update(aligned)
    # 선택 가능한 각 속성의 1-hop 네트워크 미리 계산 → 오프라인 /network 대체.
    # 제품 노드에 promo·인스타·성공소스 메타 포함(serve._keyword_net). 병합은 클라이언트에서.
    networks = {}
    for kw in sorted(selectable):
        net = serve._keyword_net(kw, d)
        networks[kw] = {"nodes": net["nodes"], "edges": net["edges"]}
    return {
        "trendAttrs": trend_attrs,
        "networks": networks,
        "_meta": {"serving_exp": serve.SERVING_EXP, "n_trends": len(trend_attrs),
                  "n_networks": len(networks)},
    }


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    data = build_data()
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        f.write("// 자동 생성 (scripts/export_dashboard.py) — 오프라인 캐시\n")
        f.write("// trendAttrs(속성 추출) + networks(1-hop 네트워크) → 서버 없이 동작\n")
        f.write("window.DASHBOARD_DATA = ")
        json.dump(data, f, ensure_ascii=False, indent=1)
        f.write(";\n")
    print(f"saved: {OUT}")
    print("meta:", data["_meta"])


if __name__ == "__main__":
    main()
