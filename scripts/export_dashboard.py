"""대시보드 정적 캐시 생성 — 추론 속성 후보만 config.js 로 배치.

개편: 단일 화면 — 트렌드 → 추론 속성(버튼, 최대 3 선택) → 1-hop 네트워크.
네트워크는 선택 의존이라 라이브 API(/network)로 처리. config.js 는 버튼 즉시 표시용
trendAttrs(기지 트렌드 → 정합 속성)만 담는다. 미등록 트렌드는 /infer(Gemma)로 fallback.

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
    for t, attrs in d["trend_attrs"].items():
        aligned = [a for a in attrs if a in gk]
        if aligned:
            trend_attrs[t] = aligned
    return {
        "trendAttrs": trend_attrs,
        "_meta": {"serving_exp": serve.SERVING_EXP, "n_trends": len(trend_attrs)},
    }


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    data = build_data()
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        f.write("// 자동 생성 (scripts/export_dashboard.py) — 추론 속성 후보 캐시\n")
        f.write("// 네트워크는 dashboard 가 API(/network)로 라이브 요청\n")
        f.write("window.DASHBOARD_DATA = ")
        json.dump(data, f, ensure_ascii=False, indent=1)
        f.write(";\n")
    print(f"saved: {OUT}")
    print("meta:", data["_meta"])


if __name__ == "__main__":
    main()
