"""조합/서브네트워크 대시보드 오프라인 캐시 → Dashboard/combo_data.js.

seed별로 선계산: 서브네트워크(레일+1hop 문맥) + 추천 + core 노드쌍 시너지 행렬.
(BFS 패스는 프론트에서 실시간 — 구조 엣지만이라 가벼움)

무거운 인과계산(combo_grow·synergy)을 여기서 한 번에 박아두면 dashboard는 서버 없이 즉시 동작.
배치 프리미티브(score_concept_batch) 적용 후 시드당 ~1-2s — 다수 시드 선계산도 부담 없음.
라이브 동적 서빙은 같은 build_seed를 쓰는 src/eval/combo_serve.py + /combo 엔드포인트(api.py).

실행: python -m scripts.export_combo_dashboard [마라 로제 ...]
"""
import json
import os
import sys

sys.path.insert(0, os.getcwd())
from src.eval.md.engine import MDEngine, EngineConfig  # noqa: E402
from src.eval.md.combo import _ConceptCache            # noqa: E402
from src.eval.combo_serve import build_seed            # noqa: E402  (단일 진실 소스)

OUT = "Dashboard/combo_data.js"
DEFAULT_SEEDS = ["마라", "로제", "흑임자", "단백질", "위스키"]


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    seeds = sys.argv[1:] or DEFAULT_SEEDS
    eng = MDEngine(EngineConfig.v2_sweepA()).run_single_inference()
    eng.build_mass()
    # ★ 라이브 /combo와 동일하게 분류 태그를 미리 캐시 → build_seed가 gtag(흥행/매개/주의)·
    #   ctag(인스타/POS/범용)를 노드에 부착. (api.py warmup과 동일 — 빠뜨리면 캐시에 태그 누락)
    from src.eval.md.classify import classify_keywords_live, classify_channel_live  # noqa: E402
    classify_keywords_live(eng)
    classify_channel_live(eng)
    sc = _ConceptCache(eng)
    data = {}
    for i, s in enumerate(seeds):
        print(f"[{i + 1}/{len(seeds)}] {s} ...", flush=True)
        d = build_seed(eng, s, sc)
        if d:
            data[s] = d
            print(f"    레일 {' ➔ '.join(d['rail'])} | 노드 {len(d['nodes'])} | 시너지쌍 {len(d['synergy'])}", flush=True)
    out = dict(seeds=list(data.keys()), data=data,
               meta=dict(model="v2_sweepA", n=len(data)))
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        f.write("// 자동 생성 (scripts/export_combo_dashboard.py) — 조합/서브네트 오프라인 캐시\n")
        f.write("window.COMBO_DATA = ")
        json.dump(out, f, ensure_ascii=False)
        f.write(";\n")
    print(f"saved: {OUT} | seeds={list(data.keys())}")


if __name__ == "__main__":
    main()
