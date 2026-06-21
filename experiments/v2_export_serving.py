"""v2_sweepA 서빙 산출물 생성 — serve.py가 읽는 weighted 엣지 4종 + relation_importance.json.

배경: v2_promote.export_v2 는 model·learned_product_scores·metrics·config 만 남겼고,
오프라인 서빙(serve.py `_data`)이 추가로 읽는 다음 파일이 누락돼 있었다:
  - weighted_product_keyword_edges.parquet  (K-P-K 순회)
  - weighted_ip_keyword_edges.parquet / weighted_product_ip_edges.parquet / weighted_trend_keyword_edges.parquet (이기종 체인)
  - relation_importance.json  (relation gate)

이 스크립트는 MDEngine(v2 로딩 + basket_comp 주입 자동)을 재사용해 위 5개를 v2_sweepA/ 에 생성한다.
learned_product_scores.parquet 는 export_v2 산출본을 그대로 둠(insta_m30 반영된 정본).

실행: python -m experiments.v2_export_serving
"""
import json
import os

import torch

from src.eval.md.engine import MDEngine, EngineConfig
from src.eval.recommend import (
    export_relation_importance,
    export_weighted_kw_edges,
    export_weighted_hetero_edges,
)


def main():
    eng = MDEngine(EngineConfig.v2_sweepA()).run_single_inference()
    assert eng.is_v2, "v2 모델이 아님 — EngineConfig.v2_sweepA() 확인"
    out_dir = eng.cfg.exp_dir
    dev = eng.device

    eidx = {et: ei.to(dev) for et, ei in eng.cache["eidx"].items()}   # basket_comp 포함
    maps = eng.cache["maps"]
    hp = torch.tensor(eng.cache["has_promo"], dtype=torch.float, device=dev)
    eattr = ({et: ea.to(dev) for et, ea in eng.cache["eattr"].items()}
             if eng.cache["eattr"] else None)

    # 1) product↔keyword 가중 엣지
    wdf = export_weighted_kw_edges(eng.model, eidx, maps, hp, eattr)
    wdf.to_parquet(os.path.join(out_dir, "weighted_product_keyword_edges.parquet"), index=False)
    print(f"  weighted_product_keyword_edges.parquet  ({len(wdf):,}행)")

    # 2) 이기종 체인 (ip_keyword / product_ip / trend_keyword)
    hetero = export_weighted_hetero_edges(eng.model, eidx, maps, hp, eattr)
    for name, hdf in hetero.items():
        hdf.to_parquet(os.path.join(out_dir, f"weighted_{name}_edges.parquet"), index=False)
        print(f"  weighted_{name}_edges.parquet  ({len(hdf):,}행)")

    # 3) relation gate
    rel = export_relation_importance(eng.model)
    with open(os.path.join(out_dir, "relation_importance.json"), "w", encoding="utf-8") as f:
        json.dump(rel, f, ensure_ascii=False, indent=2)
    print(f"  relation_importance.json  ({len(rel)}층)")

    print(f"\n완료 → {out_dir}  (serve.py SERVING_EXP='v2_sweepA' 로 전환 가능)")


if __name__ == "__main__":
    main()
