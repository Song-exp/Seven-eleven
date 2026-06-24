"""network_keyword_dictionary.csv 를 **현재 HIN 그래프 키워드 노드에서 재생성**.

시트2(키워드 추출)의 "기존 vs 신규" 경계 = 실제 네트워크 키워드와 일치해야 시트1과 안 어긋난다.
정적 현호 CSV(86.5% 일치) 대신 그래프 노드 합집합으로 생성 → 데이터 갈아끼우면 이 스크립트만 재실행.

사용:  python -m scripts.build_network_keyword_dict
출력:  src/eval/keyword_extract/data/network_keyword_dictionary.csv  (header: Keyword, utf-8-sig)
"""
from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "src" / "eval" / "keyword_extract" / "data" / "network_keyword_dictionary.csv"
# 우선순위: 로컬 원천 → 배포 번들
SRCS = [
    ROOT / "data" / "processed" / "hin" / "keyword_nodes_final.parquet",
    ROOT / "serving_assets" / "hin" / "keyword_nodes_final.parquet",
]


def main() -> None:
    src = next((p for p in SRCS if p.exists()), None)
    if src is None:
        raise FileNotFoundError(f"keyword_nodes_final.parquet 없음: {[str(p) for p in SRCS]}")
    df = pd.read_parquet(src, columns=["keyword"])
    kws = sorted({str(k).strip() for k in df["keyword"] if str(k).strip()})
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Keyword"])
        for k in kws:
            w.writerow([k])
    print(f"[network_keyword_dict] {len(kws)}개 키워드 → {OUT}  (원천: {src.name})")


if __name__ == "__main__":
    main()
