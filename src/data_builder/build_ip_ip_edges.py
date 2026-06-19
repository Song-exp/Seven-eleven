"""
ip_extra_final.csv(신규 61개 IP)를 HIN _final 파일에 반영하는 일회성 스크립트.

작업 순서:
  1. ip_nodes_final.parquet      — 61개 IP 행 append (275 → 336)
  2. ip_ip_edges_final.parquet   — 신규 생성: 기존 IP 키워드 중 신규 IP명인 것 (66개)
  3. ip_keyword_edges_final.parquet — 신규 IP의 키워드 행 append (724개)
  4. keyword_nodes_final.parquet — 신규 키워드 노드 append (24개)

멱등 보장: 각 단계에서 이미 존재하는 항목은 건너뜀.
"""

import ast
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
HIN_DIR = ROOT / "data" / "processed" / "hin"

EXTRA_CSV = HIN_DIR / "ip_extra_final.csv"
IP_NODES  = HIN_DIR / "ip_nodes_final.parquet"
IP_IP     = HIN_DIR / "ip_ip_edges_final.parquet"
IP_KW     = HIN_DIR / "ip_keyword_edges_final.parquet"
KW_NODES  = HIN_DIR / "keyword_nodes_final.parquet"


def as_list(val) -> list[str]:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return []
    if isinstance(val, (list, np.ndarray)):
        return list(val)
    try:
        return ast.literal_eval(str(val))
    except Exception:
        return [k.strip() for k in str(val).split(",") if k.strip()]


def main():
    sys.stdout.reconfigure(encoding="utf-8")

    # ── 입력 로드 ────────────────────────────────────────────────────
    extra = pd.read_csv(EXTRA_CSV)
    extra["키워드_final"] = extra["키워드_final"].apply(as_list)

    existing_ip = pd.read_parquet(IP_NODES)
    existing_ip["키워드_final"] = existing_ip["키워드_final"].apply(as_list)

    existing_kw_edges = pd.read_parquet(IP_KW)
    existing_kw_nodes = pd.read_parquet(KW_NODES)

    existing_ip_set  = set(existing_ip["ip_name"])
    new_ip_set       = set(extra["ip_name"])
    all_ip_set       = existing_ip_set | new_ip_set
    existing_kw_set  = set(existing_kw_nodes["keyword"])

    print(f"[입력] 기존 IP: {len(existing_ip_set)}개 / 신규 IP: {len(new_ip_set)}개")

    # ── Step 1: ip_nodes_final append ───────────────────────────────
    truly_new = extra[~extra["ip_name"].isin(existing_ip_set)].copy()
    if len(truly_new) == 0:
        print("[Step 1] 이미 모두 등록됨 — 건너뜀")
    else:
        combined_ip = pd.concat([existing_ip, truly_new[["ip_name", "키워드_final"]]],
                                ignore_index=True)
        combined_ip.to_parquet(IP_NODES, index=False)
        print(f"[Step 1] ip_nodes_final: {len(existing_ip)} → {len(combined_ip)}개")

    # ── Step 2: ip_ip_edges_final 생성 ──────────────────────────────
    ip_ip_rows = []
    for _, row in existing_ip.iterrows():
        for kw in row["키워드_final"]:
            if kw in new_ip_set:
                ip_ip_rows.append({"src_ip": row["ip_name"], "tgt_ip": kw})

    ip_ip_df = pd.DataFrame(ip_ip_rows).drop_duplicates()

    if IP_IP.exists():
        existing_ii = pd.read_parquet(IP_IP)
        before = len(existing_ii)
        merged = pd.concat([existing_ii, ip_ip_df]).drop_duplicates()
        merged.to_parquet(IP_IP, index=False)
        print(f"[Step 2] ip_ip_edges_final: {before} → {len(merged)}개")
    else:
        ip_ip_df.to_parquet(IP_IP, index=False)
        print(f"[Step 2] ip_ip_edges_final 신규 생성: {len(ip_ip_df)}개")

    # ── Step 3: ip_keyword_edges_final append ───────────────────────
    new_kw_edge_rows = []
    for _, row in extra.iterrows():
        for kw in row["키워드_final"]:
            if kw not in all_ip_set:    # IP명은 ip_ip_edges로 처리
                new_kw_edge_rows.append({"ip_name": row["ip_name"], "keyword": kw})

    new_kw_edges = pd.DataFrame(new_kw_edge_rows).drop_duplicates()

    # 멱등: 기존에 없는 (ip_name, keyword) 쌍만 추가
    existing_pairs = set(zip(existing_kw_edges["ip_name"], existing_kw_edges["keyword"]))
    truly_new_edges = new_kw_edges[
        ~new_kw_edges.apply(lambda r: (r["ip_name"], r["keyword"]) in existing_pairs, axis=1)
    ]
    combined_kw_edges = pd.concat([existing_kw_edges, truly_new_edges], ignore_index=True)
    combined_kw_edges.to_parquet(IP_KW, index=False)
    print(f"[Step 3] ip_keyword_edges_final: {len(existing_kw_edges)} → {len(combined_kw_edges)}개")

    # ── Step 4: keyword_nodes_final append ──────────────────────────
    new_kws = sorted(set(new_kw_edges["keyword"]) - existing_kw_set)

    if not new_kws:
        print("[Step 4] 신규 키워드 없음 — 건너뜀")
    else:
        new_kw_node_rows = pd.DataFrame({
            "keyword":          new_kws,
            "is_trend_keyword": [0] * len(new_kws),
            "추출_속성":         [[] for _ in new_kws],
            "인스타_첫_등장일":   pd.NaT,
        })
        combined_kw_nodes = pd.concat([existing_kw_nodes, new_kw_node_rows], ignore_index=True)
        combined_kw_nodes.to_parquet(KW_NODES, index=False)
        print(f"[Step 4] keyword_nodes_final: {len(existing_kw_nodes)} → {len(combined_kw_nodes)}개")
        print(f"         신규 키워드: {new_kws}")

    print("\n완료.")


if __name__ == "__main__":
    main()
