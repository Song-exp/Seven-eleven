# -*- coding: utf-8 -*-
"""
patch_ip_keywords_reextract.py — 재추출 결과를 ip_keywords.parquet에 직접 패치

IP_속성_재추출.json의 signature_keywords를 run_pipeline() 정규화 후
ip_keywords.parquet의 해당 IP 항목만 덮어씁니다.
00 파이프라인(Block 13) 실행 없이 현재 ip_keywords.parquet 수정 사항 보존.

입력:
  data/processed/IP_속성추출/IP_속성_재추출.json
  data/processed/ip_keywords.parquet

출력:
  data/processed/ip_keywords.parquet (패치된 IP만 업데이트)

실행: python src/data_builder/patch_ip_keywords_reextract.py
"""

import json
import os
import sys

import numpy as np
import pandas as pd

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.dirname(__file__))

from keyword_pipeline import run_pipeline

REEXTRACT_JSON = os.path.join(
    PROJECT_ROOT, "data", "processed", "IP_속성추출", "IP_속성_재추출.json"
)
IP_KW_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "ip_keywords.parquet")

# 패치 대상 (IP_재추출_검토반영.txt 기준)
TARGET_IPS = {
    "다이노탱", "데르뜨", "델토리", "돈키호테", "로도스",
    "마녀", "본탄", "빌로우", "세이카", "스스스",
    "정성진", "조야", "토아",
}


def to_list(v):
    if isinstance(v, (list, np.ndarray)):
        return list(v)
    return []


def main():
    print("=" * 60)
    print("ip_keywords.parquet 재추출 결과 패치")
    print("=" * 60)

    # ── 재추출 결과 로드 ──────────────────────────────────────────────────────
    if not os.path.exists(REEXTRACT_JSON):
        print(f"[오류] 재추출 결과 파일 없음: {REEXTRACT_JSON}")
        print("  ip_attribute_extractor.py --rerun-file 먼저 실행하세요.")
        return

    with open(REEXTRACT_JSON, encoding="utf-8") as f:
        reextract = json.load(f)

    # keyword → signature_keywords 매핑
    reextract_map = {
        r["keyword"]: r.get("signature_keywords", [])
        for r in reextract
        if r.get("keyword") in TARGET_IPS
    }

    print(f"\n재추출 결과 로드: {len(reextract)}개 중 패치 대상 {len(reextract_map)}개 확인")
    missing = TARGET_IPS - set(reextract_map.keys())
    if missing:
        print(f"  [주의] 아직 재추출 안 된 IP: {sorted(missing)}")

    # ── ip_keywords.parquet 로드 ──────────────────────────────────────────────
    ip = pd.read_parquet(IP_KW_PATH)
    ip["키워드"] = ip["키워드"].apply(to_list)
    print(f"\nip_keywords.parquet 로드: {len(ip)}개 IP")

    # ── 패치 적용 ─────────────────────────────────────────────────────────────
    updated, not_found, empty_kw = [], [], []

    for ip_name, raw_kws in reextract_map.items():
        mask = ip["ip_name"] == ip_name

        if not mask.any():
            not_found.append(ip_name)
            print(f"  [미발견] {ip_name} — ip_keywords에 없음 (신규 추가)")
            refined = run_pipeline(raw_kws)
            new_row = pd.DataFrame([{"ip_name": ip_name, "키워드": refined}])
            ip = pd.concat([ip, new_row], ignore_index=True)
            updated.append(ip_name)
            continue

        if not raw_kws:
            empty_kw.append(ip_name)
            print(f"  [스킵] {ip_name} — signature_keywords 비어 있음 (수동 입력 필요)")
            continue

        refined = run_pipeline(raw_kws)
        idx = ip[mask].index[0]
        before = ip.at[idx, "키워드"]
        ip.at[idx, "키워드"] = refined
        updated.append(ip_name)
        print(f"  [패치] {ip_name}")
        print(f"    before: {before}")
        print(f"    after : {refined}")

    # ── 저장 ─────────────────────────────────────────────────────────────────
    ip.to_parquet(IP_KW_PATH, index=False)

    print(f"\n=== 패치 완료 ===")
    print(f"  업데이트: {len(updated)}개  {updated}")
    if empty_kw:
        print(f"  키워드 없어 스킵: {len(empty_kw)}개  {empty_kw}")
        print(f"  → IP_속성_수동입력.json에 직접 작성 후 00 Step 4.5 실행하세요.")
    print(f"  저장: {IP_KW_PATH}")


if __name__ == "__main__":
    main()
