# -*- coding: utf-8 -*-
"""
patch_ip_keywords_all.py — ip_keywords.parquet 통합 패치

00 노트북 Block 13 직후 실행.
IP 관련 모든 수동 편집의 단일 진실 공급원(source of truth).

새로운 수동 편집이 생길 때마다 아래 섹션에만 추가하면 됩니다:
  REMOVALS  : 특정 IP에서 제거할 키워드
  DELETE_IPS: 삭제할 IP
  MERGES    : 구 IP → 신 IP 병합 (키워드 합집합 후 구 IP 삭제)
  OVERRIDES : 키워드 전체 덮어쓰기 (재추출·수동 입력 결과)

실행: python src/data_builder/patch_ip_keywords_all.py
"""

import os
import sys

import numpy as np
import pandas as pd

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.dirname(__file__))

from keyword_pipeline import run_pipeline

IP_KW_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "ip_keywords.parquet")

# ─────────────────────────────────────────────────────────────────────────────
# 수동 편집 섹션 — 변경사항은 여기에만 추가
# ─────────────────────────────────────────────────────────────────────────────

# 1. 특정 키워드 제거 (IP는 유지, 해당 키워드만 제거)
REMOVALS = {
    "라인프렌즈":    ["힙함"],
    "이정후":       ["화려함"],
    "마루짱":       ["세련"],
    "NCTWISH":      ["섬세"],
    "피스마이너스원": ["스트리머"],
    "꿈돌이":       ["꿈", "돌"],
    "마츠시게유타카": ["이름", "존재"],
    "박종혁":       ["배우", "정책"],
    "솔로지옥":     ["미스터리"],
    "MLB":          ["KBO"],
    "블루밍테일":   ["KBO"],
}

# 2. 삭제할 IP
DELETE_IPS = {
    "응답하라1985",
    "무무도사",
    "장채아",
    "최재승",
    "리복방구",
}

# 3. 병합 / 이름 변경
#    신 IP가 이미 존재하면 키워드 합집합 후 구 IP 삭제
#    신 IP가 없으면 이름만 변경
MERGES = {
    "하츄핑":    "티니핑",
    "캐치티니핑": "티니핑",
    "온정돈까스": "디진다 돈까스",
    "앙리마티스": "앙티마티스",
    "구마유시":  "T1",
    "도란":     "T1",
    "케리아":   "T1",
    "어남선생":  "류수영",
    "엠즈":     "브롤스타즈",
    "케데헌":   "케이팝데몬헌터스",
    "카러플":   "카트라이더러쉬플러스",
    "투슬리스":  "드래곤길들이기",
}

# 4. 키워드 전체 덮어쓰기 (재추출·수동 입력 결과)
#    run_pipeline() 정규화가 자동 적용됩니다
OVERRIDES = {
    # ── 재추출 성공 (ip_attribute_extractor.py --rerun-file, 2026-06-02) ──
    "다이노탱": ["귀여움", "캐릭터", "쿼카", "이미지", "콜라보", "마스코트"],
    "데르뜨":  ["베이커리", "디저트", "크림", "달콤", "프리미엄", "감성"],
    "돈키호테": ["일본", "수입", "할인", "다양성", "쇼핑", "글로벌"],
    "마녀":   ["마녀스프", "스프", "진하다", "깊은맛", "국물", "간편식"],
    "본탄":   ["본탄아메", "사탕", "달콤", "일본", "수입", "젤리"],
    "세이카":  ["젤리", "과자", "달콤", "간식", "수입", "일본"],
    # ── 수동 입력 (재추출 실패, 2026-06-02) ──
    "정성진": ["건강", "프리미엄", "고단백", "저탄수화물", "신선함", "샐러드", "헬시플레저"],
    "델토리": ["초콜릿", "스낵", "바삭함", "달콤함", "PB", "콜라보", "과자"],
    "스스스": ["샌드위치", "디저트", "베이커리", "비주얼", "트렌디", "펀슈머", "인증샷"],
    "빌로우": ["까눌레", "베이커리", "카페", "디저트", "감성"],
    "토아":   ["와인", "레드와인", "우아함", "프리미엄", "성숙함"],
    "로도스": ["명일방주", "게임", "콜라보", "캐릭터", "전략", "판타지"],
    # ── 수동 입력 (ip_keywords 검토, 2026-06-03) ──
    "별모아":  ["초코", "스낵", "별", "바삭", "팬덤", "콜라보", "TXT"],
    "조야":   ["매실", "하이볼", "샤베트", "상큼함", "시원함", "과일", "프리미엄"],
    "옐로우즈": ["오뚜기", "캐릭터", "귀여움", "마스코트", "분식", "라면", "콜라보"],
    "유우카":  ["블루아카이브", "게임", "캐릭터", "학원", "귀여움", "콜라보", "학생"],
}

# ─────────────────────────────────────────────────────────────────────────────


def to_list(v):
    if isinstance(v, (list, np.ndarray)):
        return list(v)
    return []


def main():
    print("=" * 60)
    print("ip_keywords.parquet 통합 패치")
    print("=" * 60)

    ip = pd.read_parquet(IP_KW_PATH)
    ip["키워드"] = ip["키워드"].apply(to_list)
    print(f"로드: {len(ip)}개 IP")

    # ── 1. 키워드 제거 ────────────────────────────────────────────────────────
    print("\n[1] 키워드 제거")
    for ip_name, remove in REMOVALS.items():
        mask = ip["ip_name"] == ip_name
        if not mask.any():
            continue
        idx = ip[mask].index[0]
        before = ip.at[idx, "키워드"]
        ip.at[idx, "키워드"] = [k for k in before if k not in remove]
        print(f"  {ip_name}: {remove} 제거")

    # ── 2. IP 삭제 ────────────────────────────────────────────────────────────
    print("\n[2] IP 삭제")
    for ip_name in DELETE_IPS:
        if (ip["ip_name"] == ip_name).any():
            ip = ip[ip["ip_name"] != ip_name].reset_index(drop=True)
            print(f"  {ip_name} 삭제")

    # ── 3. 병합 / 이름 변경 ───────────────────────────────────────────────────
    print("\n[3] 병합 / 이름 변경")
    to_delete = []
    for old, new in MERGES.items():
        old_mask = ip["ip_name"] == old
        if not old_mask.any():
            continue
        old_kws = to_list(ip[old_mask]["키워드"].iloc[0])
        new_mask = ip["ip_name"] == new
        if new_mask.any():
            new_idx  = ip[new_mask].index[0]
            new_kws  = to_list(ip.at[new_idx, "키워드"])
            added    = [k for k in old_kws if k not in new_kws]
            ip.at[new_idx, "키워드"] = new_kws + added
            to_delete.append(old)
            print(f"  {old} → {new} 병합 (추가: {added})")
        else:
            ip.loc[old_mask, "ip_name"] = new
            print(f"  {old} → {new} 이름 변경")

    ip = ip[~ip["ip_name"].isin(to_delete)].reset_index(drop=True)

    # ── 4. 키워드 덮어쓰기 ───────────────────────────────────────────────────
    print("\n[4] 키워드 덮어쓰기")
    for ip_name, raw_kws in OVERRIDES.items():
        refined = run_pipeline(raw_kws)
        mask = ip["ip_name"] == ip_name
        if mask.any():
            ip.at[ip[mask].index[0], "키워드"] = refined
        else:
            new_row = pd.DataFrame([{"ip_name": ip_name, "키워드": refined}])
            ip = pd.concat([ip, new_row], ignore_index=True)
        print(f"  {ip_name}: {refined}")

    # ── 저장 ─────────────────────────────────────────────────────────────────
    ip.to_parquet(IP_KW_PATH, index=False)

    csv_path = IP_KW_PATH.replace(".parquet", ".csv")
    ip_csv = ip.copy()
    ip_csv["키워드"] = ip_csv["키워드"].apply(lambda v: "|".join(v) if isinstance(v, list) else "")
    ip_csv.to_csv(csv_path, index=False, encoding="utf-8-sig")

    print(f"\n=== 완료: {len(ip)}개 IP ===")
    print(f"  parquet: {IP_KW_PATH}")
    print(f"  csv    : {csv_path}")


if __name__ == "__main__":
    main()
