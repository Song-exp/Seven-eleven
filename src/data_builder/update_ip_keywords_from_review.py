"""
ip_nodes_final.parquet의 키워드_final 업데이트 스크립트

검수 파일 4종을 적용:
  1. ip_kw_candidates_final.csv          → index 0~172 (APEC ~ 연세우유): 추가+제거
  2. ip_kw_candidates_final_원준.csv 중  → index 173~198 (옐로우즈 ~ 잠바주스): 추가+제거
  3. ip_kw_candidates_final_지윤.csv     → index 199~224 (장민호 ~ 카트라이더): 추가+제거
  4. 유미)ip_nodes_reviewed_...csv       → index 225~280 (캐치티니핑 ~ 한촌설렁탕): 직접 대체

비고 파싱 규칙:
  - "제거: X, Y, Z"     → X, Y, Z 제거 (메인/지윤 형식)
  - "X Y Z 제거"        → X, Y, Z 제거 (원준 형식, 공백 구분)
  - "제거 없음"          → 제거 없음
  - "제거 (IP 제외)"     → IP 전체 제외 플래그 (키워드 변경 없음, 경고 출력)
"""

import csv
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
HIN_DIR = ROOT / "data" / "processed" / "hin"
OUT_PATH = HIN_DIR / "ip_nodes_final.parquet"


# ---------------------------------------------------------------------------
# 헬퍼 함수
# ---------------------------------------------------------------------------

def parse_kw_str(s: str) -> list[str]:
    """쉼표 구분 키워드 문자열 → 리스트"""
    if not s or (isinstance(s, float) and np.isnan(s)):
        return []
    return [k.strip() for k in str(s).split(",") if k.strip()]


def as_list(val) -> list[str]:
    """parquet에서 읽힌 키워드 컬럼(list/ndarray/str)을 Python list로 변환"""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return []
    if isinstance(val, (list, np.ndarray)):
        return list(val)
    return parse_kw_str(str(val))


def parse_remove_note(note) -> tuple[list[str], bool]:
    """
    비고 → (제거할_키워드_목록, IP_제외_여부)

    처리 패턴:
      - "제거: X, Y, Z" → [X, Y, Z], False
      - "X Y Z 제거"    → [X, Y, Z], False  (단어 공백 구분)
      - "제거 없음"      → [], False
      - "제거 (IP 제외)" / "(IP 제외)" → [], True
    """
    if note is None or (isinstance(note, float) and np.isnan(note)):
        return [], False
    note = str(note).strip()

    # IP 제외 플래그
    if "IP 제외" in note:
        return [], True

    # 제거 없음
    if re.search(r"없음|없다|없어|^$", note):
        return [], False

    # "제거: X, Y, Z" 형식 (메인/지윤)
    m = re.search(r"제거\s*:\s*(.+)", note)
    if m:
        kws = [k.strip() for k in m.group(1).split(",") if k.strip()]
        return kws, False

    # "X Y Z 제거" 형식 (원준): 제거 앞에 오는 모든 단어
    if "제거" in note:
        before = note[: note.index("제거")].strip()
        if before:
            kws = [k.strip() for k in before.split() if k.strip()]
            return kws, False

    return [], False


def apply_update(current: list[str], add_str, note) -> tuple[list[str], bool]:
    """
    현재 키워드 리스트에 추가/제거 적용.

    Returns:
        updated_list, ip_excluded_flag
    """
    remove_list, ip_excluded = parse_remove_note(note)
    if ip_excluded:
        return current, True

    # 추가할키워드가 IP 제외 신호인 경우 처리
    add_str_s = str(add_str).strip() if add_str and not (isinstance(add_str, float) and np.isnan(add_str)) else ""
    if "IP 제외" in add_str_s:
        return current, True

    add_list = parse_kw_str(add_str)
    updated = list(current)

    for kw in add_list:
        if kw not in updated:
            updated.append(kw)

    updated = [k for k in updated if k not in remove_list]
    return updated, False


# ---------------------------------------------------------------------------
# CSV 파서: 비고 컬럼의 비인용 쉼표 대응 (Python csv 모듈 사용)
# ---------------------------------------------------------------------------

def read_review_csv(path: Path) -> pd.DataFrame:
    """
    4컬럼(ip, 현재키워드, 추가할키워드, 비고) CSV를 읽는다.
    비고에 비인용 쉼표가 있어도 마지막 컬럼 이후를 모두 비고로 합친다.
    """
    rows = []
    with open(path, encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            if not row or not row[0].strip():
                continue
            if len(row) >= 4:
                ip = row[0].strip()
                cur_kw = row[1].strip()
                add_kw = row[2].strip()
                note = ",".join(row[3:]).strip()
                rows.append(
                    {"ip": ip, "현재키워드": cur_kw, "추가할키워드": add_kw, "비고": note}
                )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 메인 로직
# ---------------------------------------------------------------------------

def main():
    sys.stdout.reconfigure(encoding="utf-8")

    # 1. ip_nodes_final 로드
    ip_df = pd.read_parquet(OUT_PATH)
    ip_df["키워드_final"] = ip_df["키워드_final"].apply(as_list)
    print(f"[로드] ip_nodes_final: {len(ip_df)}개 IP")

    # IP명 → 인덱스 맵
    ip_to_idx = {name: i for i, name in enumerate(ip_df["ip_name"])}

    # 원본 복사본 (비교용)
    original_kws = ip_df["키워드_final"].copy()

    # 제외 플래그된 IP 목록
    excluded_ips = []

    # -----------------------------------------------------------------------
    # 2. 메인 파일 (index 0~172: APEC ~ 연세우유)
    # -----------------------------------------------------------------------
    main_path = HIN_DIR / "ip_kw_candidates_final.csv"
    main_df = read_review_csv(main_path)
    print(f"\n[메인 파일] {len(main_df)}개 행 로드")

    not_found_main = []
    for _, row in main_df.iterrows():
        ip = row["ip"]
        if ip not in ip_to_idx:
            not_found_main.append(ip)
            continue
        idx = ip_to_idx[ip]
        current = ip_df.at[idx, "키워드_final"]
        updated, excl = apply_update(current, row["추가할키워드"], row["비고"])
        if excl:
            excluded_ips.append(ip)
        else:
            ip_df.at[idx, "키워드_final"] = updated

    if not_found_main:
        print(f"  ⚠ ip_nodes_final에 없는 IP ({len(not_found_main)}개): {not_found_main}")

    # -----------------------------------------------------------------------
    # 3. 원준 파일에서 index 173~198 (옐로우즈 ~ 잠바주스)
    # -----------------------------------------------------------------------
    WONJUN_TARGET = set([
        "옐로우즈", "오구", "오늘뭐먹지", "오뎅", "오징어게임", "온더고", "올드제과",
        "요아정", "위글위글", "유로모니터인터내셔널", "유미의세포들", "유해진", "윤봉길",
        "이나피스퀘어", "이모카세", "이봉원", "이석원명장", "이세계아이돌", "이스타항공",
        "이승우", "이원일", "이장우", "이정후", "이제훈", "잠뜰", "잠바주스",
    ])

    wonjun_path = HIN_DIR / "ip_kw_candidates_final_원준.csv"
    wonjun_df = read_review_csv(wonjun_path)
    wonjun_df = wonjun_df[wonjun_df["ip"].isin(WONJUN_TARGET)].reset_index(drop=True)
    print(f"\n[원준 파일] 대상 IP {len(wonjun_df)}개 ({len(WONJUN_TARGET)}개 중)")

    not_found_wonjun = []
    for _, row in wonjun_df.iterrows():
        ip = row["ip"]
        if ip not in ip_to_idx:
            not_found_wonjun.append(ip)
            continue
        idx = ip_to_idx[ip]
        current = ip_df.at[idx, "키워드_final"]
        updated, excl = apply_update(current, row["추가할키워드"], row["비고"])
        if excl:
            excluded_ips.append(ip)
        else:
            ip_df.at[idx, "키워드_final"] = updated

    missing_wonjun = WONJUN_TARGET - set(wonjun_df["ip"])
    if missing_wonjun:
        print(f"  ⚠ 원준 파일에 없는 대상 IP: {missing_wonjun}")
    if not_found_wonjun:
        print(f"  ⚠ ip_nodes_final에 없는 IP: {not_found_wonjun}")

    # -----------------------------------------------------------------------
    # 4. 지윤 파일 (index 199~224: 장민호 ~ 카트라이더) — 직접 대체
    #    현재키워드를 새 기준으로 사용하고, 비고의 제거 키워드만 추가 적용
    # -----------------------------------------------------------------------
    jiyun_path = HIN_DIR / "ip_kw_candidates_final_지윤(장민호-카트라이더).csv"
    jiyun_df = read_review_csv(jiyun_path)
    print(f"\n[지윤 파일] {len(jiyun_df)}개 행 로드 (직접 대체 방식)")

    not_found_jiyun = []
    for _, row in jiyun_df.iterrows():
        ip = row["ip"]
        if ip not in ip_to_idx:
            not_found_jiyun.append(ip)
            continue
        idx = ip_to_idx[ip]
        # 현재키워드를 새 기준으로 설정
        base = parse_kw_str(row["현재키워드"])
        remove_list, excl = parse_remove_note(row["비고"])
        if excl:
            excluded_ips.append(ip)
        else:
            updated = [k for k in base if k not in remove_list]
            ip_df.at[idx, "키워드_final"] = updated

    if not_found_jiyun:
        print(f"  ⚠ ip_nodes_final에 없는 IP ({len(not_found_jiyun)}개): {not_found_jiyun}")

    # -----------------------------------------------------------------------
    # 5. 유미 파일 (index 225~280: 캐치티니핑 ~ 한촌설렁탕) — 직접 대체
    # -----------------------------------------------------------------------
    yumi_path = HIN_DIR / "유미)ip_nodes_reviewed_until_catch_teenieping.csv"
    yumi_df = pd.read_csv(yumi_path)
    print(f"\n[유미 파일] {len(yumi_df)}개 행 로드")

    not_found_yumi = []
    for _, row in yumi_df.iterrows():
        ip = str(row["ip_name"]).strip()
        kw_val = row["키워드_final"]
        if ip not in ip_to_idx:
            not_found_yumi.append(ip)
            continue
        idx = ip_to_idx[ip]
        ip_df.at[idx, "키워드_final"] = parse_kw_str(kw_val)

    if not_found_yumi:
        print(f"  ⚠ ip_nodes_final에 없는 IP ({len(not_found_yumi)}개): {not_found_yumi}")

    # -----------------------------------------------------------------------
    # 6. 결과 요약 출력
    # -----------------------------------------------------------------------
    changed = 0
    for i in range(len(ip_df)):
        before = list(original_kws.iloc[i]) if original_kws.iloc[i] else []
        after = list(ip_df.at[i, "키워드_final"]) if ip_df.at[i, "키워드_final"] else []
        if before != after:
            changed += 1

    print(f"\n[결과] 키워드 변경된 IP: {changed} / {len(ip_df)}개")

    if excluded_ips:
        print(f"\n[주의] IP 제외 플래그 (키워드 변경 없음, 수동 처리 필요):")
        for ip in excluded_ips:
            print(f"  - {ip}")

    # -----------------------------------------------------------------------
    # 7. 저장
    # -----------------------------------------------------------------------
    ip_df.to_parquet(OUT_PATH, index=False)
    print(f"\n[저장 완료] {OUT_PATH}")
    print(f"  ip 수: {len(ip_df)}, 컬럼: {ip_df.columns.tolist()}")


if __name__ == "__main__":
    main()
