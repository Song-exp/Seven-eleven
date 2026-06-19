import csv
import re
import sys
import numpy as np
import pandas as pd
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

HIN_DIR = Path(__file__).resolve().parents[1] / "data" / "processed" / "hin"


def parse_kw_str(s):
    if not s or (isinstance(s, float) and np.isnan(s)):
        return []
    return [k.strip() for k in str(s).split(",") if k.strip()]


def as_list(val):
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return []
    if isinstance(val, (list, np.ndarray)):
        return list(val)
    return parse_kw_str(str(val))


ip_df = pd.read_parquet(HIN_DIR / "ip_nodes_final.parquet")
ip_df["키워드_final"] = ip_df["키워드_final"].apply(as_list)
ip_to_idx = {name: i for i, name in enumerate(ip_df["ip_name"])}

# ─── 지윤 파일 ───────────────────────────────────────────────
print("=" * 65)
print("【지윤 파일】 현재키워드(CSV) vs ip_nodes_final 현재 상태")
print("처리 방식: 추가/제거 적용 (추가할키워드=없음, 제거만)")
print("=" * 65)

rows = []
with open(
    HIN_DIR / "ip_kw_candidates_final_지윤(장민호-카트라이더).csv",
    encoding="utf-8",
    newline="",
) as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        if not row or not row[0].strip():
            continue
        if len(row) >= 3:
            rows.append(
                {
                    "ip": row[0].strip(),
                    "cur": row[1].strip(),
                    "note": row[3].strip() if len(row) > 3 else "",
                }
            )

diff_count = 0
for r in rows:
    ip = r["ip"]
    if ip not in ip_to_idx:
        continue
    csv_kws = parse_kw_str(r["cur"])
    actual_kws = ip_df.at[ip_to_idx[ip], "키워드_final"]
    missing = [k for k in csv_kws if k not in actual_kws]
    extra = [k for k in actual_kws if k not in csv_kws]
    if missing or extra:
        diff_count += 1
        print(f"\n[불일치] {ip}")
        if missing:
            print(f"  CSV 현재키워드에 있지만 ip_final에 없음: {missing}")
        if extra:
            print(f"  ip_final에만 있음 (CSV에 없음):         {extra}")
        print(f"  비고: {r['note']}")
    else:
        print(f"[일치]   {ip}")

print(f"\n→ 지윤 파일 총 {len(rows)}개 중 불일치 {diff_count}개")

# ─── 유미 파일 ───────────────────────────────────────────────
print()
print("=" * 65)
print("【유미 파일】 키워드_final(CSV) vs ip_nodes_final 현재 상태")
print("처리 방식: 직접 대체 (덮어쓰기)")
print("=" * 65)

yumi_df = pd.read_csv(
    HIN_DIR / "유미)ip_nodes_reviewed_until_catch_teenieping.csv"
)

diff_count_y = 0
for _, row in yumi_df.iterrows():
    ip = str(row["ip_name"]).strip()
    csv_kws = parse_kw_str(row["키워드_final"])
    if ip not in ip_to_idx:
        print(f"[없음]   {ip} ← ip_nodes_final에 없는 IP")
        continue
    actual_kws = ip_df.at[ip_to_idx[ip], "키워드_final"]
    missing = [k for k in csv_kws if k not in actual_kws]
    extra = [k for k in actual_kws if k not in csv_kws]
    if missing or extra:
        diff_count_y += 1
        print(f"\n[불일치] {ip}")
        if missing:
            print(f"  CSV에 있지만 ip_final에 없음: {missing}")
        if extra:
            print(f"  ip_final에만 있음:            {extra}")
    else:
        print(f"[일치]   {ip}")

print(f"\n→ 유미 파일 총 {len(yumi_df)}개 중 불일치 {diff_count_y}개")
