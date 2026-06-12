# -*- coding: utf-8 -*-
import pandas as pd
import json

origin_csv_path = "data/processed/hin/product_nodes.csv"
df = pd.read_csv(origin_csv_path, encoding="utf-8-sig")

# 키워드 개수 계산
def parse_kws(x):
    if pd.isna(x) or not x:
        return []
    return [k.strip() for k in str(x).split(",") if k.strip()]

df["kws_list"] = df["키워드_final"].apply(parse_kws)
df["kws_len"] = df["kws_list"].apply(len)

# 2차 정렬 고정 (kws_len 내림차순 -> ITEM_CD 오름차순)
df_sorted = df.sort_values(by=["kws_len", "ITEM_CD"], ascending=[False, True])

# 3501~3550위 추출 (0-based index: 3500 ~ 3549)
next_batch = df_sorted.iloc[3500:3550].copy()

def norm_id(x):
    try:
        val = str(int(float(x)))
    except Exception:
        val = str(x)
    return val.strip()

batch_list = []
for idx, row in next_batch.iterrows():
    batch_list.append({
        "ITEM_CD": norm_id(row["ITEM_CD"]),
        "ITEM_NM": row["ITEM_NM"],
        "편의점명": row["편의점명"],
        "키워드_final": row["키워드_final"],
        "kws_list": row["kws_list"]
    })

out_json = r"C:\Users\송정현\.gemini\antigravity-cli\brain\4e0bae23-7d9c-433c-9b96-841d4916e764\scratch\next_3501_3550_raw.json"
with open(out_json, "w", encoding="utf-8") as f:
    json.dump(batch_list, f, ensure_ascii=False, indent=2)

print(f"Extracted {len(batch_list)} items to {out_json}")
