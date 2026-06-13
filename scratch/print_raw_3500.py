# -*- coding: utf-8 -*-
import json

in_json = r"C:\Users\송정현\.gemini\antigravity-cli\brain\4e0bae23-7d9c-433c-9b96-841d4916e764\scratch\next_3451_3500_raw.json"
out_txt = r"C:\Users\송정현\.gemini\antigravity-cli\brain\4e0bae23-7d9c-433c-9b96-841d4916e764\scratch\raw_3500_printed.txt"

with open(in_json, "r", encoding="utf-8") as f:
    items = json.load(f)

with open(out_txt, "w", encoding="utf-8") as f:
    for i, item in enumerate(items):
        idx = 3451 + i
        f.write(f"[{idx}] {item['ITEM_NM']} (CD: {item['ITEM_CD']}, 편의점명: {item['편의점명']})\n")
        raw_kws = ", ".join(item["kws_list"]) if item["kws_list"] else "(None)"
        f.write(f"Raw Keywords: {raw_kws}\n\n")

print(f"Printed raw text for 3500 to {out_txt}")
