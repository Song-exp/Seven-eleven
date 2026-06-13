import sys; sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd
import os
from openpyxl import load_workbook

# ── 1. pfk 키워드에서 NCT 제거 ──────────────────────
SRC = "data/processed/hin/최종/product_final_keywords.csv"
TMP = "data/processed/hin/최종/_pfk_tmp.csv"

pfk = pd.read_csv(SRC, encoding='utf-8-sig')

nct_ids = ['GS25_NCTD아이돌컴백샌드', 'GS25_NCTDREAMXGS25아이돌컴백샌드위치']

def remove_nct(val):
    if pd.isna(val): return val
    kws = [k.strip() for k in str(val).split(',')]
    kws = [k for k in kws if k.upper() != 'NCT']
    return ', '.join(kws)

for iid in nct_ids:
    mask = pfk['ITEM_CD'] == iid
    before = pfk.loc[mask, '키워드'].values[0]
    pfk.loc[mask, '키워드'] = pfk.loc[mask, '키워드'].apply(remove_nct)
    after = pfk.loc[mask, '키워드'].values[0]
    print(f'[pfk 키워드] {iid}')
    print(f'  전: {before}')
    print(f'  후: {after}')

pfk.to_csv(TMP, index=False, encoding='utf-8-sig')
os.remove(SRC)
os.rename(TMP, SRC)
print()

# ── 2. Sheet1에 누락 제품 추가 ──────────────────────
XLSX = "data/processed/hin/최종/product_ip_mapping.xlsx"
wb = load_workbook(XLSX)
ws = wb['제품_IP_매핑']

# 마지막 행 다음에 추가
new_row = ['NCTD아이돌컴백샌드', 'NCTDREAM', 'B']
ws.append(new_row)
wb.save(XLSX)
print(f'[Sheet1 추가] {new_row}')
print('완료')
