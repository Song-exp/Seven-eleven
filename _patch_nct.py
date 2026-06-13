import sys; sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd
import os

SRC = "data/processed/hin/최종/product_final_keywords.csv"
TMP = "data/processed/hin/최종/_pfk_tmp.csv"

pfk = pd.read_csv(SRC, encoding='utf-8-sig')
mask = pfk['ITEM_CD'] == 'GS25_NCTD아이돌컴백샌드'
pfk.loc[mask, 'IP'] = 'NCTDREAM'
pfk.to_csv(TMP, index=False, encoding='utf-8-sig')

os.remove(SRC)
os.rename(TMP, SRC)
print('완료')
print(pfk[pfk['ITEM_CD'].str.contains('NCT', na=False)][['ITEM_CD','제품명','IP']].to_string())
