import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd
import ast

def parse_kw_col(series):
    kws = set()
    for val in series.dropna():
        val = str(val).strip()
        if val.startswith('['):
            try:
                items = ast.literal_eval(val)
                kws.update([k.strip() for k in items if isinstance(k, str)])
            except:
                cleaned = val.strip('[]').replace("'", '').replace('"', '')
                for k in cleaned.split(','):
                    k = k.strip()
                    if k:
                        kws.add(k)
        else:
            for k in val.split(','):
                k = k.strip()
                if k:
                    kws.add(k)
    return kws

# 1. keyword_review 키워드 목록
kr = pd.read_csv('data/processed/hin/최종/keyword_review.csv', encoding='utf-8-sig')
kr_kws = set(kr['키워드'].dropna().str.strip())
print(f'keyword_review 키워드 수: {len(kr_kws)}')

# 2. product_final_keywords.csv 키워드
pfk = pd.read_csv('data/processed/hin/최종/product_final_keywords.csv', encoding='utf-8-sig')
pfk_kws = parse_kw_col(pfk['키워드'])
print(f'product_final_keywords 키워드 수: {len(pfk_kws)}')

# 3. IP별_키워드 Sheet2
xl = pd.ExcelFile('data/processed/hin/최종/product_ip_mapping.xlsx')
sheet2 = xl.parse('IP별_키워드')
ip_kws = parse_kw_col(sheet2['키워드_final'])
print(f'IP별_키워드 키워드 수: {len(ip_kws)}')

all_src = pfk_kws | ip_kws
print(f'소스 합집합: {len(all_src)}')
print()

missing = all_src - kr_kws
extra = kr_kws - all_src
print(f'소스에 있는데 kr에 없는 것: {len(missing)}개')
print(f'kr에 있는데 소스에 없는 것: {len(extra)}개')
print()

if missing:
    print('누락 샘플 (30개):')
    for k in sorted(missing)[:30]:
        print(' ', repr(k))

if extra:
    print()
    print('kr에만 있는 것:')
    for k in sorted(extra):
        print(' ', repr(k))
