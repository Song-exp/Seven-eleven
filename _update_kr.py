import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd
import ast
from collections import defaultdict

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

def parse_kw_list(val):
    if pd.isna(val):
        return []
    val = str(val).strip()
    if val.startswith('['):
        try:
            items = ast.literal_eval(val)
            return [k.strip() for k in items if isinstance(k, str)]
        except:
            cleaned = val.strip('[]').replace("'", '').replace('"', '')
            return [k.strip() for k in cleaned.split(',') if k.strip()]
    else:
        return [k.strip() for k in val.split(',') if k.strip()]

# 소스 로드
pfk = pd.read_csv('data/processed/hin/최종/product_final_keywords.csv', encoding='utf-8-sig')
xl = pd.ExcelFile('data/processed/hin/최종/product_ip_mapping.xlsx')
sheet2 = xl.parse('IP별_키워드')

# 키워드 → 등장 제품 목록
kw_to_products = defaultdict(list)
for _, row in pfk.iterrows():
    kws = parse_kw_list(row['키워드'])
    for kw in kws:
        kw_to_products[kw].append(str(row['제품명']))

# 키워드 → 등장 IP 목록
kw_to_ips = defaultdict(list)
for _, row in sheet2.iterrows():
    kws = parse_kw_list(row['키워드_final'])
    for kw in kws:
        kw_to_ips[kw].append(str(row['ip_name']))

# 기존 keyword_review 로드
kr = pd.read_csv('data/processed/hin/최종/keyword_review.csv', encoding='utf-8-sig')

# 12개 삭제
to_remove = {'광주', '괴짜', '대중', '루틴', '외모', '전문점', '즐거움', '콜라겐', '탄수화물', '함경도', '향토', '효소'}
before = len(kr)
kr = kr[~kr['키워드'].isin(to_remove)].copy()
print(f'삭제: {before} → {len(kr)} (제거 {before - len(kr)}개)')

# 33개 추가
kr_kws = set(kr['키워드'].dropna().str.strip())
all_src = parse_kw_col(pfk['키워드']) | parse_kw_col(sheet2['키워드_final'])
missing = sorted(all_src - kr_kws)
print(f'추가할 키워드: {len(missing)}개')

new_rows = []
for kw in missing:
    products = kw_to_products.get(kw, [])
    ips = kw_to_ips.get(kw, [])
    freq = len(products) + len(ips)
    new_rows.append({
        '키워드': kw,
        '등장_제품': ', '.join(products) if products else '',
        '등장_IP': ', '.join(ips) if ips else '',
        '빈도': freq,
        '검수': None
    })

new_df = pd.DataFrame(new_rows)
kr_updated = pd.concat([kr, new_df], ignore_index=True)
kr_updated = kr_updated.sort_values('키워드').reset_index(drop=True)

print(f'최종 키워드 수: {len(kr_updated)}')

# 저장
kr_updated.to_csv('data/processed/hin/최종/keyword_review.csv', index=False, encoding='utf-8-sig')
print('저장 완료: data/processed/hin/최종/keyword_review.csv')
print()
print('추가된 키워드:')
for k in missing:
    print(f'  {k}')
