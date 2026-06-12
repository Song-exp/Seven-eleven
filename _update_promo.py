import sys; sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd
from collections import defaultdict

OUT = "data/processed/hin/최종/product_promo_keywords.csv"

# B5 vocab
b5 = pd.read_excel("data/raw/B5_MNM_DATA.xlsx")
promo_vocab = set()
for v in b5["행사유형"].dropna(): promo_vocab.add(str(v).strip())
for v in b5["행사형태"].dropna(): promo_vocab.add(str(v).strip())

# 기존 product_promo_keywords 로드
existing = pd.read_csv(OUT, encoding='utf-8-sig')
existing["ITEM_CD"] = existing["ITEM_CD"].astype(str).str.strip()
existing_map = {}  # ITEM_CD → set of kws
for _, row in existing.iterrows():
    kws = {k.strip() for k in str(row["프로모션_키워드"]).split(",") if k.strip()}
    existing_map[row["ITEM_CD"]] = kws

print(f"기존 파일: {len(existing)}개 제품")

# pos_product_features에서 프로모션 정보 추출
pos = pd.read_parquet("data/processed/pos_product_features.parquet")
pos["ITEM_CD"] = pos["ITEM_CD"].astype(str).str.strip()
pos_promo = pos[pos["has_promo_30d"] == True].copy()
print(f"pos has_promo_30d=True: {len(pos_promo)}개 제품")

def parse_promo_col(val):
    if pd.isna(val): return set()
    tokens = set()
    for t in str(val).split("|"):
        t = t.strip()
        if t in promo_vocab:
            tokens.add(t)
    return tokens

pos_promo_map = {}  # ITEM_CD → set of kws
for _, row in pos_promo.iterrows():
    cd = row["ITEM_CD"]
    kws = parse_promo_col(row["promo_types_30d"]) | parse_promo_col(row["promo_categories_30d"])
    if kws:
        pos_promo_map[cd] = kws

print(f"pos 프로모션 키워드 있는 제품: {len(pos_promo_map)}개")

# 비교 및 업데이트
added_new = 0
updated_existing = 0
added_kws_count = 0

# ITEM_CD → 제품명 (기존 파일 + pos)
cd_to_nm = existing.set_index("ITEM_CD")["제품명"].to_dict()
pos_nm = pos.set_index("ITEM_CD")["ITEM_NM"].to_dict()

merged_map = dict(existing_map)  # 복사

for cd, kws in pos_promo_map.items():
    if cd in merged_map:
        before = len(merged_map[cd])
        merged_map[cd] = merged_map[cd] | kws
        diff = len(merged_map[cd]) - before
        if diff > 0:
            updated_existing += 1
            added_kws_count += diff
        if cd not in cd_to_nm:
            cd_to_nm[cd] = pos_nm.get(cd, "")
    else:
        merged_map[cd] = kws
        cd_to_nm[cd] = pos_nm.get(cd, "")
        added_new += 1

print(f"\n신규 추가 제품: {added_new}개")
print(f"기존 제품 키워드 보강: {updated_existing}개 (키워드 {added_kws_count}개 추가)")
print(f"최종 제품 수: {len(merged_map)}개")

# 저장
rows = []
for cd, kws in sorted(merged_map.items(), key=lambda x: str(x[0])):
    rows.append({
        "ITEM_CD": cd,
        "제품명": cd_to_nm.get(cd, ""),
        "프로모션_키워드": ", ".join(sorted(kws)),
    })
out_df = pd.DataFrame(rows)
out_df.to_csv(OUT, index=False, encoding="utf-8-sig")
print(f"\n저장 완료: {OUT}")
