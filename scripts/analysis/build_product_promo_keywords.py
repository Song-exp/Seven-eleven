"""
검토_final.csv → 제품별 프로모션 키워드 CSV 생성
출력 스키마: ITEM_CD | 제품명 | 프로모션_키워드

처리 방식:
  B5_MNM_DATA.xlsx 에서 행사유형·행사형태 전체 종류를 뽑아 프로모션 키워드 목록 정의
  검토_final.csv 관련있음 컬럼에서 해당 키워드가 포함된 제품만 추출
  죽일키워드에 명시된 프로모션 키워드는 제거
  살릴=제거 / 제품명=제거 행 건너뜀
"""
import io
import re
import sys

import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

OUT = "data/processed/hin/최종/product_promo_keywords.csv"


def is_empty(v) -> bool:
    if pd.isna(v):
        return True
    return str(v).strip() == ""


# ─────────────────────────────────────────────
# 1. B5에서 프로모션 키워드 종류 수집
# ─────────────────────────────────────────────
b5 = pd.read_excel("data/raw/B5_MNM_DATA.xlsx")

promo_vocab: set[str] = set()
for v in b5["행사유형"].dropna():
    promo_vocab.add(str(v).strip())
for v in b5["행사형태"].dropna():
    promo_vocab.add(str(v).strip())

print(f"[B5] 프로모션 키워드 종류: {len(promo_vocab)}개")
for kw in sorted(promo_vocab):
    print(f"  {kw}")
print()

# ─────────────────────────────────────────────
# 2. 검토_final 로드 및 전처리
# ─────────────────────────────────────────────
BASE = "data/processed/hin/제품별키워드처리/"
검토 = pd.read_csv(
    BASE + "product_nodes_keyword_analysis_검토_final.csv", encoding="utf-8-sig"
)
pn = pd.read_parquet("data/processed/hin/구버전/product_nodes.parquet")
nm_to_cd = pn.set_index("ITEM_NM")["ITEM_CD"].to_dict()

# 살릴=제거 / 제품명=제거 행 제거
살릴_val = 검토["살릴키워드"].apply(lambda x: str(x).strip() if not is_empty(x) else "")
mask_remove = (살릴_val == "제거") | (검토["제품명"].str.strip() == "제거")
검토 = 검토[~mask_remove].copy().reset_index(drop=True)

# 상품명 수정
NAME_FIX = {
    "주)백종원스페셜우삼격": "주)백종원스페셜우삼겹",
    "간장계란소스": "돈키호테간장계란소스",
    "유자후추": "굵게간유자후추",
}
for old, new in NAME_FIX.items():
    검토.loc[검토["제품명"] == old, "제품명"] = new

print(f"[검토] {len(검토)}행")

# ─────────────────────────────────────────────
# 3. 제품별 프로모션 키워드 추출
# ─────────────────────────────────────────────
rows = []
for _, row in 검토.iterrows():
    val = str(row.get("관련있음", ""))
    kill = {t.strip() for t in str(row.get("죽일키워드", "")).split(",") if t.strip()}

    found = [
        t.strip() for t in val.split(",")
        if t.strip() in promo_vocab and t.strip() not in kill
    ]
    if not found:
        continue

    nm = str(row["제품명"]).strip()
    cd = nm_to_cd.get(nm, "")
    rows.append({
        "ITEM_CD": cd,
        "제품명": nm,
        "프로모션_키워드": ", ".join(found),
    })

out = pd.DataFrame(rows)
no_cd = (out["ITEM_CD"] == "").sum()
if no_cd:
    print(f"[경고] ITEM_CD 미매핑: {no_cd}개")

out.to_csv(OUT, index=False, encoding="utf-8-sig")
print(f"[저장] {OUT}  ({len(out)}행)")

# 키워드별 등장 제품 수
from collections import Counter
all_codes = []
for v in out["프로모션_키워드"]:
    all_codes.extend([k.strip() for k in v.split(",") if k.strip()])
counter = Counter(all_codes)
print()
print("키워드별 등장 제품 수:")
for code, cnt in sorted(counter.items(), key=lambda x: -x[1]):
    print(f"  [{cnt:4d}건] {code}")
