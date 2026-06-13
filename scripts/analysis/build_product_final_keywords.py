"""
검토_final → 최종 제품별 키워드 CSV 생성
출력 스키마: ITEM_CD | 제품명 | 키워드 | IP

처리 규칙:
  살릴=제거          → 행 제거
  살릴=O,  죽일=없음 → 관련있음
  살릴=O,  죽일=있음 → 관련있음 - 죽일
  살릴=텍스트, 죽일=없음 → 관련있음 ∪ 살릴
  살릴=텍스트, 죽일=있음 → (관련있음 ∪ 살릴) - 죽일
  살릴=빈값, 죽일=없음   → 관련있음
  살릴=빈값, 죽일=있음   → 관련있음 - 죽일
  프로모션 코드(0xxx :...) 자동 제거
"""
import io
import re
import sys

import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

BASE = "data/processed/hin/제품별키워드처리/"
PROMO = re.compile(r"^\d{3,4}\s*:")  # 0106 : 단품할인 등

# ─────────────────────────────────────────────
# 유틸
# ─────────────────────────────────────────────
def is_empty(v) -> bool:
    if pd.isna(v):
        return True
    return str(v).strip() == ""


def parse_kw(v) -> list[str]:
    """쉼표 구분 키워드 → 정제 리스트 (프로모션 코드 자동 제거)"""
    if is_empty(v):
        return []
    tokens = [x.strip() for x in str(v).split(",") if x.strip()]
    return [t for t in tokens if not PROMO.match(t)]


def build_keywords(row) -> str:
    """4컬럼 조합 → 최종 키워드 문자열 (쉼표 구분, 순서 유지)"""
    관련있음 = parse_kw(row["관련있음"])
    살릴_raw = str(row["살릴키워드"]).strip() if not is_empty(row["살릴키워드"]) else ""
    죽일_set = set(parse_kw(row["죽일키워드"]))

    # 관련있음을 base로
    if 살릴_raw not in ("O", ""):
        추가 = parse_kw(살릴_raw)
    else:
        추가 = []

    # 순서 유지하며 합집합
    seen: set[str] = set()
    merged: list[str] = []
    for kw in 관련있음 + 추가:
        if kw and kw not in seen and kw not in 죽일_set:
            seen.add(kw)
            merged.append(kw)

    return ", ".join(merged)


# ─────────────────────────────────────────────
# 1. 로드
# ─────────────────────────────────────────────
검토 = pd.read_csv(
    BASE + "product_nodes_keyword_analysis_검토_final.csv", encoding="utf-8-sig"
)
pn = pd.read_parquet("data/processed/hin/product_nodes.parquet")
nm_to_cd = pn.set_index("ITEM_NM")["ITEM_CD"].to_dict()

print(f"[로드] 검토_final: {len(검토)}행 / product_nodes: {len(pn)}행")

# ─────────────────────────────────────────────
# 2. 살릴=제거 행 제거
# ─────────────────────────────────────────────
살릴_val = 검토["살릴키워드"].apply(lambda x: str(x).strip() if not is_empty(x) else "")
mask_remove = 살릴_val == "제거"
mask_name_remove = 검토["제품명"].str.strip() == "제거"
n_remove = (mask_remove | mask_name_remove).sum()
검토 = 검토[~(mask_remove | mask_name_remove)].copy().reset_index(drop=True)
print(f"[제거] 살릴=제거 또는 제품명=제거: {n_remove}개 행 제거 → {len(검토)}행")

# ─────────────────────────────────────────────
# 3. 상품명 수정
# ─────────────────────────────────────────────
NAME_FIX = {
    "주)백종원스페셜우삼격": "주)백종원스페셜우삼겹",
    "간장계란소스": "돈키호테간장계란소스",
    "유자후추": "굵게간유자후추",
}
for old, new in NAME_FIX.items():
    mask = 검토["제품명"] == old
    if mask.sum():
        검토.loc[mask, "제품명"] = new
        print(f"[상품명 수정] {old} → {new}")

# ─────────────────────────────────────────────
# 4. 최종 키워드 생성
# ─────────────────────────────────────────────
검토["키워드"] = 검토.apply(build_keywords, axis=1)

# ─────────────────────────────────────────────
# 5. ITEM_CD 매핑
#    상품명이 수정된 경우 구 이름으로도 검색
# ─────────────────────────────────────────────
old_name_map = {new: old for old, new in NAME_FIX.items()}


def get_item_cd(prod_nm: str):
    if prod_nm in nm_to_cd:
        return nm_to_cd[prod_nm]
    if prod_nm in old_name_map:
        return nm_to_cd.get(old_name_map[prod_nm])
    return None


검토["ITEM_CD"] = 검토["제품명"].apply(get_item_cd)
n_no_cd = 검토["ITEM_CD"].isna().sum()
if n_no_cd:
    print(f"[경고] ITEM_CD 미매핑 {n_no_cd}개:")
    print(검토[검토["ITEM_CD"].isna()]["제품명"].tolist())

# ─────────────────────────────────────────────
# 6. IP 컬럼
# ─────────────────────────────────────────────
검토["IP"] = 검토["ip 비고"].apply(lambda x: str(x).strip() if not is_empty(x) else "")

# ─────────────────────────────────────────────
# 7. 결과 정리 및 통계
# ─────────────────────────────────────────────
result = 검토[["ITEM_CD", "제품명", "키워드", "IP"]].copy()

n_kw     = (result["키워드"] != "").sum()
n_no_kw  = (result["키워드"] == "").sum()
n_ip     = (result["IP"] != "").sum()

print(f"\n[최종 통계]")
print(f"  총 행수      : {len(result)}")
print(f"  키워드 있음  : {n_kw}")
print(f"  키워드 없음  : {n_no_kw}")
print(f"  IP 있음      : {n_ip}")

# 케이스별 결과 확인
sal = 검토["살릴키워드"].apply(lambda x: str(x).strip() if not is_empty(x) else "")
juk = 검토["죽일키워드"].apply(lambda x: not is_empty(x))
print(f"\n[케이스별 수량]")
print(f"  A (살릴=O,    죽일=없음) : {((sal=='O') & ~juk).sum()}")
print(f"  B (살릴=O,    죽일=있음) : {((sal=='O') &  juk).sum()}")
print(f"  C (살릴=텍스트, 죽일=없음): {((~sal.isin(['','O'])) & ~juk).sum()}")
print(f"  D (살릴=텍스트, 죽일=있음): {((~sal.isin(['','O'])) &  juk).sum()}")
print(f"  F (살릴=빈,   죽일=없음) : {((sal=='') & ~juk).sum()}")
print(f"  G (살릴=빈,   죽일=있음) : {((sal=='') &  juk).sum()}")

# ─────────────────────────────────────────────
# 8. 저장
# ─────────────────────────────────────────────
out_path = BASE + "product_final_keywords.csv"
result.to_csv(out_path, index=False, encoding="utf-8-sig")
print(f"\n[저장] {out_path}")
