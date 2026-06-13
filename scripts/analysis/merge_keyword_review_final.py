"""
2단계 병합: 6개 개인 검토 파일 → 마스터 통합
저장: product_nodes_keyword_analysis_검토_final.csv

결정사항:
- 달너새랜덤초코과자: 두 행 살릴키워드 합치기 (껍질, 바삭, 달콤)
- 스마일메롱바 → 메롱바 이름 매핑 (개인 파일 경은, 빈값이므로 실질 영향 없음)
- ★베스트바구니: 마스터에서 제거
- 짐빔파인애플·짐빔허니·달너새랜덤초코과자: 마스터 중복 행 하나씩만 유지
"""
import io
import sys

import numpy as np
import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

BASE = "data/processed/hin/제품별키워드처리/"
MERGE_COLS = ["살릴키워드", "죽일키워드", "ip 비고", "상품명 수정"]


# ──────────────────────────────────────────────
# 유틸
# ──────────────────────────────────────────────
def is_empty(val) -> bool:
    if pd.isna(val):
        return True
    return str(val).strip() == ""


def merge_two_vals(m_val, p_val):
    """병합 규칙 단일 컬럼 적용."""
    m_empty = is_empty(m_val)
    p_empty = is_empty(p_val)

    if m_empty and p_empty:
        return np.nan
    if m_empty:
        return str(p_val).strip()
    if p_empty:
        return str(m_val).strip()

    m_str = str(m_val).strip()
    p_str = str(p_val).strip()

    if m_str == p_str:
        return m_str
    if m_str == "O" and p_str != "O":
        return p_str
    if m_str != "O" and p_str == "O":
        return m_str

    # 둘 다 구체적 텍스트 → 쉼표 분리 후 중복 제거 합치기
    m_parts = [x.strip() for x in m_str.split(",") if x.strip()]
    p_parts = [x.strip() for x in p_str.split(",") if x.strip()]
    seen = set()
    merged = []
    for x in m_parts + p_parts:
        if x not in seen:
            seen.add(x)
            merged.append(x)
    return ", ".join(merged)


def resolve_personal_rows(rows: pd.DataFrame) -> pd.Series:
    """동일 제품명의 복수 개인 파일 행을 컬럼별 병합 규칙으로 단일 행으로 통합."""
    if len(rows) == 1:
        return rows.iloc[0]

    base = rows.iloc[0].copy()
    for col in MERGE_COLS:
        if col not in rows.columns:
            continue
        vals = [str(v).strip() for v in rows[col].tolist() if not is_empty(v)]
        if not vals:
            base[col] = np.nan
            continue
        if len(vals) == 1:
            base[col] = vals[0]
            continue
        # 여러 값 합치기
        seen = set()
        merged = []
        for v in vals:
            for x in [x.strip() for x in v.split(",") if x.strip()]:
                if x not in seen:
                    seen.add(x)
                    merged.append(x)
        base[col] = ", ".join(merged)
    return base


# ──────────────────────────────────────────────
# 1. 마스터 로드
# ──────────────────────────────────────────────
master = pd.read_csv(
    BASE + "product_nodes_keyword_analysis_검토.csv", encoding="utf-8-sig"
)
print(f"[마스터] 원본: {len(master)}행")

# ──────────────────────────────────────────────
# 2. 마스터 중복 3건 — 첫 행만 유지
# ──────────────────────────────────────────────
for prod in ["짐빔파인애플", "짐빔허니", "달너새랜덤초코과자"]:
    mask = master["제품명"] == prod
    if mask.sum() > 1:
        drop_idx = master.index[mask][1:]
        master = master.drop(index=drop_idx)
        print(f"  마스터 중복 제거: {prod} ({len(drop_idx)}행 제거)")

# ──────────────────────────────────────────────
# 3. ★베스트바구니 마스터에서 제거
# ──────────────────────────────────────────────
before = len(master)
master = master[master["제품명"] != "★베스트바구니"].copy()
removed = before - len(master)
if removed:
    print(f"  ★베스트바구니 {removed}행 제거")

master = master.reset_index(drop=True)
print(f"[마스터] 전처리 후: {len(master)}행\n")

# ──────────────────────────────────────────────
# 4. 개인 파일 6개 로드
# ──────────────────────────────────────────────
personal_files = {
    "경은": "검토최종(1-200)_경은.xlsx",
    "지윤": "검토최종(201-400)_지윤.xlsx",
    "현호": "검토최종(401-600)_현호.xlsx",
    "유미": "검토최종(601-800)_유미.xlsx",
    "원준": "검토최종(801-1000)_원준.xlsx",
    "정현": "검토최종(나머지)_정현.xlsx",
}
dfs = []
for name, fname in personal_files.items():
    df = pd.read_excel(BASE + fname)
    df["_source"] = name
    dfs.append(df)
personal = pd.concat(dfs, ignore_index=True)
print(f"[개인 파일] 합계: {len(personal)}행")

# ──────────────────────────────────────────────
# 5. 스마일메롱바 → 메롱바 이름 매핑
# ──────────────────────────────────────────────
n = (personal["제품명"] == "스마일메롱바").sum()
if n:
    personal.loc[personal["제품명"] == "스마일메롱바", "제품명"] = "메롱바"
    print(f"  스마일메롱바 → 메롱바 이름 매핑: {n}행")

# ──────────────────────────────────────────────
# 6. 개인 파일 제품명별 단일 행으로 통합
# ──────────────────────────────────────────────
personal_resolved = (
    personal.groupby("제품명", sort=False)
    .apply(resolve_personal_rows, include_groups=False)
    .reset_index()
)
print(f"  개인 파일 중복 통합 후 고유 제품: {len(personal_resolved)}개\n")

# ──────────────────────────────────────────────
# 7. 마스터 × 개인 파일 병합
# ──────────────────────────────────────────────
personal_map = personal_resolved.set_index("제품명")
updated_rows = 0

for idx, row in master.iterrows():
    prod = row["제품명"]
    if prod not in personal_map.index:
        continue
    p_row = personal_map.loc[prod]
    changed = False
    for col in MERGE_COLS:
        if col not in master.columns or col not in p_row.index:
            continue
        new_val = merge_two_vals(row[col], p_row[col])
        # NaN vs NaN 비교 처리
        old_empty = is_empty(row[col])
        new_empty = pd.isna(new_val) if not isinstance(new_val, str) else False
        if not (old_empty and new_empty):
            master.at[idx, col] = new_val
            if str(row[col]) != str(new_val):
                changed = True
    if changed:
        updated_rows += 1

print(f"[병합] 값이 변경된 행: {updated_rows}행")

# ──────────────────────────────────────────────
# 8. 결과 통계
# ──────────────────────────────────────────────
def count_filled(col):
    return master[col].apply(lambda x: not is_empty(x)).sum()


still_empty = master.apply(
    lambda r: is_empty(r["살릴키워드"]) and is_empty(r["죽일키워드"]), axis=1
).sum()

print(f"\n[최종 통계]")
print(f"  총 행수       : {len(master)}")
print(f"  살릴키워드 채움: {count_filled('살릴키워드')}")
print(f"  죽일키워드 채움: {count_filled('죽일키워드')}")
print(f"  둘 다 빈 행   : {still_empty}")

# ──────────────────────────────────────────────
# 9. 저장
# ──────────────────────────────────────────────
out_path = BASE + "product_nodes_keyword_analysis_검토_final.csv"
master.to_csv(out_path, index=False, encoding="utf-8-sig")
print(f"\n저장 완료: {out_path}")
