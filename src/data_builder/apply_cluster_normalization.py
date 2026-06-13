# -*- coding: utf-8 -*-
"""
CU/GS25 정규화명 클러스터링 적용

product_name_cluster_review_final.xlsx (CU, GS25 시트) 검수 결과를
instagram_engagement_with_keywords_final.csv 와 .parquet 에 반영한다.

처리 규칙:
  별도검토 = NaN   → 해당 행 정규화명(공백 제거) 그대로 사용
  별도검토 = 'O'   → 해당 원본명 행 전체 제거 (__DELETE__)
  별도검토 = 기타  → 해당 값(공백 제거)을 정규화명으로 사용

적용 범위: 편의점명 in ['CU', 'GS25'] 만 — 세븐일레븐 행은 건드리지 않음
"""

import ast
import re
import numpy as np
import pandas as pd
from pathlib import Path

BASE         = Path(__file__).resolve().parents[2]
EXCEL_PATH   = BASE / "eda" / "product_name_cluster_review_final.xlsx"
CSV_PATH     = BASE / "data" / "processed" / "instagram_engagement_with_keywords_final.csv"
PARQUET_PATH = BASE / "data" / "processed" / "instagram_engagement_with_keywords.parquet"

TARGET_BRANDS = {'CU', 'GS25'}
KW_COLS       = ['키워드', '키워드_정제']


def parse_kw(v):
    """CSV 문자열 / numpy array / list 모두 list로 통일."""
    if isinstance(v, (list, np.ndarray)):
        return list(v)
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    s = str(v).strip()
    if s in ('', 'nan', 'None', '[]'):
        return []
    # ['로 시작하면 regex로 직접 추출 (numpy repr/list repr 모두 처리)
    # ast.literal_eval을 먼저 쓰면 numpy repr ['a' 'b']을 ['ab']로 잘못 합성함
    if s.startswith('['):
        items = re.findall(r"'([^']*)'", s)
        if items:
            return [x for x in items if x.strip()]
    try:
        result = ast.literal_eval(s)
        return list(result) if isinstance(result, (list, tuple)) else [str(result)]
    except Exception:
        return [x.strip() for x in s.split(',') if x.strip()]


def build_mapping(excel_path):
    """원본명 → target 정규화명 매핑 딕셔너리 생성."""
    mapping = {}
    for sheet in ['CU', 'GS25']:
        df = pd.read_excel(excel_path, sheet_name=sheet)
        for _, row in df.iterrows():
            v = row['별도검토']
            if pd.isna(v) or str(v).strip() == '':
                target = str(row['정규화명']).strip().replace(' ', '')
            elif str(v).strip() == 'O':
                target = '__DELETE__'
            else:
                target = str(v).strip().replace(' ', '')

            for raw in str(row['유사제품리스트']).split('\n'):
                orig = re.sub(r'\s*\(\d+행\)\s*$', '', raw).strip()
                if orig:
                    mapping[orig] = target
    return mapping


def apply_to_df(df, mapping):
    """CU/GS25 행에 한해 매핑 적용. 세븐일레븐 행은 변경하지 않음."""
    mask = df['편의점명'].isin(TARGET_BRANDS)

    # 매핑에 있는 원본명만 정규화명 덮어쓰기 (없는 원본명은 기존값 유지)
    mapped = df.loc[mask, '원본명'].map(mapping)
    valid  = mapped.notna()
    df.loc[mask & valid, '정규화명'] = mapped[valid]

    # __DELETE__ 행 제거
    delete_mask = mask & (df['정규화명'] == '__DELETE__')
    n_deleted   = int(delete_mask.sum())
    df = df[~delete_mask].reset_index(drop=True)

    return df, n_deleted


def main():
    print("=== CU/GS25 정규화명 클러스터링 적용 ===\n")

    mapping  = build_mapping(EXCEL_PATH)
    n_delete = sum(1 for v in mapping.values() if v == '__DELETE__')
    print(f"매핑 딕셔너리: 총 {len(mapping)}개  (DELETE {n_delete}개)\n")

    # ── CSV ──────────────────────────────────────────────
    csv_df    = pd.read_csv(CSV_PATH, encoding='utf-8-sig')
    before    = len(csv_df)
    csv_df, n_del = apply_to_df(csv_df, mapping)

    csv_df.to_csv(CSV_PATH, index=False, encoding='utf-8-sig')
    print(f"[CSV]     {before}행 → {len(csv_df)}행  (제거 {n_del}행)")
    print(f"  CU   정규화명 고유: {csv_df[csv_df['편의점명']=='CU']['정규화명'].nunique()}")
    print(f"  GS25 정규화명 고유: {csv_df[csv_df['편의점명']=='GS25']['정규화명'].nunique()}\n")

    # ── Parquet: 세븐일레븐 행 보존 + CU/GS25는 CSV로 교체 ──────────
    # CSV가 patch 완료 상태이므로 CU/GS25 소스로 사용
    # 세븐일레븐은 parquet 유지 ('다시 봐야할 거' 등 컬럼 보존)
    pq_df  = pd.read_parquet(PARQUET_PATH)
    seven  = pq_df[pq_df['편의점명'] == '세븐일레븐'].copy()

    pq_extra_cols = [c for c in pq_df.columns if c not in csv_df.columns]
    cu_gs_new = csv_df[csv_df['편의점명'].isin(TARGET_BRANDS)].copy()
    for col in pq_extra_cols:
        cu_gs_new[col] = pd.NA
    cu_gs_new = cu_gs_new.reindex(columns=pq_df.columns)

    pq_new = pd.concat([seven, cu_gs_new], ignore_index=True)
    for col in KW_COLS:
        if col in pq_new.columns:
            pq_new[col] = pq_new[col].apply(parse_kw)
    pq_new.to_parquet(PARQUET_PATH, index=False)
    print(f"[parquet] → {len(pq_new)}행")
    print(f"  세븐일레븐: {(pq_new['편의점명']=='세븐일레븐').sum()}행")
    print(f"  CU   : {(pq_new['편의점명']=='CU').sum()}행  정규화명 고유: {pq_new[pq_new['편의점명']=='CU']['정규화명'].nunique()}")
    print(f"  GS25 : {(pq_new['편의점명']=='GS25').sum()}행  정규화명 고유: {pq_new[pq_new['편의점명']=='GS25']['정규화명'].nunique()}\n")

    print("완료.")


if __name__ == '__main__':
    main()
