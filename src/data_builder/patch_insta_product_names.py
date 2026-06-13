# -*- coding: utf-8 -*-
"""
instagram_engagement_with_keywords_final.csv 제품명 패치

케이스 A: 1행 → 2행 분리 (원본명·키워드 분리)
케이스 B: 원본명 수정 (1:1 rename)
케이스 C: 행 삭제 (행사 문구, 실제 제품 아님)
"""
import ast
import pandas as pd

CSV_PATH = r'data\processed\instagram_engagement_with_keywords_final.csv'

# ──────────────────────────────────────────────
# 케이스 A: 분리
# 각 항목: {원본명: [{원본명, 키워드?, 키워드_정제?}, ...]}
# 명시하지 않은 컬럼은 원본 행 값을 그대로 복사
# ──────────────────────────────────────────────
SPLITS = {
    '스팀베이글 2종 (맥앤치즈, 햄치즈)': [
        {'원본명': '맥앤치즈스팀베이글', '키워드': ['베이글', '안주', '치즈']},
        {'원본명': '햄치즈스팀베이글',   '키워드': ['베이글', '안주', '치즈']},
    ],
    '모찌리도후 2종': [
        {'원본명': '모찌리도후 두유', '키워드': ['두유', '디저트', '말랑', '쫀득함']},
        {'원본명': '모찌리도후 녹차', '키워드': ['녹차', '디저트', '말랑', '쫀득함']},
    ],
    '어묵 2종 (오리지널 접사각 and 봉어묵)': [
        {'원본명': '오리지널 접사각어묵', '키워드': ['간식', '꼬치', '쌀쌀함', '어묵']},
        {'원본명': '봉어묵',             '키워드': ['간식', '꼬치', '쌀쌀함', '어묵']},
    ],
    '우주 토닉 2종 (핑크피치 or 블루레몬)': [
        {'원본명': '핑크피치 우주토닉', '키워드': ['피치', '토닉', '워터', '로맨틱', '야식']},
        {'원본명': '블루레몬 우주토닉', '키워드': ['레몬', '토닉', '워터', '로맨틱', '야식']},
    ],
    '헬리녹스 부대찌개 & 모둠 오뎅탕': [
        {'원본명': '헬리녹스 부대찌개',   '키워드_정제': ['부대찌개', '뜨끈', '캠핑', '콜라보']},
        {'원본명': '헬리녹스 모둠 오뎅탕', '키워드_정제': ['오뎅',   '뜨끈', '캠핑', '콜라보']},
    ],
    '페레로로쉐 2종(T-3, T-5)': [
        {'원본명': '페레로로쉐 T-3', '키워드': ['빼빼로', '시즌', '야식', '페레로로쉐']},
        {'원본명': '페레로로쉐 T-5', '키워드': ['빼빼로', '시즌', '야식', '페레로로쉐']},
    ],
}

# ──────────────────────────────────────────────
# 케이스 B: 이름 수정
# ──────────────────────────────────────────────
RENAMES = {
    '제로윗)녹차&바닐라 파르페': '제로윗)녹차 파르페',
    '블랙페퍼닭가슴살 듬뿍 블랙페퍼닭가슴살샐러드': '블랙페퍼닭가슴살샐러드',
}

# ──────────────────────────────────────────────
# 케이스 C: 삭제 (행사 문구)
# ──────────────────────────────────────────────
DELETES = [
    '롯데 빼빼로 초코/아몬드',
    '롯데 빼빼로 2종',
    '포키 2종',
    '포키 빼빼로 2종',
    '페레로로쉐 2종',
]


def fmt_keywords(kw_list):
    """리스트를 CSV 저장용 문자열로 변환"""
    return str(kw_list)


def apply_splits(df):
    rows_to_drop = []
    rows_to_add = []

    for orig_name, new_products in SPLITS.items():
        mask = df['원본명'] == orig_name
        if not mask.any():
            print(f'  [SPLIT] 대상 없음: {orig_name}')
            continue

        orig_row = df[mask].iloc[0].copy()
        rows_to_drop.extend(df.index[mask].tolist())

        for prod in new_products:
            new_name = prod['원본명']
            # 이미 개별 제품 행이 존재하면 새 행 추가하지 않고 원본("2종") 행만 제거
            if (df['원본명'] == new_name).any():
                print(f'  [SPLIT] {orig_name}  →  {new_name} 이미 존재, 원본 행만 제거')
                continue
            new_row = orig_row.copy()
            for col, val in prod.items():
                if col in ('키워드', '키워드_정제'):
                    new_row[col] = fmt_keywords(val)
                else:
                    new_row[col] = val
            rows_to_add.append(new_row)
            print(f'  [SPLIT] {orig_name}  →  {new_name} 새 행 추가')

    df = df.drop(index=rows_to_drop)
    df = pd.concat([df, pd.DataFrame(rows_to_add)], ignore_index=True) if rows_to_add else df
    return df


def apply_renames(df):
    for old, new in RENAMES.items():
        mask = df['원본명'] == old
        if not mask.any():
            print(f'  [RENAME] 대상 없음: {old}')
            continue
        df.loc[mask, '원본명'] = new
        print(f'  [RENAME] {old}  →  {new}')
    return df


def apply_deletes(df):
    for name in DELETES:
        mask = df['원본명'] == name
        n = mask.sum()
        if n == 0:
            print(f'  [DELETE] 대상 없음: {name}')
            continue
        df = df[~mask]
        print(f'  [DELETE] {name}  ({n}행 제거)')
    return df


def main():
    df = pd.read_csv(CSV_PATH, encoding='utf-8-sig')
    before = len(df)
    print(f'로드: {before}행\n')

    print('=== SPLIT ===')
    df = apply_splits(df)
    print()

    print('=== RENAME ===')
    df = apply_renames(df)
    print()

    print('=== DELETE ===')
    df = apply_deletes(df)
    print()

    after = len(df)
    print(f'저장: {after}행  (Δ {after - before:+d})')
    df.to_csv(CSV_PATH, index=False, encoding='utf-8-sig')
    print(f'완료: {CSV_PATH}')


if __name__ == '__main__':
    main()
