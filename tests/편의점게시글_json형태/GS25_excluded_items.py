import os
import json
import re
import sys
import zipfile
import shutil
import tempfile
import pandas as pd
from xml.sax.saxutils import escape

sys.path.insert(0, os.path.dirname(__file__))
from GS25_extract_single_products import (
    process_single_product, is_food, clean_cell, convert_inline_to_shared
)
from GS25_extract_multiple_products import process_all_products


def get_category(name: str, attrs: list) -> str:
    attr_str = ' '.join(attrs)

    if any(p in name for p in [
        '앨범', 'Album', 'ALBUM', '1st EP', '2nd EP', '3rd EP', '정규앨범', '사인앨범'
    ]) or any(a in attrs for a in ['앨범', '음반']):
        return '앨범/음악'

    if any(p in name for p in ['기프트카드', '기프트 카드', '상품권', '충전권']):
        return '기프트카드/상품권'

    if any(p in name for p in ['티머니', '교통카드']):
        return '티머니/교통카드'

    if any(p in name for p in ['쇼츠', '재킷', '티셔츠']) or '의류' in attr_str:
        return '의류'

    if any(p in name for p in [
        '물티슈', '큐티슈', '생리대', '세제', '콘돔', '치약',
        '색조화장품', '니베아', '리스테린', '노더럽'
    ]) or any(a in attrs for a in [
        '화장품', '뷰티', '생리용품', '위생용품', '구강케어', '세탁세제', '립밤'
    ]):
        return '화장품/위생용품'

    if any(p in name for p in [
        '비타민', '유산균', '하루엔진', '비에날씬', '혈당', '오쏘몰',
        '리포좀', '다이어트프로틴', '프로틴', '멀티비타민'
    ]) or any(a in attrs for a in [
        '체지방 감소', '장 건강', '건강관리', '에너지 충전', '활력'
    ]):
        return '건강기능식품'

    if any(p in name for p in [
        '키링', '굿즈', '디오라마', '카메라', '텀블러', '마그넷', '아크릴'
    ]) or any(a in attrs for a in ['굿즈', '액세서리']):
        return '생활잡화/굿즈'

    if any(p in name for p in [
        '리프트권', '조이패스', '해외유심', '편의점캐시', '용돈 봉투',
        '유전자 검사권', '이치방쿠지'
    ]):
        return '이용권/금융/기타'

    return '기타 비식품'


def main():
    user_home = os.path.expanduser("~")
    input_path = os.path.join(user_home, "Desktop", "Seminar", "Seven-eleven",
                              "data", "raw", "편의점", "gs25_official_with_keywords.csv")
    output_path = os.path.join(user_home, "Desktop", "Seminar", "세븐일레븐",
                               "Workspace", "GS25_excluded_items.xlsx")

    print(f"[*] 데이터 읽기 시작: {input_path}")
    if not os.path.exists(input_path):
        print(f"[-] 파일을 찾을 수 없습니다: {input_path}")
        return

    df = pd.read_csv(input_path, encoding='utf-8-sig', low_memory=False)

    excluded_rows = []

    for _, row in df.iterrows():
        # ── single ──────────────────────────────────────────────────────
        s = process_single_product(row)
        if s:
            m = re.match(r'\{\["(.*?)"', s)
            if m:
                name = m.group(1)
                am = re.search(r':\s*(\[.*\])\}$', s, re.DOTALL)
                attrs = json.loads(am.group(1)) if am else []
                if not is_food(name, attrs):
                    excluded_rows.append({
                        '출처': 'single',
                        '카테고리': get_category(name, attrs),
                        '상품명': name,
                        '속성': ', '.join(attrs),
                        'formatted_output': s,
                        'date': row.get('date', ''),
                        'body': row.get('body', ''),
                        'likes': row.get('likes', ''),
                        'url': row.get('url', ''),
                    })

        # ── multiple ─────────────────────────────────────────────────────
        mp = process_all_products(row)
        if mp:
            names = re.findall(r"\['(.*?)',\s*\d+", mp)
            attr_blocks = re.findall(r':\s*(\[.*?\])(?=,\s*\[|\}$)', mp, re.DOTALL)
            non_food_in_post = []
            for i, name in enumerate(names):
                try:
                    attrs = json.loads(attr_blocks[i]) if i < len(attr_blocks) else []
                except Exception:
                    attrs = []
                if not is_food(name, attrs):
                    non_food_in_post.append((name, attrs))

            if non_food_in_post:
                for name, attrs in non_food_in_post:
                    excluded_rows.append({
                        '출처': 'multiple',
                        '카테고리': get_category(name, attrs),
                        '상품명': name,
                        '속성': ', '.join(attrs),
                        'formatted_output': mp,
                        'date': row.get('date', ''),
                        'body': row.get('body', ''),
                        'likes': row.get('likes', ''),
                        'url': row.get('url', ''),
                    })

    if not excluded_rows:
        print("[-] 제외된 데이터가 없습니다.")
        return

    result_df = pd.DataFrame(excluded_rows)
    result_df = result_df.drop_duplicates(subset=['출처', '상품명'])

    # 카테고리 순서 정의
    category_order = [
        '앨범/음악', '기프트카드/상품권', '티머니/교통카드', '의류',
        '화장품/위생용품', '건강기능식품', '생활잡화/굿즈', '이용권/금융/기타', '기타 비식품'
    ]
    result_df['카테고리'] = pd.Categorical(result_df['카테고리'], categories=category_order, ordered=True)
    result_df = result_df.sort_values(['카테고리', '출처', '상품명']).reset_index(drop=True)

    print(f"\n[*] 제외된 상품 카테고리별 현황:")
    for cat, grp in result_df.groupby('카테고리', observed=True):
        print(f"    {cat}: {len(grp)}개")
    print(f"    합계: {len(result_df)}개")

    # 전처리
    result_df = result_df.map(clean_cell)

    col_order = ['카테고리', '출처', '상품명', '속성', 'date', 'body', 'likes', 'url', 'formatted_output']
    result_df = result_df[col_order]

    print(f"\n[*] 저장 중: {output_path}")
    result_df.to_excel(output_path, index=False, engine='openpyxl')

    print("[*] Mac Excel 호환을 위해 shared strings 방식으로 변환 중...")
    convert_inline_to_shared(output_path)

    print(f"[+] 완료! 파일이 생성되었습니다: {output_path}")


if __name__ == "__main__":
    main()
