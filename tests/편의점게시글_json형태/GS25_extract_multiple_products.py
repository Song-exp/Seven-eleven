import os
import json
import re
import zipfile
import shutil
import tempfile
import pandas as pd
from xml.sax.saxutils import escape


def clean_price(price_val):
    if pd.isna(price_val) or price_val is None:
        return 0
    if isinstance(price_val, (int, float)):
        return int(price_val)
    cleaned = re.sub(r'[^0-9]', '', str(price_val))
    return int(cleaned) if cleaned else 0


def clean_cell(val):
    """Excel XML에서 허용되지 않는 문자 제거."""
    if not isinstance(val, str):
        return val
    return ''.join(
        c for c in val
        if c == '\t' or c == '\n' or
        (0x20 <= ord(c) <= 0xD7FF) or
        (0xE000 <= ord(c) <= 0xFFFD)
    )


def convert_inline_to_shared(xlsx_path):
    """
    openpyxl이 생성한 inline string 방식의 xlsx를
    Mac Excel과 호환되는 shared strings 방식으로 변환한다.
    """
    tmp_dir = tempfile.mkdtemp()
    try:
        # xlsx 압축 해제
        with zipfile.ZipFile(xlsx_path, 'r') as z:
            z.extractall(tmp_dir)

        sheet_path = os.path.join(tmp_dir, 'xl', 'worksheets', 'sheet1.xml')
        with open(sheet_path, 'r', encoding='utf-8') as f:
            sheet_xml = f.read()

        # inline string 셀 파싱: <c r="..." t="inlineStr"><is><t ...>VALUE</t></is></c>
        pattern = re.compile(
            r'<c ([^>]*?)t="inlineStr"([^>]*)><is><t([^>]*)>(.*?)</t></is></c>',
            re.DOTALL
        )

        shared = []
        shared_index = {}

        def get_index(text):
            if text not in shared_index:
                shared_index[text] = len(shared)
                shared.append(text)
            return shared_index[text]

        def replacer(m):
            attrs1 = m.group(1)
            attrs2 = m.group(2)
            t_attrs = m.group(3)
            raw_val = m.group(4)
            # XML 엔티티 복원 후 shared strings에 등록
            from html import unescape
            text = unescape(raw_val)
            idx = get_index(text)
            # t="inlineStr" 제거하고 t="s"로 교체
            cell_attrs = (attrs1 + attrs2).strip()
            if cell_attrs:
                return f'<c {cell_attrs} t="s"><v>{idx}</v></c>'
            return f'<c t="s"><v>{idx}</v></c>'

        new_sheet_xml = pattern.sub(replacer, sheet_xml)

        # 빈 inlineStr 셀 제거
        new_sheet_xml = re.sub(r'<c [^>]*t="inlineStr"[^>]*></c>', '', new_sheet_xml)

        # <f> 수식 태그로 저장된 셀을 shared string으로 교체 (<v></v> 포함 구조 대응)
        formula_pattern = re.compile(r'<c ([^>]*)><f>(.*?)</f>(?:<v>[^<]*</v>)?</c>', re.DOTALL)

        def formula_replacer(m):
            from html import unescape
            cell_attrs = m.group(1).strip()
            text = unescape(m.group(2))
            idx = get_index(text)
            if cell_attrs:
                return f'<c {cell_attrs} t="s"><v>{idx}</v></c>'
            return f'<c t="s"><v>{idx}</v></c>'

        new_sheet_xml = formula_pattern.sub(formula_replacer, new_sheet_xml)

        with open(sheet_path, 'w', encoding='utf-8') as f:
            f.write(new_sheet_xml)

        # sharedStrings.xml 생성
        ss_lines = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?>']
        ss_lines.append(
            f'<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
            f'count="{len(shared)}" uniqueCount="{len(shared)}">'
        )
        for s in shared:
            escaped = escape(s)
            if '\n' in s or s != s.strip():
                ss_lines.append(f'<si><t xml:space="preserve">{escaped}</t></si>')
            else:
                ss_lines.append(f'<si><t>{escaped}</t></si>')
        ss_lines.append('</sst>')

        ss_path = os.path.join(tmp_dir, 'xl', 'sharedStrings.xml')
        with open(ss_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(ss_lines))

        # [Content_Types].xml에 sharedStrings 추가
        ct_path = os.path.join(tmp_dir, '[Content_Types].xml')
        with open(ct_path, 'r', encoding='utf-8') as f:
            ct_xml = f.read()
        if 'sharedStrings' not in ct_xml:
            ct_xml = ct_xml.replace(
                '</Types>',
                '<Override PartName="/xl/sharedStrings.xml" '
                'ContentType="application/vnd.openxmlformats-officedocument.'
                'spreadsheetml.sharedStrings+xml"/></Types>'
            )
            with open(ct_path, 'w', encoding='utf-8') as f:
                f.write(ct_xml)

        # xl/_rels/workbook.xml.rels에 sharedStrings 관계 추가
        rels_path = os.path.join(tmp_dir, 'xl', '_rels', 'workbook.xml.rels')
        with open(rels_path, 'r', encoding='utf-8') as f:
            rels_xml = f.read()
        if 'sharedStrings' not in rels_xml:
            rels_xml = rels_xml.replace(
                '</Relationships>',
                '<Relationship Id="rIdSS" '
                'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" '
                'Target="sharedStrings.xml"/></Relationships>'
            )
            with open(rels_path, 'w', encoding='utf-8') as f:
                f.write(rels_xml)

        # 다시 zip으로 압축
        tmp_xlsx = xlsx_path + '.tmp'
        with zipfile.ZipFile(tmp_xlsx, 'w', zipfile.ZIP_DEFLATED) as zout:
            for root, dirs, files in os.walk(tmp_dir):
                for file in files:
                    abs_path = os.path.join(root, file)
                    arc_name = os.path.relpath(abs_path, tmp_dir)
                    zout.write(abs_path, arc_name)

        os.replace(tmp_xlsx, xlsx_path)

    finally:
        shutil.rmtree(tmp_dir)


# ── 비식품 상품명 목록 (전수조사 후 확정) ──────────────────────────────────
NON_FOOD_NAMES = {
    "&TEAM KR 1st Mini Album 'Back to Life'",
    "10CM 정규앨범 '5.0'", "1ST MINI ALBUM 'Back to Life'",
    "CORTIS The 1st EP [COLOR OUTSIDE THE LINES]",
    "CORTIS The 1st EP [COLOR OUTSIDE THE LINES] (RANDOM)",
    "TWS 4th MINI ALBUM 'play hard'", "TWS 4th Mini Album 'play hard'",
    "TWS 4th Mini Album 'play hard' 친필 사인앨범",
    "2ND MINI ALBUM 'MY, Lover'", "2nd Mini Album [MY, Lover]",
    "NCTDREAM BTTF  NCTDREAM_BTTF CHILLER  NCTDREAM_CHILLER  GoBackToTheFuture",
    "도겸X승관 1st Mini Album '소야곡'", "몬스타엑스 앨범",
    "미연 (MIYEON) 2nd Mini Album [MY, Lover]",
    "에스쿱스X민규 1st Mini Album 'HYPE VIBES'",
    "엔하이픈 DESIRE : UNLEASH 앨범",
    "아일릿 3rd Mini Album 'bomb'",
    "아일릿(ILLIT) 1st Single Album 'NOT CUTE ANYMORE'",
    "아일릿(ILLIT) 1st Single Album 'NOT CUTE ANYMORE' 친필 사인 앨범",
    "코르티스 [COLOR OUTSIDE THE LINES] 친필 사인 앨범",
    "2025 케이팝 루키 컬렉션 카드", "2025케이팝루키카드",
    "GS25 기프트카드", "Google Play 기프트카드", "Google 기프트카드",
    "STEAM 기프트카드", "갤럭시 스토어 기프트 카드",
    "구글 기프트 카드", "구글 기프트카드", "구글 플레이 기프트 카드",
    "로블록스 기프트카드",
    "BT21 The Journey 티머니카드", "GS POP티머니 친환경카드",
    "ZO&FRIENDS 티머니 카드", "광복 80주년 태극기 티머니",
    "먼작귀 힘내 티머니", "산리오캐릭터즈 스파클 LED 티머니카드",
    "엔하이픈 티머니카드", "오징어게임3 렌티큘러 티머니 카드",
    "티머니 교통카드 + 스티커",
    "나일론 밴딩 쇼츠", "시티 레저 후디드 라이트 다운 재킷",
    "니베아 딥 모이스처 올리브&레몬", "리틀리위찌 색조화장품 7종 세트",
    "리스테린X케어베어 키링 기획 세트", "노더럽XGS25 포지티브 트래블 키트",
    "생리대 기획세트", "유어스 대용량 물티슈", "유어스 크리넥스 쁘띠 물티슈",
    "크리넥스 순수소프트 퀼트 다이노탱 에디션", "화이트엔젤큐티슈",
    "비트 캡슐세제 울트라 X 잔망루피",
    "다이어트 유산균 비에날씬 프로 3개월", "멀티비타민미네랄", "오쏘몰 이뮨",
    "종근당 리포좀비타민C", "하루엔진밀크씨슬", "하루엔진비타민B",
    "하루엔진올인원", "하루엔진장건강유산균", "혈당컷다이어트", "혈당케어앤유산균",
    "간바레오또상 변온잔大(2입) 기획팩", "몽모 키링세트 3종",
    "시아오카 랜덤키링 기획세트", "쿠빅브릭 마루 굿즈",
    "코닥 차메라 키체인 디지털 카메라", "픽셀리 아크릴 디오라마",
    "헬리녹스 굿즈 세트 (플래터, 젓가락, 숟가락)",
    "엘리시안 강촌 리프트권", "프리미엄 간사이 조이패스", "올텔 해외유심",
    "편의점캐시", "티니핑 3종 용돈 봉투",
    "2026 소방관 희망나눔 달력", "붕어빵 핫팩", "아이스칠링백",
    "이치방쿠지", "젠톡 ALL 패키지 유전자 검사권", "텀블러", "텐가 콘돔",
    "미니 크리스마스 트리  크리스마스 트리세트",
}

NON_FOOD_PATTERNS = [
    '기프트카드', '기프트 카드', '티머니', '교통카드',
    '앨범', 'Mini Album', 'MINI ALBUM', '정규앨범', '1st EP', '2nd EP', '3rd EP',
    'Single Album', '사인앨범',
    '물티슈', '큐티슈', '생리대', '세제', '콘돔',
    '하루엔진', '혈당컷', '혈당케어', '비에날씬', '리포좀비타민',
    '해외유심', '리프트권', '조이패스', '유전자 검사권',
    '아크릴 디오라마', '트래블 키트',
    '이치방쿠지', '용돈 봉투', '편의점캐시',
]

NON_FOOD_ATTR_ONLY = {'앨범', '사전예약', '케이팝', 'K-POP', '티머니', '교통카드',
                      '단독', '히든이벤트', '이벤트'}
FOOD_ATTR_KEYWORDS = {
    '음료', '식품', '스낵', '간식', '과자', '빵', '커피', '맥주', '와인', '소주',
    '밥', '도시락', '김밥', '샌드위치', '라면', '우동', '떡볶이', '치킨', '피자',
    '버거', '아이스크림', '젤리', '초콜릿', '초코', '케이크', '쿠키', '캔디',
    '음식', '식사', '안주', '야식', '디저트', '주류', '막걸리', '위스키',
    '하이볼', '칵테일', '사케', '소르베', '요거트', '우유', '주스', '에너지드링크',
    '스포츠음료', '이온음료', '탄산', '소스', '양념', '국수', '떡', '순대',
    '치즈', '햄', '고기', '닭', '돼지', '소고기', '해산물', '생선', '김치',
    '두부', '계란', '야채', '과일', '견과', '팝콘', '칩',
}


def is_food(name: str, attrs: list) -> bool:
    attr_str = ' '.join(attrs)
    if any(kw in attr_str for kw in FOOD_ATTR_KEYWORDS):
        return True
    if name in NON_FOOD_NAMES:
        return False
    if any(p in name for p in NON_FOOD_PATTERNS):
        return False
    attr_set = set(attrs)
    if attr_set and attr_set.issubset(NON_FOOD_ATTR_ONLY):
        return False
    return True


ATTR_KEY_MAP = {
    'flavor_and_category': ['flavor_and_category', 'category', 'categories', '핵심키워드'],
    'collab_and_brand':    ['collab_and_brand', 'collab_brands', 'brands'],
    'promotion_type':      ['promotion_type', 'promotion', 'promotion_keywords',
                            'promotion_detail', 'promotion_details', 'event_type'],
    'tpo_context':         ['tpo_context', 'context'],
}

def extract_attrs(source):
    """source(dict)에서 표준/비표준 키를 모두 탐색해 속성 리스트 반환."""
    attrs = []
    for alt_keys in ATTR_KEY_MAP.values():
        for key in alt_keys:
            vals = source.get(key, [])
            if isinstance(vals, list):
                attrs.extend([str(v).replace('/', '').strip() for v in vals if v])
            elif isinstance(vals, str) and vals.strip():
                attrs.append(vals.replace('/', '').strip())
            if vals:
                break
    return attrs


def process_all_products(row):
    raw_json = row.get('keywords_json')
    if not isinstance(raw_json, str) or not raw_json.strip():
        return None

    try:
        data = json.loads(raw_json)
        metadata = data.get('metadata', [])

        if not metadata or len(metadata) < 2:
            return None

        # 표준/비표준 키 모두 탐색해 공통 속성 수집
        common_attributes = extract_attrs(data)

        product_results = {}

        for item in metadata:
            name = str(item.get('name', 'Unknown')).replace('/', '').strip()
            price = clean_price(item.get('price', 0))
            capacity = item.get('capacity')
            capacity_val = str(capacity).replace('/', '').strip() if capacity and not pd.isna(capacity) else "null"

            item_attributes = extract_attrs(item)
            combined_attributes = list(dict.fromkeys(item_attributes + common_attributes))
            product_key = f"['{name}', {price}, '{capacity_val}']"
            product_results[product_key] = combined_attributes

        parts = [f"{k}: {json.dumps(v, ensure_ascii=False)}" for k, v in product_results.items()]
        return '{' + ', '.join(parts) + '}'

    except Exception:
        return None


def main():
    # --- 설정 영역 ---
    user_home = os.path.expanduser("~")
    input_path = os.path.join(user_home, "Desktop", "Seminar", "Seven-eleven", "data", "raw", "편의점", "gs25_official_with_keywords.csv")
    output_path = os.path.join(user_home, "Desktop", "Seminar", "세븐일레븐", "Workspace", "GS25_multiple_products_final.xlsx")
    # ----------------

    print(f"[*] 데이터 읽기 시작: {input_path}")

    if not os.path.exists(input_path):
        print(f"[-] 파일을 찾을 수 없습니다: {input_path}")
        return

    try:
        df = pd.read_csv(input_path, encoding='utf-8-sig', low_memory=False)

        if 'keywords_json' not in df.columns:
            print(f"[-] 오류: 'keywords_json' 컬럼이 데이터에 없습니다.")
            return

        print("[*] 모든 상품 정보 추출 및 데이터 변환 중...")
        df['formatted_output'] = df.apply(process_all_products, axis=1)

        filtered_df = df[df['formatted_output'].notna()].copy()

        if filtered_df.empty:
            print("[-] 결과: 추출된 상품 데이터가 없습니다.")
            return

        # 식품만 필터링: 게시물 내 모든 상품이 식품인 행만 유지
        def keep_food_multiple(output_str):
            names = re.findall(r"\['(.*?)',\s*\d+", output_str)
            attr_blocks = re.findall(r':\s*(\[.*?\])(?:,\s*\[|$)', output_str, re.DOTALL)
            for i, name in enumerate(names):
                try:
                    attrs = json.loads(attr_blocks[i]) if i < len(attr_blocks) else []
                except Exception:
                    attrs = []
                if not is_food(name, attrs):
                    return False
            return True

        before = len(filtered_df)
        filtered_df = filtered_df[filtered_df['formatted_output'].apply(keep_food_multiple)].copy()
        print(f"[*] 비식품 제거: {before - len(filtered_df)}개 제거 → {len(filtered_df)}개 남음")

        filtered_df = filtered_df.map(clean_cell)

        cols = list(filtered_df.columns)
        if 'keywords_json' in cols:
            hin_idx = cols.index('keywords_json')
            new_cols = cols[:hin_idx+1] + ['formatted_output'] + [c for c in cols[hin_idx+1:] if c != 'formatted_output']
            filtered_df = filtered_df[new_cols]

        print(f"[*] 총 {len(filtered_df)}개의 게시글 데이터를 저장 중: {output_path}")
        filtered_df.to_excel(output_path, index=False, engine='openpyxl')

        print("[*] Mac Excel 호환을 위해 shared strings 방식으로 변환 중...")
        convert_inline_to_shared(output_path)

        print(f"[+] 완료! 파일이 생성되었습니다: {output_path}")

    except Exception as e:
        print(f"[-] 오류 발생: {e}")


if __name__ == "__main__":
    main()
