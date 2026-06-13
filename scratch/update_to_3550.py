# -*- coding: utf-8 -*-
import os

source_file = r"C:\Users\송정현\.gemini\antigravity-cli\brain\4e0bae23-7d9c-433c-9b96-841d4916e764\scratch\build_clean_3500.py"
target_file = r"C:\Users\송정현\.gemini\antigravity-cli\brain\4e0bae23-7d9c-433c-9b96-841d4916e764\scratch\build_clean_3550.py"

with open(source_file, "r", encoding="utf-8") as f:
    content = f.read()

# 1. clean_map_71 정의 텍스트 (3501~3550위 매핑)
clean_map_71_def = """
# 3501~3550위 매핑
clean_map_71 = {
  "CU_노티드아이스우유도넛": ["우유", "도넛", "간식", "노티드", "크림", "빵"],
  "CU_노티드아이스초코도넛": ["도넛", "간식", "노티드", "초코", "빵"],
  "CU_뉴욕베이글블루베리": ["간식", "베이글", "디저트", "빵"],
  "CU_닥터페퍼제로스트로베리크림": ["음료", "탄산", "제로"],
  "CU_단팥빵": ["팥", "간식", "달콤", "빵"],
  "CU_닭가슴살곡물김밥": ["곡물", "닭가슴살", "김밥", "밥", "닭"],
  "CU_닭갈비볶음밥호빵": ["야식", "간식", "호빵", "닭", "밥", "빵"],
  "CU_당과점말차레몬크림슈": ["말차", "간식", "크림", "디저트", "레몬", "빵"],
  "CU_당과점초당옥수수타르트": ["타르트", "옥수수", "간식", "디저트", "빵"],
  "CU_더건강저당바베큐샌드": ["닭가슴살", "샌드", "당", "빵", "닭"],
  "CU_더건강파로현미참치김밥": ["참치", "김밥", "밥"],
  "CU_더리얼비프버거": ["버거", "소고기", "치즈", "빵", "고기"],
  "CU_데미포크그라탕빵": ["야식", "간식", "그라탕", "빵", "고기"],
  "CU_델라페팝핑톡": ["음료"],
  "CU_동파육덮밥": ["간편", "동파육", "덮밥", "밥", "고기"],
  "CU_듀오버스터민트볼": ["캔디", "민트"],
  "CU_딸기바크씬": ["딸기", "초코", "바삭", "디저트", "아몬드"],
  "CU_딸기프렌즈": ["우유", "음료", "딸기"],
  "CU_떡볶이맛참치김밥": ["참치", "김밥", "떡볶이", "밥", "떡"],
  "CU_떡볶이맛참치마요": ["참치", "주먹밥", "떡볶이", "밥", "떡"],
  "CU_라라스윗저당흑임자소금빵": ["당", "흑임자", "소금", "빵"],
  "CU_랩노쉬고단백딸기콘": ["단백질", "딸기", "간식"],
  "CU_랩노쉬고단백초코콘": ["단백질", "초코", "간식"],
  "CU_레드쉬머피노누아": ["와인", "피노누아", "술"],
  "CU_레몬팝핑톡": ["요거트", "음료", "레몬"],
  "CU_롱롱이꿀흑임자": ["야식", "흑임자", "간식", "아몬드", "떡"],
  "CU_마라마파두부볶음면": ["마라", "두부", "볶음", "면"],
  "CU_마카다미아초코쿠키": ["쿠키", "초코", "마카다미아"],
  "CU_말차벽돌케이크": ["말차", "간식", "디저트", "빵"],
  "CU_맛없없깐풍치킨피넛": ["땅콩", "치킨", "깐풍", "닭", "고기"],
  "CU_맛폴리라구치즈부리또": ["라구", "부리또", "치즈", "빵"],
  "CU_맛폴리스프리츠하이볼": ["하이볼", "술"],
  "CU_맛폴리치즈고구마피자": ["고구마", "피자", "치즈", "빵"],
  "CU_망고화이트펄빙수": ["간식", "디저트", "망고", "빙수"],
  "CU_망곰치즈소시지빵": ["소시지", "치즈", "빵"],
  "CU_매콤칠리바삭튀김도그": ["야식", "간식", "핫도그", "칠리", "매콤", "빵"],
  "CU_맥앤치즈스팀베이글": ["야식", "베이글", "식사", "부드러움", "치즈", "빵"],
  "CU_먹태청양마요새우버거": ["버거", "새우", "마요", "간편", "빵"],
  "CU_먹태청양마요참치김밥": ["참치", "김밥", "마요", "간편", "밥"],
  "CU_메타몽소시지김밥": ["소시지", "김밥", "간편", "밥"],
  "CU_멕시칸타코치킨샌드": ["닭가슴살", "단백질", "샌드", "식사", "빵", "닭"],
  "CU_면)뉴백종원미트볼파스타": ["파스타", "고기", "간편", "면"],
  "CU_명인)찹쌀고구마부각": ["부각", "간식", "고구마", "쌀", "찹쌀"],
  "CU_명인)찹쌀연근부각": ["부각", "간식", "연근", "쌀", "찹쌀"],
  "CU_명인)찹쌀황태껍질부각": ["부각", "간식", "황태", "쌀", "찹쌀"],
  "CU_모구모구": ["음료", "달콤", "탱글함"],
  "CU_모구모구제로슈가트로피칼딜라이트": ["음료", "상큼", "달콤", "주스", "제로"],
  "CU_모구모구풍선껌젤리P": ["간식", "달콤", "젤리"],
  "CU_모티스고구마호빵": ["간식", "고구마", "호빵", "빵"],
  "CU_미나리우곡생주": ["막걸리", "전통주", "술"]
}
"""

# clean_map_missing 정의 바로 전에 clean_map_71를 삽입하자.
pos = content.find("clean_map_missing = {")
if pos == -1:
    raise ValueError("Cannot find 'clean_map_missing = {' in source file")

new_content = content[:pos] + clean_map_71_def + "\n" + content[pos:]

# 2. merged_raw.update(clean_map_71) 추가
update_pos = new_content.find("merged_raw.update(clean_map_missing)")
if update_pos == -1:
    raise ValueError("Cannot find 'merged_raw.update(clean_map_missing)'")

new_content = new_content[:update_pos] + "merged_raw.update(clean_map_71)\n" + new_content[update_pos:]

# 3. top_3500 -> top_3550 교체
new_content = new_content.replace("top_3500 = df_sorted.head(3500)", "top_3550 = df_sorted.head(3550)")
new_content = new_content.replace("for idx, row in top_3500.iterrows():", "for idx, row in top_3550.iterrows():")
new_content = new_content.replace("결정론적 3500개 분석 CSV", "결정론적 3550개 분석 CSV")
new_content = new_content.replace("top_3500.iterrows()", "top_3550.iterrows()")

with open(target_file, "w", encoding="utf-8") as f:
    f.write(new_content)

print(f"Updated script written to: {target_file}")
