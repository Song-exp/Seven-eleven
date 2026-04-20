import json
import sys

sys.stdout.reconfigure(encoding='utf-8')

NB_PATH = 'eda/ipynb/03_b5_promo_eda.ipynb'

with open(NB_PATH, encoding='utf-8') as f:
    nb = json.load(f)

def md_cell(text):
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": [text],
    }

# 삽입할 (원본 인덱스, 마크다운 내용) 목록
# 역순으로 삽입해야 인덱스가 밀리지 않음
inserts = [
    (44, """## 8-3. 번들 행사명 Top N 분석 (3+1, 10+1 등)
- 묶음·콤보·증정·장바구니 각 타입별 상위 행사명 직접 확인
- 같은 행사명에 포함된 중분류 구성도 함께 출력
- `행사명_norm` 기준으로 월별 중복 제거 후 집계"""),

    (43, """### 8-2-2. 타입별 중분류 공동출현 히트맵
- 묶음할인·콤보할인·콤보증정·장바구니할인 각각의 히트맵을 2×2로 배치
- 해당 타입에서 실제 공동출현한 중분류만 축으로 표시 (0 쌍 제외)
- `행사명_norm` 기준 집계 (월별 중복 제거)"""),

    (42, """### 8-2-1. 4개 타입 합산 중분류 공동출현 히트맵
- 묶음할인·콤보할인·콤보증정·장바구니할인을 타입별로 각각 카운트 후 합산
- 같은 행사명(`행사명_norm`)에 함께 등장한 중분류 쌍 수 집계
- 셀 값 = 해당 중분류 쌍이 공동 등장한 행사 수 (4개 타입 합계)
- 히트맵 아래 빈도 오름차순 테이블 출력"""),

    (41, """### 8-2-0. 행사명 시기 패턴 탐색
- `N월`, `NN년 N월`, `(N)` 등 시기 관련 접두/접미사 포함 행사명 샘플 확인
- 정규화 전 패턴 파악용 탐색 셀 (월별로 분리된 동일 행사 식별)"""),

    (33, """### 7-1-2. 정규화 행사명 기준 실제 운영일수 분석
- `행사명_norm` 기준으로 동일 프로모션의 여러 구간을 하나로 묶어 집계
- **총운영일수**: 구간 합산 (월 사이 공백 제외)
- **전체스팬**: 첫 개시일 ~ 마지막 종료일 (공백 포함)
- 위 7-1-1의 원본 분석(월별 단위)과 비교하여 장기 운영 프로모션 식별"""),

    (30, """### 7-0-1. 행사 개시일 시계열 분포
- 카테고리 필터링 후 `df_filtered` 기준 행사 개시 건수 월별 시각화
- `행사종료일 = 9999`(진행 중) 행은 `df_valid`로 별도 관리
- 행사별 지속일수(`행사종료일 - 행사개시일`) 계산 → `df_duration` 생성"""),

    (29, """## 7-0. 행사명 정규화
- 행사명에서 시기 관련 패턴 제거 → `행사명_norm` 컬럼 추가
  - 앞 접두사: `N월`, `N~N월`, `NN년 N월`
  - 뒤 접미사: `(1)`, `(2)` 등 순번 표기
- 정규화 후 `df_filtered`에 `행사명_norm` 컬럼이 추가되어 이후 모든 셀에서 공유
- **원본 `행사명`은 유지** — 정규화는 `행사명_norm` 컬럼으로만 적용"""),
]

# 역순 삽입
for idx, text in sorted(inserts, key=lambda x: -x[0]):
    nb['cells'].insert(idx, md_cell(text))
    print(f"Inserted markdown before (original) cell {idx}")

print(f"\nTotal cells: {len(nb['cells'])}")

with open(NB_PATH, 'w', encoding='utf-8') as f:
    json.dump(nb, f, ensure_ascii=False, indent=1)

print("Saved.")
