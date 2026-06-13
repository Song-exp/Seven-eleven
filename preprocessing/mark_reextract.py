import ast
import sys
import pandas as pd

sys.stdout.reconfigure(encoding="utf-8")

BASE = "data/processed/편의점_instagram"
FILES = {
    "단일/CU단일":   f"{BASE}/단일/CU단일.xlsx",
    "단일/세븐단일": f"{BASE}/단일/세븐단일.xlsx",
    "단일/GS25단일": f"{BASE}/단일/GS25_단일.xlsx",
    "다중/CU다중":   f"{BASE}/다중/CU다중.xlsx",
    "다중/세븐다중": f"{BASE}/다중/세븐다중.xlsx",
    "다중/GS25다중": f"{BASE}/다중/GS25_다중.xlsx",
}


def parse_formatted(s):
    """
    formatted_output 문자열에서 (price, capacity, attrs_list) 반환.
    파싱 실패 시 (None, None, None) 반환.
    """
    if not isinstance(s, str):
        return None, None, None

    s = s.strip()
    if s.startswith("{") and s.endswith("}"):
        s = s[1:-1].strip()

    # 세븐 단일 포맷: "['제품명', price, 'cap']": ['attr', ...]  (구분자 5글자: ]": [)
    if s.startswith('"[') and ']": [' in s:
        idx = s.index(']": [')
        key_str = s[1 : idx + 1]   # leading " 제거, ] 포함
        val_str = s[idx + 4 :]     # [ 부터 (]": [ 이 5글자이므로 idx+4)
    # CU 포맷 / 다중 공통 포맷: ['제품명', price, 'cap']: ['attr', ...]  (구분자 4글자: ]: [)
    elif "]: [" in s:
        idx = s.index("]: [")
        key_str = s[: idx + 1]
        val_str = s[idx + 3 :]
    else:
        return None, None, None

    try:
        key_list = ast.literal_eval(key_str)
        price = key_list[1] if len(key_list) > 1 else None
        capacity = key_list[2] if len(key_list) > 2 else None
    except Exception:
        price, capacity = None, None

    try:
        val_list = ast.literal_eval(val_str)
    except Exception:
        val_list = None

    return price, capacity, val_list


def is_price_null(price):
    """price == 0 이면 null로 처리 (LLM이 null → 0 변환)"""
    if price is None:
        return True
    return price == 0


def is_capacity_null(capacity):
    if capacity is None:
        return True
    return str(capacity).strip().lower() in ("null", "none", "")


def mark_reextract(df: pd.DataFrame) -> pd.DataFrame:
    parsed = df["formatted_output"].apply(parse_formatted)

    prices    = parsed.apply(lambda x: x[0])
    capacities = parsed.apply(lambda x: x[1])
    attrs     = parsed.apply(lambda x: x[2])

    # 조건 1: 가격 또는 용량 null → 재추출_가격용량
    flag_meta = prices.combine(capacities, func=lambda p, c: is_price_null(p) or is_capacity_null(c))
    df["재추출_가격용량"] = flag_meta.map({True: "Y", False: ""})

    # 조건 2: 속성 개수 3개 이하 → 재추출_속성
    flag_attr = attrs.apply(lambda a: len(a) <= 3 if isinstance(a, list) else True)
    df["재추출_속성"] = flag_attr.map({True: "Y", False: ""})

    return df


def main():
    for name, path in FILES.items():
        df = pd.read_excel(path)

        # 기존 플래그 컬럼 중복 방지
        for col in ["재추출_가격용량", "재추출_속성"]:
            if col in df.columns:
                df = df.drop(columns=[col])

        df = mark_reextract(df)
        df.to_excel(path, index=False)

        n_meta = (df["재추출_가격용량"] == "Y").sum()
        n_attr = (df["재추출_속성"] == "Y").sum()
        print(
            f"[{name}] 총 {len(df)}행 "
            f"| 재추출_가격용량: {n_meta}행 "
            f"| 재추출_속성: {n_attr}행"
        )


if __name__ == "__main__":
    main()
