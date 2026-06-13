import ast
import os
import pandas as pd

BASE = "data/processed/편의점_instagram"
FILES = [
    f"{BASE}/단일/CU단일.xlsx", f"{BASE}/단일/세븐단일.xlsx", f"{BASE}/단일/GS25_단일.xlsx",
    f"{BASE}/다중/CU다중.xlsx", f"{BASE}/다중/세븐다중.xlsx", f"{BASE}/다중/GS25_다중.xlsx"
]

# 검증 기준 설정 (가격)
PRICE_MIN = 500      # 500원 미만
PRICE_MAX = 15000    # 1.5만원 이상
SUSPICIOUS_VALS = [2024, 2025, 10, 100, 1, 2, 3] # 연도, 수량, 순번 등이 가격으로 오인된 경우

def parse_formatted_price(s):
    """(name, price, cap) 반환"""
    try:
        if not isinstance(s, str): return None, None, None
        s = s.strip()
        if s.startswith("{"): s = s[1:-1]
        
        # 포맷에서 키 부분(name, price, cap) 추출
        if "]: [" in s:
            key_part = s.split("]: [")[0] + "]"
        elif ']": [' in s:
            key_part = s.split(']": [')[0] + "]"
        else:
            return None, None, None
            
        key_data = ast.literal_eval(key_part)
        return key_data[0], key_data[1], key_data[2]
    except:
        return None, None, None

def verify_price_only():
    all_issues = []
    
    for path in FILES:
        if not os.path.exists(path):
            print(f"[경고] 파일 없음: {path}")
            continue
            
        file_name = os.path.basename(path)
        print(f"가격 검사 중: {file_name}")
        df = pd.read_excel(path)
        
        for i, row in df.iterrows():
            formatted = row.get("formatted_output", "")
            name, price, cap = parse_formatted_price(formatted)
            
            # 1. 가격/용량 플래그 확인
            flag_price = str(row.get("재추출_가격용량", "")).strip().upper() == "Y"
            
            # 2. 가격 이상치 확인 (플래그가 없더라도 데이터가 이상한 경우)
            price_issue = ""
            if price is not None:
                if price == 0:
                    price_issue = "가격 누락(0원)"
                elif price in SUSPICIOUS_VALS:
                    price_issue = f"의심 가격({price})"
                elif price < PRICE_MIN or price > PRICE_MAX:
                    price_issue = f"범위 이탈({price}원)"

            # 3. 용량 누락 확인 (가격은 있는데 용량이 null/none인 경우)
            cap_issue = ""
            if not flag_price and (cap is None or str(cap).lower() in ("null", "none", "")):
                cap_issue = "용량 누락"

            # 리포트에 추가할 조건: 플래그가 여전히 Y거나, 데이터가 의심스럽거나
            if flag_price or price_issue or cap_issue:
                reason = []
                if flag_price: reason.append("플래그(Y) 유지")
                if price_issue: reason.append(price_issue)
                if cap_issue: reason.append(cap_issue)

                all_issues.append({
                    "파일": file_name,
                    "상품명": name or row.get("title", "Unknown"),
                    "가격": price,
                    "용량": cap,
                    "이슈원인": " / ".join(reason),
                    "본문일부": str(row.get("body", ""))[:100] + "..."
                })

    if all_issues:
        report_df = pd.DataFrame(all_issues)
        report_path = "data/processed/price_inspection_report.csv"
        report_df.to_csv(report_path, index=False, encoding="utf-8-sig")
        print(f"\n[검사 완료] 총 {len(all_issues)}건의 확인 대상 발견!")
        print(f"가격 리포트 저장됨: {report_path}")
    else:
        print("\n[검사 완료] 가격/용량 데이터가 모두 정상 범위입니다!")

if __name__ == "__main__":
    verify_price_only()
