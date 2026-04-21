import pandas as pd
import os
import json

def main():
    # 세븐일레븐 원본 파일 (Desktop)
    input_path = '/Users/yumi/Desktop/7eleven_multiple_products_final.xlsx'
    # 결과 저장 경로 (Downloads)
    output_path = '/Users/yumi/Downloads/7eleven_single_products_exploded.xlsx'
    
    if not os.path.exists(input_path):
        print(f"❌ 원본 파일을 찾을 수 없습니다: {input_path}")
        return

    print(f"🔄 '{input_path}' 읽기 중...")
    df = pd.read_excel(input_path)
    
    print(f"📊 원본 컬럼: {df.columns.tolist()}")
    
    # "비고"와 "식품 여부" 제외 (만약 있다면)
    cols_to_drop = [c for c in ['비고', '식품 여부', '식품여부'] if c in df.columns]
    if cols_to_drop:
        print(f"🗑 제외할 컬럼: {cols_to_drop}")
        df = df.drop(columns=cols_to_drop)
    
    # formatted_output 컬럼 전처리 및 폭발(explode)
    # 세븐일레븐의 경우 JSON 문자열 안에 여러 딕셔너리 키가 있음
    def parse_formatted_output(val):
        if not isinstance(val, str) or not val.strip():
            return []
        try:
            parsed = json.loads(val)
            if isinstance(parsed, dict):
                # 각 키-값 쌍을 "키: 값" 형태의 문자열 리스트로 변환
                return [f"{json.dumps([k], ensure_ascii=False)[1:-1]}: {json.dumps(v, ensure_ascii=False)}" for k, v in parsed.items()]
            elif isinstance(parsed, list):
                # 리스트인 경우 각 항목 처리
                results = []
                for item in parsed:
                    if isinstance(item, dict):
                        for k, v in item.items():
                            results.append(f"{json.dumps([k], ensure_ascii=False)[1:-1]}: {json.dumps(v, ensure_ascii=False)}")
                return results
        except:
            # JSON이 아닌 일반 문자열인 경우 줄바꿈 분리 시도
            return val.split('\n')
        return []

    # 더 깔끔한 문자열 출력을 위한 함수 (JSON 직렬화 대신 읽기 쉬운 포맷)
    def format_pair(k, v):
        # k는 이미 "['상품명', 가격, '용량']" 형태의 문자열일 가능성이 높음
        return f"{k}: {v}"

    def parse_to_list(val):
        if not isinstance(val, str) or not val.strip():
            return []
        try:
            parsed = json.loads(val)
            if isinstance(parsed, dict):
                return [format_pair(k, v) for k, v in parsed.items()]
            elif isinstance(parsed, list):
                results = []
                for item in parsed:
                    if isinstance(item, dict):
                        for k, v in item.items():
                            results.append(format_pair(k, v))
                return results
        except:
            return [x.strip() for x in val.split('\n') if x.strip()]
        return []

    print("💥 'formatted_output' JSON 파싱 및 행 분리 중...")
    df['formatted_output'] = df['formatted_output'].apply(parse_to_list)
    
    # explode 수행
    df_exploded = df.explode('formatted_output').reset_index(drop=True)
    
    # 빈 행 제거
    df_exploded = df_exploded[df_exploded['formatted_output'].notna() & (df_exploded['formatted_output'] != "")]
    
    print(f"✅ 행 확장 완료: {len(df)}개 -> {len(df_exploded)}개")
    
    # 저장
    print(f"💾 결과 저장 중: {output_path}...")
    df_exploded.to_excel(output_path, index=False, engine='openpyxl')
    print("✨ 완료!")

if __name__ == "__main__":
    main()
