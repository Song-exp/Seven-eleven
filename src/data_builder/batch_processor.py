import polars as pl
import os
import json
import time
from tqdm import tqdm
from keyword_extractor import extract_keywords
from attribute_inferrer import infer_attributes

def batch_process_llm_features():
    """
    통합 데이터셋을 읽어 모든 제품에 대해 키워드 추출 및 속성 추론을 수행합니다.
    중간 저장 기능을 포함합니다.
    """
    print("배치 처리 시작 준비 중...")
    
    # 1. 파일 경로 설정
    input_path = "data/processed/PRODUCT_FULL_CONTEXT.parquet"
    output_path = "data/processed/PRODUCT_ENRICHED_FEATURES.parquet"
    checkpoint_path = "data/processed/llm_processing_checkpoint.json"
    
    if not os.path.exists(input_path):
        print(f"오류: {input_path} 파일이 없습니다. final_data_aggregator.py를 먼저 실행하세요.")
        return

    # 2. 데이터 로드 및 진행 상황 확인
    df = pl.read_parquet(input_path)
    total_products = len(df)
    
    processed_data = []
    processed_codes = set()
    
    # 체크포인트 로드 (기존에 작업하던 내용이 있다면)
    if os.path.exists(checkpoint_path):
        with open(checkpoint_path, "r", encoding="utf-8") as f:
            processed_data = json.load(f)
            processed_codes = {item["product_code"] for item in processed_data}
        print(f"체크포인트 로드 완료: {len(processed_codes)} / {total_products} 진행됨.")

    # 3. 배치 처리 루프
    print(f"LLM 처리 시작 (대상: {total_products - len(processed_codes)}개 상품)...")
    
    try:
        # 진행률 표시줄 추가
        for row in tqdm(df.iter_rows(named=True), total=total_products, desc="Processing Products"):
            code = row["상품코드"]
            name = row["상품명"]
            context = row["full_text_description"]
            
            # 이미 처리된 상품은 건너뜀
            if code in processed_codes:
                continue
            
            # LLM 처리 1: 키워드 추출 (gemma4:e4b)
            keywords = extract_keywords(context)
            
            # LLM 처리 2: 속성 추론 (gemma4:26b)
            attributes = infer_attributes(name) # 속성은 제품명 기반이 더 정확할 수 있음
            
            # 결과 저장
            processed_data.append({
                "product_code": code,
                "product_name": name,
                "keywords": keywords,
                "attributes": attributes,
                "processed_at": time.strftime("%Y-%m-%d %H:%M:%S")
            })
            processed_codes.add(code)
            
            # 10개 상품마다 체크포인트 저장 및 화면 출력
            if len(processed_codes) % 10 == 0:
                with open(checkpoint_path, "w", encoding="utf-8") as f:
                    json.dump(processed_data, f, ensure_ascii=False, indent=4)
                    
    except KeyboardInterrupt:
        print("\n사용자에 의해 중단되었습니다. 현재까지의 작업을 저장합니다.")
    except Exception as e:
        print(f"\n오류 발생: {e}. 현재까지의 작업을 저장합니다.")
    finally:
        # 4. 최종 결과 저장 (Parquet 변환)
        if processed_data:
            df_result = pl.from_dicts(processed_data)
            df_result.write_parquet(output_path)
            
            # 체크포인트 파일 업데이트
            with open(checkpoint_path, "w", encoding="utf-8") as f:
                json.dump(processed_data, f, ensure_ascii=False, indent=4)
                
            print(f"\n작업 완료! 결과가 {output_path}에 저장되었습니다.")
            print(f"총 {len(processed_data)}개 상품 처리 완료.")

if __name__ == "__main__":
    batch_process_llm_features()
