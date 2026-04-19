import polars as pl
import os
import glob
import json
from pathlib import Path

# 설정
RAW_APP_DIR = Path("data/raw/app")
MAPPING_FILE = RAW_APP_DIR / "상품코드목록_260416.csv"
OUTPUT_DIR = Path("data/processed/app_event_integrated")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def parse_event_value(val):
    if not val:
        return None, None
    try:
        # 이중 따옴표 처리 및 JSON 파싱
        data = json.loads(val.replace('""', '"'))
        af_content = data.get('af_content')
        af_user_id = data.get('af_customer_user_id')
        return af_content, af_user_id
    except:
        return None, None

def run_preprocessing():
    print("1. 로딩 매핑 테이블...")
    # CP949 인코딩으로 매핑 테이블 로드
    mapping_df = pl.read_csv(MAPPING_FILE, encoding="cp949", infer_schema_length=0)
    # 컬럼명이 깨질 수 있으므로 확인 후 변경 (첫번째 컬럼: 상품코드, 두번째 컬럼: 온라인코드)
    mapping_df.columns = ["pos_item_code", "online_item_code", "category_l", "category_m", "item_name", "price"]
    
    # 2. 월별 폴더 순회하며 처리
    month_folders = sorted(glob.glob(str(RAW_APP_DIR / "2025*_app_event")))
    
    for folder in month_folders:
        month = Path(folder).name.split('_')[0]
        print(f"Processing {month}...")
        
        csv_files = glob.glob(os.path.join(folder, "*.csv"))
        
        for csv_path in csv_files:
            file_name = Path(csv_path).name
            print(f"  - Reading {file_name}...")
            
            # Polars로 대용량 데이터 스캔
            df = pl.scan_csv(
                csv_path,
                ignore_errors=True,
                infer_schema_length=10000,
                truncate_ragged_lines=True
            )
            
            # 필터링: Is Primary Attribution == '참' (또는 True)
            df = df.filter(pl.col("Is Primary Attribution").cast(pl.Utf8).is_in(["참", "true", "True"]))
            
            # 필요한 컬럼만 선택 및 JSON 파싱을 위해 collect
            processed_df = df.select([
                "Event Time", "Event Name", "Event Value", "Media Source", "Campaign", "Platform", "AppsFlyer ID"
            ]).collect()
            
            # Event Value 파싱
            if len(processed_df) > 0:
                parsed = [parse_event_value(v) for v in processed_df["Event Value"]]
                
                processed_df = processed_df.with_columns([
                    pl.Series("af_content", [str(p[0]) if p[0] is not None else None for p in parsed]),
                    pl.Series("af_customer_user_id", [p[1] for p in parsed])
                ]).with_columns([
                    pl.col("af_content").str.pad_start(6, "0").alias("join_key")
                ])
                
                # 매핑 테이블 조인 (pos_item_code 기준)
                final_df = processed_df.join(
                    mapping_df, left_on="join_key", right_on="pos_item_code", how="left"
                )
                
                # 결과 저장
                out_path = OUTPUT_DIR / f"{file_name.replace('.csv', '.parquet')}"
                final_df.write_parquet(out_path)
                print(f"  - Saved to {out_path} (Rows: {len(final_df)})")

if __name__ == "__main__":
    run_preprocessing()
