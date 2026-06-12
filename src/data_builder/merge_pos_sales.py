import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

def preprocess_chunk(df):
    # 컬럼명 매핑 (GEMINI.md 명세 준수)
    column_mapping = {
        'SALE_DATE': '판매일자',
        'SALE_TIME': '판매시간',
        'STORE_CODE': '점포코드',
        'POS_NO': 'POS번호',
        'TRADE_NO': '거래번호',
        'ITEM_CODE': '상품코드',
        'SALE_QTY': '판매수량',
        'SALE_AMT': '판매금액'
    }
    df = df.rename(columns=column_mapping)
    
    # 판매수량 및 판매금액 전처리
    # (read_csv에서 thousands=',' 처리로 인해 이미 숫자형으로 변환되었으므로 콤마 제거 로직 생략)
    df['판매수량'] = pd.to_numeric(df['판매수량'], errors='coerce').fillna(0).astype(int)
    df['판매금액'] = pd.to_numeric(df['판매금액'], errors='coerce').fillna(0).astype(int)
    
    # 타입 최적화 (날짜 포맷을 명시하여 변환 속도 최적화)
    df['판매일자'] = pd.to_datetime(df['판매일자'], format='%Y%m%d', errors='coerce').dt.date
    
    return df

def merge_csv_to_parquet(input_files, output_file, chunk_size=100000):
    writer = None
    
    for file_path in input_files:
        print(f"Processing {file_path}...")
        
        # CSV 읽기: thousands=',' 옵션으로 콤마를 미리 처리하고, 식별자 컬럼들은 문자열로 지정
        chunks = pd.read_csv(
            file_path, 
            chunksize=chunk_size, 
            thousands=',', 
            dtype={
                'SALE_TIME': str,
                'STORE_CODE': str,
                'POS_NO': str,
                'TRADE_NO': str,
                'ITEM_CODE': str
            }
        )
        
        for chunk in chunks:
            processed_chunk = preprocess_chunk(chunk)
            
            # PyArrow Table로 변환
            table = pa.Table.from_pandas(processed_chunk)
            
            # 첫 번째 청크일 때 ParquetWriter 초기화
            if writer is None:
                writer = pq.ParquetWriter(output_file, table.schema, compression='snappy')
            
            writer.write_table(table)
            
    if writer:
        writer.close()
    print(f"Finished! Saved to {output_file}")

if __name__ == "__main__":
    raw_dir = "data/raw"
    processed_dir = "data/processed"
    
    # 출력 디렉토리 생성
    if not os.path.exists(processed_dir):
        os.makedirs(processed_dir)
        
    input_files = [
        os.path.join(raw_dir, "B2_POS_SALE_H1.csv"),
        os.path.join(raw_dir, "B2_POS_SALE_H2.csv")
    ]
    output_file = os.path.join(processed_dir, "B2_POS_SALE.parquet")
    
    merge_csv_to_parquet(input_files, output_file)