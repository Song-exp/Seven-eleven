import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import os
import gc

def process_large_pos_data():
    # 1. 경로 설정 (현재 프로젝트 구조에 맞게 수정)
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    raw_data_dir = os.path.join(project_root, '원본_데이터셋')
    processed_data_dir = os.path.join(project_root, '전처리_EDA', '최종')
    
    b2_paths = [
        os.path.join(raw_data_dir, 'B2_POS_SALE_H1.csv'),
        os.path.join(raw_data_dir, 'B2_POS_SALE_H2.csv')
    ]
    b4_path = os.path.join(raw_data_dir, 'B4_ITEM_DV_INFO.csv')
    
    output_dir = processed_data_dir
    output_path = os.path.join(output_dir, 'df_B2_B4_merged.parquet')

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"디렉토리 생성: {output_dir}")

    # 2. 상품 마스터(B4) 미리 로드 (상대적으로 작음)
    print("상품 마스터(B4) 로딩 중...")
    df_b4 = pd.read_csv(b4_path, low_memory=False, encoding='utf-8')
    df_b4['ITEM_CD'] = df_b4['ITEM_CD'].astype(str).str.zfill(6)
    df_b4 = df_b4[['ITEM_CD', 'ITEM_NM', 'ITEM_LRDV_NM', 'ITEM_MDDV_NM', 'ITEM_SMDV_NM']]

    # Parquet Writer 초기화
    writer = None

    # 3. CSV 파일을 청크 단위로 처리 (메모리 사용량을 일정하게 유지)
    chunk_size = 1000000  # 100만 행씩 처리
    
    for file_path in b2_paths:
        if not os.path.exists(file_path):
            print(f"파일을 찾을 수 없습니다: {file_path}")
            continue
            
        print(f"\n파일 읽기 시작: {os.path.basename(file_path)}")
        # chunksize 지정 시 메모리에 전체를 올리지 않고 Iterator를 반환
        reader = pd.read_csv(file_path, chunksize=chunk_size, low_memory=False, encoding='cp949')
        
        for i, chunk in enumerate(reader):
            # A. 청크 데이터 클리닝
            for col in ['SALE_QTY', 'SALE_AMT']:
                # str.replace 연산은 청크 단위에서 수행하여 메모리 폭증 방지
                chunk[col] = pd.to_numeric(chunk[col].astype(str).str.replace(',', ''), errors='coerce').astype('float32')
            
            chunk['ITEM_CODE'] = chunk['ITEM_CODE'].astype(str).str.zfill(6)
            
            # B. 상품 정보 결합
            merged_chunk = pd.merge(chunk, df_b4, left_on='ITEM_CODE', right_on='ITEM_CD', how='left')
            merged_chunk.drop(columns=['ITEM_CD'], inplace=True)
            
            # C. Parquet 파일에 스트리밍 작성
            table = pa.Table.from_pandas(merged_chunk)
            if writer is None:
                # 첫 청크 작성 시 스키마 정의 및 파일 생성
                writer = pq.ParquetWriter(output_path, table.schema, compression='snappy')
            
            if writer is not None:
                table = table.select(writer.schema.names)
            writer.write_table(table)
            
            if (i + 1) % 5 == 0:
                print(f" - {i+1}백만 행 처리 완료...")
            
            # D. 즉시 메모리 해제
            del chunk, merged_chunk, table
            gc.collect()

    if writer:
        writer.close()
    
    print(f"\n최종 작업 완료! 파일 저장 경로: {output_path}")
    # 결과 확인을 위한 메타데이터 출력
    final_meta = pq.read_metadata(output_path)
    print(f"최종 저장된 행 수: {final_meta.num_rows:,}")

if __name__ == "__main__":
    try:
        process_large_pos_data()
    except Exception as e:
        print(f"\n처리 중 오류 발생: {e}")
