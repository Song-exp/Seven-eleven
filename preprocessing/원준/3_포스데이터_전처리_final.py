import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import pyarrow as pa
import os
import gc

# 설정
INPUT_PATH  = 'df_전처리완료.parquet'
OUTPUT_PATH = 'pos_data_전처리완료_final.parquet'

def preprocess():
    # 실행 위치 확인 및 디렉토리 설정
    if not os.path.exists(INPUT_PATH) and os.path.exists('최종/' + INPUT_PATH):
        os.chdir('최종')
        print(f"디렉토리를 '최종'으로 이동하였습니다.")

    print(f"입력 파일 확인: {INPUT_PATH}")
    if not os.path.exists(INPUT_PATH):
        print(f"오류: {INPUT_PATH} 파일이 존재하지 않습니다.")
        return

    pf = pq.ParquetFile(INPUT_PATH)
    meta = pf.metadata
    print(f"총 Row Groups : {meta.num_row_groups}")
    print(f"총 행 수      : {meta.num_rows:,}")

    # --- 1. 사전 처리: ZERO_AMT_NEG_QTY 데이터의 선판매-후취소 매칭 식별 ---
    print("\n[Step 0] ZERO_AMT_NEG_QTY 판매 후 취소 데이터 매칭 식별 중...")
    
    # 1) 매칭이 필요한 ZERO_AMT_NEG_QTY 데이터 로드
    try:
        df_neg_all = pd.read_parquet(
            INPUT_PATH,
            filters=[("flag_이상치_유형", "=", "ZERO_AMT_NEG_QTY")],
            columns=["거래_고유키", "점포코드", "상품코드", "판매시간_dt"]
        )
    except Exception as e:
        print(f"데이터 로드 중 오류 발생: {e}")
        return

    df_neg_all["판매시간_dt"] = pd.to_datetime(df_neg_all["판매시간_dt"])
    print(f"  대상(ZERO_AMT_NEG_QTY) 행수: {len(df_neg_all):,}")

    # 2) 매칭용 판매 데이터 (NORMAL + GIFT_OR_ZERO_QTY) 로드
    # 메모리 효율을 위해 대상 상품코드로 필터링하여 로드
    target_products = df_neg_all["상품코드"].unique().tolist()
    df_sales_match = pd.read_parquet(
        INPUT_PATH,
        filters=[
            ("flag_이상치_유형", "in", ["NORMAL", "GIFT_OR_ZERO_QTY"]),
            ("상품코드", "in", target_products)
        ],
        columns=["점포코드", "상품코드", "판매시간_dt"]
    )
    df_sales_match["판매시간_dt"] = pd.to_datetime(df_sales_match["판매시간_dt"])
    print(f"  매칭용 판매(NORMAL/GIFT) 행수: {len(df_sales_match):,}")

    # 3) merge_asof: 취소 시점 이전 동일 점포·상품의 판매거래 탐색
    neg_sorted = df_neg_all.sort_values("판매시간_dt")
    sales_sorted = df_sales_match.sort_values("판매시간_dt")

    df_matched = pd.merge_asof(
        neg_sorted,
        sales_sorted[["점포코드", "상품코드", "판매시간_dt"]].rename(columns={"판매시간_dt": "_판매시간"}),
        left_on="판매시간_dt",
        right_on="_판매시간",
        by=["점포코드", "상품코드"],
        direction="backward"
    )

    # 매칭 성공 = _판매시간이 존재하는 행의 고유키 추출
    valid_zero_amt_neg_keys = set(df_matched.dropna(subset=["_판매시간"])["거래_고유키"])
    print(f"  ZERO_AMT_NEG_QTY 매칭 성공(유지): {len(valid_zero_amt_neg_keys):,}건")
    print(f"  ZERO_AMT_NEG_QTY 매칭 실패(삭제): {len(df_neg_all) - len(valid_zero_amt_neg_keys):,}건")

    del df_neg_all, df_sales_match, df_matched, neg_sorted, sales_sorted
    gc.collect()

    # --- 2. 청크 단위 본 처리 및 저장 ---
    print("\n[Step 1-3] 청크 단위 전처리 및 저장 시작...")
    
    KEEP_FLAGS = {'NORMAL', 'REFUND_NEGATIVE', 'GIFT_OR_ZERO_QTY', 'ZERO_AMT_NEG_QTY'}
    KEYWORD_PAT = '공병|수수료'

    total_in = 0
    removed_flag = 0      # 기타 이상치(DATA_SUSPECT 등) 삭제
    removed_keyword = 0   # 공병/수수료/공병공박스 삭제
    removed_unmatched = 0 # 이전 거래 없는 ZERO_AMT_NEG_QTY 삭제
    total_out = 0

    writer = None

    for rg_idx in range(meta.num_row_groups):
        chunk = pf.read_row_group(rg_idx).to_pandas()
        n_in = len(chunk)
        total_in += n_in

        # --- Step 1: 유지 대상 플래그 필터링 ---
        mask_keep = chunk['flag_이상치_유형'].isin(KEEP_FLAGS)
        removed_flag += (n_in - mask_keep.sum())
        chunk = chunk[mask_keep]

        # --- Step 2: 공통 삭제 로직 (공병/수수료/공병공박스) ---
        # 1) REFUND_NEGATIVE 중 상품명에 '공병' 또는 '수수료' 포함
        is_refund = chunk['flag_이상치_유형'] == 'REFUND_NEGATIVE'
        has_keyword = chunk['상품명'].str.contains(KEYWORD_PAT, na=False)
        # 2) 대분류가 '공병공박스'인 경우 (전체 대상)
        is_empty_box = chunk['상품대분류명'] == '공병공박스'
        
        mask_keyword_remove = (is_refund & has_keyword) | is_empty_box
        removed_keyword += mask_keyword_remove.sum()
        chunk = chunk[~mask_keyword_remove]

        # --- Step 3: 이전 거래 없는 ZERO_AMT_NEG_QTY만 제거 ---
        # (REFUND_NEGATIVE와 GIFT_OR_ZERO_QTY는 조건 없이 보존)
        is_zero_amt_neg = chunk['flag_이상치_유형'] == 'ZERO_AMT_NEG_QTY'
        is_unmatched = is_zero_amt_neg & ~chunk['거래_고유키'].isin(valid_zero_amt_neg_keys)
        
        removed_unmatched += is_unmatched.sum()
        chunk = chunk[~is_unmatched]

        # 최종 출력 및 저장
        total_out += len(chunk)
        chunk = chunk.drop(columns=['flag_이상치_유형'])
        
        table = pa.Table.from_pandas(chunk, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(OUTPUT_PATH, table.schema)
        writer.write_table(table)

        if (rg_idx + 1) % 10 == 0 or rg_idx == meta.num_row_groups - 1:
            print(f"  [RG {rg_idx+1:3d}/{meta.num_row_groups}] 누적 입력: {total_in:,} / 누적 출력: {total_out:,}")

    if writer:
        writer.close()

    print("\n" + "="*50)
    print("          전처리 결과 요약")
    print("="*50)
    print(f"  ▸ 원본 행 수                : {total_in:>12,} 행")
    print(f"  ▸ Step1 삭제 (기타 이상치)   : {removed_flag:>12,} 행")
    print(f"  ▸ Step2 삭제 (공병/수수료 등): {removed_keyword:>12,} 행")
    print(f"  ▸ Step3 삭제 (이력없는 ZERO): {removed_unmatched:>12,} 행")
    print(f"  ▸ 최종 행 수                : {total_out:>12,} 행")
    print("="*50)
    print(f"저장 완료: {os.path.abspath(OUTPUT_PATH)}")

if __name__ == "__main__":
    preprocess()
