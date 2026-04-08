import polars as pl
import datetime
import os

# [Copy-Paste Block for Jupyter Notebook]
# Step 1.0: 정밀 NPD 식별 (Burn-in Period 기반 데이터 절벽 보정)

def get_true_npd_list(b2_path, burn_in_days=14):
    """
    데이터 시작일로부터 일정 기간(burn_in_days)을 관측 기간으로 설정하여,
    기존 상품(Legacy)을 제외한 순수 신제품(NPD)만을 식별합니다.
    """
    print(f"--- [Step 1.0] 정밀 NPD 식별 프로세스 시작 ---")
    b2_lazy = pl.scan_parquet(b2_path)
    
    # 1. 데이터 시작일 확인
    start_date = b2_lazy.select(pl.col("판매일자").min()).collect().item()
    start_dt = datetime.datetime.strptime(start_date, "%Y%m%d")
    
    # 2. Burn-in 종료일 계산 (기존 상품 식별 기간)
    burn_in_end_dt = start_dt + datetime.timedelta(days=burn_in_days)
    burn_in_threshold = burn_in_end_dt.strftime("%Y%m%d")
    
    print(f" - 데이터 시작일: {start_date}")
    print(f" - 기존 상품 식별 기간(Burn-in): {start_date} ~ {burn_in_threshold} (총 {burn_in_days}일)")

    # 3. 기존 상품(Legacy) 리스트 확보
    # 초기 관측 기간 동안 한 번이라도 판매된 기록이 있는 상품들
    legacy_items = (
        b2_lazy.filter(pl.col("판매일자") <= burn_in_threshold)
        .select("상품코드").unique().collect()
    )
    legacy_set = set(legacy_items["상품코드"].to_list())
    
    # 4. 진짜 신제품(NPD) 식별
    # 초기 관측 기간 이후에 '처음' 나타난 상품들만 필터링
    true_npd_df = (
        b2_lazy.filter(~pl.col("상품코드").is_in(list(legacy_set)))
        .group_by("상품코드")
        .agg(pl.col("판매일자").min().alias("true_launch_dt"))
        .collect()
        .sort("true_launch_dt")
    )
    
    true_npd_set = set(true_npd_df["상품코드"].to_list())
    
    print(f"\n[식별 결과]")
    print(f" - 전체 고유 상품 수: {len(legacy_set) + len(true_npd_set):,} 개")
    print(f" - 기존 상품(Legacy) 수: {len(legacy_set):,} 개")
    print(f" - 보정된 진짜 신제품(NPD) 수: {len(true_npd_set):,} 개")
    
    return true_npd_set, true_npd_df

# [실행부] 노트북 최상단에서 한 번만 실행
B2_PATH = "../../data/processed/B2_POS_SALE_CLEANED.parquet"
if os.path.exists(B2_PATH):
    TRUE_NPD_SET, TRUE_NPD_DF = get_true_npd_list(B2_PATH)
    # 이제 이 TRUE_NPD_SET을 이후 모든 분석 함수의 인자로 전달합니다.
else:
    print(f"파일을 찾을 수 없습니다: {B2_PATH}")
