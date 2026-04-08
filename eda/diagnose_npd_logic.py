import polars as pl
import os

# [Copy-Paste Block for Jupyter Notebook]
# NPD 식별 로직 진단 및 데이터 절벽(Data Cliff) 보정

def diagnose_npd_identification(b2_path):
    print(f"--- [진단 시작] NPD 식별 로직 무결성 검토 ---")
    
    # 1. 전체 날짜 범위 확인
    b2_lazy = pl.scan_parquet(b2_path)
    date_range = b2_lazy.select([
        pl.col("판매일자").min().alias("start_date"),
        pl.col("판매일자").max().alias("end_date")
    ]).collect()
    
    start_date = date_range["start_date"][0]
    end_date = date_range["end_date"][0]
    print(f" - 데이터 시작일: {start_date}")
    print(f" - 데이터 종료일: {end_date}")

    # 2. 상품별 최초 등장일 집계
    launch_summary = (
        b2_lazy.group_by("상품코드")
        .agg(pl.col("판매일자").min().alias("first_appearance"))
        .collect()
        .group_by("first_appearance")
        .agg(pl.len().alias("new_item_count"))
        .sort("first_appearance")
    )

    # 3. 데이터 시작일(첫날)에 쏠린 상품 수 확인
    first_day_count = launch_summary.filter(pl.col("first_appearance") == start_date)["new_item_count"][0]
    print(f" - 데이터 첫날({start_date})에 등장한 상품 수: {first_day_count:,} 개 (가짜 신제품 가능성 99%)")
    
    # 4. 진짜 신제품(NPD) 식별을 위한 Burn-in 기간 설정
    # 데이터 시작 후 14일간은 기존 상품을 파악하는 기간으로 설정
    import datetime
    start_dt = datetime.datetime.strptime(start_date, "%Y%m%d")
    burn_in_end_dt = start_dt + datetime.timedelta(days=14)
    burn_in_threshold = burn_in_end_dt.strftime("%Y%m%d")
    
    # 5. 보정된 NPD 리스트 추출
    # 초기 14일간 판매 기록이 전혀 없다가, 그 이후에 처음 나타난 상품만 NPD
    legacy_items = (
        b2_lazy.filter(pl.col("판매일자") < burn_in_threshold)
        .select("상품코드").unique().collect()
    )
    
    true_npd = (
        b2_lazy.filter(~pl.col("상품코드").is_in(legacy_items["상품코드"]))
        .group_by("상품코드")
        .agg(pl.col("판매일자").min().alias("true_launch_dt"))
        .collect()
    )
    
    print(f"\n[보정 결과]")
    print(f" - 식별 기간(Burn-in) 내 발견된 기존 상품: {len(legacy_items):,} 개")
    print(f" - 이후 발견된 진짜 신제품(NPD): {len(true_npd):,} 개")
    print(f" - 전체 상품 중 진짜 NPD 비중: {len(true_npd) / (len(legacy_items) + len(true_npd)) * 100:.2f}%")
    
    return true_npd

# 실행부
B2_PATH = "../../data/processed/B2_POS_SALE_CLEANED.parquet"
if os.path.exists(B2_PATH):
    true_npd_list = diagnose_npd_identification(B2_PATH)
else:
    print(f"파일을 찾을 수 없습니다: {B2_PATH}")
