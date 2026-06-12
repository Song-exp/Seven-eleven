import pandas as pd
import polars as pl
import os

# Paths
ROOT = r"C:\Users\송정현\Documents\Projects\박재홍교수님세미나\Projects\20기\7eleven_npd_framework"
B2_PATH = os.path.join(ROOT, "data", "processed", "POS 전처리 최종", "pos_data_food_final_상품단위변환전.parquet")
B4_PATH = os.path.join(ROOT, "data", "processed", "B4_ITEM_DV_INFO.parquet")
CLUSTER_XLSX = os.path.join(ROOT, "eda", "00_NPD_lifecycle_clusters.xlsx")
PARETO_FILTER_XLSX = os.path.join(ROOT, "eda", "카테고리필터링_pareto기준자르기.xlsx")

def export_success_failure():
    print("🚀 성공/부진 상품군 엑셀 추출 시작...")
    
    # 1. B4 마스터 데이터 로드
    try:
        b4_df = pl.read_parquet(B4_PATH)
    except:
        b4_df = pl.read_csv(B4_PATH.replace('.parquet', '.csv'), ignore_errors=True, infer_schema_length=10000)
    
    if "ITEM_CD" in b4_df.columns:
        b4_df = b4_df.rename({"ITEM_CD": "상품코드"})
    if "상품명" in b4_df.columns and "ITEM_NM" not in b4_df.columns:
        b4_df = b4_df.rename({"상품명": "ITEM_NM", "대분류명": "ITEM_LGDV_NM", "중분류명": "ITEM_MDDV_NM"})
    b4_df = b4_df.with_columns(pl.col("상품코드").cast(pl.Utf8))
    b4_pd = b4_df.select(["상품코드", "ITEM_NM", "ITEM_LGDV_NM", "ITEM_MDDV_NM"]).to_pandas().drop_duplicates("상품코드")

    # 2. 파레토 필터링된 카테고리만 사용 (Notebook 2-1 절과 동일)
    pareto_filter_df = pd.read_excel(PARETO_FILTER_XLSX)
    survived_cats = set(pareto_filter_df.loc[pareto_filter_df["사용여부"] == "O", "ITEM_MDDV_NM"].tolist())
    
    # 3. NPD 식별 (Notebook 1.0 절과 동일)
    # 14일 번인 기간 사용
    b2_lazy = pl.scan_parquet(B2_PATH)
    start_date = b2_lazy.select(pl.col("영업일자").min()).collect().item()
    import datetime
    start_dt = datetime.datetime.strptime(str(start_date), "%Y%m%d")
    burn_in_threshold = int((start_dt + datetime.timedelta(days=14)).strftime("%Y%m%d"))
    
    legacy_items = b2_lazy.filter(pl.col("영업일자") < burn_in_threshold).select("상품코드").unique().collect()["상품코드"].to_list()
    legacy_set = set(legacy_items)
    
    # 식품 카테고리 필터
    food_categories = ['음료', '과자', '유음료', '미반', '면', '냉장', '맥주', '즉석음료', '빵', '전통주', '아이스크림', '조리빵', '즉석 식품', '건강/기호식품', '가공식품', '양주와인', '디저트', '안주', '신선', '간식', '조미료/건물', '냉동']
    food_item_list = b4_df.filter(pl.col("ITEM_LGDV_NM").is_in(food_categories)).select("상품코드").to_series().to_list()
    
    npd_candidates = b2_lazy.filter(pl.col("상품코드").is_in(food_item_list) & ~pl.col("상품코드").is_in(legacy_items)).group_by("상품코드").agg(pl.col("영업일자").min().alias("launch_dt")).collect()
    
    # 1개월 매출 산출
    npd_launch_df = npd_candidates.with_columns([
        pl.col("launch_dt").cast(pl.Utf8).str.to_date("%Y%m%d").alias("launch_date")
    ]).with_columns([
        (pl.col("launch_date") + pl.duration(days=30)).alias("end_date")
    ]).with_columns([
        pl.col("end_date").dt.strftime("%Y%m%d").cast(pl.Int64).alias("end_dt")
    ])
    
    b2_joined = b2_lazy.filter(pl.col("상품코드").is_in(npd_candidates["상품코드"].to_list())).join(npd_launch_df.lazy(), on="상품코드", how="inner")
    
    npd_sales = b2_joined.filter((pl.col("영업일자") >= pl.col("launch_dt")) & (pl.col("영업일자") <= pl.col("end_dt"))).group_by("상품코드").agg(pl.col("매출금액").sum().alias("1month_sales")).collect().to_pandas()
    
    # Ghost NPD 제거 (매출 <= 0)
    npd_sales = npd_sales[npd_sales["1month_sales"] > 0]
    
    # 4. 생존 카테고리 필터링
    npd_full = pd.merge(npd_sales, b4_pd, on="상품코드", how="left")
    npd_filtered = npd_full[npd_full["ITEM_MDDV_NM"].isin(survived_cats)].copy()
    
    # 5. 파레토 성공 여부 판단 (상위 20%)
    npd_filtered['rank_pct'] = npd_filtered.groupby('ITEM_MDDV_NM')['1month_sales'].rank(pct=True)
    npd_filtered['파레토_성공여부'] = npd_filtered['rank_pct'].apply(lambda x: '성공' if x >= 0.8 else '부진')
    
    # 6. 클러스터 정보 병합 (있을 경우)
    if os.path.exists(CLUSTER_XLSX):
        cluster_df = pd.read_excel(CLUSTER_XLSX)
        print(f"Cluster columns: {cluster_df.columns.tolist()}")
        
        # 상품코드 컬럼 찾기 (한글 '상품코드' 또는 'ITEM_CD' 등)
        code_col = None
        for col in cluster_df.columns:
            if '코드' in str(col) or 'CD' in str(col).upper():
                code_col = col
                break
        
        if code_col:
            cluster_df[code_col] = cluster_df[code_col].astype(str)
            cluster_df = cluster_df.rename(columns={code_col: '상품코드'})
            
            # 클러스터 레이블 컬럼 찾기
            label_col = None
            for col in cluster_df.columns:
                if '레이블' in str(col) or 'LABEL' in str(col).upper() or 'cluster' in str(col).lower():
                    if col != '상품코드':
                        label_col = col
                        break
            
            if label_col:
                cluster_df = cluster_df.rename(columns={label_col: '생애주기_클러스터'})
            
            if '생애주기_클러스터' in cluster_df.columns:
                npd_filtered = pd.merge(npd_filtered, cluster_df[['상품코드', '생애주기_클러스터']], on='상품코드', how='left')
                
                def get_cluster_status(val):
                    if pd.isna(val): return "미분류"
                    # Notebook: Cluster 1 is "부진" (L-curve)
                    if "부진" in str(val) or "1" in str(val): return "부진"
                    return "성공/보통"
                
                npd_filtered['클러스터_성공여부'] = npd_filtered['생애주기_클러스터'].apply(get_cluster_status)

    # 7. 최종 라벨링 (파레토 성공 or 클러스터 성공 기준 종합)
    # 여기서는 파레토를 주 지표로 사용
    npd_filtered['최종_분류'] = npd_filtered['파레토_성공여부']
    
    # 컬럼 정리
    final_cols = ['상품코드', 'ITEM_NM', 'ITEM_LGDV_NM', 'ITEM_MDDV_NM', '1month_sales', 'rank_pct', '파레토_성공여부']
    if '생애주기_클러스터' in npd_filtered.columns:
        final_cols += ['생애주기_클러스터', '클러스터_성공여부']
    
    out_path = os.path.join(ROOT, "eda", "성공_부진_상품군_리스트.xlsx")
    npd_filtered[final_cols].sort_values(['ITEM_MDDV_NM', '1month_sales'], ascending=[True, False]).to_excel(out_path, index=False)
    
    print(f"✅ 성공/부진 상품군 리스트 저장 완료: {out_path}")

if __name__ == "__main__":
    export_success_failure()
