import polars as pl
import os

def aggregate_final_knowledge_base():
    """
    B4, B5, 전처리 POS 데이터를 결합하여 제품별 통합 텍스트 설명을 생성합니다.
    (시간대 정보는 제외하고 제품 정체성과 행사 정보에 집중합니다.)
    """
    print("데이터 로딩 및 통합 시작 (시간대 제외 버전)...")
    
    # 1. 파일 경로 설정
    current_dir = os.getcwd()
    base_path = "7eleven_npd_framework" if not current_dir.endswith("7eleven_npd_framework") else "."

    pos_path = os.path.join(base_path, "data/processed/pos_data_전처리완료_final.parquet")
    b4_path = os.path.join(base_path, "data/processed/B4_ITEM_DV_INFO.parquet")
    b5_path = os.path.join(base_path, "data/processed/B5_MNM_DATA.parquet")
    output_path = os.path.join(base_path, "data/processed/PRODUCT_FULL_CONTEXT.parquet")

    if not os.path.exists(pos_path):
        print(f"오류: {pos_path} 파일이 없습니다.")
        return

    # 2. POS 데이터에서 유효 상품 리스트만 추출 (시간대 분석 제외)
    print("유효 판매 상품 리스트 확보 중...")
    df_pos_unique = pl.read_parquet(pos_path).select("상품코드").unique()

    # 3. B4 상품 마스터 정보 결합
    print("B4 상품 마스터 정보 결합 중...")
    df_b4 = pl.read_parquet(b4_path)
    df_b4 = df_b4.rename({
        df_b4.columns[0]: "상품코드",
        df_b4.columns[1]: "상품명",
        df_b4.columns[2]: "대분류명",
        df_b4.columns[3]: "중분류명",
        df_b4.columns[4]: "소분류명"
    })
    
    # 판매 실적이 있는 상품만 필터링하여 B4 결합
    df_merged = df_pos_unique.join(df_b4, on="상품코드", how="inner")

    # 4. B5 행사 정보 결합
    print("B5 행사 정보 결합 중...")
    if os.path.exists(b5_path):
        df_b5 = pl.read_parquet(b5_path)
        df_b5_agg = df_b5.group_by("상품명").agg([
            pl.col("행사명").unique().alias("promo_list")
        ]).with_columns([
            pl.col("promo_list").list.join(", ").alias("promotion_text")
        ])
        df_merged = df_merged.join(df_b5_agg, on="상품명", how="left")
    else:
        df_merged = df_merged.with_columns(pl.lit("").alias("promotion_text"))

    # 5. 최종 통합 텍스트 생성 (시간대 제외)
    print("최종 통합 텍스트 생성 중...")
    df_merged = df_merged.fill_null("")
    
    df_final = df_merged.with_columns([
        (
            pl.col("상품명") + " (카테고리: " + 
            pl.col("대분류명") + "-" + pl.col("중분류명") + "-" + pl.col("소분류명") + "). " +
            pl.when(pl.col("promotion_text") == "").then(pl.lit("진행 중인 행사가 없습니다."))
            .otherwise("진행 행사: " + pl.col("promotion_text"))
        ).alias("full_text_description")
    ])

    # 6. 결과 저장
    df_final.select([
        "상품코드", "상품명", "full_text_description"
    ]).write_parquet(output_path)
    
    print(f"작업 완료! {len(df_final)}개의 제품에 대한 통합 데이터가 갱신되었습니다.")
    print(f"저장 위치: {output_path}")

    # 샘플 확인
    print("\n[갱신된 데이터 샘플]")
    if len(df_final) > 0:
        print(df_final.select(["상품명", "full_text_description"]).head(3))

if __name__ == "__main__":
    aggregate_final_knowledge_base()
