"""
Steps 1-3: 비NPD 5개 중분류 → NPD 확장 스크립트

대상 중분류: 막걸리, 전통음료, 기타전통주, 와인, 노벨티
조건: POS 기준 첫판매일 >= 2025-01-15 AND sales_30d_amt > 0

실행 후 변경 파일:
  - data/processed/pos_b4_product_pool.parquet  (is_npd 업데이트)
  - data/processed/pos_b4_product_pool.csv
  - data/processed/pos_product_features.parquet  (신규 행 append)
  - data/processed/B4_ITEM_DV_INFO_filtered.parquet  (신규 NPD 행 추가)

이후: 01b 노트북 전체 재실행
"""

import os
import polars as pl
import pandas as pd

ROOT = r"C:\Users\송정현\Documents\Projects\박재홍교수님세미나\Projects\20기\7eleven_npd_framework"
POS_PATH = os.path.join(ROOT, "data", "processed", "POS 전처리 최종",
                        "pos_data_food_final_상품단위변환전.parquet")
B4_PATH  = os.path.join(ROOT, "data", "processed", "B4_ITEM_DV_INFO.parquet")
B5_PATH  = os.path.join(ROOT, "data", "processed", "B5_MNM_DATA.parquet")
POOL_PQ  = os.path.join(ROOT, "data", "processed", "pos_b4_product_pool.parquet")
POOL_CSV = os.path.join(ROOT, "data", "processed", "pos_b4_product_pool.csv")
B4_FILT  = os.path.join(ROOT, "data", "processed", "B4_ITEM_DV_INFO_filtered.parquet")
POS_FEAT = os.path.join(ROOT, "data", "processed", "pos_product_features.parquet")

TARGET_CATS = ["막걸리", "전통음료", "기타전통주", "와인", "노벨티"]
NPD_THRESH  = 20250115  # burn-in 14일 기준 NPD 날짜 컷

# ════════════════════════════════════════════════════════════
# Phase A: 후보 ITEM_CD 식별 (첫판매일 >= 2025-01-15)
# ════════════════════════════════════════════════════════════
print("[Phase A] 후보 ITEM_CD 식별 중...")

pool = pd.read_parquet(POOL_PQ)
pool["ITEM_CD"] = pool["ITEM_CD"].astype(str)

# pos_product_features 미포함 제품을 기준으로 후보 선정 (재실행 안전)
existing_feat_ids = set(pd.read_parquet(POS_FEAT)["ITEM_CD"].astype(str).tolist())
candidate_ids = pool.loc[
    pool["중분류"].isin(TARGET_CATS) &
    ~pool["ITEM_CD"].isin(existing_feat_ids),
    "ITEM_CD"
].tolist()
print(f"  pos_product_features 미등록 5개 중분류: {len(candidate_ids)}개")

pos_lazy = pl.scan_parquet(POS_PATH)

first_sale_df = (
    pos_lazy
    .filter(pl.col("상품코드").is_in(candidate_ids))
    .group_by("상품코드")
    .agg(pl.col("영업일자").min().alias("첫판매일_int"))
    .collect()
)

npd_cand = first_sale_df.filter(pl.col("첫판매일_int") >= NPD_THRESH)
npd_ids_phase_a = npd_cand["상품코드"].to_list()
print(f"  첫판매일 >= {NPD_THRESH}: {len(npd_ids_phase_a)}개")

# 중분류 분포 출력
pool_pl = pl.from_pandas(pool[["ITEM_CD", "중분류"]])
dist_a = (
    npd_cand
    .with_columns(pl.col("상품코드").alias("ITEM_CD"))
    .join(pool_pl, on="ITEM_CD", how="left")
    .group_by("중분류")
    .agg(pl.len().alias("count"))
    .sort("count", descending=True)
)
print("  [Phase A 중분류 분포]")
for row in dist_a.iter_rows(named=True):
    print(f"    {row['중분류']}: {row['count']}")

# ════════════════════════════════════════════════════════════
# Phase B: 30일 매출 집계
# ════════════════════════════════════════════════════════════
print("\n[Phase B] 30일 매출 집계 중 (POS 2.2GB lazy scan, 약 2-3분 소요)...")

npd_launch = (
    npd_cand
    .with_columns(
        pl.col("첫판매일_int")
        .cast(pl.Utf8)
        .str.strptime(pl.Date, "%Y%m%d")
        .alias("첫판매일_dt")
    )
    .with_columns(
        (pl.col("첫판매일_dt") + pl.duration(days=30)).alias("end_dt")
    )
    .with_columns(
        pl.col("end_dt").dt.strftime("%Y%m%d").cast(pl.Int64).alias("end_dt_int")
    )
)

sales_30d = (
    pos_lazy
    .filter(pl.col("상품코드").is_in(npd_ids_phase_a))
    .join(
        npd_launch.lazy().select(["상품코드", "첫판매일_int", "end_dt_int"]),
        on="상품코드", how="inner"
    )
    .filter(
        (pl.col("영업일자") >= pl.col("첫판매일_int")) &
        (pl.col("영업일자") <= pl.col("end_dt_int"))
    )
    .group_by("상품코드")
    .agg(
        pl.col("매출수량").sum().cast(pl.Float32).alias("sales_30d_qty"),
        pl.col("매출금액").sum().cast(pl.Float32).alias("sales_30d_amt"),
        pl.col("영업일자").n_unique().cast(pl.UInt32).alias("sales_days_observed"),
    )
    .collect()
)

# sales_30d_amt > 0 필터 (음수 순매출 제거)
sales_pos = sales_30d.filter(pl.col("sales_30d_amt") > 0)
final_ids = sales_pos["상품코드"].to_list()
removed_cnt = len(npd_ids_phase_a) - len(final_ids)
print(f"  sales_30d_amt <= 0 제거: {removed_cnt}개")
print(f"  최종 신규 NPD: {len(final_ids)}개")

sales_pos = sales_pos.with_columns(
    (pl.col("sales_30d_qty") / 30.0).cast(pl.Float32).alias("daily_velocity")
)

# ════════════════════════════════════════════════════════════
# Phase C: 프로모션 피처 (B5)
# ════════════════════════════════════════════════════════════
print("\n[Phase C] B5 프로모션 피처 계산 중...")

b5_raw = pl.read_parquet(B5_PATH)

# 날짜 컬럼 타입 확인 후 Date로 변환
start_col_type = b5_raw.schema.get("행사개시일")
if start_col_type in (pl.Utf8, pl.String):
    b5_raw = b5_raw.with_columns(
        pl.col("행사개시일").str.strptime(pl.Date, "%Y-%m-%dT%H:%M:%S%.f", strict=False),
        pl.col("행사종료일").str.strptime(pl.Date, "%Y-%m-%dT%H:%M:%S%.f", strict=False),
    )
else:
    b5_raw = b5_raw.with_columns(
        pl.col("행사개시일").cast(pl.Date),
        pl.col("행사종료일").cast(pl.Date),
    )

b5_sub = (
    b5_raw
    .filter(pl.col("상품코드").is_in(final_ids))
    .select([
        "상품코드", "행사명", "행사유형",
        pl.col("대분류").alias("promo_category"),
        pl.col("행사개시일").alias("promo_start"),
        pl.col("행사종료일").alias("promo_end"),
    ])
)

launch_for_promo = (
    npd_launch
    .filter(pl.col("상품코드").is_in(final_ids))
    .select(["상품코드", "첫판매일_dt", "end_dt"])
)

b5_joined = (
    b5_sub
    .join(launch_for_promo, on="상품코드", how="inner")
    .filter(
        (pl.col("promo_start") <= pl.col("end_dt")) &
        (pl.col("promo_end")   >= pl.col("첫판매일_dt"))
    )
)

# 프로모션 있는 제품 집계
promo_agg = (
    b5_joined
    .group_by("상품코드")
    .agg(
        pl.col("행사명").n_unique().cast(pl.Int64).alias("promo_count_30d"),
        pl.col("행사명").drop_nulls().unique().sort().str.join(", ").alias("promo_names_30d"),
        pl.col("행사유형").drop_nulls().unique().sort().str.join(", ").alias("promo_types_30d"),
        pl.col("promo_category").drop_nulls().unique().sort().str.join(", ").alias("promo_categories_30d"),
    )
    .with_columns(pl.lit(True).alias("has_promo_30d"))
)

# 프로모션 없는 제품 (anti-join)
no_promo_ids = (
    pl.Series("상품코드", final_ids)
    .to_frame()
    .join(promo_agg.select("상품코드"), on="상품코드", how="anti")
    ["상품코드"].to_list()
)
no_promo = pl.DataFrame({
    "상품코드":           no_promo_ids,
    "promo_count_30d":    [0] * len(no_promo_ids),
    "promo_names_30d":    [""] * len(no_promo_ids),
    "promo_types_30d":    [""] * len(no_promo_ids),
    "promo_categories_30d": [""] * len(no_promo_ids),
    "has_promo_30d":      [False] * len(no_promo_ids),
}).with_columns(pl.col("promo_count_30d").cast(pl.Int64))

promo_full = pl.concat([promo_agg, no_promo])
print(f"  프로모션 있는 제품: {len(promo_agg)}, 없는 제품: {len(no_promo)}")

# ════════════════════════════════════════════════════════════
# Phase D: B4 메타 병합 + 최종 DataFrame 조립
# ════════════════════════════════════════════════════════════
print("\n[Phase D] B4 메타 병합 및 DataFrame 조립 중...")

b4 = (
    pl.read_parquet(B4_PATH)
    .select(["ITEM_CD", "ITEM_NM", "ITEM_LRDV_NM", "ITEM_MDDV_NM", "ITEM_SMDV_NM"])
    .filter(pl.col("ITEM_CD").is_in(final_ids))
    .unique("ITEM_CD")
)

launch_final = (
    npd_launch
    .filter(pl.col("상품코드").is_in(final_ids))
    .select(["상품코드", "첫판매일_dt"])
    .rename({"상품코드": "ITEM_CD"})
)

new_features_pl = (
    b4
    .join(launch_final,                                          on="ITEM_CD", how="left")
    .join(sales_pos.rename({"상품코드": "ITEM_CD"}),            on="ITEM_CD", how="left")
    .join(promo_full.rename({"상품코드": "ITEM_CD"}),           on="ITEM_CD", how="left")
    .with_columns(pl.lit("생존").alias("생존여부"))
    .select([
        "ITEM_CD", "ITEM_NM", "ITEM_LRDV_NM", "ITEM_MDDV_NM", "ITEM_SMDV_NM",
        "생존여부",
        pl.col("첫판매일_dt").alias("첫판매일"),
        "sales_30d_qty", "sales_30d_amt", "sales_days_observed", "daily_velocity",
        pl.col("promo_count_30d").cast(pl.Int64),
        "promo_names_30d", "promo_types_30d", "promo_categories_30d",
        "has_promo_30d",
    ])
)

new_features = new_features_pl.to_pandas()
new_features["첫판매일"] = pd.to_datetime(new_features["첫판매일"])
for col in ["sales_30d_qty", "sales_30d_amt", "daily_velocity"]:
    new_features[col] = new_features[col].astype("float32")
new_features["sales_days_observed"] = new_features["sales_days_observed"].astype("uint32")
new_features["promo_count_30d"]     = new_features["promo_count_30d"].astype("int64")
new_features["has_promo_30d"]       = new_features["has_promo_30d"].astype(bool)
for col in ["promo_names_30d", "promo_types_30d", "promo_categories_30d"]:
    new_features[col] = new_features[col].fillna("").astype(str)

# 중분류 분포 출력 (B4에 없는 제품 확인용)
merged_check = new_features.merge(pool[["ITEM_CD", "중분류"]], on="ITEM_CD", how="left")
print("  [최종 신규 NPD 중분류 분포]")
print(merged_check["중분류"].value_counts().to_string())

# ════════════════════════════════════════════════════════════
# Step 1: pos_b4_product_pool 업데이트
# ════════════════════════════════════════════════════════════
print("\n[Step 1] pos_b4_product_pool 업데이트...")

pool.loc[pool["ITEM_CD"].isin(final_ids), "is_npd"] = True
pool.to_parquet(POOL_PQ, index=False)
pool.to_csv(POOL_CSV, index=False, encoding="utf-8-sig")
print(f"  저장 완료 - is_npd=True 총 {int(pool['is_npd'].sum())}개")

# ════════════════════════════════════════════════════════════
# Step 2: pos_product_features append
# ════════════════════════════════════════════════════════════
print("\n[Step 2] pos_product_features.parquet append...")

existing_feat = pd.read_parquet(POS_FEAT)
print(f"  기존: {len(existing_feat)}행")

new_only = new_features[~new_features["ITEM_CD"].isin(existing_feat["ITEM_CD"])]
print(f"  신규 (중복 제외): {len(new_only)}행")

combined = pd.concat([existing_feat, new_only], ignore_index=True)
combined["sales_30d_qty"]       = combined["sales_30d_qty"].astype("float32")
combined["sales_30d_amt"]       = combined["sales_30d_amt"].astype("float32")
combined["sales_days_observed"] = combined["sales_days_observed"].astype("uint32")
combined["daily_velocity"]      = combined["daily_velocity"].astype("float32")
combined["promo_count_30d"]     = combined["promo_count_30d"].astype("int64")
combined["has_promo_30d"]       = combined["has_promo_30d"].astype(bool)
combined["첫판매일"]            = pd.to_datetime(combined["첫판매일"])

combined.to_parquet(POS_FEAT, index=False)
print(f"  저장 완료 - 총 {len(combined)}행")

# ════════════════════════════════════════════════════════════
# Step 3: B4_ITEM_DV_INFO_filtered append
# ════════════════════════════════════════════════════════════
print("\n[Step 3] B4_ITEM_DV_INFO_filtered.parquet 업데이트...")

b4_filt = pd.read_parquet(B4_FILT)
print(f"  기존: {len(b4_filt)}행, is_npd=True: {int(b4_filt['is_npd'].sum())}")

new_b4_rows = new_only[
    ["ITEM_CD", "ITEM_NM", "ITEM_LRDV_NM", "ITEM_MDDV_NM", "ITEM_SMDV_NM", "생존여부"]
].copy()
new_b4_rows["is_npd"] = True

b4_combined = pd.concat([b4_filt, new_b4_rows], ignore_index=True)
b4_combined.to_parquet(B4_FILT, index=False)
print(f"  저장 완료 - 총 {len(b4_combined)}행, is_npd=True: {int(b4_combined['is_npd'].sum())}")

# ════════════════════════════════════════════════════════════
print("\n✅ Steps 1-3 완료.")
print(f"   신규 NPD 추가: {len(new_only)}개")
print("   변경 파일:")
print("   - pos_b4_product_pool.parquet / .csv")
print("   - pos_product_features.parquet")
print("   - B4_ITEM_DV_INFO_filtered.parquet")
print("\n   다음 단계: 01b 노트북 전체 재실행 (npd_success_labels.csv 재생성)")
