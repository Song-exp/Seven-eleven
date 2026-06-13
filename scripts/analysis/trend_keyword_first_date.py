"""
trend_keywords.parquet의 트렌드_키워드별 knewnew_merged.csv 첫 등장일 매핑
"""
import pandas as pd

RAW_CSV = "data/raw/knewnew_merged.csv"
TREND_PARQUET = "data/processed/trend_keywords.parquet"
OUT_PARQUET = "data/processed/trend_keywords_with_first_date.parquet"
OUT_CSV = "data/processed/trend_keywords_with_first_date.csv"

# 1. 원본 데이터 로드
raw = pd.read_csv(RAW_CSV)
raw = raw.dropna(subset=["keywords", "date"])
raw["date"] = pd.to_datetime(raw["date"])

# 2. keywords 컬럼 explode → (date, keyword) 쌍 생성
raw["kw_list"] = raw["keywords"].str.split(",").apply(lambda lst: [k.strip() for k in lst])
exploded = raw[["date", "kw_list"]].explode("kw_list").rename(columns={"kw_list": "keyword"})
exploded = exploded[exploded["keyword"] != ""]

# 3. 키워드별 첫 등장일 집계
first_date_map = exploded.groupby("keyword")["date"].min()

# 4. trend_keywords에 첫 등장일 컬럼 추가
trend = pd.read_parquet(TREND_PARQUET)
trend["첫_등장일"] = trend["트렌드_키워드"].map(first_date_map)

# 5. 결과 확인
matched = trend["첫_등장일"].notna().sum()
total = len(trend)
print(f"매핑 완료: {matched}/{total}개 트렌드 키워드에 첫 등장일 할당 ({matched/total*100:.1f}%)")
print(f"미매핑(None): {total - matched}개")
print()
print("=== 샘플 (상위 10개) ===")
print(trend[["트렌드_키워드", "첫_등장일"]].head(10).to_string(index=False))
print()
print("=== 미매핑 키워드 샘플 ===")
print(trend[trend["첫_등장일"].isna()]["트렌드_키워드"].head(20).tolist())

# 6. 저장
trend.to_parquet(OUT_PARQUET, index=False)
print(f"\n저장 완료: {OUT_PARQUET}")

# CSV 저장 시 numpy 배열의 자동 개행(\n) 제거
trend_csv = trend.copy()
trend_csv["추출_속성"] = trend_csv["추출_속성"].apply(
    lambda x: str(list(x)).replace("\n", " ") if hasattr(x, "__iter__") and not isinstance(x, str) else str(x).replace("\n", " ")
)
trend_csv.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
print(f"저장 완료: {OUT_CSV}")
