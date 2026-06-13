import polars as pl
import os

# 🚀 [핵심] 프로젝트 루트 경로를 자동으로 찾아주는 함수입니다.
def get_root():
    # 현재 파일의 절대 경로를 가져옵니다.
    path = os.path.dirname(os.path.abspath(__file__))
    # 'data' 폴더가 나올 때까지 상위 폴더로 올라갑니다.
    while not os.path.exists(os.path.join(path, 'data')) and path != os.path.dirname(path):
        path = os.path.dirname(path)
    return path

# 1. 자동 경로 설정
ROOT = get_root()
B2_PATH = os.path.join(ROOT, 'data/processed/B2_FOOD_POS_SALE.parquet')
B4_PATH = os.path.join(ROOT, 'data/processed/B4_CLEAN_FOOD_ITEM.parquet')

def inspect_data():
    print(f"✅ 감지된 프로젝트 루트: {ROOT}")
    print("🚀 [데이터 진단] 식품 전용 데이터 세트 무결성 점검 시작\n")

    for name, path in [("B2 (식품 판매)", B2_PATH), ("B4 (식품 마스터)", B4_PATH)]:
        if not os.path.exists(path):
            print(f"❌ 파일을 찾을 수 없습니다: {path}")
            print(f"   (현재 경로 설정이 잘못되었거나 파일이 이동되었을 수 있습니다.)")
            continue
            
        print(f"--- [{name} 데이터 점검] ---")
        lazy_df = pl.scan_parquet(path)
        
        # 1. 스키마 확인
        print(f"📍 1. 컬럼 구성 및 타입:")
        schema = lazy_df.collect_schema()
        for col, dtype in schema.items():
            print(f"   - {col}: {dtype}")
        
        # 2. 전체 규모 확인
        total_rows = lazy_df.select(pl.len()).collect().item()
        print(f"\n📍 2. 전체 행 수: {total_rows:,} 건")
        
        # 3. 결측치(Null) 점검
        print(f"\n📍 3. 컬럼별 결측치 수:")
        null_counts = lazy_df.select(pl.all().null_count()).collect()
        print(null_counts)
        
        # 4. 데이터 샘플 (상위 3개)
        print(f"\n📍 4. 데이터 샘플:")
        print(lazy_df.head(3).collect())
        print("\n" + "="*50 + "\n")

if __name__ == "__main__":
    inspect_data()
