import polars as pl
import glob
from pathlib import Path
from datetime import datetime

# 1. 설정: 처리된 Parquet 파일이 있는 경로
# 주피터 노트북 파일이 eda/ 폴더 내에 있다면 아래 상대 경로('../')를 사용합니다.
# 만약 프로젝트 루트에 노트북이 있다면 'data/processed/app_event/'로 수정하세요.
PROCESSED_DIR = Path("../data/processed/app_event/")
parquet_files = glob.glob(str(PROCESSED_DIR / "*.parquet"))
