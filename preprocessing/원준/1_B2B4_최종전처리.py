"""
=============================================================================
파일명  : 2_B2B4_최종전처리.py
목적    : df_B2_B4_merged.parquet → 컬럼명 변경, 파생변수 생성, 결측치 파악,
          이상치 플래그 추가 후 /전처리_EDA/최종/ 에 저장
작성일  : 2026-04-11
주의    : 파일이 매우 크므로 PyArrow iter_batches 를 활용한 청크 단위 처리를
          기본으로 설계합니다. 이상치(음수/0값 데이터)는 임의 삭제/상계 없이
          플래그 컬럼(flag_이상치_유형)을 추가하고, 별도 검토용 Parquet 파일로
          분리 저장합니다.
=============================================================================
"""

# ── 0. 라이브러리 임포트 ────────────────────────────────────────────────────
import os
import gc
import warnings

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

warnings.filterwarnings("ignore")

# ── 1. 경로 설정 ────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
DATA_PROCESSED_DIR = os.path.join(PROJECT_ROOT, "전처리_EDA", "최종")

INPUT_PATH = os.path.join(DATA_PROCESSED_DIR, "df_B2_B4_merged.parquet")
OUTPUT_DIR = os.path.join(DATA_PROCESSED_DIR)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 최종 전처리 완료 파일 경로
OUTPUT_MAIN    = os.path.join(OUTPUT_DIR, "df_전처리완료.parquet")
# 이상치 검토용 분리 파일 경로 (사용자가 추후 검토·의사결정)
OUTPUT_REVIEW  = os.path.join(OUTPUT_DIR, "df_이상치_검토용.parquet")
# 결측치 요약 리포트 경로
OUTPUT_MISSING = os.path.join(OUTPUT_DIR, "결측치_요약.csv")

# ── 2. 컬럼명 매핑 딕셔너리 ─────────────────────────────────────────────────
# 원본 영문 컬럼명 → 한글 컬럼명 (향후 추가 컬럼이 생기면 여기에 추가)
RENAME_MAP = {
    "SALE_DATE"      : "영업일자",
    "SALE_TIME"      : "판매시분초",
    "STORE_CODE"     : "점포코드",
    "POS_NO"         : "POS번호",
    "TRADE_NO"       : "거래번호",
    "ITEM_CODE"      : "상품코드",
    "SALE_QTY"       : "매출수량",
    "SALE_AMT"       : "매출금액",
    "ITEM_NM"        : "상품명",
    "ITEM_LRDV_NM"   : "상품대분류명",
    "ITEM_MDDV_NM"   : "상품중분류명",
    "ITEM_SMDV_NM"   : "상품소분류명",
    # "PROMO_YN"     : "프로모션여부",
}

# ── 3. 파생변수 생성 헬퍼 함수 ──────────────────────────────────────────────

def _make_datetime_col(df: pd.DataFrame) -> pd.Series:
    """
    '영업일자'(YYYYMMDD 형식 문자열/정수)와 '판매시분초'(HHMMSS 6자리 문자열/정수)를
    결합하여 pandas datetime Series 를 반환한다.

    처리 전략:
      - 두 컬럼 모두 정수형 또는 문자열로 저장될 수 있으므로 str 변환 후 결합.
      - '판매시분초'가 없거나 결측인 경우 '000000' 으로 대체하여 파싱 오류를 방지.
      - pyarrow.to_pandas() 이후 hour ≥ 24 같은 비정상 값이 포함될 수 있으므로,
        errors='coerce' 로 변환 실패 시 NaT 처리.
    """
    date_str = df["영업일자"].astype(str).str.strip().str[:8]  # 앞 8자리만
    if "판매시분초" in df.columns:
        time_str = (
            df["판매시분초"].astype(str).str.strip()
                          .str.zfill(6)             # 6자리 미만이면 앞에 0 채움
                          .str[:6]                  # 앞 6자리만
        )
    else:
        time_str = pd.Series(["000000"] * len(df), index=df.index)

    combined = date_str + time_str                  # 'YYYYMMDDHHMMSS' 형태
    return pd.to_datetime(combined, format="%Y%m%d%H%M%S", errors="coerce")


def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    '판매시간_dt' 파생 변수를 생성하고 시간 관련 파생 컬럼을 추가한다.

    파생 컬럼:
      - 판매시간_dt  : datetime64[ns]  (영업일자 + 판매시분초 결합)
      - 판매월       : int8   (1~12)
      - 판매주       : int8   (ISO week: 1~53)
      - 판매일       : int8   (1~31)
      - 판매요일     : str    ('월'~'일', 가독성을 위해 한글)
      - 판매시간대   : int8   (0~23시, 24시간제)
    """
    df["판매시간_dt"] = _make_datetime_col(df)

    # 판매시간_dt 에서 파생 (NaT 는 각 필드도 NaN/None 으로 처리됨)
    df["판매월"]    = df["판매시간_dt"].dt.month.astype("Int8")
    df["판매주"]    = df["판매시간_dt"].dt.isocalendar().week.astype("Int8")
    df["판매일"]    = df["판매시간_dt"].dt.day.astype("Int8")
    df["판매시간대"] = df["판매시간_dt"].dt.hour.astype("Int8")

    # 요일: 0=월요일 … 6=일요일 → 한글 매핑
    day_map = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일"}
    df["판매요일"] = df["판매시간_dt"].dt.dayofweek.map(day_map).astype("category")

    return df


def add_trade_key(df: pd.DataFrame) -> pd.DataFrame:
    """
    '점포코드' + '_' + 'POS번호' + '_' + '거래번호' 를 결합하여
    '거래_고유키' 컬럼을 생성한다.

    컬럼이 정수형으로 저장된 경우에도 str 변환 후 결합하며,
    POS번호·거래번호는 숫자 앞자리 0 유지를 위해 zero-pad 처리하지 않는다
    (원본 데이터의 자릿수 관례를 그대로 따름).
    """
    df["거래_고유키"] = (
        df["영업일자"].astype(str) + "_" +
        df["점포코드"].astype(str) + "_" +
        df["POS번호"].astype(str)  + "_" +
        df["거래번호"].astype(str)
    )
    return df


def add_anomaly_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    매출수량·매출금액 이상치에 플래그(flag_이상치_유형) 컬럼을 추가한다.

    ※ 아래 비즈니스 로직 분석 섹션을 반드시 먼저 읽고
       실제 처리 방향(삭제 or 유지)은 사용자가 결정한다.

    플래그 분류 체계:
      - 'NORMAL'              : 정상 거래 (매출수량 > 0 AND 매출금액 > 0)
      - 'GIFT_OR_ZERO_QTY'    : 매출수량=0이거나 매출금액=0
                                → 증정품, 샘플, 결합행사 등 가능성 높음
      - 'REFUND_NEGATIVE'     : 매출수량 < 0 AND 매출금액 < 0
                                → 반품/환불 처리 가능성 높음
      - 'PARTIAL_CANCEL'      : 매출수량 < 0 XOR 매출금액 < 0
                                (한쪽만 음수) → 부분 취소, 할인 조정 가능성
      - 'ZERO_AMT_NEG_QTY'    : 매출수량 < 0 AND 매출금액 = 0
                                → 재고 조정, 손실 처리 가능성
      - 'DATA_SUSPECT'        : 매출금액 < 0 AND 매출수량 > 0
                                (양의 수량에 음의 금액) → 시스템 오류 가능성 높음

    매출수량·매출금액 모두 존재하지 않는 컬럼은 플래그 생성을 건너뜀.
    """
    if "매출수량" not in df.columns or "매출금액" not in df.columns:
        df["flag_이상치_유형"] = "UNKNOWN_MISSING_COLS"
        return df

    qty = df["매출수량"]
    amt = df["매출금액"]

    conditions = [
        # 0순위: 결측값 → NaN 비교는 항상 False이므로 가장 먼저 체크
        qty.isna() | amt.isna(),
        # 1순위: 정상
        (qty > 0) & (amt > 0),
        # 2순위: 수량/금액 둘 다 음수 → 환불/반품
        (qty < 0) & (amt < 0),
        # 3순위: 수량 < 0, 금액 = 0 → 재고 조정/손실
        (qty < 0) & (amt == 0),
        # 4순위: 금액 < 0, 수량 > 0 → 시스템 오류 의심
        (qty > 0) & (amt < 0),
        # 5순위: 수량만 음수 (부분 취소/할인 조정)
        (qty < 0) & (amt > 0),
        # 6순위: 수량=0 또는 금액=0 → 증정품 가능성
        (qty == 0) | (amt == 0),
    ]
    choices = [
        "MISSING_VALUE",
        "NORMAL",
        "REFUND_NEGATIVE",
        "ZERO_AMT_NEG_QTY",
        "DATA_SUSPECT",
        "PARTIAL_CANCEL",
        "GIFT_OR_ZERO_QTY",
    ]

    df["flag_이상치_유형"] = np.select(conditions, choices, default="NORMAL")
    df["flag_이상치_유형"] = df["flag_이상치_유형"].astype("category")

    return df


# ── 4. 결측치 누적 집계 함수 ────────────────────────────────────────────────

def accumulate_missing(missing_acc: dict, chunk_df: pd.DataFrame) -> dict:
    """
    청크 단위로 컬럼별 결측치 수를 누적 집계한다.
    (전체를 메모리에 올리지 않고 결측치 통계를 산출하기 위함)
    """
    for col in chunk_df.columns:
        cnt = int(chunk_df[col].isna().sum())
        missing_acc[col] = missing_acc.get(col, 0) + cnt
    return missing_acc


# ── 5. PyArrow 스키마 추론 (첫 번째 청크 기반) ──────────────────────────────

def infer_output_schema(sample_df: pd.DataFrame) -> pa.Schema:
    """
    전처리된 첫 번째 청크를 기반으로 pyarrow 스키마를 추론한다.
    이 스키마는 이후 ParquetWriter 에 일관되게 적용된다.
    """
    return pa.Schema.from_pandas(sample_df, preserve_index=False)


# ── 6. 단일 청크 전처리 파이프라인 ─────────────────────────────────────────

def preprocess_chunk(
    df: pd.DataFrame,
    rename_map: dict,
    available_rename: dict,
) -> pd.DataFrame:
    """
    하나의 청크(pd.DataFrame)에 모든 전처리 단계를 순서대로 적용하고 반환한다.

    Parameters
    ----------
    df              : 원본 청크 데이터프레임
    rename_map      : 전체 컬럼 매핑 딕셔너리
    available_rename: 실제 파일에 존재하는 컬럼만 필터링된 매핑 딕셔너리

    Returns
    -------
    pd.DataFrame : 전처리 완료 청크
    """
    # Step 1. 컬럼명 변경
    df = df.rename(columns=available_rename)

    # Step 2. 시간 파생 변수 생성
    df = add_time_features(df)

    # Step 3. 거래 고유키 생성
    df = add_trade_key(df)

    # Step 4. 이상치 플래그 추가
    df = add_anomaly_flags(df)

    return df


# ── 7. 메인 처리 루프 ────────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print("🚀 df_B2_B4_merged.parquet 메모리 효율적 전처리 시작")
    print("=" * 65)

    # ── 7-1. Parquet 파일 메타데이터 확인 (데이터 로드 없이) ──────────────
    meta = pq.read_metadata(INPUT_PATH)
    schema = pq.read_schema(INPUT_PATH)
    file_size_gb = os.path.getsize(INPUT_PATH) / 1e9
    original_cols = schema.names

    print(f"\n📂 입력 파일 크기 : {file_size_gb:.2f} GB")
    print(f"📊 총 행 수       : {meta.num_rows:,}")
    print(f"📦 Row Group 수   : {meta.num_row_groups}")
    print(f"🏷️  원본 컬럼 ({len(original_cols)}개): {original_cols}")

    # ── 7-2. 실제로 존재하는 컬럼만 rename 대상으로 필터링 ─────────────────
    available_rename = {k: v for k, v in RENAME_MAP.items() if k in original_cols}
    missing_from_map = [c for c in original_cols if c not in RENAME_MAP]
    print(f"\n✅ 컬럼명 변경 대상: {len(available_rename)}개")
    print(f"   (매핑 정의 없는 컬럼 {len(missing_from_map)}개는 원본명 유지)")
    if missing_from_map:
        print(f"   → 원본명 유지 컬럼: {missing_from_map}")

    # ── 7-3. 청크 처리 파라미터 ────────────────────────────────────────────
    # Row Group 수가 많으면 배치 크기를 늘려도 되지만,
    # 메모리 안전을 위해 200만 행으로 고정
    BATCH_SIZE = 2_000_000

    pf = pq.ParquetFile(INPUT_PATH)

    total_rows      = 0
    missing_acc     = {}      # 결측치 누적 집계 딕셔너리
    flag_counts_acc = {}      # 이상치 플래그 분포 누적 집계

    # ParquetWriter: 스키마는 첫 청크에서 확정
    writer_main   = None
    writer_review = None
    output_schema = None

    batch_num = 0
    print(f"\n🔄 청크 단위 처리 시작 (batch_size={BATCH_SIZE:,})...\n")

    for batch in pf.iter_batches(batch_size=BATCH_SIZE):
        batch_num += 1
        df_raw = batch.to_pandas()

        chunk_size = len(df_raw)
        total_rows += chunk_size

        # ── (a) 전처리 적용 ──────────────────────────────────────────────
        df_proc = preprocess_chunk(df_raw, RENAME_MAP, available_rename)
        del df_raw
        gc.collect()

        # ── (b) 결측치 누적 집계 ─────────────────────────────────────────
        missing_acc = accumulate_missing(missing_acc, df_proc)

        # ── (c) 이상치 플래그 분포 누적 ──────────────────────────────────
        if "flag_이상치_유형" in df_proc.columns:
            chunk_flag = df_proc["flag_이상치_유형"].value_counts().to_dict()
            for k, v in chunk_flag.items():
                flag_counts_acc[k] = flag_counts_acc.get(k, 0) + int(v)

        # ── (d) 첫 번째 청크에서 스키마 확정 & Writer 초기화 ─────────────
        if writer_main is None:
            output_schema = infer_output_schema(df_proc)
            writer_main   = pq.ParquetWriter(OUTPUT_MAIN,   output_schema, compression="snappy")
            writer_review = pq.ParquetWriter(OUTPUT_REVIEW, output_schema, compression="snappy")
            print(f"   ✅ 출력 스키마 확정 (컬럼 {len(output_schema.names)}개)")
            print(f"   ✅ Parquet Writer 초기화 완료\n")

        # ── (e) 전체 데이터 → 메인 파일 저장 ────────────────────────────
        table_proc = pa.Table.from_pandas(df_proc, schema=output_schema, preserve_index=False)
        writer_main.write_table(table_proc)

        # ── (f) 이상치 행 → 검토용 파일 분리 저장 ────────────────────────
        # 'NORMAL' 이 아닌 모든 행을 검토용으로 분리 (플래그 기준)
        if "flag_이상치_유형" in df_proc.columns:
            df_review = df_proc[df_proc["flag_이상치_유형"] != "NORMAL"].copy()
            if len(df_review) > 0:
                table_review = pa.Table.from_pandas(
                    df_review, schema=output_schema, preserve_index=False
                )
                writer_review.write_table(table_review)
            del df_review

        del df_proc, table_proc
        gc.collect()

        if batch_num % 5 == 0 or chunk_size < BATCH_SIZE:
            print(f"   [Batch {batch_num:>3}]  누적 처리 행: {total_rows:>12,}")

    # ── 7-4. Writer 닫기 ────────────────────────────────────────────────────
    if writer_main:
        writer_main.close()
    if writer_review:
        writer_review.close()

    print(f"\n✅ 전처리 완료: 총 {total_rows:,}행 처리")

    # ── 8. 결측치 요약 리포트 저장 ──────────────────────────────────────────
    print("\n📋 결측치 요약 리포트 생성 중...")
    if missing_acc:
        missing_df = pd.DataFrame.from_dict(
            missing_acc, orient="index", columns=["결측치_수"]
        )
        missing_df.index.name = "컬럼명"
        missing_df["결측치_비율(%)"] = (missing_df["결측치_수"] / total_rows * 100).round(4)
        missing_df = missing_df.sort_values("결측치_수", ascending=False)
        missing_df.to_csv(OUTPUT_MISSING, encoding="utf-8-sig")

        print("\n[결측치 현황 (상위 15개)]")
        print(missing_df.head(15).to_string())
    else:
        print("   결측치 집계 결과 없음.")

    # ── 9. 이상치 플래그 분포 출력 ──────────────────────────────────────────
    print("\n📊 이상치 플래그 분포 (전체 데이터 기준):")
    print("-" * 55)
    for flag, cnt in sorted(flag_counts_acc.items(), key=lambda x: -x[1]):
        pct = cnt / total_rows * 100
        print(f"  {flag:<25} : {cnt:>12,}행  ({pct:>6.3f}%)")
    print("-" * 55)
    non_normal = sum(v for k, v in flag_counts_acc.items() if k != "NORMAL")
    print(f"  {'NORMAL 외 합계':<25} : {non_normal:>12,}행  ({non_normal/total_rows*100:>6.3f}%)")

    # ── 10. 최종 파일 정보 출력 ─────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("💾 저장 파일 목록")
    print("=" * 65)

    out_files = {
        "전처리 완료 (전체)": OUTPUT_MAIN,
        "이상치 검토용 분리": OUTPUT_REVIEW,
        "결측치 요약 CSV"   : OUTPUT_MISSING,
    }
    for label, path in out_files.items():
        if os.path.exists(path):
            size_mb = os.path.getsize(path) / 1e6
            print(f"  [{label}]  →  {path}  ({size_mb:.1f} MB)")
        else:
            print(f"  [{label}]  →  저장 실패 또는 내용 없음")

    print("\n🎉 모든 전처리 단계 완료!")
    print("=" * 65)
    print()
    print("📌 다음 단계 안내:")
    print("   1. '결측치_요약.csv' 를 열어 각 컬럼의 결측치 비율을 확인하세요.")
    print("   2. 'df_이상치_검토용.parquet' 를 열어 flag_이상치_유형 별로")
    print("      각 이상치의 비즈니스 의미를 검토하고 처리 방향을 결정하세요.")
    print("      (삭제 / 유지 / 변환 등)")
    print("   3. 최종 의사결정 후 df_전처리완료.parquet 에서 필터링 적용을")
    print("      별도 스크립트로 작성하여 후속 EDA 에 활용하세요.")


# ── 실행 진입점 ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
