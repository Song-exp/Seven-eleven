"""퀵커머스 시간대별 동반구매 Lift 계산 스크립트.

B3(퀵커머스)의 주문시간으로 시간대 분류 →
B2(오프라인 POS)에서 같은 거래키로 상품코드 조인 →
시간대별 Lift 계산

입력:
  /Users/hajiyoon/workspace/data/processed/B3_OLN_ODR_DLVR.parquet
  /Users/hajiyoon/workspace/data/processed/pos_data_food_final_정제완료.parquet
  data/processed/hin/product_nodes.parquet  (세븐일레븐 상품 필터용)

출력:
  data/processed/quick_timeslot_lift_morning.csv  (06~10시)
  data/processed/quick_timeslot_lift_lunch.csv    (11~13시)
  data/processed/quick_timeslot_lift_snack.csv    (14~16시)
  data/processed/quick_timeslot_lift_dinner.csv   (17~20시)
  data/processed/quick_timeslot_lift_night.csv    (21~02시)

실행:
  cd /Users/hajiyoon/workspace/711project/7eleven_npd_share
  source .venv/bin/activate
  python scripts/compute_quick_timeslot_lift.py
"""
import os
from itertools import combinations

import pandas as pd

B3_PATH = "/Users/hajiyoon/workspace/data/processed/B3_OLN_ODR_DLVR.parquet"
B2_PATH = "/Users/hajiyoon/workspace/data/processed/pos_data_food_final_정제완료.parquet"
PRODUCT_NODES_PATH = "data/processed/hin/product_nodes.parquet"
OUT_DIR = "data/processed"

MIN_SUPPORT = 3     # 퀵커머스는 거래량이 적으므로 오프라인(5)보다 낮게 설정
MIN_LIFT = 3.0

TIMESLOTS = {
    "morning": [6,  7,  8,  9, 10],
    "lunch":   [11, 12, 13],
    "snack":   [14, 15, 16],
    "dinner":  [17, 18, 19, 20],
    "night":   [21, 22, 23, 0,  1,  2],
}


def norm_id(x):
    try:
        return str(int(float(x)))
    except (ValueError, TypeError):
        return str(x)


def compute_lift(baskets, item_freq, n_transactions, min_support, min_lift):
    pair_count = {}
    for items in baskets:
        unique_items = list(set(items))
        if len(unique_items) < 2:
            continue
        for a, b in combinations(sorted(unique_items), 2):
            pair_count[(a, b)] = pair_count.get((a, b), 0) + 1

    rows = []
    for (a, b), cnt in pair_count.items():
        if cnt < min_support:
            continue
        pa = item_freq.get(a, 0) / n_transactions
        pb = item_freq.get(b, 0) / n_transactions
        if pa == 0 or pb == 0:
            continue
        lift = (cnt / n_transactions) / (pa * pb)
        if lift >= min_lift:
            rows.append({
                "상품코드_A": a, "상품코드_B": b,
                "동반구매빈도(Support)": cnt, "향상도(Lift)": round(lift, 4),
            })

    return pd.DataFrame(rows).sort_values("향상도(Lift)", ascending=False)


def main():
    print("B3 퀵커머스 데이터 로드 중...")
    b3 = pd.read_parquet(B3_PATH)
    b3["거래_고유키"] = (
        b3["영업일자"].astype(str) + "_" +
        b3["점포코드"].astype(str) + "_" +
        b3["POS번호"].astype(str) + "_" +
        b3["거래번호"].astype(str)
    )
    # 주문시간 HHMMSS → 시간(0~23)
    b3["주문시간대"] = (b3["주문시간"].astype(str).str.zfill(6).str[:2].astype(int))
    print(f"  퀵커머스 거래: {b3['거래_고유키'].nunique():,}건")

    print("B2 POS 데이터 로드 중...")
    b2 = pd.read_parquet(B2_PATH, columns=["거래_고유키", "상품코드", "상품명"])
    b2["상품코드"] = b2["상품코드"].apply(norm_id)

    # 세븐일레븐 product_nodes 상품만 필터
    pn = pd.read_parquet(PRODUCT_NODES_PATH)
    seven_ids = set(
        pn[pn["편의점명"] == "세븐일레븐"]["ITEM_CD"].apply(norm_id)
    )
    b2 = b2[b2["상품코드"].isin(seven_ids)].copy()
    print(f"  세븐일레븐 필터 후: {len(b2):,}행 / 상품 {b2['상품코드'].nunique():,}개")

    # 상품코드 → 상품명 맵
    name_map = b2.drop_duplicates("상품코드").set_index("상품코드")["상품명"].to_dict()

    # B3 거래키로 필터 — 퀵커머스 거래에 해당하는 B2 행만 추출
    quick_keys = set(b3["거래_고유키"])
    b2_quick = b2[b2["거래_고유키"].isin(quick_keys)].copy()
    print(f"  퀵커머스 거래 매칭: {len(b2_quick):,}행 / 거래 {b2_quick['거래_고유키'].nunique():,}건")

    # 거래키 → 시간대 맵
    key_to_hour = b3.drop_duplicates("거래_고유키").set_index("거래_고유키")["주문시간대"].to_dict()
    b2_quick["주문시간대"] = b2_quick["거래_고유키"].map(key_to_hour)

    os.makedirs(OUT_DIR, exist_ok=True)

    for slot_name, hours in TIMESLOTS.items():
        print(f"\n[{slot_name}] 시간대 {hours} 처리 중...")
        slot_df = b2_quick[b2_quick["주문시간대"].isin(hours)]
        n_tx = slot_df["거래_고유키"].nunique()
        print(f"  거래 수: {n_tx:,} / 행 수: {len(slot_df):,}")

        if n_tx < 10:
            print(f"  거래 수 부족 — 건너뜀")
            continue

        baskets = slot_df.groupby("거래_고유키")["상품코드"].apply(list)
        item_freq = slot_df["상품코드"].value_counts().to_dict()

        lift_df = compute_lift(baskets, item_freq, n_tx, MIN_SUPPORT, MIN_LIFT)

        if lift_df.empty:
            print(f"  결과 없음 (support≥{MIN_SUPPORT}, lift≥{MIN_LIFT} 조건 미충족)")
            continue

        lift_df["상품명_A"] = lift_df["상품코드_A"].map(name_map)
        lift_df["상품명_B"] = lift_df["상품코드_B"].map(name_map)
        lift_df = lift_df[["상품명_A", "상품명_B", "상품코드_A", "상품코드_B",
                            "동반구매빈도(Support)", "향상도(Lift)"]]

        out_path = os.path.join(OUT_DIR, f"quick_timeslot_lift_{slot_name}.csv")
        lift_df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"  저장 완료: {out_path} ({len(lift_df):,}쌍)")


if __name__ == "__main__":
    main()
