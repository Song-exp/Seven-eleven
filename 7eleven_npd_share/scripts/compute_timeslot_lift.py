"""시간대별 동반구매 Lift 계산 스크립트.

입력:
  /Users/hajiyoon/workspace/data/processed/pos_data_food_final_정제완료.parquet
  data/processed/hin/product_nodes.parquet  (세븐일레븐 상품 필터용)

출력:
  data/processed/timeslot_lift_morning.csv  (06~10시)
  data/processed/timeslot_lift_lunch.csv    (11~13시)
  data/processed/timeslot_lift_snack.csv    (14~16시)
  data/processed/timeslot_lift_dinner.csv   (17~20시)
  data/processed/timeslot_lift_night.csv    (21~02시)

실행:
  cd /Users/hajiyoon/workspace/711project/7eleven_npd_share
  source .venv/bin/activate
  python scripts/compute_timeslot_lift.py
"""
import os
from itertools import combinations

import pandas as pd

POS_PATH = "/Users/hajiyoon/workspace/data/processed/pos_data_food_final_정제완료.parquet"
PRODUCT_NODES_PATH = "data/processed/hin/product_nodes.parquet"
OUT_DIR = "data/processed"

MIN_SUPPORT = 5    # 시간대별 동반구매 최소 빈도 (전체 기준 100보다 낮게 설정)
MIN_LIFT = 3.0     # 최소 Lift 임계값

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


def compute_lift(baskets: pd.Series, item_freq: dict, n_transactions: int, min_support: int, min_lift: float):
    """바스켓 Series(거래별 상품 리스트) → Lift 데이터프레임."""
    pair_count: dict = {}
    for items in baskets:
        if len(items) < 2:
            continue
        for a, b in combinations(sorted(items), 2):
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
            rows.append({"상품코드_A": a, "상품코드_B": b,
                         "동반구매빈도(Support)": cnt, "향상도(Lift)": round(lift, 4)})

    return pd.DataFrame(rows).sort_values("향상도(Lift)", ascending=False)


def main():
    print("POS 데이터 로드 중...")
    pos = pd.read_parquet(POS_PATH, columns=["거래_고유키", "상품코드", "상품명", "판매시간대"])
    pos["상품코드"] = pos["상품코드"].apply(norm_id)

    # 세븐일레븐 product_nodes 상품만 필터
    pn = pd.read_parquet(PRODUCT_NODES_PATH)
    seven_ids = set(
        pn[pn["편의점명"] == "세븐일레븐"]["ITEM_CD"].apply(norm_id)
    )
    pos = pos[pos["상품코드"].isin(seven_ids)].copy()
    print(f"세븐일레븐 필터 후: {len(pos):,}행 / 상품 {pos['상품코드'].nunique():,}개")

    # 상품코드 → 상품명 맵
    name_map = pos.drop_duplicates("상품코드").set_index("상품코드")["상품명"].to_dict()

    os.makedirs(OUT_DIR, exist_ok=True)

    for slot_name, hours in TIMESLOTS.items():
        print(f"\n[{slot_name}] 시간대 {hours} 처리 중...")
        slot_df = pos[pos["판매시간대"].isin(hours)]
        n_tx = slot_df["거래_고유키"].nunique()
        print(f"  거래 수: {n_tx:,} / 행 수: {len(slot_df):,}")

        # 거래별 상품 리스트
        baskets = slot_df.groupby("거래_고유키")["상품코드"].apply(list)

        # 상품별 등장 빈도
        item_freq = slot_df["상품코드"].value_counts().to_dict()

        lift_df = compute_lift(baskets, item_freq, n_tx, MIN_SUPPORT, MIN_LIFT)

        if lift_df.empty:
            print(f"  결과 없음 (support≥{MIN_SUPPORT}, lift≥{MIN_LIFT} 조건 미충족)")
            continue

        # 상품명 추가
        lift_df["상품명_A"] = lift_df["상품코드_A"].map(name_map)
        lift_df["상품명_B"] = lift_df["상품코드_B"].map(name_map)
        lift_df = lift_df[["상품명_A", "상품명_B", "상품코드_A", "상품코드_B",
                            "동반구매빈도(Support)", "향상도(Lift)"]]

        out_path = os.path.join(OUT_DIR, f"timeslot_lift_{slot_name}.csv")
        lift_df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"  저장 완료: {out_path} ({len(lift_df):,}쌍)")


if __name__ == "__main__":
    main()
