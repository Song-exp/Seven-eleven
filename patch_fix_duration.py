import json
import sys
import ast

sys.stdout.reconfigure(encoding='utf-8')

NB_PATH = 'eda/ipynb/03_b5_promo_eda.ipynb'

with open(NB_PATH, encoding='utf-8') as f:
    nb = json.load(f)

# Find the normalized duration cell (contains sum_interval_days)
target_idx = None
for i, c in enumerate(nb['cells']):
    if 'sum_interval_days' in ''.join(c['source']):
        target_idx = i
        break

print(f"Found target cell at index {target_idx}")

cell_lines = [
    "# 정규화 행사명 기준 — 구간 합산 총 운영일수 분석\n",
    "import pandas as pd\n",
    "\n",
    "df_intervals = (\n",
    "    df_filtered\n",
    "    .filter(pl.col('행사종료일').dt.year() != 9999)\n",
    "    .group_by('행사명_norm')\n",
    "    .agg([\n",
    "        pl.col('행사개시일').alias('시작목록'),\n",
    "        pl.col('행사종료일').alias('종료목록'),\n",
    "        pl.col('중분류명').first().alias('중분류명'),\n",
    "        pl.col('프로모션_타입').first().alias('프로모션_타입'),\n",
    "    ])\n",
    "    .to_pandas()\n",
    ")\n",
    "\n",
    "def sum_interval_days(starts, ends):\n",
    "    intervals = sorted(zip(\n",
    "        [pd.Timestamp(s) for s in starts],\n",
    "        [pd.Timestamp(e) for e in ends],\n",
    "    ))\n",
    "    cur_s, cur_e = intervals[0]\n",
    "    total = 0\n",
    "    for s, e in intervals[1:]:\n",
    "        if s <= cur_e + pd.Timedelta(days=1):\n",
    "            cur_e = max(cur_e, e)\n",
    "        else:\n",
    "            total += (cur_e - cur_s).days + 1\n",
    "            cur_s, cur_e = s, e\n",
    "    total += (cur_e - cur_s).days + 1\n",
    "    return total\n",
    "\n",
    "df_intervals['총운영일수'] = df_intervals.apply(\n",
    "    lambda r: sum_interval_days(r['시작목록'], r['종료목록']), axis=1\n",
    ")\n",
    "df_intervals['전체스팬'] = df_intervals.apply(\n",
    "    lambda r: (pd.Timestamp(max(r['종료목록'])) - pd.Timestamp(min(r['시작목록']))).days + 1, axis=1\n",
    ")\n",
    "\n",
    "df_norm_dur = (\n",
    "    df_intervals[['행사명_norm', '중분류명', '프로모션_타입', '총운영일수', '전체스팬']]\n",
    "    .sort_values('총운영일수', ascending=False)\n",
    "    .reset_index(drop=True)\n",
    ")\n",
    "\n",
    "print(f\"정규화 후 고유 행사명: {len(df_norm_dur)}개\\n\")\n",
    "print(\"=== 총 운영일수 Top 30 (구간 합산, 공백 제외) ===\")\n",
    "print(df_norm_dur.head(30).to_string(index=False))\n",
    "\n",
    "fig, axes = plt.subplots(1, 2, figsize=(14, 5))\n",
    "axes[0].hist(df_norm_dur['총운영일수'], bins=30, color='steelblue', edgecolor='white')\n",
    "axes[0].set_title('총 운영일수 분포 (구간 합산, 공백 제외)')\n",
    "axes[0].set_xlabel('일수')\n",
    "axes[1].hist(df_norm_dur['전체스팬'], bins=30, color='coral', edgecolor='white')\n",
    "axes[1].set_title('전체 스팬 분포 (첫 개시일 ~ 마지막 종료일)')\n",
    "axes[1].set_xlabel('일수')\n",
    "plt.suptitle('정규화 행사명 기준 지속기간 분석', fontsize=12)\n",
    "plt.tight_layout()\n",
    "plt.show()\n",
]

ast.parse(''.join(cell_lines))
print("Syntax OK")

nb['cells'][target_idx]['source'] = cell_lines
nb['cells'][target_idx]['outputs'] = []
nb['cells'][target_idx]['execution_count'] = None

with open(NB_PATH, 'w', encoding='utf-8') as f:
    json.dump(nb, f, ensure_ascii=False, indent=1)

print(f"Cell {target_idx} updated.")
