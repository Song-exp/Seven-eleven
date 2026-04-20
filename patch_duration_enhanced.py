import json
import sys
import ast

sys.stdout.reconfigure(encoding='utf-8')

NB_PATH = 'eda/ipynb/03_b5_promo_eda.ipynb'

with open(NB_PATH, encoding='utf-8') as f:
    nb = json.load(f)

target_idx = None
for i, c in enumerate(nb['cells']):
    if 'sum_interval_days' in ''.join(c['source']):
        target_idx = i
        break
print(f"Target cell: {target_idx}")

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
    "df_intervals['최초개시일'] = df_intervals['시작목록'].apply(\n",
    "    lambda x: pd.Timestamp(min(x)).strftime('%Y-%m-%d')\n",
    ")\n",
    "df_intervals['최종종료일'] = df_intervals['종료목록'].apply(\n",
    "    lambda x: pd.Timestamp(max(x)).strftime('%Y-%m-%d')\n",
    ")\n",
    "df_intervals['구간수'] = df_intervals['시작목록'].apply(len)\n",
    "\n",
    "df_norm_dur = (\n",
    "    df_intervals[['행사명_norm', '중분류명', '프로모션_타입',\n",
    "                  '최초개시일', '최종종료일', '구간수', '총운영일수', '전체스팬']]\n",
    "    .sort_values('총운영일수', ascending=False)\n",
    "    .reset_index(drop=True)\n",
    ")\n",
    "\n",
    "print(f\"정규화 후 고유 행사명: {len(df_norm_dur)}개\")\n",
    "print(f\"총운영일수 중앙값: {df_norm_dur['총운영일수'].median():.0f}일 / 평균: {df_norm_dur['총운영일수'].mean():.0f}일\\n\")\n",
    "\n",
    "print(\"=== 장기 운영 Top 30 (구간 합산 기준) ===\")\n",
    "print(df_norm_dur.head(30).to_string(index=False))\n",
    "\n",
    "# 분포 시각화 + 장기 행사 바차트\n",
    "fig, axes = plt.subplots(1, 3, figsize=(20, 6))\n",
    "\n",
    "axes[0].hist(df_norm_dur['총운영일수'], bins=40, color='steelblue', edgecolor='white')\n",
    "axes[0].set_title('총 운영일수 분포 (구간 합산)')\n",
    "axes[0].set_xlabel('일수')\n",
    "\n",
    "axes[1].hist(df_norm_dur['전체스팬'], bins=40, color='coral', edgecolor='white')\n",
    "axes[1].set_title('전체 스팬 분포 (첫날~마지막날)')\n",
    "axes[1].set_xlabel('일수')\n",
    "\n",
    "top20 = df_norm_dur.head(20)\n",
    "labels = [n[:25] + '…' if len(n) > 25 else n for n in top20['행사명_norm']]\n",
    "axes[2].barh(range(len(top20)), top20['총운영일수'], color='steelblue', label='총운영일수')\n",
    "axes[2].barh(range(len(top20)), top20['전체스팬'], color='lightcoral', alpha=0.5, label='전체스팬')\n",
    "axes[2].set_yticks(range(len(top20)))\n",
    "axes[2].set_yticklabels(labels, fontsize=8)\n",
    "axes[2].invert_yaxis()\n",
    "axes[2].set_title('장기 운영 행사 Top 20\\n(파랑=실운영일수, 빨강=전체스팬)')\n",
    "axes[2].set_xlabel('일수')\n",
    "axes[2].legend(fontsize=8)\n",
    "\n",
    "plt.suptitle('정규화 행사명 기준 지속기간 분석', fontsize=13)\n",
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
