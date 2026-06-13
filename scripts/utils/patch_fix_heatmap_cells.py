import json
import sys
import ast

sys.stdout.reconfigure(encoding='utf-8')

NB_PATH = 'eda/ipynb/03_b5_promo_eda.ipynb'

with open(NB_PATH, encoding='utf-8') as f:
    nb = json.load(f)

print(f"Total cells: {len(nb['cells'])}")
for i in [42, 43]:
    print(f"Cell {i} preview: {''.join(nb['cells'][i]['source'])[:60]}")

# ── Cell 42: 합산 히트맵 + 테이블 (행사명_norm 직접 사용) ───────────────
cell42_lines = [
    "from itertools import combinations\n",
    "from collections import Counter\n",
    "import numpy as np\n",
    "\n",
    "target_types = ['묶음할인', '콤보할인', '콤보증정', '장바구니할인']\n",
    "\n",
    "total_counter = Counter()\n",
    "for ptype in target_types:\n",
    "    df_p = df_filtered.filter(pl.col('프로모션_타입') == ptype)\n",
    "    event_cats_p = (\n",
    "        df_p\n",
    "        .group_by('행사명_norm')\n",
    "        .agg(pl.col('중분류명').drop_nulls().unique().alias('중분류_목록'))\n",
    "        .filter(pl.col('중분류_목록').list.len() >= 2)\n",
    "    ).to_pandas()\n",
    "    for cats in event_cats_p['중분류_목록']:\n",
    "        for pair in combinations(sorted(cats), 2):\n",
    "            total_counter[pair] += 1\n",
    "\n",
    "all_cats = sorted(set(c for pair in total_counter for c in pair))\n",
    "n = len(all_cats)\n",
    "cat_idx = {c: i for i, c in enumerate(all_cats)}\n",
    "print(f'중분류 수: {n}개 | 총 조합 쌍 수: {len(total_counter)}개')\n",
    "\n",
    "matrix = np.zeros((n, n), dtype=int)\n",
    "for (a, b), cnt in total_counter.items():\n",
    "    i, j = cat_idx[a], cat_idx[b]\n",
    "    matrix[i, j] = cnt\n",
    "    matrix[j, i] = cnt\n",
    "\n",
    "fig, ax = plt.subplots(figsize=(18, 15))\n",
    "im = ax.imshow(matrix, cmap='YlOrRd', aspect='auto')\n",
    "ax.set_xticks(range(n))\n",
    "ax.set_yticks(range(n))\n",
    "ax.set_xticklabels(all_cats, rotation=45, ha='right', fontsize=8)\n",
    "ax.set_yticklabels(all_cats, fontsize=8)\n",
    "for i in range(n):\n",
    "    for j in range(n):\n",
    "        if matrix[i, j] > 0:\n",
    "            ax.text(j, i, str(matrix[i, j]),\n",
    "                    ha='center', va='center', fontsize=7,\n",
    "                    color='black' if matrix[i, j] < matrix.max() * 0.7 else 'white')\n",
    "plt.colorbar(im, ax=ax, shrink=0.8)\n",
    "ax.set_title(\n",
    "    '묶음할인·콤보할인·콤보증정·장바구니할인\\n중분류 공동출현 합산 히트맵\\n(값 = 4개 타입 합산 행사 수, 월별 중복 제거)',\n",
    "    fontsize=12\n",
    ")\n",
    "plt.tight_layout()\n",
    "plt.show()\n",
    "\n",
    "pair_df = (\n",
    "    pd.DataFrame(\n",
    "        [(a, b, cnt) for (a, b), cnt in total_counter.items() if cnt > 0],\n",
    "        columns=['중분류_A', '중분류_B', '공동출현_행사수']\n",
    "    )\n",
    "    .sort_values('공동출현_행사수', ascending=True)\n",
    "    .reset_index(drop=True)\n",
    ")\n",
    "print(f'\\n총 {len(pair_df)}개 조합 (0 제외)\\n')\n",
    "print(pair_df.to_string(index=False))\n",
]

# ── Cell 43: 타입별 개별 히트맵 (행사명_norm 직접 사용) ────────────────
cell43_lines = [
    "from itertools import combinations\n",
    "from collections import Counter\n",
    "import numpy as np\n",
    "\n",
    "target_types = ['묶음할인', '콤보할인', '콤보증정', '장바구니할인']\n",
    "fig, axes = plt.subplots(2, 2, figsize=(24, 20))\n",
    "axes = axes.flatten()\n",
    "\n",
    "for ax_idx, ptype in enumerate(target_types):\n",
    "    df_p = df_filtered.filter(pl.col('프로모션_타입') == ptype)\n",
    "    event_cats_p = (\n",
    "        df_p\n",
    "        .group_by('행사명_norm')\n",
    "        .agg(pl.col('중분류명').drop_nulls().unique().alias('중분류_목록'))\n",
    "        .filter(pl.col('중분류_목록').list.len() >= 2)\n",
    "    ).to_pandas()\n",
    "\n",
    "    pair_counter_p = Counter()\n",
    "    for cats in event_cats_p['중분류_목록']:\n",
    "        for pair in combinations(sorted(cats), 2):\n",
    "            pair_counter_p[pair] += 1\n",
    "\n",
    "    if not pair_counter_p:\n",
    "        axes[ax_idx].set_title(f'{ptype}\\n(공동출현 없음)')\n",
    "        axes[ax_idx].axis('off')\n",
    "        continue\n",
    "\n",
    "    cats_p = sorted(set(c for pair in pair_counter_p for c in pair))\n",
    "    n_p = len(cats_p)\n",
    "    cat_idx_p = {c: i for i, c in enumerate(cats_p)}\n",
    "    matrix_p = np.zeros((n_p, n_p), dtype=int)\n",
    "    for (a, b), cnt in pair_counter_p.items():\n",
    "        i, j = cat_idx_p[a], cat_idx_p[b]\n",
    "        matrix_p[i, j] = cnt\n",
    "        matrix_p[j, i] = cnt\n",
    "\n",
    "    ax = axes[ax_idx]\n",
    "    im = ax.imshow(matrix_p, cmap='YlOrRd', aspect='auto')\n",
    "    ax.set_xticks(range(n_p))\n",
    "    ax.set_yticks(range(n_p))\n",
    "    ax.set_xticklabels(cats_p, rotation=45, ha='right', fontsize=7)\n",
    "    ax.set_yticklabels(cats_p, fontsize=7)\n",
    "    for i in range(n_p):\n",
    "        for j in range(n_p):\n",
    "            if matrix_p[i, j] > 0:\n",
    "                ax.text(j, i, str(matrix_p[i, j]),\n",
    "                        ha='center', va='center', fontsize=6,\n",
    "                        color='black' if matrix_p[i, j] < matrix_p.max() * 0.7 else 'white')\n",
    "    plt.colorbar(im, ax=ax, shrink=0.8)\n",
    "    ax.set_title(f'{ptype} — 중분류 공동출현\\n(월별 중복 제거, 값 = 행사 수)', fontsize=10)\n",
    "\n",
    "plt.suptitle('프로모션 타입별 중분류 공동출현 히트맵 (월별 중복 제거)', fontsize=14, y=1.01)\n",
    "plt.tight_layout()\n",
    "plt.show()\n",
]

for lines, label, idx in [(cell42_lines, 'Cell42', 42), (cell43_lines, 'Cell43', 43)]:
    try:
        ast.parse(''.join(lines))
        nb['cells'][idx]['source'] = lines
        nb['cells'][idx]['outputs'] = []
        nb['cells'][idx]['execution_count'] = None
        print(f"{label} OK")
    except SyntaxError as e:
        print(f"{label} SyntaxError: {e}")
        sys.exit(1)

with open(NB_PATH, 'w', encoding='utf-8') as f:
    json.dump(nb, f, ensure_ascii=False, indent=1)

print("Done.")
