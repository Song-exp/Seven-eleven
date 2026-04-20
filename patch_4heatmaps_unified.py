import json
import sys
import ast

sys.stdout.reconfigure(encoding='utf-8')

NB_PATH = 'eda/ipynb/03_b5_promo_eda.ipynb'

with open(NB_PATH, encoding='utf-8') as f:
    nb = json.load(f)

# Find cell 40 (the one we inserted) and replace it
# Verify it's the right cell
src_preview = ''.join(nb['cells'][40]['source'])[:80]
print(f"Cell 40 preview: {src_preview}")

code_lines = [
    "from itertools import combinations\n",
    "from collections import Counter\n",
    "import numpy as np\n",
    "\n",
    "target_types = ['묶음할인', '콤보할인', '콤보증정', '장바구니할인']\n",
    "\n",
    "# 각 타입별 (행사명 → 중분류 집합) 계산\n",
    "type_pair_counters = {}\n",
    "for ptype in target_types:\n",
    "    df_p = df_filtered.filter(pl.col('프로모션_타입') == ptype)\n",
    "    event_cats_p = (\n",
    "        df_p\n",
    "        .group_by('행사명')\n",
    "        .agg(pl.col('중분류명').drop_nulls().unique().alias('중분류_목록'))\n",
    "        .filter(pl.col('중분류_목록').list.len() >= 2)\n",
    "    ).to_pandas()\n",
    "    pair_counter_p = Counter()\n",
    "    for cats in event_cats_p['중분류_목록']:\n",
    "        for pair in combinations(sorted(cats), 2):\n",
    "            pair_counter_p[pair] += 1\n",
    "    type_pair_counters[ptype] = pair_counter_p\n",
    "\n",
    "# 4개 타입 전체 중분류 합집합 → 공통 축\n",
    "all_cats_union = sorted(set(\n",
    "    c\n",
    "    for counter in type_pair_counters.values()\n",
    "    for pair in counter\n",
    "    for c in pair\n",
    "))\n",
    "n = len(all_cats_union)\n",
    "cat_idx = {c: i for i, c in enumerate(all_cats_union)}\n",
    "print(f'공통 중분류 수: {n}개')\n",
    "\n",
    "fig, axes = plt.subplots(2, 2, figsize=(24, 20))\n",
    "axes = axes.flatten()\n",
    "\n",
    "for ax_idx, ptype in enumerate(target_types):\n",
    "    pair_counter_p = type_pair_counters[ptype]\n",
    "    matrix_p = np.zeros((n, n), dtype=int)\n",
    "    for (a, b), cnt in pair_counter_p.items():\n",
    "        i, j = cat_idx[a], cat_idx[b]\n",
    "        matrix_p[i, j] = cnt\n",
    "        matrix_p[j, i] = cnt\n",
    "\n",
    "    ax = axes[ax_idx]\n",
    "    im = ax.imshow(matrix_p, cmap='YlOrRd', aspect='auto')\n",
    "    ax.set_xticks(range(n))\n",
    "    ax.set_yticks(range(n))\n",
    "    ax.set_xticklabels(all_cats_union, rotation=45, ha='right', fontsize=7)\n",
    "    ax.set_yticklabels(all_cats_union, fontsize=7)\n",
    "    for i in range(n):\n",
    "        for j in range(n):\n",
    "            if matrix_p[i, j] > 0:\n",
    "                ax.text(j, i, str(matrix_p[i, j]),\n",
    "                        ha='center', va='center', fontsize=6,\n",
    "                        color='black' if matrix_p[i, j] < matrix_p.max() * 0.7 else 'white')\n",
    "    plt.colorbar(im, ax=ax, shrink=0.8)\n",
    "    ax.set_title(f'{ptype} — 중분류 공동출현\\n(값 = 함께 등장한 행사 수)', fontsize=10)\n",
    "\n",
    "plt.suptitle('프로모션 타입별 중분류 공동출현 히트맵 (공통 축)', fontsize=14, y=1.01)\n",
    "plt.tight_layout()\n",
    "plt.show()\n",
]

src_str = ''.join(code_lines)
try:
    ast.parse(src_str)
    print("Syntax OK")
except SyntaxError as e:
    print(f"Syntax Error: {e}")
    sys.exit(1)

nb['cells'][40]['source'] = code_lines
nb['cells'][40]['outputs'] = []
nb['cells'][40]['execution_count'] = None

with open(NB_PATH, 'w', encoding='utf-8') as f:
    json.dump(nb, f, ensure_ascii=False, indent=1)

print("Cell 40 updated.")
