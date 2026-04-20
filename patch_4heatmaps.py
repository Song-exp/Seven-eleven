import json
import sys
import ast

sys.stdout.reconfigure(encoding='utf-8')

NB_PATH = 'eda/ipynb/03_b5_promo_eda.ipynb'

with open(NB_PATH, encoding='utf-8') as f:
    nb = json.load(f)

code_lines = [
    "from itertools import combinations\n",
    "from collections import Counter\n",
    "import numpy as np\n",
    "\n",
    "target_types = ['묶음할인', '콤보할인', '콤보증정', '장바구니할인']\n",
    "fig, axes = plt.subplots(2, 2, figsize=(22, 18))\n",
    "axes = axes.flatten()\n",
    "\n",
    "for ax_idx, ptype in enumerate(target_types):\n",
    "    df_p = df_filtered.filter(pl.col('프로모션_타입') == ptype)\n",
    "\n",
    "    event_cats_p = (\n",
    "        df_p\n",
    "        .group_by('행사명')\n",
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
    "    all_cats_p = sorted(set(c for pair in pair_counter_p for c in pair))\n",
    "    n_p = len(all_cats_p)\n",
    "    cat_idx_p = {c: i for i, c in enumerate(all_cats_p)}\n",
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
    "    ax.set_xticklabels(all_cats_p, rotation=45, ha='right', fontsize=7)\n",
    "    ax.set_yticklabels(all_cats_p, fontsize=7)\n",
    "    for i in range(n_p):\n",
    "        for j in range(n_p):\n",
    "            if matrix_p[i, j] > 0:\n",
    "                ax.text(j, i, str(matrix_p[i, j]),\n",
    "                        ha='center', va='center', fontsize=6,\n",
    "                        color='black' if matrix_p[i, j] < matrix_p.max() * 0.7 else 'white')\n",
    "    plt.colorbar(im, ax=ax, shrink=0.8)\n",
    "    ax.set_title(f'{ptype} — 중분류 공동출현\\n(값 = 함께 등장한 행사 수)', fontsize=10)\n",
    "\n",
    "plt.suptitle('프로모션 타입별 중분류 공동출현 히트맵', fontsize=14, y=1.01)\n",
    "plt.tight_layout()\n",
    "plt.show()\n",
]

# Validate syntax
src_str = ''.join(code_lines)
try:
    ast.parse(src_str)
    print("Syntax OK")
except SyntaxError as e:
    print(f"Syntax Error: {e}")
    sys.exit(1)

new_cell = {
    "cell_type": "code",
    "execution_count": None,
    "metadata": {},
    "outputs": [],
    "source": code_lines,
}

# Insert after cell 39 (index 39), so it becomes cell 40
nb['cells'].insert(40, new_cell)
print(f"Inserted at index 40, total cells now: {len(nb['cells'])}")

with open(NB_PATH, 'w', encoding='utf-8') as f:
    json.dump(nb, f, ensure_ascii=False, indent=1)

print("Saved.")
