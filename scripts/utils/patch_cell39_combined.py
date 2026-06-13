import json
import sys
import ast

sys.stdout.reconfigure(encoding='utf-8')

NB_PATH = 'eda/ipynb/03_b5_promo_eda.ipynb'

with open(NB_PATH, encoding='utf-8') as f:
    nb = json.load(f)

src_preview = ''.join(nb['cells'][39]['source'])[:80]
print(f"Cell 39 preview: {src_preview}")

code_lines = [
    "from itertools import combinations\n",
    "from collections import Counter\n",
    "import numpy as np\n",
    "\n",
    "target_types = ['묶음할인', '콤보할인', '콤보증정', '장바구니할인']\n",
    "\n",
    "# 타입별로 각각 카운트 후 합산\n",
    "total_counter = Counter()\n",
    "for ptype in target_types:\n",
    "    df_p = df_filtered.filter(pl.col('프로모션_타입') == ptype)\n",
    "    event_cats_p = (\n",
    "        df_p\n",
    "        .group_by('행사명')\n",
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
    "    '묶음할인·콤보할인·콤보증정·장바구니할인\\n중분류 공동출현 합산 히트맵\\n(값 = 4개 타입 합산 행사 수)',\n",
    "    fontsize=12\n",
    ")\n",
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

nb['cells'][39]['source'] = code_lines
nb['cells'][39]['outputs'] = []
nb['cells'][39]['execution_count'] = None

with open(NB_PATH, 'w', encoding='utf-8') as f:
    json.dump(nb, f, ensure_ascii=False, indent=1)

print("Cell 39 updated.")
