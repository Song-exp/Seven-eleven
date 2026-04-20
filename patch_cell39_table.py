import json
import sys
import ast

sys.stdout.reconfigure(encoding='utf-8')

NB_PATH = 'eda/ipynb/03_b5_promo_eda.ipynb'

with open(NB_PATH, encoding='utf-8') as f:
    nb = json.load(f)

current = ''.join(nb['cells'][39]['source'])

# Append table output lines
append_lines = [
    "\n",
    "# 빈도 오름차순 정리 (0 제외)\n",
    "pair_df = (\n",
    "    pd.DataFrame(\n",
    "        [(a, b, cnt) for (a, b), cnt in total_counter.items() if cnt > 0],\n",
    "        columns=['중분류_A', '중분류_B', '공동출현_행사수']\n",
    "    )\n",
    "    .sort_values('공동출현_행사수', ascending=True)\n",
    "    .reset_index(drop=True)\n",
    ")\n",
    "print(f'총 {len(pair_df)}개 조합 (0 제외)\\n')\n",
    "print(pair_df.to_string(index=False))\n",
]

new_source = nb['cells'][39]['source'] + append_lines

src_str = ''.join(new_source)
try:
    ast.parse(src_str)
    print("Syntax OK")
except SyntaxError as e:
    print(f"Syntax Error: {e}")
    import sys; sys.exit(1)

nb['cells'][39]['source'] = new_source
nb['cells'][39]['outputs'] = []
nb['cells'][39]['execution_count'] = None

with open(NB_PATH, 'w', encoding='utf-8') as f:
    json.dump(nb, f, ensure_ascii=False, indent=1)

print("Cell 39 updated with pair table.")
