import json
import sys
import ast

sys.stdout.reconfigure(encoding='utf-8')

NB_PATH = 'eda/ipynb/03_b5_promo_eda.ipynb'

with open(NB_PATH, encoding='utf-8') as f:
    nb = json.load(f)

code_lines = [
    "# 행사명 패턴 탐색 — 시기별로 분리된 동일 행사 확인\n",
    "import re\n",
    "\n",
    "target_types = ['묶음할인', '콤보할인', '콤보증정', '장바구니할인']\n",
    "event_names = (\n",
    "    df_filtered\n",
    "    .filter(pl.col('프로모션_타입').is_in(target_types))\n",
    "    .select('행사명')\n",
    "    .unique()\n",
    "    .sort('행사명')\n",
    "    ['행사명']\n",
    "    .to_list()\n",
    ")\n",
    "\n",
    "# 숫자·월·년도·기간 패턴이 포함된 행사명 샘플 출력\n",
    "patterns = re.compile(r'(\\d{4}|\\d{1,2}월|\\d+차|\\d+회|\\(|\\[)')\n",
    "matched = [n for n in event_names if patterns.search(str(n))][:60]\n",
    "print(f'시기 관련 패턴 포함 행사명 샘플 ({len(matched)}개 발췌):\\n')\n",
    "for n in matched:\n",
    "    print(' ', n)\n"
]

src_str = ''.join(code_lines)
ast.parse(src_str)

new_cell = {
    "cell_type": "code",
    "execution_count": None,
    "metadata": {},
    "outputs": [],
    "source": code_lines,
}

# Insert before cell 39
nb['cells'].insert(39, new_cell)
print(f"Inserted inspect cell at index 39, total cells: {len(nb['cells'])}")

with open(NB_PATH, 'w', encoding='utf-8') as f:
    json.dump(nb, f, ensure_ascii=False, indent=1)

print("Saved.")
