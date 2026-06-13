import json
import sys
import ast

sys.stdout.reconfigure(encoding='utf-8')

NB_PATH = 'eda/ipynb/03_b5_promo_eda.ipynb'

with open(NB_PATH, encoding='utf-8') as f:
    nb = json.load(f)

# norm_event 정의 셀 바로 뒤 (index 30이 정규화 코드 셀)
# 31은 markdown(7-0-1), 32는 시계열 코드
# norm cell = 30, 그 직후(31)에 markdown이 있으므로 32 뒤에 삽입
# 실제로는 norm cell 바로 뒤 = 31에 삽입하고 기존 markdown을 뒤로 밀기

code_lines = [
    "# 정규화로 합쳐진 그룹 확인 — 어떤 행사명들이 묶였는지\n",
    "norm_map = (\n",
    "    df_filtered\n",
    "    .select(['행사명', '행사명_norm'])\n",
    "    .unique()\n",
    "    .to_pandas()\n",
    ")\n",
    "\n",
    "# 2개 이상의 원본 행사명이 같은 norm으로 묶인 그룹\n",
    "merged = (\n",
    "    norm_map.groupby('행사명_norm')['행사명']\n",
    "    .apply(list)\n",
    "    .reset_index()\n",
    ")\n",
    "merged = merged[merged['행사명'].apply(len) >= 2].sort_values(\n",
    "    '행사명', key=lambda x: x.apply(len), ascending=False\n",
    ").reset_index(drop=True)\n",
    "\n",
    "print(f'정규화로 합쳐진 그룹 수: {len(merged)}개\\n')\n",
    "for _, row in merged.iterrows():\n",
    "    print(f'[norm] {row[\"행사명_norm\"]}')\n",
    "    for orig in sorted(row['행사명']):\n",
    "        print(f'       └ {orig}')\n",
    "    print()\n",
]

ast.parse(''.join(code_lines))

nb['cells'].insert(31, {
    "cell_type": "code",
    "execution_count": None,
    "metadata": {},
    "outputs": [],
    "source": code_lines,
})

with open(NB_PATH, 'w', encoding='utf-8') as f:
    json.dump(nb, f, ensure_ascii=False, indent=1)

print(f"Inserted at 31, total: {len(nb['cells'])} cells")
