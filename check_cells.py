import json
import sys

sys.stdout.reconfigure(encoding='utf-8')

with open('eda/ipynb/03_b5_promo_eda.ipynb', encoding='utf-8') as f:
    nb = json.load(f)

print(f'Total cells: {len(nb["cells"])}')
for i in range(36, len(nb['cells'])):
    src = ''.join(nb['cells'][i]['source'])
    print(f'--- Cell {i} ({nb["cells"][i]["cell_type"]}) ---')
    print(src[:200])
    print()
