import json
import sys
sys.stdout.reconfigure(encoding='utf-8')

with open('eda/ipynb/03_b5_promo_eda.ipynb', encoding='utf-8') as f:
    nb = json.load(f)

for i in [27, 28, 29, 30]:
    src = ''.join(nb['cells'][i]['source'])
    print(f'=== Cell {i} ({nb["cells"][i]["cell_type"]}) ===')
    print(src[:400])
    print()
