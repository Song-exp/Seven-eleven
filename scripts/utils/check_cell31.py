import json
import sys
sys.stdout.reconfigure(encoding='utf-8')

with open('eda/ipynb/03_b5_promo_eda.ipynb', encoding='utf-8') as f:
    nb = json.load(f)

# Find cells around 30-34 (duration analysis area)
for i in range(29, 36):
    src = ''.join(nb['cells'][i]['source'])
    print(f'=== Cell {i} ({nb["cells"][i]["cell_type"]}) ===')
    print(src[:600])
    print()
