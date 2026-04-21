import json, sys
sys.stdout.reconfigure(encoding='utf-8')
with open('eda/ipynb/03_b5_promo_eda.ipynb', encoding='utf-8') as f:
    nb = json.load(f)
for i in [34, 35]:
    print(f'=== Cell {i} ===')
    print(''.join(nb['cells'][i]['source']))
    print()
