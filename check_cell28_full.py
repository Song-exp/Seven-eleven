import json
import sys
sys.stdout.reconfigure(encoding='utf-8')

with open('eda/ipynb/03_b5_promo_eda.ipynb', encoding='utf-8') as f:
    nb = json.load(f)

src = ''.join(nb['cells'][28]['source'])
print(src)
