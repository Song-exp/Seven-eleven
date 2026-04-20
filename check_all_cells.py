import json
import sys
sys.stdout.reconfigure(encoding='utf-8')

with open('eda/ipynb/03_b5_promo_eda.ipynb', encoding='utf-8') as f:
    nb = json.load(f)

print(f"Total cells: {len(nb['cells'])}\n")
for i, c in enumerate(nb['cells']):
    src = ''.join(c['source'])
    preview = src[:120].replace('\n', ' ')
    print(f"[{i:02d}] {c['cell_type']:8s} | {preview}")
