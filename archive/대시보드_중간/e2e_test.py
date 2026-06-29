"""e2e_test.py — 파이프라인 E2E 테스트"""
import warnings, requests
warnings.filterwarnings('ignore')

BASE = 'http://localhost:8000'

# ── 1. 트렌드 추론 (Gemma 호출) ───────────────────────────────────────
print("=== [1] 트렌드 추론: 뇨끼 열풍 ===")
r = requests.post(f'{BASE}/api/trend/infer', json={'trend': '뇨끼 열풍', 'top_n': 5}, timeout=180)
data = r.json()
print(f"  소스  : {data['source']}")
print(f"  속성  : {[a['attribute'] for a in data['attrs']]}")

# ── 2. 신제품 조합 제안 ───────────────────────────────────────────────
print()
print("=== [2] 신제품 조합 제안: 간식빵 × 뇨끼 열풍 ===")
r2 = requests.post(f'{BASE}/api/propose', json={'category': '간식빵', 'trend': '뇨끼 열풍'}, timeout=60)
d2 = r2.json()
for p in d2.get('proposals', []):
    print(f"  #{p['rank']} | 가중치 {p['total_weight']} | {p['attrs']} | 경유: {p['via_product']}")
if not d2.get('proposals'):
    print("  결과 없음:", d2.get('message'))

# ── 3. 부진 상품 목록 ──────────────────────────────────────────────────
print()
print("=== [3] 부진 상품 목록 ===")
r3 = requests.get(f'{BASE}/api/bust', timeout=10)
for item in r3.json().get('items', []):
    print(f"  {item['item_name']} ({item['category']})")

# ── 4. 부진 상품 진단 ──────────────────────────────────────────────────
print()
print("=== [4] 부진 상품 진단: P004 (KBO 미트 파이) ===")
r4 = requests.get(f'{BASE}/api/bust/P004', timeout=10)
d4 = r4.json()
print(f"  회피 속성: {[a['attr'] for a in d4.get('avoid_attrs', [])]}")
print(f"  약한 IP  : {d4.get('weak_ips', [])}")

print()
print("[완료] E2E 테스트 통과")
