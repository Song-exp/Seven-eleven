import os, json

base = r'c:\Users\alexj\2026_캡스톤_디자인\세븐일레븐_프로젝트\세븐일레븐_내부데이터\대시보드_중간'

# ── 0. 파일 목록 존재 확인 ─────────────────────────────────────────────
files = [
    'requirements.txt', '.gitignore', '.env',
    'data/network.json', 'create_dummy_data.py',
    'server.py', 'llm_connector.py', 'graph_engine.py',
    'network_builder.py', 'config.js', 'dashboard.html'
]
print("=== [TASK 0] 파일 존재 여부 ===")
for f in files:
    path = os.path.join(base, f)
    exists = os.path.exists(path)
    size = os.path.getsize(path) if exists else 0
    status = "OK     " if exists else "MISSING"
    print(f"  [{status}] {f}  ({size:,} bytes)")

# ── 1. .env 모델 설정 확인 ────────────────────────────────────────────
print()
print("=== [TASK 1] .env 모델 설정 확인 ===")
env_path = os.path.join(base, '.env')
with open(env_path) as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith('#'):
            print(f"  {line}")

# ── 2. network.json 스키마 검증 ───────────────────────────────────────
print()
print("=== [TASK 2] network.json 스키마 검증 ===")
net_path = os.path.join(base, 'data', 'network.json')
with open(net_path, encoding='utf-8') as f:
    net = json.load(f)

nodes = net['nodes']
edges = net['edges']
node_ids = {n['id'] for n in nodes}

# 타입별 수
types = {}
for n in nodes:
    t = n.get('type', 'unknown')
    types[t] = types.get(t, 0) + 1
print(f"  노드 수: {len(nodes)}  |  {types}")

# 엣지 타입별 수
etypes = {}
for e in edges:
    t = e.get('edge_type', 'unknown')
    etypes[t] = etypes.get(t, 0) + 1
print(f"  엣지 수: {len(edges)}  |  {etypes}")

# 엣지 유효성
errors = [e for e in edges if e['source'] not in node_ids or e['target'] not in node_ids]
print(f"  엣지 오류: {len(errors)}개  {'(없음 - 정상)' if not errors else str(errors[:3])}")

# 부진 상품 확인
busts = [n for n in nodes if n.get('type') == 'product' and n.get('survived') is False]
print(f"  부진 상품(survived=False): {[n['label'] for n in busts]}")
