"""
두 상품 비교 HIN 네트워크
A: KBO)끝내기홈런미트부리또130g  (#3B82F6, x=-700)
B: KBO)롯데자이언츠거인단팥빵100g (#F97316, x=+700)

레이아웃:
  A 노드 — 왼쪽 고정
  B 노드 — 오른쪽 고정
  A 전용 속성 (#93C5FD) — 왼쪽 반원
  B 전용 속성 (#FDBA74) — 오른쪽 반원
  공유 속성  (#7C3AED) — 가운데 세로 배열
  동시구매 파트너 (상위 8개씩) — 외곽 호(arc)

출력: network_html/compare_two_products.html
"""
import os, json, math
import pandas as pd
import polars as pl
import networkx as nx

# ── 경로 ──────────────────────────────────────────────────────
BASE_DIR = os.path.abspath(os.path.join(os.getcwd(), '..'))
PROC     = os.path.join(BASE_DIR, 'data', 'processed')
EDA_DIR  = os.path.join(BASE_DIR, 'eda')
OUT_DIR  = os.path.join(EDA_DIR, 'network_html')
os.makedirs(OUT_DIR, exist_ok=True)

# ── 두 앵커 상품 ──────────────────────────────────────────────
CODE_A = '125394'   # KBO)끝내기홈런미트부리또130g
CODE_B = '125340'   # KBO)롯데자이언츠거인단팥빵100g
COLOR_A = '#3B82F6'
COLOR_B = '#F97316'
COLOR_A_ONLY  = '#93C5FD'   # A 전용 속성 (하늘)
COLOR_B_ONLY  = '#FDBA74'   # B 전용 속성 (주황)
COLOR_SHARED  = '#7C3AED'   # 공유 속성 (보라)
COLOR_PARTNER = '#64748B'   # 동시구매 파트너 (회색)

# ── 데이터 로드 ────────────────────────────────────────────────
prod      = pl.read_parquet(os.path.join(PROC, 'product_master_dataset.parquet'))
edges_all = pl.read_parquet(os.path.join(PROC, 'final_edgelist_with_trend.parquet'))

code_to_name = dict(zip(prod['ITEM_CD'].to_list(), prod['ITEM_NM'].to_list()))
NAME_A = code_to_name.get(CODE_A, CODE_A)
NAME_B = code_to_name.get(CODE_B, CODE_B)
print(f'A: {NAME_A}')
print(f'B: {NAME_B}')

# ── 속성 엣지 ─────────────────────────────────────────────────
pa = edges_all.filter(pl.col('edge_type') == '제품-일반속성').to_pandas()
attrs_A = set(pa[pa['src_node'] == CODE_A]['dst_node'].tolist())
attrs_B = set(pa[pa['src_node'] == CODE_B]['dst_node'].tolist())
attrs_shared  = attrs_A & attrs_B
attrs_A_only  = attrs_A - attrs_B
attrs_B_only  = attrs_B - attrs_A

print(f'A 전용 속성: {len(attrs_A_only)}개  B 전용 속성: {len(attrs_B_only)}개  공유: {len(attrs_shared)}개')

# 속성 수 제한 (화면 가독성)
MAX_ATTR = 20
attrs_A_only  = set(list(attrs_A_only)[:MAX_ATTR])
attrs_B_only  = set(list(attrs_B_only)[:MAX_ATTR])
attrs_shared  = set(list(attrs_shared)[:MAX_ATTR])

# ── 동시구매 파트너 (상위 8개) ────────────────────────────────
pp = edges_all.filter(pl.col('edge_type') == '제품-제품').to_pandas()

def top_partners(code, n=8):
    mask = (pp['src_node'] == code) | (pp['dst_node'] == code)
    sub  = pp[mask].copy()
    sub['partner'] = sub.apply(
        lambda r: r['dst_node'] if r['src_node'] == code else r['src_node'], axis=1)
    sub = sub.sort_values('weight', ascending=False)
    return sub[['partner', 'weight']].head(n).values.tolist()

partners_A = top_partners(CODE_A)
partners_B = top_partners(CODE_B)

# 파트너 코드 집합 (양쪽 중복 제거)
partner_set = {p for p, _ in partners_A} | {p for p, _ in partners_B}
print(f'동시구매 파트너: A={len(partners_A)}개  B={len(partners_B)}개')

# ══════════════════════════════════════════════════════════════
# 좌표 계산 (fixed physics=false)
# ══════════════════════════════════════════════════════════════
CX_A, CX_B = -700, 700
CY_C = 0          # 가운데 Y 중심

def semicircle_coords(n, cx, side='left', r=280):
    """반원 좌표 — side='left': π/2 ~ 3π/2, side='right': -π/2 ~ π/2"""
    if n == 0:
        return []
    if side == 'left':
        angles = [math.pi/2 + i * math.pi / max(n - 1, 1) for i in range(n)]
    else:
        angles = [-math.pi/2 + i * math.pi / max(n - 1, 1) for i in range(n)]
    return [(round(cx + r * math.cos(a), 1), round(CY_C + r * math.sin(a), 1))
            for a in angles]

def vertical_coords(n, cx=0, y_span=500):
    if n == 0:
        return []
    step = y_span / max(n - 1, 1)
    return [(cx, round(-y_span / 2 + i * step, 1)) for i in range(n)]

def arc_coords(n, cx, side='left', r=550):
    """파트너 외곽 호 좌표"""
    if n == 0:
        return []
    if side == 'left':
        angles = [math.pi * 0.6 + i * (math.pi * 0.8) / max(n - 1, 1) for i in range(n)]
    else:
        angles = [-math.pi * 0.4 - i * (math.pi * 0.8) / max(n - 1, 1) for i in range(n)]
    return [(round(cx + r * math.cos(a), 1), round(CY_C + r * math.sin(a), 1))
            for a in angles]


# 좌표 매핑
coord_map = {}
coord_map[CODE_A] = (CX_A, 0)
coord_map[CODE_B] = (CX_B, 0)

a_only_list  = list(attrs_A_only)
b_only_list  = list(attrs_B_only)
shared_list  = list(attrs_shared)

for node, (x, y) in zip(a_only_list,  semicircle_coords(len(a_only_list),  CX_A, 'left')):
    coord_map[node] = (x, y)
for node, (x, y) in zip(b_only_list,  semicircle_coords(len(b_only_list),  CX_B, 'right')):
    coord_map[node] = (x, y)
for node, (x, y) in zip(shared_list,  vertical_coords(len(shared_list))):
    coord_map[node] = (x, y)

pa_list = [p for p, _ in partners_A]
pb_list = [p for p, _ in partners_B]
for node, (x, y) in zip(pa_list, arc_coords(len(pa_list), CX_A, 'left')):
    coord_map[node] = (x, y)
for node, (x, y) in zip(pb_list, arc_coords(len(pb_list), CX_B, 'right')):
    coord_map.setdefault(node, (x, y))   # B 파트너가 A 파트너와 겹칠 경우 우선 A좌표 유지

# ══════════════════════════════════════════════════════════════
# vis.js 노드 / 엣지 빌드
# ══════════════════════════════════════════════════════════════
vis_nodes = []
vis_edges = []
eid = 0


def push_node(nid, label, color, border, size, shape='dot', bold=False):
    x, y = coord_map.get(nid, (0, 0))
    name_full = code_to_name.get(nid, nid)
    vis_nodes.append({
        'id':          nid,
        'label':       '',
        'hiddenLabel': label,
        'shape':       shape,
        'size':        size,
        'color': {'background': color, 'border': border,
                  'highlight': {'background': color, 'border': '#ffffff'},
                  'hover':     {'background': color, 'border': '#ffffff'}},
        'font':        {'color': '#ffffff', 'size': 11, 'bold': bold,
                        'face': 'Malgun Gothic, sans-serif'},
        'borderWidth': 3 if bold else 1,
        'shadow':      bold,
        'physics':     False,
        'x':           x,
        'y':           y,
        'title':       f'<div style="font-family:Malgun Gothic,sans-serif;font-size:13px">'
                       f'<b>{name_full}</b><br>코드: {nid}</div>',
    })


def push_edge(src, dst, color, width=1.0):
    global eid
    vis_edges.append({'id': eid, 'from': src, 'to': dst,
                      'color': {'color': color}, 'width': width})
    eid += 1


# 앵커 상품 A, B
push_node(CODE_A, NAME_A[:12] + ('…' if len(NAME_A) > 12 else ''),
          COLOR_A, '#ffffff', 40, bold=True)
push_node(CODE_B, NAME_B[:12] + ('…' if len(NAME_B) > 12 else ''),
          COLOR_B, '#ffffff', 40, bold=True)

# 속성 노드
for attr in a_only_list:
    push_node(attr, attr, COLOR_A_ONLY, '#60A5FA', 14)
    push_edge(CODE_A, attr, COLOR_A_ONLY, 1.2)

for attr in b_only_list:
    push_node(attr, attr, COLOR_B_ONLY, '#FB923C', 14)
    push_edge(CODE_B, attr, COLOR_B_ONLY, 1.2)

for attr in shared_list:
    push_node(attr, attr, COLOR_SHARED, '#A78BFA', 16)
    push_edge(CODE_A, attr, COLOR_SHARED, 1.5)
    push_edge(CODE_B, attr, COLOR_SHARED, 1.5)

# 동시구매 파트너
added_partners = set()
w_vals = [w for _, w in partners_A + partners_B]
w_min, w_max = (min(w_vals), max(w_vals)) if w_vals else (1, 1)

for partner, w in partners_A:
    if partner not in added_partners and partner not in {CODE_A, CODE_B}:
        pname = code_to_name.get(partner, partner)
        push_node(partner, pname[:10] + ('…' if len(pname) > 10 else ''),
                  COLOR_PARTNER, '#94A3B8', 12)
        added_partners.add(partner)
    edge_w = 0.5 + (w - w_min) / (w_max - w_min + 1e-9) * 3
    push_edge(CODE_A, partner, '#93C5FD', round(edge_w, 2))

for partner, w in partners_B:
    if partner not in added_partners and partner not in {CODE_A, CODE_B}:
        pname = code_to_name.get(partner, partner)
        push_node(partner, pname[:10] + ('…' if len(pname) > 10 else ''),
                  COLOR_PARTNER, '#94A3B8', 12)
        added_partners.add(partner)
    edge_w = 0.5 + (w - w_min) / (w_max - w_min + 1e-9) * 3
    push_edge(CODE_B, partner, '#FDBA74', round(edge_w, 2))

nodes_json = json.dumps(vis_nodes, ensure_ascii=False)
edges_json = json.dumps(vis_edges, ensure_ascii=False)

# ══════════════════════════════════════════════════════════════
# HTML 출력
# ══════════════════════════════════════════════════════════════
html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>두 상품 비교 네트워크</title>
<style>
  body {{ margin: 0; background: #ffffff;
         font-family: 'Malgun Gothic', 'Apple SD Gothic Neo', sans-serif; }}
  #network {{ width: 100%; height: 100vh; }}
  #legend {{
    position: fixed; top: 16px; left: 16px; z-index: 9999;
    background: rgba(255,255,255,0.97); border: 1px solid #e2e8f0;
    border-radius: 10px; padding: 14px 18px; font-size: 13px;
    color: #1e293b; line-height: 2; min-width: 210px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.1);
  }}
  .vis-tooltip {{
    font-family: 'Malgun Gothic', sans-serif !important;
    font-size: 13px; padding: 8px 12px; border-radius: 6px;
    max-width: 280px; background: rgba(255,255,255,0.97);
    border: 1px solid #e2e8f0; color: #1e293b;
    box-shadow: 0 4px 12px rgba(0,0,0,0.15);
  }}
</style>
<script src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
</head>
<body>
<div id="legend">
  <b style="font-size:14px">🔵 A 상품</b><br>
  <span style="color:{COLOR_A}">●</span> {NAME_A[:16]}<br>
  <b style="font-size:14px">🟠 B 상품</b><br>
  <span style="color:{COLOR_B}">●</span> {NAME_B[:16]}<br><br>
  <span style="color:{COLOR_A_ONLY}">●</span> A 전용 속성<br>
  <span style="color:{COLOR_B_ONLY}">●</span> B 전용 속성<br>
  <span style="color:{COLOR_SHARED}">●</span> 공유 속성<br>
  <span style="color:{COLOR_PARTNER}">●</span> 동시구매 파트너<br><br>
  <span style="font-size:11px;color:#64748b">
    클릭: 1-hop 강조 / 호버: 레이블<br>
    빈 공간 클릭: 초기화
  </span>
</div>
<div id="network"></div>
<script>
  var nodesRaw = {nodes_json};
  nodesRaw = nodesRaw.map(function(n) {{
    if (n.title) {{
      var div = document.createElement('div');
      div.style.cssText = 'font-family:Malgun Gothic,sans-serif;font-size:13px;line-height:1.6';
      div.innerHTML = n.title;
      n.title = div;
    }}
    return n;
  }});
  var nodes = new vis.DataSet(nodesRaw);
  var edges = new vis.DataSet({edges_json});
  var container = document.getElementById('network');
  var network = new vis.Network(container,
    {{nodes: nodes, edges: edges}},
    {{
      nodes:   {{borderWidth: 2, shadow: false}},
      edges:   {{smooth: {{type: 'continuous'}}}},
      physics: {{enabled: false}},
      interaction: {{
        dragNodes: true, dragView: true, zoomView: true,
        hover: true, tooltipDelay: 80,
        navigationButtons: true, keyboard: true
      }}
    }}
  );

  // 원본 색상 저장
  var origNodeColor = {{}};
  var origEdgeColor = {{}};
  var origEdgeWidth = {{}};
  nodes.forEach(function(n) {{ origNodeColor[n.id] = JSON.parse(JSON.stringify(n.color)); }});
  edges.forEach(function(e) {{ origEdgeColor[e.id] = JSON.parse(JSON.stringify(e.color)); origEdgeWidth[e.id] = e.width || 1; }});

  // 호버: 레이블
  network.on('hoverNode', function(p) {{
    var n = nodes.get(p.node);
    if (n && n.hiddenLabel) nodes.update({{id: p.node, label: n.hiddenLabel}});
  }});
  network.on('blurNode', function(p) {{ nodes.update({{id: p.node, label: ''}}); }});

  // 클릭: 1-hop
  var activeNode = null;
  network.on('click', function(params) {{
    if (params.nodes.length === 0) {{ restoreAll(); activeNode = null; return; }}
    var nid = params.nodes[0];
    if (activeNode === nid) {{ restoreAll(); activeNode = null; return; }}
    activeNode = nid;
    var neighbors = new Set(network.getConnectedNodes(nid));
    neighbors.add(nid);
    var connEdges = new Set(network.getConnectedEdges(nid));
    var nu = [];
    nodes.forEach(function(n) {{
      if (neighbors.has(n.id)) {{
        nu.push({{id: n.id, color: JSON.parse(JSON.stringify(origNodeColor[n.id])), opacity: 1.0, borderWidth: 3}});
      }} else {{
        nu.push({{id: n.id,
          color: {{background:'#e8ecef',border:'#ced4da',
                  highlight:{{background:'#e8ecef',border:'#ced4da'}},
                  hover:{{background:'#e8ecef',border:'#ced4da'}}}},
          opacity: 0.15}});
      }}
    }});
    nodes.update(nu);
    var eu = [];
    edges.forEach(function(e) {{
      if (connEdges.has(e.id)) {{
        eu.push({{id: e.id, color: JSON.parse(JSON.stringify(origEdgeColor[e.id])), width: origEdgeWidth[e.id] * 2}});
      }} else {{
        eu.push({{id: e.id, color:{{color:'#eeeeee'}}, width: 0.3}});
      }}
    }});
    edges.update(eu);
  }});

  function restoreAll() {{
    var nu = [];
    nodes.forEach(function(n) {{ nu.push({{id: n.id, color: JSON.parse(JSON.stringify(origNodeColor[n.id])), opacity: 1.0}}); }});
    nodes.update(nu);
    var eu = [];
    edges.forEach(function(e) {{ eu.push({{id: e.id, color: JSON.parse(JSON.stringify(origEdgeColor[e.id])), width: origEdgeWidth[e.id]}}); }});
    edges.update(eu);
  }}
</script>
</body>
</html>"""

html_path = os.path.join(OUT_DIR, 'compare_two_products.html')
with open(html_path, 'w', encoding='utf-8') as f:
    f.write(html)
print(f'✅ HTML: {html_path}')
print(f'  노드 {len(vis_nodes)}개  엣지 {len(vis_edges)}개')
