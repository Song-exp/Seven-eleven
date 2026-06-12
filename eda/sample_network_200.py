"""
200개 샘플 HIN 네트워크
출력 1: sample_network_200.png  — 대표 정적 이미지 (matplotlib)
출력 2: sample_network_200.html — 인터랙티브 (클릭 → 1-hop 강조, 호버 → 레이블)

색상 규칙:
  앵커 6개   → 클러스터 색상 (노랑/주황/빨강)
  나머지 상품 → 다크네이비 #0D1F2D
  속성 노드  → #10B981 그린

상품 선택 전략:
  속성 IDF 기반 차별성 점수 상위 200개 선발
  (희귀 속성을 많이 가진 상품 = 다른 상품과 차이가 많이 나는 상품)
"""
import os, json, math
import pandas as pd
import polars as pl
import networkx as nx
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

# ── 경로 ──────────────────────────────────────────────────────
BASE_DIR = os.path.abspath(os.path.join(os.getcwd(), '..'))
PROC     = os.path.join(BASE_DIR, 'data', 'processed')
EDA_DIR  = os.path.join(BASE_DIR, 'eda')
OUT_DIR  = os.path.join(EDA_DIR, 'network_html')
os.makedirs(OUT_DIR, exist_ok=True)

# ── 데이터 로드 ────────────────────────────────────────────────
prod       = pl.read_parquet(os.path.join(PROC, 'product_master_dataset.parquet'))
edges_all  = pl.read_parquet(os.path.join(PROC, 'final_edgelist_with_trend.parquet'))
cluster_df = pd.read_excel(os.path.join(EDA_DIR, '00_NPD_lifecycle_clusters.xlsx'))

cluster_map  = {str(int(r['상품코드'])).zfill(6): int(r['cluster'])
                for _, r in cluster_df.iterrows()}
code_to_name = dict(zip(prod['ITEM_CD'].to_list(), prod['ITEM_NM'].to_list()))

# ══════════════════════════════════════════════════════════════
# 색상 정의
# ══════════════════════════════════════════════════════════════
CLUSTER_COLOR = {0: '#FCE378', 1: '#FF8C42', 2: '#EF4444'}
PROD_DEFAULT  = '#0D1F2D'   # 앵커 외 상품 — 다크네이비
ATTR_COLOR    = '#10B981'   # 속성 — 그린

# ── 앵커 상품 6개 (클러스터 색상 부여) ───────────────────────
ANCHOR_CODES = [
    '127814',  # PB)쪼코쪼코초코우유500ml      클러스터 0
    '125843',  # 이스타)도쿠시마라면큰컵        클러스터 0
    '125040',  # PB)이봉원의봉새우볶음밥260g    클러스터 1
    '125394',  # KBO)끝내기홈런미트부리또130g   클러스터 1
    '125340',  # KBO)롯데자이언츠거인단팥빵100g 클러스터 2
    '123788',  # PB)직화불막창180g              클러스터 2
]
anchor_set = set(ANCHOR_CODES)


def prod_color(code):
    if code in anchor_set:
        cl = cluster_map.get(code)
        return CLUSTER_COLOR.get(cl, PROD_DEFAULT)
    return PROD_DEFAULT


# ── 상품 200개 선발: 속성 IDF 차별성 점수 기반 ───────────────
all_codes = prod['ITEM_CD'].to_list()
N_ALL     = len(all_codes)
N_PROD    = 200

pa_idf_base = (
    edges_all.filter(pl.col('edge_type') == '제품-일반속성')
             .to_pandas()
)
attr_doc_cnt = pa_idf_base.groupby('dst_node')['src_node'].nunique()
idf_map      = {attr: math.log(N_ALL / cnt) for attr, cnt in attr_doc_cnt.items()}

prod_attr_sets = pa_idf_base.groupby('src_node')['dst_node'].apply(set).to_dict()
all_code_set   = set(all_codes)
distinct_score = {
    code: sum(idf_map.get(a, 0) for a in attrs)
    for code, attrs in prod_attr_sets.items()
    if code in all_code_set
}

# 앵커 우선 포함, 나머지는 차별성 점수 내림차순
ranked = sorted(distinct_score.items(), key=lambda x: -x[1])
sample_200 = list(ANCHOR_CODES)
for code, score in ranked:
    if len(sample_200) >= N_PROD:
        break
    if code not in anchor_set:
        sample_200.append(code)
if len(sample_200) < N_PROD:
    print(f'⚠ 선발 상품 {len(sample_200)}개 (목표 {N_PROD}개에 미달)')
sample_set = set(sample_200)
print(f'차별성 기반 상품 선발 완료: {len(sample_200)}개')

# ── 엣지 필터링 ───────────────────────────────────────────────
pa_all = (
    edges_all.filter(pl.col('edge_type') == '제품-일반속성')
             .filter(pl.col('src_node').is_in(sample_set))
             .to_pandas()
)

# 속성 2:1 비율 (연결 상품 수 기준 상위 400개)
attr_degree = (pa_all.groupby('dst_node')['src_node']
               .nunique().reset_index()
               .rename(columns={'src_node': 'prod_count'})
               .sort_values('prod_count', ascending=False))
N_ATTR    = 2 * N_PROD
top_attrs = set(attr_degree.head(N_ATTR)['dst_node'].tolist())
pa_edges  = pa_all[pa_all['dst_node'].isin(top_attrs)]

pp_edges = (
    edges_all.filter(pl.col('edge_type') == '제품-제품')
             .filter(pl.col('src_node').is_in(sample_set) &
                     pl.col('dst_node').is_in(sample_set))
             .sort('weight', descending=True)
             .head(3000)
             .to_pandas()
)

print(f'상품: {len(sample_200)}개  속성: {len(top_attrs)}개  비율 {len(top_attrs)/len(sample_200):.1f}:1')
print(f'제품-속성 엣지: {len(pa_edges):,}  제품-제품 엣지: {len(pp_edges):,}')

# ══════════════════════════════════════════════════════════════
# NetworkX 그래프 + 중심성 계산
# ══════════════════════════════════════════════════════════════
G = nx.Graph()
for c in sample_200:
    G.add_node(c, node_type='product')
for a in top_attrs:
    G.add_node(a, node_type='attr')
for _, row in pa_edges.iterrows():
    G.add_edge(row['src_node'], row['dst_node'], weight=1.0)
for _, row in pp_edges.iterrows():
    G.add_edge(row['src_node'], row['dst_node'], weight=float(row['weight']))

print(f'NetworkX 그래프 — 노드: {G.number_of_nodes():,}  엣지: {G.number_of_edges():,}')
print('중심성 계산 중...')

deg_cent = nx.degree_centrality(G)
btw_cent = nx.betweenness_centrality(G, normalized=True, weight='weight')
try:
    eig_cent = nx.eigenvector_centrality(G, max_iter=500, weight='weight')
    print('  ✓ Degree / Betweenness / Eigenvector')
except nx.PowerIterationFailedConvergence:
    eig_cent = deg_cent.copy()
    print('  ✓ Degree / Betweenness  ⚠ Eigenvector→Degree 대체')


def normalize(d):
    vmin, vmax = min(d.values()), max(d.values())
    span = vmax - vmin + 1e-9
    return {k: (v - vmin) / span for k, v in d.items()}


deg_n = normalize(deg_cent)
btw_n = normalize(btw_cent)
eig_n = normalize(eig_cent)
hub_score = {n: (deg_n[n] + btw_n[n] + eig_n[n]) / 3 for n in G.nodes()}

# 노드 크기
PROD_SZ_MIN, PROD_SZ_MAX = 10, 45
ATTR_SZ_MIN, ATTR_SZ_MAX = 12, 60
attr_cnt_map = attr_degree.set_index('dst_node')['prod_count'].to_dict()
attr_max     = attr_degree['prod_count'].max()


def prod_size(code):
    return PROD_SZ_MIN + hub_score.get(code, 0) * (PROD_SZ_MAX - PROD_SZ_MIN)


def attr_size(attr):
    return ATTR_SZ_MIN + (attr_cnt_map.get(attr, 1) / attr_max) * (ATTR_SZ_MAX - ATTR_SZ_MIN)


# spring_layout (공유 좌표)
print('layout 계산 중...')
pos = nx.spring_layout(G, seed=42, k=2.5, iterations=80)
SCALE = 1500
coord = {n: (pos[n][0] * SCALE, pos[n][1] * SCALE) for n in G.nodes()}

# ══════════════════════════════════════════════════════════════
# [출력 1] 정적 PNG — matplotlib
# ══════════════════════════════════════════════════════════════
print('\nPNG 생성 중...')

plt.rcParams['font.family'] = 'Malgun Gothic'
plt.rcParams['axes.unicode_minus'] = False

fig, ax = plt.subplots(figsize=(20, 16))
fig.patch.set_alpha(0)
ax.set_facecolor('none')

nx_pos = pos

edge_prod_attr = [(row['src_node'], row['dst_node']) for _, row in pa_edges.iterrows()]
edge_prod_prod = [(row['src_node'], row['dst_node']) for _, row in pp_edges.iterrows()]

nx.draw_networkx_edges(G, nx_pos, edgelist=edge_prod_attr,
                       edge_color='#c8d6e5', width=0.4, alpha=0.5, ax=ax)
nx.draw_networkx_edges(G, nx_pos, edgelist=edge_prod_prod,
                       edge_color='#aaaaaa', width=0.6, alpha=0.35, ax=ax)

non_anchor = [c for c in sample_200 if c not in anchor_set]
non_anchor_sizes = [prod_size(c) * 8 for c in non_anchor]
nx.draw_networkx_nodes(G, nx_pos, nodelist=non_anchor,
                       node_color=PROD_DEFAULT, node_size=non_anchor_sizes,
                       alpha=0.75, ax=ax)

attr_list  = list(top_attrs)
attr_sizes = [attr_size(a) * 10 for a in attr_list]
nx.draw_networkx_nodes(G, nx_pos, nodelist=attr_list,
                       node_color=ATTR_COLOR, node_size=attr_sizes,
                       alpha=0.8, ax=ax)

top_attrs_label = attr_degree.head(30)['dst_node'].tolist()
attr_label_map = {a: a for a in top_attrs_label if a in G.nodes()}
nx.draw_networkx_labels(G, nx_pos, labels=attr_label_map,
                        font_size=6, font_color='white',
                        font_family='Malgun Gothic', ax=ax)

for code in ANCHOR_CODES:
    cl  = cluster_map.get(code)
    col = CLUSTER_COLOR.get(cl, PROD_DEFAULT)
    sz  = prod_size(code) * 25
    nx.draw_networkx_nodes(G, nx_pos, nodelist=[code],
                           node_color=col, node_size=sz,
                           edgecolors='white', linewidths=2.5, ax=ax)
    nm = code_to_name.get(code, code)
    nx.draw_networkx_labels(G, nx_pos, labels={code: nm},
                            font_size=7, font_color='black',
                            font_weight='bold', font_family='Malgun Gothic', ax=ax)

legend_patches = [
    mpatches.Patch(color='#FCE378', label='클러스터 0 (균일수요)'),
    mpatches.Patch(color='#FF8C42', label='클러스터 1'),
    mpatches.Patch(color='#EF4444', label='클러스터 2 (단기흥행)'),
    mpatches.Patch(color='#0D1F2D', label='기타 상품'),
    mpatches.Patch(color='#10B981', label='속성 키워드'),
]
ax.legend(handles=legend_patches, loc='upper left', fontsize=11,
          framealpha=0.9, edgecolor='#e2e8f0')
ax.set_title('7-Eleven NPD HIN 네트워크 — 200개 상품 샘플\n(노드 크기: 중심성 허브점수 / 상품:속성 = 1:2)',
             fontsize=15, fontweight='bold', pad=15)
ax.axis('off')
plt.tight_layout()

png_path = os.path.join(OUT_DIR, 'sample_network_200.png')
plt.savefig(png_path, dpi=180, bbox_inches='tight', transparent=True)
plt.close()
print(f'✅ PNG: {png_path}')

# ══════════════════════════════════════════════════════════════
# [출력 2] 인터랙티브 HTML
# ══════════════════════════════════════════════════════════════
print('HTML 생성 중...')

vis_nodes = []
vis_edges = []

for code in sample_200:
    bg        = prod_color(code)
    is_anchor = code in anchor_set
    name      = code_to_name.get(code, code)
    cl        = cluster_map.get(code)
    cl_label  = f'클러스터 {cl}' if cl is not None else '비NPD'
    x, y      = coord.get(code, (0, 0))

    vis_nodes.append({
        'id':          code,
        'nodeType':    'product',
        'label':       '',
        'hiddenLabel': name[:10] + '…' if len(name) > 10 else name,
        'shape':       'dot',
        'size':        round(prod_size(code), 1),
        'color': {
            'background': bg,
            'border':     '#ffffff' if is_anchor else bg,
            'highlight':  {'background': bg, 'border': '#ffffff'},
            'hover':      {'background': bg, 'border': '#ffffff'},
        },
        'font':        {'color': '#f8fafc', 'size': 10,
                        'face': 'Malgun Gothic, Apple SD Gothic Neo, sans-serif'},
        'borderWidth': 2.5 if is_anchor else 1,
        'shadow':      is_anchor,
        'physics':     False,
        'x':           round(x, 2),
        'y':           round(y, 2),
        'title': (
            f'<span style="font-family:\'Malgun Gothic\',sans-serif;">'
            f'🛍️ <b>{name}</b><br>코드: {code}<br>{cl_label}'
            + (' ★앵커' if is_anchor else '') +
            f'<br>허브점수: {hub_score.get(code,0):.3f}'
            f'</span>'
        ),
    })

for attr in top_attrs:
    x, y = coord.get(attr, (0, 0))
    cnt  = attr_cnt_map.get(attr, 1)
    vis_nodes.append({
        'id':          attr,
        'nodeType':    'attr',
        'label':       '',
        'hiddenLabel': attr,
        'shape':       'dot',
        'size':        round(attr_size(attr), 1),
        'color': {
            'background': ATTR_COLOR,
            'border':     '#059669',
            'highlight':  {'background': '#34D399', 'border': '#ffffff'},
            'hover':      {'background': '#34D399', 'border': '#ffffff'},
        },
        'font':        {'color': '#f0f4f8', 'size': 10,
                        'face': 'Malgun Gothic, Apple SD Gothic Neo, sans-serif'},
        'borderWidth': 1,
        'shadow':      True,
        'physics':     False,
        'x':           round(x, 2),
        'y':           round(y, 2),
        'title': (
            f'<span style="font-family:\'Malgun Gothic\',sans-serif;">'
            f'🔑 속성: <b>{attr}</b><br>연결 상품 수: {cnt}개</span>'
        ),
    })

eid = 0
for _, row in pa_edges.iterrows():
    vis_edges.append({'id': eid, 'from': row['src_node'], 'to': row['dst_node'],
                      'color': {'color': '#c8d6e5'}, 'width': 1})
    eid += 1

if len(pp_edges) > 0:
    w_min, w_max = pp_edges['weight'].min(), pp_edges['weight'].max()
    for _, row in pp_edges.iterrows():
        w = (row['weight'] - w_min) / (w_max - w_min + 1e-9) * 3 + 0.5
        vis_edges.append({'id': eid, 'from': row['src_node'], 'to': row['dst_node'],
                          'color': {'color': '#aaaaaa'}, 'width': round(float(w), 2)})
        eid += 1

nodes_json = json.dumps(vis_nodes, ensure_ascii=False)
edges_json = json.dumps(vis_edges, ensure_ascii=False)

js_highlight = f"""
<script>
var activeNode = null;
var origNodeColor = {{}};
var origEdgeColor = {{}};
var origEdgeWidth = {{}};

nodes.forEach(function(n) {{
  origNodeColor[n.id] = JSON.parse(JSON.stringify(n.color));
}});
edges.forEach(function(e) {{
  origEdgeColor[e.id] = JSON.parse(JSON.stringify(e.color));
  origEdgeWidth[e.id] = e.width || 1;
}});

network.on('hoverNode', function(params) {{
  var n = nodes.get(params.node);
  if (n && n.hiddenLabel) {{
    nodes.update({{id: params.node, label: n.hiddenLabel}});
  }}
}});

network.on('blurNode', function(params) {{
  nodes.update({{id: params.node, label: ''}});
}});

network.on('click', function(params) {{
  if (params.nodes.length === 0) {{
    restoreAll();
    activeNode = null;
    return;
  }}
  var nid = params.nodes[0];
  if (activeNode === nid) {{
    restoreAll();
    activeNode = null;
    return;
  }}
  activeNode = nid;
  applyHighlight(nid);
}});

function applyHighlight(nodeId) {{
  var neighbors = new Set(network.getConnectedNodes(nodeId));
  neighbors.add(nodeId);
  var connectedEdges = new Set(network.getConnectedEdges(nodeId));

  var nodeUpd = [];
  nodes.forEach(function(n) {{
    if (neighbors.has(n.id)) {{
      nodeUpd.push({{id: n.id, color: JSON.parse(JSON.stringify(origNodeColor[n.id])), opacity: 1.0, borderWidth: 3}});
    }} else {{
      nodeUpd.push({{
        id: n.id,
        color: {{background:'#e8ecef', border:'#ced4da',
                highlight:{{background:'#e8ecef',border:'#ced4da'}},
                hover:{{background:'#e8ecef',border:'#ced4da'}}}},
        opacity: 0.2
      }});
    }}
  }});
  nodes.update(nodeUpd);

  var edgeUpd = [];
  edges.forEach(function(e) {{
    if (connectedEdges.has(e.id)) {{
      edgeUpd.push({{id: e.id, color:{{color:'#F97316'}}, width: 2.5}});
    }} else {{
      edgeUpd.push({{id: e.id, color:{{color:'#eeeeee'}}, width: 0.3}});
    }}
  }});
  edges.update(edgeUpd);
}}

function restoreAll() {{
  var nodeUpd = [];
  nodes.forEach(function(n) {{
    nodeUpd.push({{id: n.id, color: JSON.parse(JSON.stringify(origNodeColor[n.id])), opacity: 1.0}});
  }});
  nodes.update(nodeUpd);
  var edgeUpd = [];
  edges.forEach(function(e) {{
    edgeUpd.push({{id: e.id, color: JSON.parse(JSON.stringify(origEdgeColor[e.id])),
                  width: origEdgeWidth[e.id]}});
  }});
  edges.update(edgeUpd);
}}
</script>
"""

html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>200개 샘플 HIN — 중심성 허브 · 클릭 1-hop 강조</title>
<style>
  body {{ margin: 0; font-family: 'Malgun Gothic', 'Apple SD Gothic Neo', 'Nanum Gothic', sans-serif; background: #ffffff; }}
  #network {{ width: 100%; height: 100vh; border: none; }}
  .vis-tooltip {{
    font-family: 'Malgun Gothic', sans-serif !important;
    font-size: 13px; padding: 8px 12px; border-radius: 6px;
    max-width: 320px; background: rgba(255,255,255,0.97);
    border: 1px solid #e2e8f0; color: #1e293b;
    box-shadow: 0 4px 12px rgba(0,0,0,0.15);
  }}
</style>
<script src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
</head>
<body>
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
      nodes:   {{borderWidth: 2, shadow: true}},
      edges:   {{color: {{color: "#aaaaaa"}}, smooth: {{type: "continuous"}}}},
      physics: {{enabled: false}},
      interaction: {{
        dragNodes: true, dragView: true, zoomView: true,
        hover: true, tooltipDelay: 80,
        navigationButtons: true, keyboard: true
      }}
    }}
  );
</script>
{js_highlight}
</body>
</html>"""

html_path = os.path.join(OUT_DIR, 'sample_network_200.html')
with open(html_path, 'w', encoding='utf-8') as f:
    f.write(html)
print(f'✅ HTML: {html_path}')
print(f'\n노드 {len(vis_nodes)}개  엣지 {len(vis_edges)}개')
print(f'  앵커 6개 (클러스터 색상) + 비앵커 {len(sample_200)-6}개 (다크네이비) + 속성 {len(top_attrs)}개')
