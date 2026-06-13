import os, json, math
import pandas as pd
import polars as pl
import networkx as nx
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

# Paths
BASE_DIR = os.getcwd()
DATA_DIR = os.path.join(BASE_DIR, '대시보드_중간', 'data')
OUT_DIR = os.path.join(BASE_DIR, 'eda', 'network_html')
os.makedirs(OUT_DIR, exist_ok=True)

print(f"BASE_DIR: {BASE_DIR}")
print(f"DATA_DIR: {DATA_DIR}")

# Load Data
try:
    prod = pl.read_parquet(os.path.join(DATA_DIR, 'final_product_keywords.parquet'))
    edges_all = pl.read_parquet(os.path.join(DATA_DIR, 'final_edgelist_with_trend.parquet'))
except Exception as e:
    print(f"Error loading data: {e}")
    exit(1)

# Find Product Code and Name columns (Handling Korean encoding issues in some environments)
# We use indexing if name matching fails
code_col = prod.columns[0] # Assuming first is code
name_col = prod.columns[1] # Assuming second is name
for c in prod.columns:
    if '코드' in c or 'CD' in c: code_col = c
    if '명' in c or 'NM' in c: name_col = c

code_to_name = dict(zip(prod[code_col].to_list(), prod[name_col].to_list()))
print(f"Code column: {code_col}, Name column: {name_col}")

# Identify edge types (Product-Attribute, Product-Product, IP-Attribute, etc.)
# We use the actual column names from the data
# src_type, dst_type columns are useful if edge_type string is hard to match
pa_edges = edges_all.filter((pl.col('src_type') == 'product') & (pl.col('dst_type') == 'attribute'))
if pa_edges.height == 0:
    # Fallback to string matching if types are not set correctly
    pa_edges = edges_all.filter(pl.col('edge_type').str.contains('속성') | pl.col('edge_type').str.contains('attribute'))

print(f"Found {pa_edges.height} Product-Attribute edges")

# Select 200 products by degree
top_200_codes = (pa_edges.group_by('src_node').len()
                 .sort('len', descending=True)
                 .head(200)['src_node'].to_list())

sample_set = set(top_200_codes)

# Filter edges for the sample
pa_sample = pa_edges.filter(pl.col('src_node').is_in(sample_set))

# Limit attributes (Top 200 by connectivity to these products)
top_attrs = (pa_sample.group_by('dst_node').len()
             .sort('len', descending=True)
             .head(200)['dst_node'].to_list())
top_attr_set = set(top_attrs)
pa_sample = pa_sample.filter(pl.col('dst_node').is_in(top_attr_set))

# Product-Product edges
pp_edges = edges_all.filter((pl.col('src_type') == 'product') & (pl.col('dst_type') == 'product'))
if pp_edges.height == 0:
    pp_edges = edges_all.filter(pl.col('edge_type').str.contains('제품-제품') | pl.col('edge_type').str.contains('product-product'))
pp_sample = pp_edges.filter(pl.col('src_node').is_in(sample_set) & pl.col('dst_node').is_in(sample_set))

# Build NetworkX
G = nx.Graph()
for c in top_200_codes:
    G.add_node(c, type='product')
for a in top_attrs:
    G.add_node(a, type='attribute')

for row in pa_sample.to_dicts():
    G.add_edge(row['src_node'], row['dst_node'], weight=1.0)
for row in pp_sample.to_dicts():
    # Weight might be string or float
    try:
        w = float(row.get('weight', 1.0))
    except:
        w = 1.0
    G.add_edge(row['src_node'], row['dst_node'], weight=w)

print(f"Graph built: Nodes={G.number_of_nodes()}, Edges={G.number_of_edges()}")

# Save as HTML (vis.js)
vis_nodes = []
for n, d in G.nodes(data=True):
    is_prod = d.get('type') == 'product'
    name = code_to_name.get(n, n)
    vis_nodes.append({
        'id': n,
        'label': '', # Hide label by default to avoid clutter
        'title': name if is_prod else n, # Tooltip
        'color': '#3B82F6' if is_prod else '#10B981',
        'size': 15 if is_prod else 7,
        'shape': 'dot'
    })

vis_edges = []
for u, v, d in G.edges(data=True):
    vis_edges.append({'from': u, 'to': v, 'width': 1})

html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>7-Eleven HIN Sample (200 Products)</title>
    <script type="text/javascript" src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
    <style>
        body {{ margin: 0; padding: 0; }}
        #network {{ width: 100vw; height: 100vh; background-color: #f8f9fa; }}
        #info {{ position: absolute; top: 10px; left: 10px; z-index: 10; background: white; padding: 10px; border-radius: 5px; box-shadow: 0 0 10px rgba(0,0,0,0.1); font-family: sans-serif; }}
    </style>
</head>
<body>
    <div id="info">
        <b>7-Eleven HIN Network Sample</b><br>
        Blue: Products | Green: Attributes<br>
        Hover for names, Scroll to zoom
    </div>
    <div id="network"></div>
    <script type="text/javascript">
        var nodes = new vis.DataSet({json.dumps(vis_nodes, ensure_ascii=False)});
        var edges = new vis.DataSet({json.dumps(vis_edges, ensure_ascii=False)});
        var container = document.getElementById('network');
        var data = {{ nodes: nodes, edges: edges }};
        var options = {{
            nodes: {{
                font: {{ size: 12 }}
            }},
            edges: {{
                color: {{ color: '#cccccc', opacity: 0.4 }},
                smooth: {{ type: 'continuous' }}
            }},
            physics: {{
                enabled: true,
                solver: 'forceAtlas2Based',
                forceAtlas2Based: {{
                    gravitationalConstant: -50,
                    centralGravity: 0.01,
                    springLength: 100,
                    springConstant: 0.08
                }},
                stabilization: {{ iterations: 100 }}
            }},
            interaction: {{
                hover: true,
                tooltipDelay: 100
            }}
        }};
        var network = new vis.Network(container, data, options);
    </script>
</body>
</html>
"""

output_path = os.path.join(OUT_DIR, 'fast_sample_network.html')
with open(output_path, 'w', encoding='utf-8') as f:
    f.write(html)

print(f"Success! Visualization saved to: {output_path}")
