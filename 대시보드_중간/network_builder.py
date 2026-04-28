"""
network_builder.py
───────────────────────────────────────────────────────────────────────
NetworkX 그래프를 구축하는 모듈.

두 가지 모드를 지원:
  [모드 A] Parquet 기반 빌드  (data/product_master_dataset.parquet + ip_master_dataset.parquet)
  [모드 B] 사전 구축 네트워크 로드 (data/network.json)  ← 실제 네트워크 데이터 수령 시 사용

실행 시 data/network.json이 존재하면 [모드 B], 없으면 [모드 A]로 자동 전환.

엣지 타입 및 가중치:
  - 상품 ↔ 속성  : 속성이 상품에 등장한 빈도 (트렌드_속성_빈도 dict 값 / 일반_속성 = 1)
  - 상품 ↔ IP    : IP가 해당 상품을 언급한 게시글 수 (post_dates 길이)
  - 상품 ↔ 행사  : 1 (이진 연결)
  - 상품 ↔ 상품  : 공유 속성(일반+트렌드) 교집합 크기 (임계값 이상만 생성)
  - IP   ↔ 속성  : 속성이 IP 게시글에 등장한 빈도 (트렌드_속성_빈도 dict 값)
"""

import json
import os
import pandas as pd
import networkx as nx
from typing import Optional

# ── 설정 ───────────────────────────────────────────────────────────────
DATA_DIR            = os.path.join(os.path.dirname(__file__), "data")
NETWORK_JSON_PATH   = os.path.join(DATA_DIR, "network.json")
PRODUCT_PARQUET     = os.path.join(DATA_DIR, "product_master_dataset.parquet")
IP_PARQUET          = os.path.join(DATA_DIR, "ip_master_dataset.parquet")

# 상품-상품 엣지: 공유 속성 수가 이 값 이상일 때만 생성 (엣지 폭발 방지)
PROD_PROD_THRESHOLD = 2


# ═══════════════════════════════════════════════════════════════════════
# 공개 API
# ═══════════════════════════════════════════════════════════════════════

def load_graph() -> nx.Graph:
    """
    그래프를 로드하여 반환.
    data/network.json 존재 → 모드 B (사전 구축 네트워크 로드)
    없으면              → 모드 A (parquet으로부터 빌드)
    """
    if os.path.exists(NETWORK_JSON_PATH):
        print(f"[network_builder] 모드 B: {NETWORK_JSON_PATH} 로드")
        return _load_from_json(NETWORK_JSON_PATH)
    else:
        print("[network_builder] 모드 A: parquet으로부터 그래프 빌드")
        return _build_from_parquet(PRODUCT_PARQUET, IP_PARQUET)


def get_all_attributes(G: nx.Graph) -> list[str]:
    """그래프에서 type=attribute인 노드의 label 목록 반환 (LLM 호출용)."""
    return [
        G.nodes[n]['label']
        for n in G.nodes
        if G.nodes[n].get('type') == 'attribute'
    ]


def get_meta(G: nx.Graph) -> dict:
    """노드/엣지 수, 타입별 분포 반환."""
    type_counts = {}
    for n in G.nodes:
        t = G.nodes[n].get('type', 'unknown')
        type_counts[t] = type_counts.get(t, 0) + 1
    return {
        "total_nodes": G.number_of_nodes(),
        "total_edges": G.number_of_edges(),
        "node_types": type_counts,
    }


def inject_trend_node(G: nx.Graph, trend_text: str, inferred_attrs: list[dict]) -> str:
    """
    LLM이 추론한 속성을 트렌드 노드로 임시 주입.
    inferred_attrs: [{"attribute": str, "score": float}, ...]
    Returns: 생성된 trend_node_id
    """
    trend_id = f"trend_{trend_text}"
    G.add_node(trend_id, label=trend_text, type='trend')

    for item in inferred_attrs:
        attr_id = f"attr_{item['attribute']}"
        if G.has_node(attr_id):
            weight = round(item.get('score', 1.0), 4)
            _add_or_update_edge(G, trend_id, attr_id, weight)
        else:
            # 네트워크에 없는 속성이면 새 속성 노드도 추가
            G.add_node(attr_id, label=item['attribute'], type='attribute')
            _add_or_update_edge(G, trend_id, attr_id, round(item.get('score', 1.0), 4))

    print(f"[network_builder] 트렌드 노드 주입: '{trend_text}' → {len(inferred_attrs)}개 속성 연결")
    return trend_id


def check_trend_node(G: nx.Graph, trend_text: str) -> Optional[str]:
    """
    트렌드 텍스트와 일치하는 노드 id 반환.
    없으면 None.
    label 완전 일치 → type=trend or attribute 우선 탐색.
    """
    for node_id, data in G.nodes(data=True):
        if data.get('label') == trend_text and data.get('type') in ('trend', 'attribute'):
            return node_id
    return None


# ═══════════════════════════════════════════════════════════════════════
# 모드 B: network.json 로드
# ═══════════════════════════════════════════════════════════════════════

def _load_from_json(path: str) -> nx.Graph:
    """
    network.json 스키마:
    {
      "nodes": [{"id": str, "label": str, "type": str, ...}],
      "edges": [{"source": str, "target": str, "weight": float, "edge_type": str}]
    }
    """
    with open(path, encoding='utf-8') as f:
        data = json.load(f)

    G = nx.Graph()

    for node in data.get('nodes', []):
        node_id = node.pop('id')
        G.add_node(node_id, **node)

    for edge in data.get('edges', []):
        src = edge['source']
        tgt = edge['target']
        w   = edge.get('weight', 1.0)
        et  = edge.get('edge_type', '')
        if G.has_node(src) and G.has_node(tgt):
            _add_or_update_edge(G, src, tgt, w, edge_type=et)
        else:
            print(f"  [경고] 엣지 건너뜀: {src} → {tgt} (노드 미존재)")

    print(f"  로드 완료: 노드 {G.number_of_nodes()}, 엣지 {G.number_of_edges()}")
    return G


# ═══════════════════════════════════════════════════════════════════════
# 모드 A: parquet → 그래프 빌드
# ═══════════════════════════════════════════════════════════════════════

def _build_from_parquet(product_path: str, ip_path: str) -> nx.Graph:
    prod_df = pd.read_parquet(product_path)
    ip_df   = pd.read_parquet(ip_path)

    G = nx.Graph()
    _add_nodes(G, prod_df, ip_df)
    _add_edges(G, prod_df, ip_df)

    print(f"  빌드 완료: 노드 {G.number_of_nodes()}, 엣지 {G.number_of_edges()}")
    return G, prod_df, ip_df   # df도 반환해 서버에서 재활용 가능


def _add_nodes(G: nx.Graph, prod_df: pd.DataFrame, ip_df: pd.DataFrame):
    """4종 노드 추가."""

    # 상품 노드
    for _, row in prod_df.iterrows():
        G.add_node(
            str(row['ITEM_CD']),
            label    = row['ITEM_NM'],
            type     = 'product',
            category = _first(row.get('categories')),
            price    = row.get('p_price'),
            survived = bool(row.get('is_survived', True)),
        )

    # 속성 노드 (일반 + 트렌드 통합, 중복 제거)
    all_attrs: set[str] = set()
    for _, row in prod_df.iterrows():
        all_attrs.update(_safe_list(row.get('일반_속성')))
        all_attrs.update(_safe_list(row.get('트렌드_속성')))
    for ip_row in ip_df.itertuples():
        all_attrs.update(_safe_list(ip_row.트렌드_속성))
    for attr in all_attrs:
        G.add_node(f"attr_{attr}", label=attr, type='attribute')

    # IP 노드
    for _, row in ip_df.iterrows():
        G.add_node(f"ip_{row['ip_name']}", label=row['ip_name'], type='ip')

    # 행사 노드
    if 'event_name' in prod_df.columns:
        for ev in prod_df['event_name'].dropna().unique():
            G.add_node(f"event_{ev}", label=ev, type='event')


def _add_edges(G: nx.Graph, prod_df: pd.DataFrame, ip_df: pd.DataFrame):
    """5종 엣지 추가."""

    # ip_name → post_dates 길이 lookup (IP-상품 가중치용)
    ip_post_count = {
        row['ip_name']: len(_safe_list(row.get('post_dates')))
        for _, row in ip_df.iterrows()
    }

    for _, row in prod_df.iterrows():
        item_id = str(row['ITEM_CD'])

        # ── 1. 상품 ↔ 일반_속성 (weight = 1)
        for attr in _safe_list(row.get('일반_속성')):
            _add_or_update_edge(G, item_id, f"attr_{attr}", 1, edge_type='prod_attr')

        # ── 2. 상품 ↔ 트렌드_속성 (weight = 빈도)
        freq_dict = row.get('트렌드_속성_빈도') or {}
        if isinstance(freq_dict, str):
            import ast
            try:
                freq_dict = ast.literal_eval(freq_dict)
            except Exception:
                freq_dict = {}
        for attr in _safe_list(row.get('트렌드_속성')):
            w = freq_dict.get(attr, 1)
            _add_or_update_edge(G, item_id, f"attr_{attr}", w, edge_type='prod_trend_attr')

        # ── 3. 상품 ↔ IP (weight = 게시글 수)
        for ip in _safe_list(row.get('linked_ips')):
            w = ip_post_count.get(ip, 1)
            _add_or_update_edge(G, item_id, f"ip_{ip}", w, edge_type='prod_ip')

        # ── 4. 상품 ↔ 행사 (weight = 1)
        ev = row.get('event_name')
        if pd.notna(ev) and ev:
            _add_or_update_edge(G, item_id, f"event_{ev}", 1, edge_type='prod_event')

    # ── 5. IP ↔ 속성 (weight = 빈도)
    for _, row in ip_df.iterrows():
        ip_id    = f"ip_{row['ip_name']}"
        freq_dict = row.get('트렌드_속성_빈도') or {}
        if isinstance(freq_dict, str):
            import ast
            try:
                freq_dict = ast.literal_eval(freq_dict)
            except Exception:
                freq_dict = {}
        for attr in _safe_list(row.get('트렌드_속성')):
            w = freq_dict.get(attr, 1)
            _add_or_update_edge(G, ip_id, f"attr_{attr}", w, edge_type='ip_attr')

    # ── 6. 상품 ↔ 상품 (weight = 공유 속성 수, 카테고리 내, 임계값 이상만)
    _add_prod_prod_edges(G, prod_df)


def _add_prod_prod_edges(G: nx.Graph, prod_df: pd.DataFrame):
    """카테고리 내 상품-상품 엣지 (공유 속성 임계값 이상)."""
    cat_col = 'categories'
    # categories가 리스트면 첫 번째 항목 기준으로 그룹화
    prod_df = prod_df.copy()
    prod_df['_cat'] = prod_df[cat_col].apply(_first)

    for cat, group in prod_df.groupby('_cat'):
        items = group.to_dict('records')
        for i in range(len(items)):
            set_i = set(_safe_list(items[i].get('일반_속성')) +
                        _safe_list(items[i].get('트렌드_속성')))
            for j in range(i + 1, len(items)):
                set_j = set(_safe_list(items[j].get('일반_속성')) +
                            _safe_list(items[j].get('트렌드_속성')))
                shared = len(set_i & set_j)
                if shared >= PROD_PROD_THRESHOLD:
                    _add_or_update_edge(
                        G,
                        str(items[i]['ITEM_CD']),
                        str(items[j]['ITEM_CD']),
                        shared,
                        edge_type='prod_prod'
                    )


# ═══════════════════════════════════════════════════════════════════════
# 내부 유틸
# ═══════════════════════════════════════════════════════════════════════

def _add_or_update_edge(G: nx.Graph, u: str, v: str, weight: float, edge_type: str = ''):
    """엣지가 이미 있으면 weight 누적, 없으면 신규 생성."""
    if not G.has_node(u) or not G.has_node(v):
        return
    if G.has_edge(u, v):
        G[u][v]['weight'] += weight
    else:
        G.add_edge(u, v, weight=weight, edge_type=edge_type)


def _safe_list(val) -> list:
    """NaN, None, 문자열 모두 안전하게 list로 변환."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return []
    if isinstance(val, list):
        return val
    try:
        import ast
        parsed = ast.literal_eval(val)
        return parsed if isinstance(parsed, list) else [parsed]
    except Exception:
        return [v.strip() for v in str(val).split(',') if v.strip()]


def _first(val) -> str:
    """리스트면 첫 번째 항목, 아니면 문자열 그대로."""
    lst = _safe_list(val)
    return lst[0] if lst else (str(val) if val else '')


# ── 단독 실행 시 그래프 빌드 후 메타 출력 ──────────────────────────────
if __name__ == '__main__':
    result = load_graph()
    G = result[0] if isinstance(result, tuple) else result
    meta = get_meta(G)
    print("\n[그래프 메타]")
    print(f"  전체 노드: {meta['total_nodes']}")
    print(f"  전체 엣지: {meta['total_edges']}")
    print(f"  노드 타입별: {meta['node_types']}")
