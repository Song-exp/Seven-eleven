"""
graph_engine.py
───────────────────────────────────────────────────────────────────────
가중치 우선 BFS 기반 신제품 속성 조합 탐색 엔진.

트렌드 노드에서 출발하여 최대 3홉 이내에서
누적 가중치 합이 가장 높은 속성 조합을 탐색한다.
"""

import heapq
import networkx as nx


def propose_by_trend(
    G: nx.Graph,
    trend_node_id: str,
    category: str,
    item_cd: str | None = None,
    pinned_attrs: list[str] | None = None,
    pinned_ips:   list[str] | None = None,
    hop_limit: int = 3,
    top_k: int = 3,
) -> list[dict]:
    """
    트렌드 노드에서 출발하는 가중치 우선 BFS.

    Args:
        G:              NetworkX 그래프
        trend_node_id:  탐색 시작 트렌드 노드 id (예: "trend_뇨끼 열풍")
        category:       필터할 상품 카테고리 (예: "간식빵")
        hop_limit:      최대 홉 수 (기본 3)
        top_k:          반환할 상위 조합 수

    Returns:
        [
          {
            "rank": 1,
            "attrs": ["쫀득한 식감", "고소함", "카페 TPO"],
            "total_weight": 2.34,
            "path_labels": ["뇨끼 열풍", "쫀득한 식감", "소금빵", "고소함"],
            "via_product": "소금빵",
            "hops": 3,
          },
          ...
        ]
    """
    if not G.has_node(trend_node_id):
        return []

    pinned_attrs = pinned_attrs or []
    pinned_ips   = pinned_ips   or []
    pinned_ip_ids = {f"ip_{ip}" for ip in pinned_ips}

    results: list[dict] = []

    # 우선순위 큐: (-누적가중치, 홉수, 현재노드id, 경로[id], 방문집합)
    initial = (-0.0, 0, trend_node_id, [trend_node_id], frozenset([trend_node_id]))
    heap = [initial]

    while heap:
        neg_w, hops, cur, path, visited = heapq.heappop(heap)
        cumulative_w = -neg_w

        # 속성 노드에 도달 시 결과 수집
        if hops > 0 and G.nodes[cur].get("type") == "attribute":
            attr_ids  = [n for n in path if G.nodes[n].get("type") == "attribute"]
            prod_ids  = [n for n in path if G.nodes[n].get("type") == "product"]
            bfs_attrs = [G.nodes[n]["label"] for n in attr_ids]
            # pinned_attrs를 앞에 병합 (중복 제거, 순서 유지)
            merged = list(dict.fromkeys(pinned_attrs + bfs_attrs))
            results.append({
                "attrs":        merged,
                "total_weight": round(cumulative_w, 4),
                "path_labels":  [G.nodes[n].get("label", n) for n in path],
                "via_product":  G.nodes[prod_ids[0]]["label"] if prod_ids else None,
                "hops":         hops,
            })

        if hops >= hop_limit:
            continue

        for neighbor in G.neighbors(cur):
            if neighbor in visited:
                continue

            node_type = G.nodes[neighbor].get("type")

            # product 노드: 경로당 1개만 경유 + 상품/카테고리/IP 필터
            if node_type == "product":
                if any(G.nodes[p].get("type") == "product" for p in path):
                    continue
                if item_cd:
                    if neighbor != item_cd:
                        continue
                else:
                    prod_cat = G.nodes[neighbor].get("category", "")
                    if isinstance(prod_cat, list):
                        prod_cat = prod_cat[0] if prod_cat else ""
                    if category and prod_cat != category:
                        continue
                # IP 핀: 선택한 IP와 연결된 상품만 통과
                if pinned_ip_ids:
                    if not any(nb in pinned_ip_ids for nb in G.neighbors(neighbor)):
                        continue

            edge_w = G[cur][neighbor].get("weight", 1.0)
            heapq.heappush(heap, (
                -(cumulative_w + edge_w),
                hops + 1,
                neighbor,
                path + [neighbor],
                visited | {neighbor},
            ))

    return _deduplicate_and_rank(results, top_k)


def _deduplicate_and_rank(results: list[dict], top_k: int) -> list[dict]:
    """중복 속성 조합 제거 후 가중치 내림차순 정렬, 상위 top_k 반환."""
    seen: set = set()
    unique: list[dict] = []

    for r in sorted(results, key=lambda x: -x["total_weight"]):
        key = frozenset(r["attrs"])
        if not key:          # 빈 속성 조합 제외
            continue
        if key not in seen:
            seen.add(key)
            unique.append(r)
        if len(unique) >= top_k:
            break

    for i, r in enumerate(unique):
        r["rank"] = i + 1

    return unique


def derive_from_product(G: nx.Graph, item_cd: str, top_k: int = 4) -> dict:
    """
    히트 상품 기반 파생 아이디어 생성.
    이 상품과 연결된 유사 상품들의 속성 중
    원본 상품에 없는 속성을 '추가 가능한 파생 속성'으로 제안한다.
    """
    if not G.has_node(item_cd):
        return {}

    item_nm  = G.nodes[item_cd].get("label", item_cd)
    item_cat = G.nodes[item_cd].get("category", "")

    # 원본 상품의 속성 (label → weight)
    base_attrs: dict[str, float] = {
        G.nodes[nb].get("label", ""): G[item_cd][nb].get("weight", 1.0)
        for nb in G.neighbors(item_cd)
        if G.nodes[nb].get("type") == "attribute" and G.nodes[nb].get("label")
    }

    ideas = []
    for nb in G.neighbors(item_cd):
        if G.nodes[nb].get("type") != "product":
            continue
        nb_nm  = G.nodes[nb].get("label", nb)
        edge_w = G[item_cd][nb].get("weight", 1.0)

        nb_attrs = {
            G.nodes[a].get("label", ""): G[nb][a].get("weight", 1.0)
            for a in G.neighbors(nb)
            if G.nodes[a].get("type") == "attribute" and G.nodes[a].get("label")
        }

        # 원본에 없는 새 속성만 추출
        new_attrs = [a for a in nb_attrs if a and a not in base_attrs]
        if not new_attrs:
            continue

        carry = sorted(base_attrs, key=lambda x: -base_attrs[x])[:3]
        ideas.append({
            "derived_from":    nb_nm,
            "new_attrs":       new_attrs[:3],
            "carry_attrs":     carry,
            "combined_attrs":  list(dict.fromkeys(carry + new_attrs[:3])),
            "edge_weight":     round(edge_w, 2),
        })

    ideas.sort(key=lambda x: -x["edge_weight"])
    return {
        "item_cd":    item_cd,
        "item_nm":    item_nm,
        "category":   item_cat,
        "base_attrs": sorted(base_attrs, key=lambda x: -base_attrs[x])[:5],
        "ideas":      ideas[:top_k],
    }


def get_bust_diagnosis(G: nx.Graph, item_cd: str) -> dict:
    """
    부진 상품의 연결 속성 분석.
    연결된 속성 노드 중 가중치 하위 속성 → 회피 속성으로 진단.

    Returns:
        {
          "item_cd": str,
          "item_name": str,
          "avoid_attrs": [{"attr": str, "weight": float}, ...],
          "weak_ips": [str, ...],
        }
    """
    if not G.has_node(item_cd):
        return {}

    node_data = G.nodes[item_cd]
    attr_edges = []
    ip_edges   = []

    for neighbor in G.neighbors(item_cd):
        ntype  = G.nodes[neighbor].get("type")
        weight = G[item_cd][neighbor].get("weight", 1.0)
        label  = G.nodes[neighbor].get("label", neighbor)

        if ntype == "attribute":
            attr_edges.append({"attr": label, "weight": weight})
        elif ntype == "ip":
            ip_edges.append({"ip": label, "weight": weight})

    # 가중치 오름차순 정렬 → 하위 속성이 회피 속성 후보
    attr_edges.sort(key=lambda x: x["weight"])
    avoid_threshold = len(attr_edges) // 3 if attr_edges else 0
    avoid_attrs = attr_edges[:max(avoid_threshold, 3)]

    # IP 가중치 하위 → 약한 IP 연결
    ip_edges.sort(key=lambda x: x["weight"])
    weak_ips = [e["ip"] for e in ip_edges[:3]]

    return {
        "item_cd":    item_cd,
        "item_name":  node_data.get("label", item_cd),
        "avoid_attrs": avoid_attrs,
        "weak_ips":    weak_ips,
        "price":       node_data.get("price"),
        "survived":    node_data.get("survived"),
    }
