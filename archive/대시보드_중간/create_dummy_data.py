"""
create_dummy_data.py
───────────────────────────────────────────────────────────────────────
실제 네트워크 데이터 수령 전 테스트용 더미 network.json 생성 스크립트.

[스키마 규칙 — 실제 데이터 삽입 시 동일 형식 유지]
노드:
  { "id": str, "label": str, "type": "product|attribute|ip|event",
    "category": str (product 전용),
    "price": int (product 전용),
    "survived": bool (product 전용) }

엣지:
  { "source": str, "target": str, "weight": float,
    "edge_type": "prod_attr|prod_ip|prod_event|prod_prod|ip_attr" }

실행:
  python create_dummy_data.py
  → data/network.json 생성 (서버 시작 시 자동으로 이 파일 로드)
"""

import json
import os

OUT_DIR  = os.path.join(os.path.dirname(__file__), "data")
OUT_PATH = os.path.join(OUT_DIR, "network.json")
os.makedirs(OUT_DIR, exist_ok=True)

# ═══════════════════════════════════════════════════════════════════════
# 노드 정의
# ═══════════════════════════════════════════════════════════════════════

PRODUCT_NODES = [
    # ── 간식빵 카테고리
    {"id": "P001", "label": "7-SELECT 소금빵",        "type": "product", "category": "간식빵",      "price": 1800, "survived": True},
    {"id": "P002", "label": "7-SELECT 버터 크루아상",  "type": "product", "category": "간식빵",      "price": 1900, "survived": True},
    {"id": "P003", "label": "말차 크림 롤빵",          "type": "product", "category": "간식빵",      "price": 2200, "survived": True},
    {"id": "P004", "label": "KBO 미트 파이",           "type": "product", "category": "간식빵",      "price": 3500, "survived": False},  # 부진
    # ── 하이볼 카테고리
    {"id": "P005", "label": "제로 유자 하이볼 250ml",  "type": "product", "category": "하이볼",      "price": 2800, "survived": True},
    {"id": "P006", "label": "제로 말차 하이볼 250ml",  "type": "product", "category": "하이볼",      "price": 2800, "survived": True},
    {"id": "P007", "label": "직화 불닭볶음 도시락",    "type": "product", "category": "도시락",      "price": 4500, "survived": False},  # 부진
    # ── 젤리/스낵
    {"id": "P008", "label": "딸기 쫀득 젤리",          "type": "product", "category": "젤리",        "price": 1500, "survived": True},
    # ── 음료
    {"id": "P009", "label": "7-SELECT 말차 라떼",      "type": "product", "category": "컵커피",      "price": 2000, "survived": True},
    # ── 프로틴/시리얼
    {"id": "P010", "label": "그린바이옴 프로틴 그래놀라","type": "product", "category": "프로틴/시리얼","price": 3200, "survived": False},  # 부진
]

ATTRIBUTE_NODES = [
    # 식감
    {"id": "attr_쫀득한 식감",  "label": "쫀득한 식감",  "type": "attribute"},
    {"id": "attr_바삭함",       "label": "바삭함",        "type": "attribute"},
    {"id": "attr_촉촉함",       "label": "촉촉함",        "type": "attribute"},
    {"id": "attr_부드러운",     "label": "부드러운",      "type": "attribute"},
    {"id": "attr_쌉쌀함",       "label": "쌉쌀함",        "type": "attribute"},
    # 맛
    {"id": "attr_고소함",       "label": "고소함",        "type": "attribute"},
    {"id": "attr_달달함",       "label": "달달함",        "type": "attribute"},
    {"id": "attr_상큼함",       "label": "상큼함",        "type": "attribute"},
    {"id": "attr_감칠맛",       "label": "감칠맛",        "type": "attribute"},
    # 원재료/테마
    {"id": "attr_말차",         "label": "말차",          "type": "attribute"},
    {"id": "attr_유자",         "label": "유자",          "type": "attribute"},
    {"id": "attr_딸기",         "label": "딸기",          "type": "attribute"},
    {"id": "attr_소금",         "label": "소금",          "type": "attribute"},
    {"id": "attr_이탈리안",     "label": "이탈리안",      "type": "attribute"},
    {"id": "attr_일본 풍미",    "label": "일본 풍미",     "type": "attribute"},
    # 건강/기능
    {"id": "attr_제로슈가",     "label": "제로슈가",      "type": "attribute"},
    {"id": "attr_저칼로리",     "label": "저칼로리",      "type": "attribute"},
    {"id": "attr_건강함",       "label": "건강함",        "type": "attribute"},
    {"id": "attr_고단백",       "label": "고단백",        "type": "attribute"},
    # 가치
    {"id": "attr_프리미엄",     "label": "프리미엄",      "type": "attribute"},
    {"id": "attr_가성비",       "label": "가성비",        "type": "attribute"},
    {"id": "attr_시즌 한정",    "label": "시즌 한정",     "type": "attribute"},
    {"id": "attr_IP 콜라보",    "label": "IP 콜라보",     "type": "attribute"},
    {"id": "attr_인증샷",       "label": "인증샷",        "type": "attribute"},
    # TPO (Time·Place·Occasion)
    {"id": "attr_카페 TPO",     "label": "카페 TPO",      "type": "attribute"},
    {"id": "attr_아침 TPO",     "label": "아침 TPO",      "type": "attribute"},
    {"id": "attr_야식 TPO",     "label": "야식 TPO",      "type": "attribute"},
    {"id": "attr_혼술 TPO",     "label": "혼술 TPO",      "type": "attribute"},
    {"id": "attr_간식 상황",    "label": "간식 상황",     "type": "attribute"},
    {"id": "attr_점심 TPO",     "label": "점심 TPO",      "type": "attribute"},
]

IP_NODES = [
    {"id": "ip_먹방크리에이터A", "label": "먹방크리에이터A", "type": "ip"},
    {"id": "ip_카페인플루언서B", "label": "카페인플루언서B", "type": "ip"},
    {"id": "ip_헬스라이프C",     "label": "헬스라이프C",     "type": "ip"},
    {"id": "ip_홈술인플루언서D", "label": "홈술인플루언서D", "type": "ip"},
]

EVENT_NODES = [
    {"id": "event_봄시즌",     "label": "2025 봄 시즌 이벤트",  "type": "event"},
    {"id": "event_여름프로모", "label": "2025 여름 프로모션",    "type": "event"},
    {"id": "event_신제품론칭", "label": "신제품 론칭 이벤트",   "type": "event"},
]

# ═══════════════════════════════════════════════════════════════════════
# 엣지 정의
# ═══════════════════════════════════════════════════════════════════════

PROD_ATTR_EDGES = [
    # P001 소금빵
    {"source": "P001", "target": "attr_쫀득한 식감", "weight": 5, "edge_type": "prod_attr"},
    {"source": "P001", "target": "attr_고소함",       "weight": 4, "edge_type": "prod_attr"},
    {"source": "P001", "target": "attr_소금",         "weight": 3, "edge_type": "prod_attr"},
    {"source": "P001", "target": "attr_카페 TPO",     "weight": 3, "edge_type": "prod_attr"},
    {"source": "P001", "target": "attr_바삭함",       "weight": 2, "edge_type": "prod_attr"},
    {"source": "P001", "target": "attr_인증샷",       "weight": 2, "edge_type": "prod_attr"},
    # P002 크루아상
    {"source": "P002", "target": "attr_바삭함",       "weight": 5, "edge_type": "prod_attr"},
    {"source": "P002", "target": "attr_고소함",       "weight": 4, "edge_type": "prod_attr"},
    {"source": "P002", "target": "attr_카페 TPO",     "weight": 4, "edge_type": "prod_attr"},
    {"source": "P002", "target": "attr_촉촉함",       "weight": 3, "edge_type": "prod_attr"},
    {"source": "P002", "target": "attr_프리미엄",     "weight": 3, "edge_type": "prod_attr"},
    {"source": "P002", "target": "attr_달달함",       "weight": 2, "edge_type": "prod_attr"},
    # P003 말차 크림빵
    {"source": "P003", "target": "attr_말차",         "weight": 5, "edge_type": "prod_attr"},
    {"source": "P003", "target": "attr_달달함",       "weight": 4, "edge_type": "prod_attr"},
    {"source": "P003", "target": "attr_카페 TPO",     "weight": 4, "edge_type": "prod_attr"},
    {"source": "P003", "target": "attr_쌉쌀함",       "weight": 3, "edge_type": "prod_attr"},
    {"source": "P003", "target": "attr_쫀득한 식감",  "weight": 3, "edge_type": "prod_attr"},
    {"source": "P003", "target": "attr_시즌 한정",    "weight": 2, "edge_type": "prod_attr"},
    # P004 KBO 미트 파이 (부진 — 가중치 낮음)
    {"source": "P004", "target": "attr_고소함",       "weight": 2, "edge_type": "prod_attr"},
    {"source": "P004", "target": "attr_바삭함",       "weight": 2, "edge_type": "prod_attr"},
    {"source": "P004", "target": "attr_간식 상황",    "weight": 1, "edge_type": "prod_attr"},
    {"source": "P004", "target": "attr_IP 콜라보",    "weight": 1, "edge_type": "prod_attr"},
    {"source": "P004", "target": "attr_가성비",       "weight": 1, "edge_type": "prod_attr"},
    # P005 제로 유자 하이볼
    {"source": "P005", "target": "attr_제로슈가",     "weight": 5, "edge_type": "prod_attr"},
    {"source": "P005", "target": "attr_유자",         "weight": 4, "edge_type": "prod_attr"},
    {"source": "P005", "target": "attr_상큼함",       "weight": 4, "edge_type": "prod_attr"},
    {"source": "P005", "target": "attr_혼술 TPO",     "weight": 3, "edge_type": "prod_attr"},
    {"source": "P005", "target": "attr_야식 TPO",     "weight": 3, "edge_type": "prod_attr"},
    # P006 제로 말차 하이볼
    {"source": "P006", "target": "attr_제로슈가",     "weight": 4, "edge_type": "prod_attr"},
    {"source": "P006", "target": "attr_말차",         "weight": 3, "edge_type": "prod_attr"},
    {"source": "P006", "target": "attr_쌉쌀함",       "weight": 3, "edge_type": "prod_attr"},
    {"source": "P006", "target": "attr_혼술 TPO",     "weight": 4, "edge_type": "prod_attr"},
    {"source": "P006", "target": "attr_야식 TPO",     "weight": 2, "edge_type": "prod_attr"},
    # P007 불닭볶음 도시락 (부진)
    {"source": "P007", "target": "attr_가성비",       "weight": 2, "edge_type": "prod_attr"},
    {"source": "P007", "target": "attr_감칠맛",       "weight": 2, "edge_type": "prod_attr"},
    {"source": "P007", "target": "attr_점심 TPO",     "weight": 1, "edge_type": "prod_attr"},
    {"source": "P007", "target": "attr_간식 상황",    "weight": 1, "edge_type": "prod_attr"},
    # P008 딸기 쫀득 젤리
    {"source": "P008", "target": "attr_딸기",         "weight": 5, "edge_type": "prod_attr"},
    {"source": "P008", "target": "attr_달달함",       "weight": 4, "edge_type": "prod_attr"},
    {"source": "P008", "target": "attr_쫀득한 식감",  "weight": 4, "edge_type": "prod_attr"},
    {"source": "P008", "target": "attr_상큼함",       "weight": 3, "edge_type": "prod_attr"},
    {"source": "P008", "target": "attr_IP 콜라보",    "weight": 4, "edge_type": "prod_attr"},
    {"source": "P008", "target": "attr_인증샷",       "weight": 3, "edge_type": "prod_attr"},
    # P009 말차 라떼
    {"source": "P009", "target": "attr_말차",         "weight": 5, "edge_type": "prod_attr"},
    {"source": "P009", "target": "attr_쌉쌀함",       "weight": 4, "edge_type": "prod_attr"},
    {"source": "P009", "target": "attr_아침 TPO",     "weight": 4, "edge_type": "prod_attr"},
    {"source": "P009", "target": "attr_카페 TPO",     "weight": 3, "edge_type": "prod_attr"},
    {"source": "P009", "target": "attr_달달함",       "weight": 2, "edge_type": "prod_attr"},
    # P010 프로틴 그래놀라 (부진)
    {"source": "P010", "target": "attr_건강함",       "weight": 3, "edge_type": "prod_attr"},
    {"source": "P010", "target": "attr_저칼로리",     "weight": 3, "edge_type": "prod_attr"},
    {"source": "P010", "target": "attr_아침 TPO",     "weight": 2, "edge_type": "prod_attr"},
    {"source": "P010", "target": "attr_고단백",       "weight": 2, "edge_type": "prod_attr"},
    {"source": "P010", "target": "attr_가성비",       "weight": 1, "edge_type": "prod_attr"},
]

PROD_IP_EDGES = [
    {"source": "P001", "target": "ip_먹방크리에이터A", "weight": 5, "edge_type": "prod_ip"},
    {"source": "P001", "target": "ip_카페인플루언서B", "weight": 3, "edge_type": "prod_ip"},
    {"source": "P002", "target": "ip_카페인플루언서B", "weight": 6, "edge_type": "prod_ip"},
    {"source": "P003", "target": "ip_카페인플루언서B", "weight": 8, "edge_type": "prod_ip"},
    {"source": "P003", "target": "ip_먹방크리에이터A", "weight": 4, "edge_type": "prod_ip"},
    {"source": "P005", "target": "ip_홈술인플루언서D", "weight": 7, "edge_type": "prod_ip"},
    {"source": "P005", "target": "ip_헬스라이프C",     "weight": 4, "edge_type": "prod_ip"},
    {"source": "P006", "target": "ip_홈술인플루언서D", "weight": 6, "edge_type": "prod_ip"},
    {"source": "P008", "target": "ip_먹방크리에이터A", "weight": 9, "edge_type": "prod_ip"},
    {"source": "P009", "target": "ip_카페인플루언서B", "weight": 7, "edge_type": "prod_ip"},
    {"source": "P010", "target": "ip_헬스라이프C",     "weight": 2, "edge_type": "prod_ip"},
]

PROD_EVENT_EDGES = [
    {"source": "P001", "target": "event_봄시즌",     "weight": 1, "edge_type": "prod_event"},
    {"source": "P002", "target": "event_봄시즌",     "weight": 1, "edge_type": "prod_event"},
    {"source": "P003", "target": "event_신제품론칭", "weight": 1, "edge_type": "prod_event"},
    {"source": "P005", "target": "event_여름프로모", "weight": 1, "edge_type": "prod_event"},
    {"source": "P006", "target": "event_여름프로모", "weight": 1, "edge_type": "prod_event"},
]

# 상품-상품: 같은 카테고리 내 공유 속성 ≥ 2 (수동 계산)
PROD_PROD_EDGES = [
    # 간식빵 카테고리
    {"source": "P001", "target": "P002", "weight": 3, "edge_type": "prod_prod"},  # 고소함, 바삭함, 카페TPO
    {"source": "P001", "target": "P003", "weight": 2, "edge_type": "prod_prod"},  # 쫀득한식감, 카페TPO
    {"source": "P001", "target": "P004", "weight": 2, "edge_type": "prod_prod"},  # 고소함, 바삭함
    {"source": "P002", "target": "P003", "weight": 2, "edge_type": "prod_prod"},  # 고소함, 카페TPO
    {"source": "P002", "target": "P004", "weight": 2, "edge_type": "prod_prod"},  # 고소함, 바삭함
    # 하이볼 카테고리
    {"source": "P005", "target": "P006", "weight": 3, "edge_type": "prod_prod"},  # 제로슈가, 혼술TPO, 야식TPO
]

IP_ATTR_EDGES = [
    # 먹방크리에이터A: 달콤하고 쫀득한 간식 전문
    {"source": "ip_먹방크리에이터A", "target": "attr_달달함",      "weight": 8, "edge_type": "ip_attr"},
    {"source": "ip_먹방크리에이터A", "target": "attr_쫀득한 식감", "weight": 6, "edge_type": "ip_attr"},
    {"source": "ip_먹방크리에이터A", "target": "attr_카페 TPO",    "weight": 5, "edge_type": "ip_attr"},
    {"source": "ip_먹방크리에이터A", "target": "attr_바삭함",      "weight": 4, "edge_type": "ip_attr"},
    {"source": "ip_먹방크리에이터A", "target": "attr_인증샷",      "weight": 5, "edge_type": "ip_attr"},
    # 카페인플루언서B: 말차·카페 전문
    {"source": "ip_카페인플루언서B", "target": "attr_말차",        "weight": 7, "edge_type": "ip_attr"},
    {"source": "ip_카페인플루언서B", "target": "attr_카페 TPO",    "weight": 8, "edge_type": "ip_attr"},
    {"source": "ip_카페인플루언서B", "target": "attr_고소함",      "weight": 5, "edge_type": "ip_attr"},
    {"source": "ip_카페인플루언서B", "target": "attr_프리미엄",    "weight": 6, "edge_type": "ip_attr"},
    {"source": "ip_카페인플루언서B", "target": "attr_쌉쌀함",      "weight": 4, "edge_type": "ip_attr"},
    # 헬스라이프C: 건강/제로슈가 전문
    {"source": "ip_헬스라이프C",     "target": "attr_건강함",      "weight": 9, "edge_type": "ip_attr"},
    {"source": "ip_헬스라이프C",     "target": "attr_제로슈가",    "weight": 8, "edge_type": "ip_attr"},
    {"source": "ip_헬스라이프C",     "target": "attr_저칼로리",    "weight": 7, "edge_type": "ip_attr"},
    {"source": "ip_헬스라이프C",     "target": "attr_아침 TPO",    "weight": 5, "edge_type": "ip_attr"},
    {"source": "ip_헬스라이프C",     "target": "attr_고단백",      "weight": 6, "edge_type": "ip_attr"},
    # 홈술인플루언서D: 혼술/하이볼 전문
    {"source": "ip_홈술인플루언서D", "target": "attr_혼술 TPO",    "weight": 10, "edge_type": "ip_attr"},
    {"source": "ip_홈술인플루언서D", "target": "attr_야식 TPO",    "weight": 8,  "edge_type": "ip_attr"},
    {"source": "ip_홈술인플루언서D", "target": "attr_제로슈가",    "weight": 6,  "edge_type": "ip_attr"},
    {"source": "ip_홈술인플루언서D", "target": "attr_상큼함",      "weight": 5,  "edge_type": "ip_attr"},
]

# ═══════════════════════════════════════════════════════════════════════
# 조립 및 저장
# ═══════════════════════════════════════════════════════════════════════

def build_network() -> dict:
    nodes = PRODUCT_NODES + ATTRIBUTE_NODES + IP_NODES + EVENT_NODES
    edges = (PROD_ATTR_EDGES + PROD_IP_EDGES + PROD_EVENT_EDGES
             + PROD_PROD_EDGES + IP_ATTR_EDGES)
    return {"nodes": nodes, "edges": edges}


def validate_network(network: dict):
    """엣지의 source/target이 모두 존재하는 노드인지 검증."""
    node_ids = {n["id"] for n in network["nodes"]}
    errors = []
    for e in network["edges"]:
        if e["source"] not in node_ids:
            errors.append(f"[엣지 오류] source '{e['source']}' 미존재")
        if e["target"] not in node_ids:
            errors.append(f"[엣지 오류] target '{e['target']}' 미존재")
    return errors


if __name__ == "__main__":
    network = build_network()
    errors  = validate_network(network)

    if errors:
        print("⚠ 검증 실패:")
        for e in errors:
            print(" ", e)
    else:
        with open(OUT_PATH, "w", encoding="utf-8") as f:
            json.dump(network, f, ensure_ascii=False, indent=2)

        node_types = {}
        for n in network["nodes"]:
            t = n["type"]
            node_types[t] = node_types.get(t, 0) + 1

        print(f"✔ {OUT_PATH} 생성 완료")
        print(f"  노드: {len(network['nodes'])}개 → {node_types}")
        print(f"  엣지: {len(network['edges'])}개")
        print(f"\n실행 방법:")
        print(f"  uvicorn server:app --port 8000 --reload")
