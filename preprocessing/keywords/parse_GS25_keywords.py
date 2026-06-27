import csv
import json
import pprint

CSV_PATH = "/Users/hyunoworld/Desktop/Seminar/Seven-eleven/data/raw/편의점/gs25_official_with_keywords.csv"

def parse_keywords_json(raw: str) -> dict:
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}

def build_product_dict(csv_path: str) -> dict:
    """
    keywords_json의 metadata를 파싱해 아래 형식으로 변환:
      { (상품명, 가격, 용량): [속성1, 속성2, ...] }

    속성 = flavor_and_category + collab_and_brand + promotion_type + tpo_context
    가격/용량이 없으면 None으로 저장.
    동일 키가 여러 번 등장하면 속성 리스트를 합집합으로 병합.
    """
    product_dict: dict[tuple, set] = {}

    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            kw = parse_keywords_json(row.get("keywords_json", ""))
            metadata = kw.get("metadata", [])
            if not metadata:
                continue

            for item in metadata:
                name = item.get("name")
                if not name:
                    continue

                price = item.get("price")       # int or None
                capacity = item.get("capacity") # str or None

                key = (name, price, capacity)

                attrs = (
                    item.get("flavor_and_category", [])
                    + item.get("collab_and_brand", [])
                    + item.get("promotion_type", [])
                    + item.get("tpo_context", [])
                )

                if key not in product_dict:
                    product_dict[key] = set()
                product_dict[key].update(attrs)

    # set -> list (순서 유지를 원하면 sorted() 적용 가능)
    return {k: list(v) for k, v in product_dict.items()}


def display(product_dict: dict, limit: int = 20):
    """사용자가 요청한 형식으로 출력: {[이름, 가격, 용량]: [속성...]}"""
    print(f"총 제품 수: {len(product_dict)}\n")
    for i, ((name, price, capacity), attrs) in enumerate(product_dict.items()):
        if i >= limit:
            print(f"... (이하 {len(product_dict) - limit}개 생략)")
            break
        key_repr = [name, price, capacity]
        print(f"{{{key_repr}: {attrs}}}")


if __name__ == "__main__":
    product_dict = build_product_dict(CSV_PATH)
    display(product_dict, limit=30)

    # 전체 결과를 JSON으로 저장 (키를 문자열로 직렬화)
    output_path = "gs25_product_attrs.json"
    serializable = {
        json.dumps([k[0], k[1], k[2]], ensure_ascii=False): v
        for k, v in product_dict.items()
    }
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(serializable, f, ensure_ascii=False, indent=2)
    print(f"\n전체 결과 저장 완료: {output_path}")