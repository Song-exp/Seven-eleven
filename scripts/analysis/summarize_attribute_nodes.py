import json
import os
from collections import Counter

# 설정
BASE_DIR = "data/processed/IP_속성추출"
FILES = ["IP_속성.json", "제품_속성.json"]

def summarize_nodes():
    print("🔍 카테고리별 추출 키워드(노드) 요약 시작...")
    
    # 카테고리별 키워드를 담을 딕셔너리
    node_pool = {
        "signature_keywords (IP이미지)": [],
        "flavor (맛)": [],
        "texture (식감)": [],
        "ingredients (원재료)": [],
        "tpo (상황)": []
    }

    for filename in FILES:
        path = os.path.join(BASE_DIR, filename)
        if not os.path.exists(path): continue
        
        with open(path, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                for item in data:
                    # 각 필드별로 데이터 수집 (리스트 형태인 경우만)
                    if 'signature_keywords' in item and isinstance(item['signature_keywords'], list):
                        node_pool["signature_keywords (IP이미지)"].extend(item['signature_keywords'])
                    if 'flavor' in item and isinstance(item['flavor'], list):
                        node_pool["flavor (맛)"].extend(item['flavor'])
                    if 'texture' in item and isinstance(item['texture'], list):
                        node_pool["texture (식감)"].extend(item['texture'])
                    if 'ingredients' in item and isinstance(item['ingredients'], list):
                        node_pool["ingredients (원재료)"].extend(item['ingredients'])
                    if 'tpo' in item and isinstance(item['tpo'], list):
                        node_pool["tpo (상황)"].extend(item['tpo'])
            except Exception as e:
                print(f"⚠️ {filename} 처리 중 오류: {e}")

    # 결과 리포트 생성
    summary_report = ["# 추출 속성 키워드(노드) 요약 리포트\n\n"]
    
    print("\n" + "="*60)
    for category, keywords in node_pool.items():
        if not keywords: continue
        
        # 빈도 계산 및 정렬
        counts = Counter(keywords)
        unique_sorted = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        
        total_unique = len(unique_sorted)
        print(f"\n📌 {category}")
        print(f"  - 고유 노드 수: {total_unique}개")
        
        # 상위 20개 콘솔 출력용
        top_20 = [f"{k}({v})" for k, v in unique_sorted[:20]]
        print(f"  - 주요 키워드(빈도): {', '.join(top_20)} ...")
        
        # 리포트 파일용 내용 구성
        summary_report.append(f"## {category} (총 {total_unique}개)\n")
        summary_report.append("### [빈도순 상위 30개]\n")
        summary_report.append(", ".join([f"{k}({v})" for k, v in unique_sorted[:30]]) + "\n\n")
        summary_report.append("### [전체 목록 (가나다순)]\n")
        summary_report.append(", ".join(sorted(counts.keys())) + "\n\n")
        summary_report.append("---\n\n")

    # 파일로 저장
    report_path = os.path.join(BASE_DIR, "추출_키워드_요약본.md")
    with open(report_path, "w", encoding="utf-8") as f:
        f.writelines(summary_report)
    
    print("\n" + "="*60)
    print(f"✨ 요약 완료! 상세 리포트는 마크다운 파일로 저장되었습니다.")
    print(f"📂 저장 위치: {report_path}")

if __name__ == "__main__":
    summarize_nodes()
