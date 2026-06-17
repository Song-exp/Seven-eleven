#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import pandas as pd
import numpy as np

# sys.stdout/stdin encoding 설정 (Windows 등에서 한글 깨짐 방지)
try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stdin.reconfigure(encoding='utf-8')
except AttributeError:
    # Python 3.7 미만 버전 대응
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')

# 파일 경로 정의 (상대 경로로 변환하여 이식성 높임)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PATHS = {
    "product": os.path.join(BASE_DIR, "data", "processed", "hin", "product_nodes.parquet"),
    "keyword": os.path.join(BASE_DIR, "data", "processed", "hin", "keyword_nodes.parquet"),
    "ip": os.path.join(BASE_DIR, "data", "processed", "hin", "ip_nodes.parquet")
}

def load_data():
    """데이터프레임을 로드합니다."""
    data = {}
    for name, path in PATHS.items():
        if not os.path.exists(path):
            print(f"[오류] 파일이 존재하지 않습니다: {path}")
            sys.exit(1)
        try:
            data[name] = pd.read_parquet(path)
        except Exception as e:
            print(f"[오류] {name} 파일을 읽는 중 에러 발생: {e}")
            sys.exit(1)
    return data["product"], data["keyword"], data["ip"]

def search_by_keyword(query, df_prod, df_key, df_ip):
    """지정된 키워드로 상품 및 IP를 검색합니다."""
    query = query.strip()
    if not query:
        print("검색어를 입력해주세요.")
        return

    # 1. keyword_nodes에서 입력한 검색어가 부분 일치하는 실제 키워드 후보 탐색
    matched_keywords = df_key[df_key['keyword'].str.contains(query, case=False, na=False)]['keyword'].tolist()
    
    if not matched_keywords:
        print(f"\n❌ '{query}'와(과) 일치하거나 포함하는 등록된 키워드가 없습니다.")
        return

    # 정확히 일치하는 키워드가 있는지 확인
    exact_matches = [kw for kw in matched_keywords if kw.lower() == query.lower()]
    
    # 검색할 키워드 결정 (정확히 일치하는 것이 있다면 우선 사용, 없다면 부분 일치하는 모든 키워드 검색)
    target_keywords = exact_matches if exact_matches else matched_keywords
    
    print(f"\n🔍 검색어 '{query}' 결과 (매칭된 키워드: {', '.join(target_keywords)})")
    print("=" * 80)

    for kw in target_keywords:
        print(f"\n📌 키워드: [{kw}]")
        print("-" * 50)
        
        # 2. 관련 상품 검색
        def contains_kw(x):
            if x is None:
                return False
            if isinstance(x, (list, np.ndarray)):
                return kw in x
            return kw == x
            
        prod_matches = df_prod[df_prod['키워드_final'].apply(contains_kw)]
        print(f"🛍️  관련 상품: 총 {len(prod_matches)}개")
        
        if len(prod_matches) > 0:
            # 출력할 컬럼 정의
            cols_to_show = ['ITEM_CD', 'ITEM_NM', '편의점명', '성공여부']
            # 실제로 있는 컬럼만 필터링
            cols_to_show = [c for c in cols_to_show if c in prod_matches.columns]
            
            # 상위 15개만 정렬 또는 출력
            sample_df = prod_matches[cols_to_show].head(15)
            print(sample_df.to_string(index=False))
            if len(prod_matches) > 15:
                print(f"   ... 외 {len(prod_matches)-15}개의 상품이 더 있습니다.")
        else:
            print("   등록된 상품이 없습니다.")
            
        # 3. 관련 IP 검색
        ip_matches = df_ip[df_ip['키워드_final'].apply(contains_kw)]
        print(f"🎬  관련 IP: 총 {len(ip_matches)}개")
        if len(ip_matches) > 0:
            ip_names = ip_matches['ip_name'].tolist()
            print(f"   IP명: {', '.join(ip_names)}")
        else:
            print("   등록된 IP가 없습니다.")
        print("-" * 50)

def main():
    print("==================================================")
    print("   7-Eleven NPD 키워드 기반 제품/IP 검색 도구")
    print("==================================================")
    print("데이터를 로드하는 중입니다...")
    df_prod, df_key, df_ip = load_data()
    print("데이터 로드 완료!\n")

    # Command line argument가 있으면 즉시 검색 후 종료
    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
        search_by_keyword(query, df_prod, df_key, df_ip)
        return

    # 인자가 없으면 대화형(Interactive) 루프로 실행
    print("👉 검색할 키워드를 입력하세요. (종료하려면 'exit' 또는 'q' 입력)")
    while True:
        try:
            query = input("\n🔎 검색 키워드 입력: ").strip()
            if query.lower() in ['exit', 'q', 'quit']:
                print("검색 도구를 종료합니다.")
                break
            if not query:
                continue
            search_by_keyword(query, df_prod, df_key, df_ip)
        except KeyboardInterrupt:
            print("\n검색 도구를 종료합니다.")
            break
        except Exception as e:
            print(f"오류가 발생했습니다: {e}")

if __name__ == "__main__":
    main()
