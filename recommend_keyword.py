#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import pandas as pd
import numpy as np
from collections import Counter

try:
    sys.stdout.reconfigure(encoding='utf-8')
except AttributeError:
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PATHS = {
    "product": os.path.join(BASE_DIR, "data", "processed", "hin", "product_nodes.parquet"),
    "keyword": os.path.join(BASE_DIR, "data", "processed", "hin", "keyword_nodes.parquet")
}

def load_data():
    df_prod = pd.read_parquet(PATHS["product"])
    df_key = pd.read_parquet(PATHS["keyword"])
    return df_prod, df_key

def recommend_substitution_or_split(q, df_prod, df_key):
    q = q.strip()
    if not q:
        return ""

    # 빈도 사전 생성
    all_kws = []
    for kws in df_prod['키워드_final']:
        if isinstance(kws, (list, np.ndarray)):
            all_kws.extend(kws)
    freq_map = Counter(all_kws)
    
    master_keywords = set(df_key['keyword'].tolist())

    # 1. 두 단어로 쪼개기 시도
    splits = []
    for i in range(1, len(q)):
        left = q[:i]
        right = q[i:]
        if left in master_keywords and right in master_keywords:
            left_freq = freq_map.get(left, 0)
            right_freq = freq_map.get(right, 0)
            splits.append((left, left_freq, right, right_freq))
            
    if splits:
        # 빈도 합이 가장 높은 분할 리턴
        splits.sort(key=lambda x: x[1] + x[3], reverse=True)
        best_split = splits[0]
        return f"{q} -> {best_split[0]}({best_split[1]}), {best_split[2]}({best_split[3]})"

    # 2. 빈도가 높은 대표어로 대체 시도
    candidates = []
    for kw in master_keywords:
        if kw == q:
            continue
        if q in kw or kw in q:
            candidates.append((kw, freq_map.get(kw, 0)))
            
    if candidates:
        # 빈도가 높은 순서대로 정렬
        candidates.sort(key=lambda x: x[1], reverse=True)
        best_sub = candidates[0][0]
        best_sub_freq = candidates[0][1]
        return f"{q} -> {best_sub}({best_sub_freq})"
        
    return f"{q} -> 추천 대체어 없음(0)"

def main():
    if len(sys.argv) < 2:
        print("사용법: python recommend_keyword.py <키워드>")
        sys.exit(1)
        
    df_prod, df_key = load_data()
    q = " ".join(sys.argv[1:])
    result = recommend_substitution_or_split(q, df_prod, df_key)
    print(result)

if __name__ == "__main__":
    main()
