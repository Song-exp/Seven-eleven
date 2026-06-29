import sys
import pandas as pd
sys.stdout.reconfigure(encoding='utf-8')

try:
    df_ie = pd.read_csv("data/processed/instagram_engagement_with_keywords_final.csv")
    targets = [
        "거인단팥빵", "씨앗호떡빵", "호랑이초코롤", 
        "간장치킨볼", "빠지락라면",
        "트러플감자칩", "까망짜장", "아망추", "아샷추", "산리오"
    ]
    
    print("\n--- Match in Instagram Engagement ---")
    for t in targets:
        matched = df_ie[df_ie['정규화명'].str.contains(t, na=False) | df_ie['원본명'].str.contains(t, na=False)]
        for idx, row in matched.iterrows():
            print(f"Original: {row['원본명']} | Normalized: {row['정규화명']} | Likes: {row['좋아요 수']} | Store: {row['편의점명']}")
except Exception as e:
    print(f"Error: {e}")
