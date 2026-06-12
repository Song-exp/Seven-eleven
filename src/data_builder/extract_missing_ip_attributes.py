"""
누락된 IP 브랜드 속성 추가 추출 스크립트

대상: data/processed/IP_속성추출/참조데이터_community_1hop_mapping_final.csv
컬럼: IP/브랜드_누락된것
기능: 기존에 추출된 IP_속성 결과에 없는 키워드만 골라 LLM 추출 진행
"""

import os
import sys
import json
import pandas as pd
import time

# 프로젝트 루트를 path에 추가
sys.path.append(os.getcwd())

from src.data_builder.ip_attribute_extractor import (
    collect_enriched_text,
    extract_ip_attributes,
    _load_checkpoint,
    _save_checkpoint,
    CHECKPOINT_PATH,
    OUTPUT_JSON_PATH,
    OUTPUT_PARQUET_PATH,
    IP_FOLDER
)

def extract_missing_from_csv():
    # 1. 대상 CSV 로드 (깨진 한글 대응을 위해 encoding 시도)
    csv_path = os.path.join(IP_FOLDER, "참조데이터_community_1hop_mapping_final.csv")
    if not os.path.exists(csv_path):
        # 혹시 파일명이 다를 경우를 대비해 폴더 내 csv 검색
        csv_files = [f for f in os.listdir(IP_FOLDER) if f.endswith('.csv')]
        if not csv_files:
            print(f"[-] CSV 파일을 찾을 수 없습니다: {IP_FOLDER}")
            return
        csv_path = os.path.join(IP_FOLDER, csv_files[0])
    
    print(f"[*] 데이터 로드 중: {csv_path}")
    try:
        df_missing = pd.read_csv(csv_path, encoding='utf-8-sig')
    except:
        df_missing = pd.read_csv(csv_path, encoding='cp949')

    if 'IP/브랜드_누락된것' not in df_missing.columns:
        print(f"[-] 'IP/브랜드_누락된것' 컬럼이 없습니다. 확인된 컬럼: {df_missing.columns.tolist()}")
        return

    # 1. '없음' 제외 및 데이터 수집
    # 결측치 제거 후 문자열로 변환
    raw_values = df_missing['IP/브랜드_누락된것'].dropna().astype(str)

    # '없음' 텍스트가 포함된 데이터 제외
    filtered_values = raw_values[raw_values != '없음']

    # 모든 데이터를 하나의 문자열로 합치고 쉼표로 분리
    all_text = ",".join(filtered_values)
    keywords_list = [k.strip() for k in all_text.split(",") if k.strip()]

    # 중복 제거 및 '없음' 키워드 한 번 더 필터링 (혹시 쉼표 사이에 섞여 있을 경우 대비)
    target_keywords = sorted(list(set([k for k in keywords_list if k != '없음'])))

    print(f"[*] 데이터 정제 후 총 {len(target_keywords)}개의 유니크 키워드 확보")

    # 2. 기존 결과 로드
    checkpoint = _load_checkpoint(CHECKPOINT_PATH)
    done_keywords = set(checkpoint.keys())
    
    # 3. 신규 키워드 필터링
    new_keywords = [k for k in target_keywords if k not in done_keywords]
    print(f"[*] 이미 처리된 키워드: {len(done_keywords)}개")
    print(f"[*] 새로 추출할 키워드: {len(new_keywords)}개")

    if not new_keywords:
        print("[!] 추가로 추출할 키워드가 없습니다.")
        return

    # 4. 추출 진행
    for i, kw in enumerate(new_keywords, 1):
        print(f"[{i}/{len(new_keywords)}] '{kw}' 처리 중...")
        
        try:
            # 텍스트 수집 (나무위키 + 네이버)
            wiki_text, text_source = collect_enriched_text(kw)
            
            # LLM 속성 추출
            attrs = extract_ip_attributes(kw, wiki_text)
            
            if not attrs:
                print(f"  [!] '{kw}' 속성 추출 실패 (LLM 응답 없음)")
                continue

            # 결과 기록
            record = {
                "keyword": kw,
                "file_source": "manual_missing_csv",
                "wiki_text_snippet": wiki_text[:300] if wiki_text else "",
                "text_source": text_source,
                **attrs,
            }
            
            checkpoint[kw] = record
            _save_checkpoint(CHECKPOINT_PATH, checkpoint)
            
            kws_preview = ", ".join(attrs.get("signature_keywords", [])[:5])
            print(f"  → 완료: type={attrs.get('ip_type', '?')} | keywords=[{kws_preview}]")
            
            # API 과부하 방지 및 안정성
            time.sleep(1.0)
            
        except Exception as e:
            print(f"  [!] '{kw}' 처리 중 예외 발생: {e}")
            continue

    # 5. 최종 결과 저장 (기존 파일 덮어쓰기/업데이트)
    final_records = list(checkpoint.values())
    
    with open(OUTPUT_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(final_records, f, ensure_ascii=False, indent=2)
    
    df_final = pd.DataFrame(final_records)
    df_final.to_parquet(OUTPUT_PARQUET_PATH, index=False)
    
    print("\n" + "="*50)
    print(f"[*] 모든 작업 완료!")
    print(f"[*] 최종 추출 데이터: {len(df_final)}건")
    print(f"[*] 저장 위치: {OUTPUT_JSON_PATH}")
    print(f"[*] 저장 위치: {OUTPUT_PARQUET_PATH}")
    print("="*50)

if __name__ == "__main__":
    extract_missing_from_csv()
