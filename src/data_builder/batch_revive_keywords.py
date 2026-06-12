import os
import sys
import pandas as pd
import time

# keyword_extractor.py가 있는 경로를 sys.path에 추가
sys.path.insert(0, os.path.dirname(__file__))
from keyword_extractor import extract_keywords

# ==========================================
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# 입력 및 출력 파일 경로
INPUT_XLSX = os.path.join(PROJECT_ROOT, "src", "data_builder", "revive_final_for_extraction.xlsx")
EXISTING_CSV = os.path.join(PROJECT_ROOT, "data", "processed", "blog_with_keywords_filtered.csv")

CHECKPOINT_CSV = os.path.join(PROJECT_ROOT, "data", "processed", "revive_keywords_checkpoint.csv")
FINAL_MERGED_CSV = os.path.join(PROJECT_ROOT, "data", "processed", "blog_with_keywords_merged_v2.csv")

CHECKPOINT_EVERY = 5 # 5건마다 저장
# ==========================================

def main():
    print("=" * 60)
    print("재수집 데이터 키워드 추출 및 병합 배치 시작")
    print("=" * 60)

    # 1. 입력 데이터 로드
    if not os.path.exists(INPUT_XLSX):
        print(f"❌ 입력 파일을 찾을 수 없습니다: {INPUT_XLSX}")
        return

    df_new = pd.read_excel(INPUT_XLSX)
    print(f"[1/4] 신규 데이터 로드 완료: {len(df_new)}행")

    # 2. 체크포인트 로드 또는 초기화
    if os.path.exists(CHECKPOINT_CSV):
        print(f"[2/4] 체크포인트 로드: {CHECKPOINT_CSV}")
        # dtype을 str로 명시하여 float64 변환 에러 방지
        df_work = pd.read_csv(CHECKPOINT_CSV, encoding="utf-8-sig", dtype={'review_keywords': str, 'hin_keywords': str})
        df_work["review_keywords"] = df_work["review_keywords"].fillna("")
        df_work["hin_keywords"] = df_work["hin_keywords"].fillna("")
        
        done_mask = df_work["review_keywords"].notna() & (df_work["review_keywords"] != "")
        done_count = done_mask.sum()
        pending_df = df_work[~done_mask].copy()
    else:
        print("[2/4] 신규 추출 프로세스 시작")
        df_work = df_new.copy()
        df_work["review_keywords"] = ""
        df_work["hin_keywords"] = ""
        # 컬럼 타입을 확실하게 object(str)로 변경
        df_work = df_work.astype({'review_keywords': str, 'hin_keywords': str})
        done_count = 0
        pending_df = df_work.copy()

    print(f"      진행 상황: {done_count} / {len(df_work)} 완료 (남은 대상: {len(pending_df)}건)")

    # 3. 키워드 추출 루프
    if not pending_df.empty:
        print("\n[3/4] LLM 키워드 추출 시작 (Ollama)")
        try:
            for i, idx in enumerate(pending_df.index, start=1):
                content = pending_df.loc[idx, "본문내용"]
                product_name = pending_df.loc[idx, "product_name"]
                
                print(f"  [{i}/{len(pending_df)}] '{product_name}' 추출 중...", end=" ", flush=True)
                
                # 텍스트가 너무 길면 자름 (Ollama 컨텍스트 고려)
                text_to_process = str(content)[:3500] 
                
                # 키워드 추출 (keyword_extractor.py의 로직 사용)
                keywords = extract_keywords(text_to_process)
                
                if keywords:
                    kw_str = ", ".join(keywords)
                    df_work.at[idx, "review_keywords"] = kw_str
                    df_work.at[idx, "hin_keywords"] = kw_str # 현재 로직상 동일하게 설정
                    print(f"→ 성공 ({len(keywords)}개)")
                else:
                    print("→ 실패 (빈 결과)")

                # 중간 저장
                if i % CHECKPOINT_EVERY == 0:
                    df_work.to_csv(CHECKPOINT_CSV, index=False, encoding="utf-8-sig")
                    print(f"      >>> {i}건 처리 후 중간 저장 완료")
                
                time.sleep(0.5) # 서버 부하 방지
        except KeyboardInterrupt:
            print("\n🛑 사용자에 의해 중단되었습니다. 현재까지의 결과를 저장합니다.")
        
        # 최종 체크포인트 저장
        df_work.to_csv(CHECKPOINT_CSV, index=False, encoding="utf-8-sig")

    # 4. 기존 데이터와 병합
    print("\n[4/4] 기존 데이터셋과 병합 시작")
    if os.path.exists(EXISTING_CSV):
        df_old = pd.read_csv(EXISTING_CSV, encoding="utf-8-sig")
        print(f"      기존 데이터: {len(df_old)}행")
        
        # 추출 완료된 신규 데이터만 필터링
        df_new_done = df_work[df_work["review_keywords"].notna() & (df_work["review_keywords"] != "")].copy()
        
        # 컬럼 순서 맞추기 (기존 데이터 기준)
        common_cols = df_old.columns.tolist()
        df_new_final = df_new_done.reindex(columns=common_cols).fillna("")
        
        # 병합 (중복 제거 포함 - product_name + 본문내용 기준)
        merged = pd.concat([df_old, df_new_final], ignore_index=True)
        # 본문 내용이 같으면 중복으로 간주하여 제거 (신구 데이터 겹침 방지)
        merged = merged.drop_duplicates(subset=["product_name", "본문내용"], keep="last")
        
        merged.to_csv(FINAL_MERGED_CSV, index=False, encoding="utf-8-sig")
        print(f"✅ 병합 완료: {FINAL_MERGED_CSV}")
        print(f"      최종 합계: {len(merged)}행 (증분: {len(df_new_final)}행)")
    else:
        print(f"⚠️ 기존 데이터를 찾을 수 없어 신규 데이터만 저장합니다: {FINAL_MERGED_CSV}")
        df_work.to_csv(FINAL_MERGED_CSV, index=False, encoding="utf-8-sig")

    print("\n" + "=" * 60)
    print("모든 작업이 완료되었습니다.")
    print("=" * 60)

if __name__ == "__main__":
    main()
