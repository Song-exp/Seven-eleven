import pandas as pd
import numpy as np
import gc
import os

# 설정
INPUT_PATH  = 'pos_data_전처리완료_final.parquet'
OUTPUT_PATH = 'pos_data_food_final_상품단위변환전.parquet'

def preprocess():
    # 실행 위치 확인 및 디렉토리 설정
    # 현재 디렉토리에 파일이 없고 '최종' 폴더에 있다면 이동
    if not os.path.exists(INPUT_PATH) and os.path.exists('최종/' + INPUT_PATH):
        os.chdir('최종')
        print(f"디렉토리를 '최종'으로 이동하였습니다.")

    print(f"데이터 로딩을 시작합니다: {INPUT_PATH}")
    try:
        # 전체 데이터를 df_pos로 로드
        df_pos = pd.read_parquet(INPUT_PATH)
        print(f"로드 성공! (전체 행 수: {len(df_pos):,})")
    except Exception as e:
        print(f"로드 실패: {e}")
        return

    # 1. 객단가 생성 (Transformation)
    # 매출수량이 0인 경우를 대비해 replace 후 계산, 메모리 절약을 위해 float32 사용
    # 주의: 비정상 객단가 제거 로직은 포함하지 않음 (요청사항 반영)
    print("객단가(Unit Price) 계산 중...")
    df_pos['객단가'] = (df_pos['매출금액'] / df_pos['매출수량'].replace(0, np.nan)).astype('float32')
    gc.collect()

    # 2. 카테고리 필터링 (Filtering)
    food_categories = {
        '음료', '과자', '유음료', '미반', '면', '냉장', '맥주', '즉석음료', '빵', '전통주', 
        '아이스크림', '조리빵', '즉석 식품', '건강/기호식품', '가공식품', '양주와인', '디저트', 
        '안주', '신선', '간식', '조미료/건물', '냉동'
    }

    print("상품대분류명 공백 제거 및 식품 카테고리 필터링 중...")
    df_pos['상품대분류명'] = df_pos['상품대분류명'].str.strip()
    
    # [검증] 지정된 모든 카테고리가 데이터에 존재하는지 확인
    actual_categories = set(df_pos['상품대분류명'].unique())
    missing_categories = food_categories - actual_categories
    
    if missing_categories:
        print(f"  ⚠️ 주의: 지정된 카테고리 중 다음 {len(missing_categories)}개 항목이 데이터에 존재하지 않습니다: {missing_categories}")
    else:
        print(f"  ✅ 확인: 지정된 {len(food_categories)}개의 모든 식품 카테고리가 데이터 내에 존재함을 확인했습니다.")

    # 식품이 아닌 카테고리 인덱스 추출 후 삭제 (In-place)
    drop_indices = df_pos[~df_pos['상품대분류명'].isin(food_categories)].index
    df_pos.drop(drop_indices, inplace=True)
    
    print(f"식품 카테고리 필터링 완료 (남은 식품 행 수: {len(df_pos):,})")
    del drop_indices
    gc.collect()

    # 3. 각 대분류별 매출수량 상위 1% (Q99) 제거 (Core Logic)
    print("각 상품대분류별 매출수량 임계치(Q99) 계산 및 극단치 제거 중...")
    
    # 각 카테고리별 매출수량의 0.99 분위수(Q99) 계산
    qty_q99_dict = df_pos.groupby('상품대분류명')['매출수량'].quantile(0.99).to_dict()
    
    # 제거할 인덱스 추적
    outlier_indices = []
    for cat, q99_val in qty_q99_dict.items():
        # 해당 카테고리 내에서 해당 카테고리의 Q99를 초과하는 행의 인덱스 추출
        idx = df_pos[(df_pos['상품대분류명'] == cat) & (df_pos['매출수량'] > q99_val)].index
        outlier_indices.extend(idx)
    
    print(f"제거될 매출수량 극단치(각 카테고리별 Q99 초과) 행 수: {len(outlier_indices):,}")
    
    # 데이터 직접 삭제 (In-place)
    df_pos.drop(outlier_indices, inplace=True)
    
    print(f"최종 필터링 완료 (최종 행 수: {len(df_pos):,})")
    del outlier_indices, qty_q99_dict
    gc.collect()

    # 4. 결과 저장
    print(f"데이터 저장 중: {OUTPUT_PATH}")
    try:
        # 인덱스 제외하고 저장
        df_pos.to_parquet(OUTPUT_PATH, engine='pyarrow', index=False)
        print(f"저장 완료! (최종 경로: {os.path.abspath(OUTPUT_PATH)})")
    except Exception as e:
        print(f"저장 중 오류 발생: {e}")

if __name__ == "__main__":
    preprocess()
