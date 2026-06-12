import os
import sys
import time

# 프로젝트 루트 경로 추가
sys.path.append(os.getcwd())

# 각 스크립트의 main 함수 임포트
try:
    from src.data_builder.extract_expanded_ip_batch import main as run_ip_expanded_batch
    from src.data_builder.extract_product_attributes import main as run_product_attribute_batch
except ImportError as e:
    print(f"❌ 모듈 임포트 실패: {e}")
    sys.exit(1)

def run_master_pipeline():
    start_time = time.time()
    
    print("="*60)
    print("🌟 [통합 속성 추출 파이프라인] 전체 실행 시작")
    print("="*60)

    # 1단계: 확장 IP 사전 기반 속성 추출
    try:
        print("\n[Step 1] 확장 IP 사전 속성 추출 시작 (나무위키/네이버 검색 포함)...")
        run_ip_expanded_batch()
        print("\n✅ [Step 1] 완료!")
    except KeyboardInterrupt:
        print("\n⚠️ 사용자에 의해 Step 1 중단됨.")
        return
    except Exception as e:
        print(f"\n❌ [Step 1] 실행 중 오류 발생: {e}")
        print("잠시 대기 후 다음 단계로 진행합니다...")
        time.sleep(5)

    # 2단계: 상품 키워드 기반 속성 추출
    try:
        print("\n[Step 2] 상품 키워드 속성 추출 시작 (Few-Shot 추론)...")
        run_product_attribute_batch()
        print("\n✅ [Step 2] 완료!")
    except KeyboardInterrupt:
        print("\n⚠️ 사용자에 의해 Step 2 중단됨.")
    except Exception as e:
        print(f"\n❌ [Step 2] 실행 중 오류 발생: {e}")

    end_time = time.time()
    duration = (end_time - start_time) / 3600

    print("\n" + "="*60)
    print("✨ [모든 속성 추출 프로세스 종료]")
    print(f"⏱️ 총 소요 시간: {duration:.2f}시간")
    print("📂 IP 결과: data/processed/IP_속성추출/IP_속성.json")
    print("📂 상품 결과: data/processed/IP_속성추출/제품_속성.json")
    print("="*60)

if __name__ == "__main__":
    run_master_pipeline()
