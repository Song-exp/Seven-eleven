import os
import sys
import json
import time
import requests
from tqdm import tqdm

# 기존 IP 추출 로직을 재사용하기 위해 경로 추가 및 임포트
sys.path.append(os.getcwd())
try:
    from src.data_builder.ip_attribute_extractor import (
        collect_enriched_text,
        _load_checkpoint,
        _save_checkpoint,
        CHECKPOINT_PATH,
        OUTPUT_JSON_PATH,
        MODEL_NAME,
        TEMPERATURE,
        TIMEOUT_LLM
    )
except ImportError:
    print("❌ src/data_builder/ip_attribute_extractor.py를 찾을 수 없습니다.")
    sys.exit(1)

# ─── 1. 확장 키워드 사전 (제공된 구조 그대로 반영) ──────────────────────────

GS25_COLLAB_EXPANDED = {
    "게임_IP": {
        "블루 아카이브": ["블루아카", "블루아카이브", "몰루", "아로나"],
        "메이플스토리": ["메이플스토리", "메이플", "핑크빈", "예티", "주황버섯", "슬라임"],
        "승리의 여신: 니케": ["니케", "NIKKE", "시프트업"],
        "명조": ["명조", "워더링웨이브", "워더링"],
        "명일방주": ["명일방주", "로도스"],
    },
    "K팝_아이돌": {
        "세븐틴": ["세븐틴", "SVT", "SEVENTEEN", "캐럿"],
        "PLAVE": ["플레이브", "PLAVE", "예준", "노아", "밤비", "은호", "하민"],
    },
    "콘텐츠_IP": {
        "진격의 거인": ["진격의거인", "진격거", "리바이"],
        "흑백요리사": ["흑백요리사", "요리계급전쟁", "백수저", "흑수저", "급식대가", "나폴리맛피아", "이모카세", "안성재", "백종원"],
        "무신사": ["무신사", "무탠다드"],
        "몬치치": ["몬치치"],
        "EBSi": ["EBSi", "EBS"],
        "젼언니": ["젼언니", "멜로우빈"],
        "경동시장": ["경동시장"],
    }
}

CU_COLLAB_EXPANDED = {
    "게임_IP": {
        "닌텐도 피크민": ["피크민", "PIKMIN", "닌텐도"],
        "디아블로 IV": ["디아블로", "디아블로4", "성역"],
        "T1": ["T1", "페이커", "FAKER", "제오페구케"],
    },
    "K팝_아이돌": {
        "TXT": ["투모로우바이투게더", "TXT", "투바투", "MOA"],
        "지드래곤": ["피스마이너스원", "지드래곤", "GD", "권지용", "PMO", "데이지"],
        "스트레이키즈": ["스트레이키즈", "StrayKids", "스키즈", "SKZ"],
        "이세계아이돌": ["이세계아이돌", "이세돌", "아이네", "징버거", "릴파", "주르르", "고세구", "비챤"],
    },
    "캐릭터_IP": {
        "포켓몬스터": ["포켓몬", "포켓몬스터", "피카츄", "메타몽", "띠부씰"],
        "스누피": ["스누피", "피너츠", "찰리브라운"],
        "짱구": ["짱구", "짱구는못말려", "흰둥이", "짱아", "초코비", "못말려"],
        "가나디": ["가나디", "GANADI"],
        "티니핑": ["티니핑", "캐치티니핑", "하츄핑"],
        "빵빵이": ["빵빵이", "옥지"],
        "해리스트위드": ["해리스트위드"],
    },
    "F&B_브랜드": {
        "백종원": ["백종원", "백주부", "더본코리아"],
        "명륜진사갈비": ["명륜진사", "진사갈비"],
        "노티드": ["노티드", "Knotted", "슈가베어"],
        "비비고": ["비비고", "BIBIGO"],
        "오징어게임": ["오징어게임", "성기훈"],
    }
}

# ─── 2. 개선된 프롬프트 (다양한 층위 커버) ────────────────────────────────
EXPANDED_SYSTEM_PROMPT = """
당신은 IP 콜라보레이션 기획 및 네트워크 데이터 구조화 전문가입니다.
주어진 [참고 텍스트]를 바탕으로 분석 대상 IP인 **[{ip_name}]**(대분류: **[{category_hint}]**)의 핵심 속성을 분석하세요.

[핵심 미션: 범용적 디자인/컨셉 키워드 도출]
참고 텍스트에서 이 IP를 '이종 산업의 상품(패키지 디자인, 맛, 브랜드 컨셉 등)'으로 치환했을 때 추출될 수 있는 **범용적 이미지/감성 키워드**만 뽑으세요.

[분석 및 추론 규칙]
1. 카테고리별 특성을 살리되, 최종 키워드는 '추상화/범용화' 과정을 거쳐야 합니다.
   - [주의 예시]: 'CARAT(세븐틴 팬덤)' -> '청량함', '열정' / '학원도시(블루아카이브)' -> '학교', '청춘', '귀여움'
2. 아래의 [범용 키워드 가이드]를 참고하여 타겟 IP가 지닌 분위기를 일반화하세요.
   - 감성/무드: 귀여움, 힙함, 키치함, 청량함, 레트로, 미니멀, 화려함, 몽환적, 따뜻함, 시크함, 다크함
   - 라이프스타일/TPO: 일상, 힐링, 스트레스해소, 당충전, 홈파티, 소확행, 트렌디, 에너제틱, 스포티
   - 미각/시각 연상: 달콤함, 상큼함, 부드러움, 강렬함, 깔끔함, 매콤함, 다채로움
3. [고유명사 절대 배제 규칙 - 매우 중요] 
   - IP의 이름(예: 세븐틴, 블루아카이브), 캐릭터명, 팬덤명(예: CARAT), 세계관 내 고유명사는 "절대" 키워드 리스트에 포함하지 마세요. 
   - 오직 디자인이나 맛으로 치환 가능한 범용적인 감성 어휘만 허용됩니다.
4. [복합어 보존 규칙 - 매우 중요] 
   - 널리 쓰이는 범용적 복합어(예: '새콤달콤', '단짠단짠', '시즌한정')는 그 자체로 의미를 가집니다. 임의로 단어를 쪼개지 마세요.

[출력 포맷 고정]
- 반드시 아래의 순수 JSON 형식으로만 응답하세요. 
- 마크다운 기호(```json 등)나 인사말, 부연 설명은 "절대" 출력하지 마십시오.

{{
  "ip_type": "다음 중 가장 적합한 1개만 선택 (캐릭터·마스코트, K팝아이돌, 배우·인플루언서, 콘텐츠(영화·애니·게임), 브랜드·F&B)",
  "target_age": ["다음 중 해당되는 것을 모두 선택 (10대이하, 2030, 40대이상, 전연령)"],
  "signature_keywords": ["범용적 감성 및 이미지 키워드 5~8개"]
}}
"""
# ─── 3. 실행 로직 ──────────────────────────────────────────────────────────

def extract_ip_attributes_expanded(ip_name, text, category_hint):
    """카테고리 힌트를 포함하여 LLM 속성 추론 실행"""
    url = "http://localhost:11434/api/generate"
    prompt = f"{EXPANDED_SYSTEM_PROMPT.format(ip_name=ip_name, category_hint=category_hint)}\n\n[참고 텍스트]:\n{text}"
    
    payload = {
        "model": MODEL_NAME,
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "options": {"temperature": TEMPERATURE}
    }
    
    try:
        response = requests.post(url, json=payload, timeout=TIMEOUT_LLM)
        response.raise_for_status()
        return json.loads(response.json().get("response", "{}"))
    except Exception as e:
        print(f"  [!] '{ip_name}' LLM 추론 중 오류 발생: {e}")
        return None

def main():
    print("🚀 확장 IP 사전 기반 속성 추출 및 통합 프로세스 시작...")

    # 1. 딕셔너리에서 타겟 IP 리스트 추출 (IP명, 카테고리 힌트)
    targets = []
    for d in [GS25_COLLAB_EXPANDED, CU_COLLAB_EXPANDED]:
        for category_name, content in d.items():
            for ip_name in content.keys():
                targets.append((ip_name, category_name))

    # 2. 기존 결과(txt 기반) 로드
    checkpoint = _load_checkpoint(CHECKPOINT_PATH)
    print(f"[*] 기존 추출 데이터 로드 완료 ({len(checkpoint)}개 IP)")

    # 3. 신규 IP 필터링 (중복 방지)
    todo = [t for t in targets if t[0] not in checkpoint]
    print(f"[*] 새로 추출할 신규 IP: {len(todo)}개")

    if not todo:
        print("✅ 새로 추가할 IP가 없습니다. 모든 작업이 이미 완료되었습니다.")
        return

    # 4. 추출 진행
    for ip_name, category in tqdm(todo, desc="Processing Expanded IPs"):
        # 텍스트 수집 (나무위키/네이버)
        wiki_text, source = collect_enriched_text(ip_name)
        
        # LLM 속성 추론 (층위별 프롬프트 적용)
        attrs = extract_ip_attributes_expanded(ip_name, wiki_text, category)
        
        if attrs:
            # 결과 기록 (기존 구조와 동일하게 유지하여 통합)
            record = {
                "keyword": ip_name,
                "category_hint": category,
                "wiki_text_snippet": wiki_text[:300] if wiki_text else "",
                "text_source": source,
                **attrs
            }
            
            # 체크포인트 및 최종 JSON에 실시간 저장 (통합)
            checkpoint[ip_name] = record
            _save_checkpoint(CHECKPOINT_PATH, checkpoint)
            
            # 최종 JSON 파일 업데이트
            with open(OUTPUT_JSON_PATH, "w", encoding="utf-8") as f:
                json.dump(list(checkpoint.values()), f, ensure_ascii=False, indent=2)
            
            # 안정적인 처리를 위해 잠시 대기
            time.sleep(0.5)

    print(f"\n✨ 모든 작업이 완료되었습니다! 총 {len(checkpoint)}개의 IP 속성이 통합 저장되었습니다.")
    print(f"📂 결과 확인: {OUTPUT_JSON_PATH}")

if __name__ == "__main__":
    main()
