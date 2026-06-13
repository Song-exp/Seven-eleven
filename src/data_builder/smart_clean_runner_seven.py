"""
smart_clean_runner_seven.py
세븐일레븐 미검수 제품 키워드 복합어 정제 배치 실행기

기존 smart_clean_result.xlsx(GS25·CU 검수 완료본)에 없는
세븐일레븐 제품만 골라 동일한 복합어 분해를 수행합니다.

사전 조건:
  - Ollama 실행 중 (localhost:11434), gemma4:e4b 모델 pull 완료

출력 (기존 파일과 분리):
  - smart_clean_checkpoint_seven.parquet
  - smart_clean_checkpoint_words_seven.json
  - smart_clean_result_seven.xlsx   ← 검수 후 df_compare_keywords.xlsx에 병합
      · 제품명 / 정제_전_키워드 / 확정_키워드 / 생존후보_키워드
"""

import ast
import json
import re
import requests
from collections import Counter
from pathlib import Path

import pandas as pd

# ── 경로 설정 ──────────────────────────────────────────────────────────────────
BASE_DIR  = Path(r"C:\Users\송정현\Documents\Projects\박재홍교수님세미나\Projects\20기\7eleven_npd_framework")
DATA_DIR  = BASE_DIR / "data" / "processed" / "편의점_instagram"

RAW_XLSX         = DATA_DIR / "merged_instagram_products_final.xlsx"
EXISTING_RESULT  = DATA_DIR / "smart_clean_result.xlsx"        # 기존 GS25·CU 결과
CHECKPOINT_PKL   = DATA_DIR / "smart_clean_checkpoint_seven.parquet"
CHECKPOINT_WORDS = DATA_DIR / "smart_clean_checkpoint_words_seven.json"
FINAL_PATH       = DATA_DIR / "smart_clean_result_seven.xlsx"

CHECKPOINT_EVERY = 10
TARGET_BRAND     = "세븐"

# ── Ollama 설정 ────────────────────────────────────────────────────────────────
OLLAMA_URL  = "http://localhost:11434/api/generate"
MODEL_NAME  = "gemma4:e4b"
TEMPERATURE = 0.0
TIMEOUT     = 180
CHUNK_SIZE  = 10

DECOMPOSE_PROMPT = """[Role]
당신은 한국어 형태소 분석에 능통한 최고 수준의 NLP 전문가이자, 편의점/식품 리테일 트렌드 데이터 분석가입니다.

[Task]
제공된 입력(Input) 데이터를 분석하여 (1) 단순 정제된 형태소는 `confirmed`에, (2) 도메인에서 가치 있는 최종 키워드는 `rescued`에 구분하여 추출하세요.

[Domain Context : 편의점/식품 트렌드 독립 키워드]
- 조리법 수식어: 군(구운), 볶음, 튀김, 찜, 절임, 생, 훈제
- 맛/식감 수식어: 꿀, 달달, 매콤, 고소, 쫄깃, 바삭, 촉촉, 진한, 담백
- 트렌드/맥락: 챌린지, 배달, 홈술, 혼밥, 캐릭터, 콜라보, 시즌, 한정
- 건강/웰니스 수식어: 건강, 저칼로리, 단백질, 비건, 유기농, 저당, 고단백
- 제품 특성 수식어: 믹스, 블렌드, 리얼, 다크, 딥, 더블, 트리플, 스페셜
- 원산지/브랜드 수식어: 두바이, 벨기에, 제주, 국내산 등 제품 정체성을 규정하는 명사
- 고유명사 및 IP (절대 분리 금지): 인물(최강록, 이장우, 추성훈 등), 캐릭터/콘텐츠(산리오캐릭터즈, 헬로키티, 하츄핑, 주토피아, K LEAGUE 등), 브랜드/기업(SK하이닉스, 아티제 등)

[Input Format]
index: compound="전체복합어", core="핵심어", remainder="나머지부분"

[Rules]
1. confirmed (정제된 형태소):
   - 입력값의 `remainder`에서 의미 있는 형태소(명사, 동사/형용사 어근, 영문)만 추출하여 배열로 만듭니다.
   - [Negative Rule] '-맛', '-용', '-형', '-식' 등의 접미사와 조사는 철저히 배제(삭제)합니다. 절대 포함하지 마세요.
2. rescued (보존/구출된 최종 키워드):
   - `core` 단어는 항상 `rescued`에 포함합니다.
   - `confirmed`에 포함된 수식어도 `rescued`에 함께 포함합니다.
   - [제외 예외] 수식어가 제품의 특성을 전혀 나타내지 않는 순수 범용 접두어(예: 가정용, 업소용, 고급형)이면 수식어는 제외하고 `core`만 담습니다.
   - [고유 합성어 예외 규칙] `compound`가 '군고구마'처럼 분리 시 의미가 훼손되는 고유 단어인 경우, 파편화된 수식어('군')는 제외하고 합성어 전체('군고구마')와 `core`('고구마')만 담습니다.
   - [★ IP/고유명사 절대 보존 규칙 ★] `compound`나 `remainder`에 인물명, 캐릭터명, 브랜드명, 엔터테인먼트 IP가 포함된 경우, 이를 절대 형태소 단위로 쪼개지 말고 원형 그대로 `rescued`와 `confirmed`에 보존하십시오.

[Examples]
Input:
0: compound="매콤떡볶이맛", core="떡볶이", remainder="매콤맛"
1: compound="군고구마", core="고구마", remainder="군"
2: compound="산리오캐릭터즈우유", core="우유", remainder="산리오캐릭터즈"
3: compound="가정용우유", core="우유", remainder="가정용"

Output:
{"0": {"confirmed": ["매콤"], "rescued": ["매콤", "떡볶이"]}, "1": {"confirmed": ["군"], "rescued": ["군고구마", "고구마"]}, "2": {"confirmed": ["산리오캐릭터즈"], "rescued": ["산리오캐릭터즈", "우유"]}, "3": {"confirmed": ["가정"], "rescued": ["우유"]}}

[Output Format - JSON ONLY, 마크다운 코드 블록(```json)이나 부연 설명 텍스트 절대 금지]

[Input]
"""


# ── 전처리 함수 (노트북 Phase 1A Step 2~5 재구현) ─────────────────────────────

def safe_parse_attrs(val):
    if pd.isna(val) or str(val).strip() in ('', '[]'):
        return []
    try:
        parsed = ast.literal_eval(str(val))
        return list(parsed) if isinstance(parsed, (list, tuple)) else [str(parsed)]
    except Exception:
        return [s.strip() for s in str(val).split(',') if s.strip()]


STOPWORDS = {
    '이벤트', '참여', '댓글', '팔로우', '당첨', '경품', '사전예약', '선착순', '증정', '할인',
    '행사', '원플러스원', '투플러스원', '1+1', '2+1', '공식', '계정', '어플', '앱',
    '포켓CU', '우리동네GS', '없음',
    '신상', '신제품', 'NEW', '추천', '인기', '대박', '출시', '한정판', '한정', '단독',
    '주목', '달려가세요', '쟁여두세요', '필수', '박스', '기획', '패키지', '에디션',
    '컬렉션', '시리즈',
    '세븐일레븐', 'CU', 'GS25', '씨유', '지에스', '편의점', '세븐', '편의점신상',
    '맛있다', '예쁘다', '좋아요', '강추', '비주얼', '꿀맛', '존맛', '미쳤다', '역대급', 'JMT',
}

def clean_keywords(attrs):
    if not isinstance(attrs, list):
        return []
    cleaned = []
    for kw in attrs:
        kw = str(kw).replace(' ', '').strip()
        if kw in STOPWORDS or len(kw) <= 1:
            continue
        if re.search(r'\d+(ml|g|kg|l|개|입|봉|팩|병|캔)', kw, re.I):
            continue
        cleaned.append(kw)
    return list(set(cleaned))


STRICT_TARGETS = ['경주', '고소', '짭잘', '골든', '공부', '안유성']
REMOVE_PARTS   = ['각종', '의맛']

def surgical_clean(attrs):
    if not isinstance(attrs, list):
        return []
    res = []
    for kw in attrs:
        kw = str(kw).strip()
        hit = next((t for t in STRICT_TARGETS if t in kw), None)
        if hit:
            res.append(hit)
            continue
        for part in REMOVE_PARTS:
            kw = kw.replace(part, '')
        if len(kw.strip()) > 1:
            res.append(kw.strip())
    return list(set(res))


def normalize_product_name(name):
    if not isinstance(name, str):
        return name
    name = re.sub(r'\[.*?\]|\(.*?\)', '', name)
    for word in ['신상', '한정판', '2+1', '1+1', '증정', '단독', '출시', 'NEW']:
        name = name.replace(word, '')
    name = re.sub(r'[^a-zA-Z0-9가-힣\s]', ' ', name)
    return re.sub(r'\s+', ' ', name).strip()


def build_input_df():
    """raw Excel에서 세븐일레븐 미검수 제품만 추출해 pre_smart_clean 형태로 반환."""
    raw = pd.read_excel(RAW_XLSX)
    existing_names = set(pd.read_excel(EXISTING_RESULT)['제품명'].astype(str))

    df = raw[raw['brand'] == TARGET_BRAND].copy()
    df['p_attrs_list']    = df['p_attrs'].apply(safe_parse_attrs)
    df['p_attrs_cleaned'] = df['p_attrs_list'].apply(clean_keywords)

    # 제품명 정규화 + 중복 병합
    df['p_name'] = df['p_name'].apply(normalize_product_name)
    df = (df.groupby('p_name', as_index=False)
            .agg({'p_attrs_cleaned': lambda s: list(set(kw for lst in s for kw in (lst if isinstance(lst, list) else [])))}))

    df['p_attrs_cleaned'] = df['p_attrs_cleaned'].apply(surgical_clean)
    df['p_attrs_rescued'] = [[] for _ in range(len(df))]

    # 이미 검수된 제품 제외
    df = df[~df['p_name'].isin(existing_names)].reset_index(drop=True)
    print(f"세븐일레븐 미검수 제품: {len(df)}개")
    return df


# ── Ollama 헬퍼 (smart_clean_runner.py와 동일) ─────────────────────────────────

def _parse_json_safe(raw: str) -> dict:
    raw = re.sub(r"```(?:json)?", "", raw).strip()
    start, end = raw.find("{"), raw.rfind("}")
    if start == -1 or end == -1:
        return {}
    raw = raw[start:end + 1]
    raw = re.sub(r",\s*([}\]])", r"\1", raw)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


def _call_ollama(entries: list) -> dict:
    lines = [
        f'{i}: compound="{e["compound"]}", core="{e["target"]}", remainder="{e["remainder"]}"'
        for i, e in enumerate(entries)
    ]
    payload = {
        "model":   MODEL_NAME,
        "prompt":  DECOMPOSE_PROMPT + "\n".join(lines),
        "stream":  False,
        "options": {"temperature": TEMPERATURE},
    }
    try:
        resp = requests.post(OLLAMA_URL, json=payload, timeout=TIMEOUT)
        resp.raise_for_status()
        return _parse_json_safe(resp.json().get("response", ""))
    except requests.exceptions.Timeout:
        print(f"  [decompose] 타임아웃 ({TIMEOUT}s)")
        return {}
    except Exception as e:
        print(f"  [decompose] 오류: {e}")
        return {}


def decompose_entries(entries: list) -> dict:
    result = {}
    for chunk_start in range(0, len(entries), CHUNK_SIZE):
        chunk = entries[chunk_start:chunk_start + CHUNK_SIZE]
        data  = _call_ollama(chunk)
        for i, e in enumerate(chunk):
            raw_item = data.get(str(i), {})
            if not isinstance(raw_item, dict):
                raw_item = {}
            result[e["compound"]] = {
                "confirmed": [m for m in raw_item.get("confirmed", []) if isinstance(m, str) and m.strip()],
                "rescued":   [m for m in raw_item.get("rescued",   []) if isinstance(m, str) and m.strip()],
            }
    return result


# ── 체크포인트 저장 ────────────────────────────────────────────────────────────

def save_checkpoint(df_original: pd.DataFrame, df_current: pd.DataFrame,
                    processed_words: set, cycle: int) -> None:
    df_current[["p_name", "p_attrs_cleaned", "p_attrs_rescued"]].to_parquet(
        CHECKPOINT_PKL, index=False
    )
    with open(CHECKPOINT_WORDS, "w", encoding="utf-8") as f:
        json.dump(sorted(processed_words), f, ensure_ascii=False, indent=2)

    result = pd.DataFrame({
        "제품명":         df_original["p_name"].values,
        "정제_전_키워드":  df_original["p_attrs_cleaned"].apply(
            lambda x: ", ".join(x) if isinstance(x, list) else ""
        ).values,
        "확정_키워드":    df_current["p_attrs_cleaned"].apply(
            lambda x: ", ".join(sorted(set(x))) if isinstance(x, list) else ""
        ).values,
        "생존후보_키워드": df_current["p_attrs_rescued"].apply(
            lambda x: ", ".join(sorted(set(x))) if isinstance(x, list) else ""
        ).values,
    })
    result.to_excel(FINAL_PATH, index=False)
    print(f"  💾 체크포인트 저장 (사이클 {cycle}, 처리 완료: {len(processed_words)}개)")


# ── 메인 실행 ──────────────────────────────────────────────────────────────────

def main():
    if CHECKPOINT_PKL.exists():
        print("📂 체크포인트 발견 — 이어서 실행합니다.")
        df = pd.read_parquet(CHECKPOINT_PKL)
        processed_words = set(
            json.load(open(CHECKPOINT_WORDS, encoding="utf-8"))
            if CHECKPOINT_WORDS.exists() else []
        )
        df_original = build_input_df()
        # 체크포인트 기준으로 df_original 정렬 맞추기
        df = df[df["p_name"].isin(set(df_original["p_name"]))].reset_index(drop=True)
    else:
        print("📂 처음 실행 — raw Excel에서 세븐일레븐 미검수 제품 로드")
        df = build_input_df()
        df_original = df.copy()
        processed_words = set()

    for col in ["p_attrs_cleaned", "p_attrs_rescued"]:
        if col not in df.columns:
            df[col] = [[] for _ in range(len(df))]
        df[col] = df[col].apply(lambda x: list(x) if not isinstance(x, list) else x)

    vocab_size = len(set(kw for lst in df["p_attrs_cleaned"] for kw in lst))
    print(f"vocab 크기: {vocab_size}개 | 이미 처리: {len(processed_words)}개\n")

    cycle_total = 0
    skipped     = 0

    while True:
        all_kws      = [kw for lst in df["p_attrs_cleaned"] for kw in lst]
        counts       = Counter(all_kws)
        global_vocab = set(counts.keys())

        remaining = [
            kw for kw, _ in counts.most_common()
            if kw not in processed_words and len(kw) > 1
        ]
        if not remaining:
            print(f"🏁 완료  (건너뜀: {skipped}개)")
            break

        active_this_batch = 0

        for target_kw in remaining:
            if target_kw in processed_words:
                continue

            derived_map = {
                kw: kw.replace(target_kw, "").strip()
                for kw in global_vocab
                if target_kw in kw and kw != target_kw
            }
            derived_map = {kw: rem for kw, rem in derived_map.items() if rem}

            processed_words.add(target_kw)

            if not derived_map:
                skipped += 1
                continue

            cycle_total += 1
            active_this_batch += 1
            print(f"\n🔥 [사이클 {cycle_total}] 타겟: '{target_kw}' (빈도 {counts[target_kw]}회)")
            print("=" * 60)

            seen_rems = {}
            entries = []
            for compound, rem in derived_map.items():
                if rem not in seen_rems:
                    seen_rems[rem] = compound
                entries.append({"compound": compound, "target": target_kw, "remainder": rem})

            analysis = decompose_entries(entries)

            clean_map   = {}
            rescued_map = {}

            for compound, rem in derived_map.items():
                result = analysis.get(compound, {"confirmed": [], "rescued": []})
                confirmed       = result["confirmed"]
                rescued         = result["rescued"]
                valid_confirmed = [m for m in confirmed if m in global_vocab]

                clean_list   = list(set([target_kw] + valid_confirmed))
                rescue_list  = [m for m in rescued if m not in global_vocab]

                clean_map[compound]   = clean_list
                rescued_map[compound] = rescue_list

                tag = "✔️ " if valid_confirmed or rescue_list else "❌ "
                print(f"  {tag} '{compound}'")
                print(f"      확정:     {clean_list}")
                if rescue_list:
                    print(f"      생존후보: {rescue_list}")

            def apply_clean(attr_list, cm=clean_map):
                out = []
                for item in attr_list:
                    out.extend(cm[item] if item in cm else [item])
                return list(set(out))

            def apply_rescue(attr_list, rm=rescued_map, existing_rescued=None):
                out = list(existing_rescued) if existing_rescued else []
                for item in attr_list:
                    if item in rm:
                        out.extend(rm[item])
                return list(set(out))

            df["p_attrs_cleaned"] = df["p_attrs_cleaned"].apply(apply_clean)
            df["p_attrs_rescued"] = [
                apply_rescue(orig, rescued_map, ex)
                for orig, ex in zip(df["p_attrs_cleaned"], df["p_attrs_rescued"])
            ]

            if active_this_batch >= CHECKPOINT_EVERY:
                print(f"\n  [진행] 처리 {cycle_total}회 | 건너뜀 {skipped}개")
                save_checkpoint(df_original, df, processed_words, cycle_total)
                break
        else:
            if active_this_batch > 0:
                save_checkpoint(df_original, df, processed_words, cycle_total)

    save_checkpoint(df_original, df, processed_words, cycle_total)
    vocab_after  = len(set(kw for lst in df["p_attrs_cleaned"] for kw in lst))
    rescued_all  = set(kw for lst in df["p_attrs_rescued"] for kw in lst)

    print(f"\n{'='*60}")
    print(f"✅ 완료")
    print(f"  처리 단어    : {len(processed_words)}개")
    print(f"  vocab 변화   : {vocab_size} → {vocab_after}개")
    print(f"  생존후보 총계 : {len(rescued_all)}개")
    print(f"  결과 저장    : {FINAL_PATH}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
