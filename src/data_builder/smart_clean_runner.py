"""
smart_clean_runner.py
인스타그램 키워드 복합어 정제 배치 실행기

사전 조건:
  - Ollama 실행 중 (localhost:11434), gemma4:e4b 모델 pull 완료
  - 노트북 셀 [Step 5.4-prep] 실행 완료 → pre_smart_clean.parquet 생성

출력:
  - smart_clean_checkpoint.parquet   : 10사이클마다 갱신 (재시작 복구용)
  - smart_clean_checkpoint_words.json: 처리 완료 단어 목록
  - smart_clean_result.xlsx          : 최종 결과
      · 제품명 / 정제_전 / 확정_키워드(vocab 매칭) / 생존후보(LLM 판단)
"""

import json
import re
import requests
from collections import Counter
from pathlib import Path

import pandas as pd

# ── 경로 설정 ──────────────────────────────────────────────────────────────────
BASE_DIR  = Path(r"C:\Users\송정현\Documents\Projects\박재홍교수님세미나\Projects\20기\7eleven_npd_framework")
DATA_DIR  = BASE_DIR / "data" / "processed" / "편의점_instagram"

INPUT_PATH       = DATA_DIR / "pre_smart_clean.parquet"
CHECKPOINT_PKL   = DATA_DIR / "smart_clean_checkpoint.parquet"
CHECKPOINT_WORDS = DATA_DIR / "smart_clean_checkpoint_words.json"
FINAL_PATH       = DATA_DIR / "smart_clean_result.xlsx"

CHECKPOINT_EVERY = 10   # 실제 Ollama 호출이 발생한 사이클 기준

# ── Ollama 설정 ────────────────────────────────────────────────────────────────
OLLAMA_URL  = "http://localhost:11434/api/generate"
MODEL_NAME  = "gemma4:e4b"
TEMPERATURE = 0.0
TIMEOUT     = 180
CHUNK_SIZE  = 10   # 한 번의 Ollama 호출당 최대 entries 수

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
   - [★ IP/고유명사 절대 보존 규칙 ★] `compound`나 `remainder`에 인물명, 캐릭터명, 브랜드명, 엔터테인먼트 IP(예: 산리오캐릭터즈, 흑백요리사, 하츄핑 등)가 포함된 경우, 이를 절대 형태소 단위(예: '산리오', '캐릭터', '즈' 등)로 쪼개지 말고 원형 그대로 `rescued`와 `confirmed`에 보존하십시오.

[Examples]
Input:
0: compound="매콤떡볶이맛", core="떡볶이", remainder="매콤맛"
1: compound="군고구마", core="고구마", remainder="군"
2: compound="산리오캐릭터즈우유", core="우유", remainder="산리오캐릭터즈"
3: compound="가정용우유", core="우유", remainder="가정용"
4: compound="최강록(흑백요리사)도시락", core="도시락", remainder="최강록(흑백요리사)"
5: compound="SK하이닉스스낵", core="스낵", remainder="SK하이닉스"

Output:
{"0": {"confirmed": ["매콤"], "rescued": ["매콤", "떡볶이"]}, "1": {"confirmed": ["군"], "rescued": ["군고구마", "고구마"]}, "2": {"confirmed": ["산리오캐릭터즈"], "rescued": ["산리오캐릭터즈", "우유"]}, "3": {"confirmed": ["가정"], "rescued": ["우유"]}, "4": {"confirmed": ["최강록(흑백요리사)"], "rescued": ["최강록(흑백요리사)", "도시락"]}, "5": {"confirmed": ["SK하이닉스"], "rescued": ["SK하이닉스", "스낵"]}}

[Output Format - JSON ONLY, 마크다운 코드 블록(```json)이나 부연 설명 텍스트 절대 금지]

[Input]
"""


# ── 헬퍼 함수 ──────────────────────────────────────────────────────────────────
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
    """entries 청크 하나를 Ollama에 호출. 오류 시 빈 dict 반환."""
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
        print(f"  [decompose] 타임아웃 ({TIMEOUT}s, {len(entries)}개 entries)")
        return {}
    except Exception as e:
        print(f"  [decompose] 오류: {e}")
        return {}


def decompose_entries(entries: list) -> dict:
    """
    복합어 리스트를 CHUNK_SIZE 단위로 나눠 Ollama 호출.

    entries: [{"compound": "군고구마맛", "target": "고구마", "remainder": "군맛"}, ...]
    반환:    {"군고구마맛": {"confirmed": [...], "rescued": [...]}, ...}
    """
    if not entries:
        return {}

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
    # parquet: cleaned + rescued 컬럼 저장 (재시작 복구용)
    df_current[["p_name", "p_attrs_cleaned", "p_attrs_rescued"]].to_parquet(
        CHECKPOINT_PKL, index=False
    )

    # json: 처리 완료 단어 목록
    with open(CHECKPOINT_WORDS, "w", encoding="utf-8") as f:
        json.dump(sorted(processed_words), f, ensure_ascii=False, indent=2)

    # excel: 사람이 읽을 수 있는 비교표 (확정 / 생존후보 분리)
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
    # 1. 데이터 로드 (체크포인트 우선)
    if CHECKPOINT_PKL.exists():
        print("📂 체크포인트 발견 — 이어서 실행합니다.")
        df = pd.read_parquet(CHECKPOINT_PKL)
        processed_words = set(
            json.load(open(CHECKPOINT_WORDS, encoding="utf-8"))
            if CHECKPOINT_WORDS.exists() else []
        )
        df_original = pd.read_parquet(INPUT_PATH)
    else:
        print("📂 처음 실행 — pre_smart_clean.parquet 로드")
        df = pd.read_parquet(INPUT_PATH)
        df_original = df.copy()
        processed_words = set()

    # 컬럼 타입 정규화
    for col in ["p_attrs_cleaned", "p_attrs_rescued"]:
        if col not in df.columns:
            df[col] = [[] for _ in range(len(df))]
        df[col] = df[col].apply(lambda x: list(x) if not isinstance(x, list) else x)

    vocab_size = len(set(kw for lst in df["p_attrs_cleaned"] for kw in lst))
    print(f"vocab 크기: {vocab_size}개 | 이미 처리: {len(processed_words)}개\n")

    # 2. 실행 루프 (파생어 있는 사이클 CHECKPOINT_EVERY개마다 저장)
    cycle_total  = 0
    skipped      = 0

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

            # 파생 키워드 수집 (없으면 조용히 스킵)
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

            # ── 실제 처리 ──────────────────────────────────────────────────
            cycle_total += 1
            active_this_batch += 1
            print(f"\n🔥 [사이클 {cycle_total}] 타겟: '{target_kw}' (빈도 {counts[target_kw]}회)")
            print("=" * 60)

            # 중복 remainder 제거 후 entries 구성
            seen_rems = {}
            entries = []
            for compound, rem in derived_map.items():
                if rem not in seen_rems:
                    seen_rems[rem] = compound
                    entries.append({"compound": compound, "target": target_kw, "remainder": rem})
                else:
                    # 같은 remainder를 가진 복합어는 대표 compound 결과를 재사용
                    entries.append({"compound": compound, "target": target_kw, "remainder": rem})

            analysis = decompose_entries(entries)

            # 교체 맵 생성
            clean_map   = {}   # compound → 확정 키워드 리스트
            rescued_map = {}   # compound → 생존후보 리스트

            for compound, rem in derived_map.items():
                result = analysis.get(compound, {"confirmed": [], "rescued": []})

                confirmed    = result["confirmed"]
                rescued      = result["rescued"]
                valid_confirmed = [m for m in confirmed if m in global_vocab]

                clean_list   = list(set([target_kw] + valid_confirmed))
                rescue_list  = [m for m in rescued if m not in global_vocab]

                clean_map[compound]   = clean_list
                rescued_map[compound] = rescue_list

                # 출력
                tag = "✔️ " if valid_confirmed or rescue_list else "❌ "
                print(f"  {tag} '{compound}'")
                print(f"      확정:     {clean_list}")
                if rescue_list:
                    print(f"      생존후보: {rescue_list}")

            # df 반영
            def apply_clean(attr_list, cm=clean_map):
                out = []
                for item in attr_list:
                    out.extend(cm[item] if item in cm else [item])
                return list(set(out))

            def apply_rescue(attr_list, rm=rescued_map, existing_rescued=None):
                # 기존 rescued 유지 + 새 rescued 추가
                out = list(existing_rescued) if existing_rescued else []
                for item in attr_list:
                    if item in rm:
                        out.extend(rm[item])
                return list(set(out))

            new_cleaned  = df["p_attrs_cleaned"].apply(apply_clean)
            new_rescued  = [
                apply_rescue(orig, rescued_map, ex)
                for orig, ex in zip(df["p_attrs_cleaned"], df["p_attrs_rescued"])
            ]
            df["p_attrs_cleaned"] = new_cleaned
            df["p_attrs_rescued"] = new_rescued

            # CHECKPOINT_EVERY 사이클마다 저장 후 vocab 재계산
            if active_this_batch >= CHECKPOINT_EVERY:
                print(f"\n  [진행] 처리 {cycle_total}회 | 건너뜀 {skipped}개")
                save_checkpoint(df_original, df, processed_words, cycle_total)
                break
        else:
            # for 루프가 break 없이 끝난 경우 → 마지막 배치 저장
            if active_this_batch > 0:
                save_checkpoint(df_original, df, processed_words, cycle_total)

    # 3. 최종 저장
    save_checkpoint(df_original, df, processed_words, cycle_total)
    vocab_after = len(set(kw for lst in df["p_attrs_cleaned"] for kw in lst))
    rescued_all = set(kw for lst in df["p_attrs_rescued"] for kw in lst)

    print(f"\n{'='*60}")
    print(f"✅ 완료")
    print(f"  처리 단어    : {len(processed_words)}개")
    print(f"  vocab 변화   : {vocab_size} → {vocab_after}개")
    print(f"  생존후보 총계 : {len(rescued_all)}개")
    print(f"  결과 저장    : {FINAL_PATH}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
