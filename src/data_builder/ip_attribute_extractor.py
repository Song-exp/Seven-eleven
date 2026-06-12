"""
IP 속성 추출 파이프라인

data/processed/IP_속성추출/ 폴더의 모든 .txt 파일을 스캔하여:
  1. 나무위키 텍스트 수집 (실패 시 Naver 통합검색 폴백)
  2. Ollama LLM으로 8종 속성 추출
  3. JSON/Parquet 저장 (체크포인트 지원, 중단 후 재시작 가능)

실행:
  python src/data_builder/ip_attribute_extractor.py           # 전체 배치
  python src/data_builder/ip_attribute_extractor.py --sample  # 샘플 3개 테스트
  python src/data_builder/ip_attribute_extractor.py --keyword 블랙핑크
"""

import json
import os
import re
import sys
import time
from html.parser import HTMLParser

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()
sys.stdout.reconfigure(encoding="utf-8")

# ─── 설정 ────────────────────────────────────────────────────────────────────

MODEL_NAME = "gemma4:26b"
TEMPERATURE = 0.1
TIMEOUT_LLM = 600  # 26b 타임아웃 대응: 10분
TIMEOUT_HTTP = 15
WIKI_TEXT_LIMIT = 2000   # 나무위키 개요 핵심만 (소형 LLM 과부하 방지)
NAVER_TEXT_LIMIT = 1000  # Naver 블로그/뉴스 → 대중 언어로 signature_keywords 보강

IP_FOLDER = "data/processed/IP_속성추출"
CHECKPOINT_PATH = f"{IP_FOLDER}/ip_attributes_checkpoint.json"
OUTPUT_JSON_PATH = f"{IP_FOLDER}/IP_속성.json"
OUTPUT_PARQUET_PATH = f"{IP_FOLDER}/IP_속성.parquet"
OUTPUT_KW_JSON_PATH = f"{IP_FOLDER}/IP_키워드사전.json"
RERUN_JSON_PATH = f"{IP_FOLDER}/IP_속성_재추출.json"
RERUN_PARQUET_PATH = f"{IP_FOLDER}/IP_속성_재추출.parquet"

# 속성별 허용 어휘 (프롬프트 제약용)
ALLOWED_VALUES = {
    "ip_type": ["캐릭터·마스코트", "K팝아이돌", "배우·인플루언서", "콘텐츠(영화·애니·예능)", "브랜드·기타"],
    "target_age": ["10대이하", "2030", "40대이상", "전연령"],
}

SYSTEM_PROMPT = """
당신은 IP 속성 추출 전문가입니다. 주어진 [IP 이름]과 [참고 텍스트]를 분석하여 JSON 형식으로만 응답하세요.

[출력 포맷 고정]
{
  "ip_type": "하나의 값 선택 (캐릭터·마스코트, K팝아이돌, 배우·인플루언서, 콘텐츠(영화·애니·예능), 브랜드·기타)",
  "target_age": ["리스트로 선택 (10대이하, 2030, 40대이상, 전연령)"],
  "signature_keywords": ["단독 명사 키워드 5~8개"]
}

[signature_keywords 정제 원칙 — 반드시 순서대로 적용]
① 수식어+명사 복합 표현 → 구성 요소 각각으로 분리
   형용사/수식어와 명사를 독립 키워드로 쪼갠다.
   예) 가벼운 식감 → 가벼움, 식감 / 고소한 풍미 → 고소함, 풍미 / 겉바속촉 → 바삭함, 촉촉함

② 서술·부가 표현 제거 → 핵심 명사 하나로 축소
   강조어, 동사, 부연 설명을 걷어내고 명사 한 단어만 남긴다.
   예) 감칠맛 폭발 → 감칠맛 / 건강한재료 → 건강 / 계절감 있는 → 계절

③ 유사어·표기 변형 → 대표 키워드로 통일
   같은 의미의 다른 표기를 하나로 통일하여 중복을 방지한다.
   예) 달걀 → 계란 / 계피 → 시나몬 / GenZ → 젠지 / 골든 → 골드

④ 브랜드·팀명·상품명 → 상위 카테고리로 대체
   특정 고유명사는 그것이 속하는 범주로 대체한다.
   예) NC다이노스·SSG랜더스 → KBO / T멤버십 → SKT / 구매이벤트 → 프로모션

⑤ 삭제 대상 (채택 불가)
   너무 서술적이거나, 모호하거나, 명사화하기 어려운 표현은 제거한다.
   예) 겹겹이, 결 살아있는 식감, DHA/EPA → 삭제

[추가 필수 규칙]
- signature_keywords는 IP 고유의 분위기·이미지·특징을 나타내는 단독 명사 5~8개로 구성할 것.
- JSON 외에 어떤 텍스트나 코드 블록도 출력하지 마십시오.
- 타 브랜드명이나 부정적 이슈 키워드는 배제하십시오.

[Few-Shot]
IP: 짱구는 못말려
참고: 일본의 국민 애니메이션. 짱구가 초코비라는 과자를 즐겨 먹음. 엉뚱하고 장난꾸러기 같은 행동이 특징.
{"ip_type": "콘텐츠(영화·애니·예능)", "target_age": ["10대이하", "2030"], "signature_keywords": ["초코비", "귀여움", "엉뚱함", "키치함", "장난기"]}
※ '장난꾸러기' → ②원칙 적용 → '장난기'로 축소

IP: 세븐틴
참고: 13인조 K팝 그룹. 강렬한 퍼포먼스와 팬들의 열정적인 응원으로 유명. SM이 아닌 플레디스 소속.
{"ip_type": "K팝아이돌", "target_age": ["2030"], "signature_keywords": ["퍼포먼스", "팀워크", "에너지", "카리스마", "힙함"]}
※ '강렬한 퍼포먼스' → ①원칙: '퍼포먼스'만 채택(강렬함은 카리스마로 흡수) / '플레디스' → ④원칙: 배제
"""


# ─── HTML → 텍스트 변환 (pipeline1_price_fill.py 패턴 재사용) ─────────────────

class _StripHTML(HTMLParser):
    def __init__(self):
        super().__init__()
        self._parts = []
        self._skip = False

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style", "head", "nav", "footer"):
            self._skip = True

    def handle_endtag(self, tag):
        if tag in ("script", "style", "head", "nav", "footer"):
            self._skip = False

    def handle_data(self, data):
        if not self._skip and data.strip():
            self._parts.append(data.strip())

    def get_text(self):
        return " ".join(self._parts)


def html_to_text(html: str) -> str:
    p = _StripHTML()
    try:
        p.feed(html)
        return p.get_text()
    except Exception:
        return re.sub(r"<[^>]+>", " ", html)


# ─── 나무위키 크롤링 ─────────────────────────────────────────────────────────

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9",
}


def _clean_wiki_noise(text: str) -> str:
    """나무위키 텍스트에서 불필요한 리스트/표 노이즈 제거."""
    lines = text.split(" ")  # html_to_text가 공백으로 join하므로 공백 기준 분리
    cleaned = []
    for part in lines:
        part = part.strip()
        if not part:
            continue
        # 날짜/숫자로만 구성된 긴 패턴 제외 (앨범 목록 등)
        if re.match(r"^[\d\.\s\-]{8,}$", part):
            continue
        # 표 구성요소로 보이는 짧은 반복 패턴 제외
        if "|" in part and len(part) < 20:
            continue
        cleaned.append(part)
    return " ".join(cleaned)


def fetch_namuwiki_text(keyword: str, retries: int = 2, backoff: float = 20.0) -> str:
    """나무위키에서 키워드 본문 텍스트 수집. 노이즈 필터링 추가."""
    url = f"https://namu.wiki/w/{requests.utils.quote(keyword)}"
    for attempt in range(1 + retries):
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=TIMEOUT_HTTP)
            if resp.status_code == 200:
                text = html_to_text(resp.text)
                # 핵심 섹션부터 시작하도록 절단
                match = re.search(r"(개요|소개|프로필|설명|1\.\s)", text)
                if match:
                    text = text[match.start():]

                text = re.sub(r"\[편집\]|\[추가\]|\[수정\]", "", text)
                text = _clean_wiki_noise(text)
                return text[:WIKI_TEXT_LIMIT].strip()
        except requests.exceptions.Timeout:
            if attempt < retries:
                wait = backoff * (attempt + 1)
                print(f"  [나무위키 타임아웃] {keyword} → {wait:.0f}초 대기 후 재시도 ({attempt+1}/{retries})")
                time.sleep(wait)
            else:
                print(f"  [나무위키 최종 실패] {keyword}: 재시도 소진")
        except Exception as e:
            print(f"  [나무위키 오류] {keyword}: {e}")
            break
    return ""


# ─── Naver 검색 (나무위키와 항상 병합) ──────────────────────────────────────

def _fetch_naver_text(keyword: str) -> str:
    """Naver 블로그/뉴스에서 일반 대중의 IP 묘사 텍스트 수집."""
    cid = os.getenv("NAVER_CLIENT_ID", "")
    csec = os.getenv("NAVER_CLIENT_SECRET", "")
    if not cid or not csec:
        return ""

    combined_text = []
    for stype in ["blog", "news"]:
        try:
            resp = requests.get(
                f"https://openapi.naver.com/v1/search/{stype}.json",
                headers={"X-Naver-Client-Id": cid, "X-Naver-Client-Secret": csec},
                params={"query": keyword, "display": 5, "sort": "sim"},
                timeout=10,
            )
            if resp.status_code == 200:
                for item in resp.json().get("items", []):
                    snippet = (
                        item.get("title", "") + " " + item.get("description", "")
                    ).replace("<b>", "").replace("</b>", "")
                    combined_text.append(snippet)
        except Exception:
            continue

    return " ".join(combined_text)[:NAVER_TEXT_LIMIT].strip()


def collect_enriched_text(keyword: str) -> tuple[str, str]:
    """
    나무위키 + Naver 블로그/뉴스를 항상 병합하여 반환.
    - 나무위키: IP 정보·특징 (최대 4000자)
    - Naver: 일반인의 IP 묘사 언어 → signature_keywords 품질 향상 (최대 1000자)
    """
    wiki_text = fetch_namuwiki_text(keyword)
    naver_text = _fetch_naver_text(keyword)

    if wiki_text and naver_text:
        combined = f"[나무위키]\n{wiki_text}\n\n[대중 반응]\n{naver_text}"
        return combined, "namuwiki+naver"
    elif wiki_text:
        return wiki_text, "namuwiki"
    elif naver_text:
        return naver_text, "naver_only"
    return "", "none"


# ─── LLM 속성 추출 (attribute_inferrer.py 패턴 재사용) ──────────────────────

def _parse_llm_output(raw: str) -> dict:
    """LLM 출력 문자열(JSON) → 속성 딕셔너리 파싱. 실패 시 raw_response에 원문 저장."""
    raw = raw.strip()
    # format:json 모드에서는 raw 자체가 유효한 JSON
    try:
        return json.loads(raw)
    except Exception:
        pass
    # 마크다운 코드블록 안에 JSON이 있는 경우 (```json ... ```)
    try:
        json_match = re.search(r"(\{.*\})", raw, re.DOTALL)
        if json_match:
            return json.loads(json_match.group(1))
    except Exception:
        pass
    print(f"  [파싱 실패 → raw 저장] {raw[:80]}...")
    return {"raw_response": raw}


def extract_ip_attributes(keyword: str, wiki_text: str, hint: str = "") -> dict:
    """Ollama LLM으로 IP 속성 8종 추출."""
    hint_line = f"\n힌트: {hint}" if hint else ""
    prompt = (
        f"{SYSTEM_PROMPT}\n\n"
        f"IP: {keyword}{hint_line}\n"
        f"참고: {wiki_text if wiki_text else '정보 없음'}\n"
        f"출력:"
    )
    payload = {
        "model": MODEL_NAME,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": TEMPERATURE},
    }
    try:
        resp = requests.post(
            "http://localhost:11434/api/generate",
            json=payload,
            timeout=TIMEOUT_LLM,
        )
        resp.raise_for_status()
        raw = resp.json().get("response", "").strip()
        return _parse_llm_output(raw)
    except requests.exceptions.Timeout:
        print(f"  [LLM 타임아웃] {keyword}")
    except Exception as e:
        print(f"  [LLM 오류] {keyword}: {e}")
    return {}


# ─── 키워드 파싱 ────────────────────────────────────────────────────────────

def _parse_txt_file(txt_path: str) -> list[str]:
    """단일 .txt 파일에서 키워드 리스트 추출 (헤더 라인 제외).
    'IP명:컨텍스트' 형식이면 IP명만 반환 (컨텍스트는 _parse_txt_file_with_context 사용).
    """
    with open(txt_path, encoding="utf-8") as f:
        lines = f.readlines()

    keywords = []
    for line in lines:
        line = line.strip()
        if not line or line.startswith("IP/") or line.startswith("IP "):
            continue
        for kw in line.split(","):
            kw = kw.split(":")[0].strip()  # 콜론 이후 컨텍스트 제거
            if kw:
                keywords.append(kw)
    return keywords


def _parse_txt_file_with_context(txt_path: str) -> list[tuple[str, str]]:
    """단일 .txt 파일에서 (키워드, 컨텍스트) 튜플 리스트 추출.
    'IP명:컨텍스트' 형식이면 컨텍스트 포함, 아니면 컨텍스트는 빈 문자열.
    """
    with open(txt_path, encoding="utf-8") as f:
        lines = f.readlines()

    results = []
    for line in lines:
        line = line.strip()
        if not line or line.startswith("IP/") or line.startswith("IP "):
            continue
        for entry in line.split(","):
            entry = entry.strip()
            if not entry:
                continue
            if ":" in entry:
                kw, ctx = entry.split(":", 1)
                results.append((kw.strip(), ctx.strip()))
            else:
                results.append((entry, ""))
    return results


def scan_ip_folder(folder: str) -> list[tuple[str, str]]:
    """
    폴더 내 모든 .txt 파일을 스캔하여 (keyword, source_name) 튜플 리스트 반환.
    같은 키워드가 여러 파일에 있을 경우 첫 번째 출처만 유지(중복 제거).
    """
    seen: dict[str, str] = {}  # keyword → source
    for fname in sorted(os.listdir(folder)):
        if not fname.endswith(".txt"):
            continue
        source_name = os.path.splitext(fname)[0]  # 확장자 제거한 파일명
        fpath = os.path.join(folder, fname)
        for kw in _parse_txt_file(fpath):
            if kw not in seen:
                seen[kw] = source_name

    return list(seen.items())  # [(keyword, source), ...]


# ─── 체크포인트 유틸 ────────────────────────────────────────────────────────

def _load_checkpoint(path: str) -> dict:
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {}


def _save_checkpoint(path: str, data: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ─── 배치 처리 메인 ─────────────────────────────────────────────────────────

def batch_extract_all_ips(
    ip_folder: str = IP_FOLDER,
    output_json: str = OUTPUT_JSON_PATH,
    output_parquet: str = OUTPUT_PARQUET_PATH,
    checkpoint_path: str = CHECKPOINT_PATH,
) -> None:
    kw_source_pairs = scan_ip_folder(ip_folder)
    print(f"총 {len(kw_source_pairs)}개 키워드 발견 (폴더: {ip_folder})")

    # 출처별 키워드 수 요약
    from collections import Counter
    src_counts = Counter(src for _, src in kw_source_pairs)
    for src, cnt in sorted(src_counts.items()):
        print(f"  {src}: {cnt}개")

    checkpoint = _load_checkpoint(checkpoint_path)
    done = set(checkpoint.keys())
    remaining = [(kw, src) for kw, src in kw_source_pairs if kw not in done]
    print(f"이미 처리: {len(done)}개 → 남은 키워드: {len(remaining)}개")

    for i, (kw, file_source) in enumerate(remaining, 1):
        total = len(kw_source_pairs)
        print(f"[{len(done) + i}/{total}] {kw} ({file_source}) 처리 중...")

        wiki_text, text_source = collect_enriched_text(kw)
        attrs = _normalize_attrs(extract_ip_attributes(kw, wiki_text))

        record = {
            "keyword": kw,
            "file_source": file_source,
            "wiki_text_snippet": wiki_text[:300] if wiki_text else "",
            "text_source": text_source,
            **attrs,
        }
        checkpoint[kw] = record
        _save_checkpoint(checkpoint_path, checkpoint)

        kws_preview = ", ".join(attrs.get("signature_keywords", [])[:5])
        print(f"  → ip_type={attrs.get('ip_type', '?')} | age={attrs.get('target_age', '?')} | keywords=[{kws_preview}]")
        time.sleep(1.0)

    # 최종 결과 저장
    # 모든 레코드에 대해 정규화 재수행 및 스키마 강제 고정 (과거 오염된 체크포인트 데이터 정화)
    clean_records = []
    for raw_rec in checkpoint.values():
        norm = _normalize_attrs(raw_rec)
        clean_rec = {
            "keyword": raw_rec.get("keyword", ""),
            "file_source": raw_rec.get("file_source", ""),
            "wiki_text_snippet": raw_rec.get("wiki_text_snippet", ""),
            "text_source": raw_rec.get("text_source", ""),
            "ip_type": norm.get("ip_type", ""),
            "target_age": norm.get("target_age", []),
            "signature_keywords": norm.get("signature_keywords", []),
        }
        clean_records.append(clean_rec)

    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(clean_records, f, ensure_ascii=False, indent=2)
    print(f"\n[완료] JSON 저장: {output_json}")

    df = pd.DataFrame(clean_records)
    df.to_parquet(output_parquet, index=False)
    print(f"[완료] Parquet 저장: {output_parquet} ({len(df)}행)")

    # IP : 속성키워드 사전 저장 (2컬럼)
    kw_dict = [
        {"IP": r["keyword"], "속성키워드": r.get("signature_keywords", [])}
        for r in clean_records
        if r.get("signature_keywords")
    ]
    with open(OUTPUT_KW_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(kw_dict, f, ensure_ascii=False, indent=2)
    print(f"[완료] IP 키워드사전 저장: {OUTPUT_KW_JSON_PATH} ({len(kw_dict)}개)")

    # 간단한 품질 리포트
    print("\n── 속성 null 비율 ──")
    for col in ["ip_type", "target_age", "signature_keywords"]:
        if col in df.columns:
            if col in ["target_age", "signature_keywords"]:
                empty_rate = (df[col].apply(len) == 0).mean() * 100
            else:
                empty_rate = (df[col] == "").mean() * 100
            print(f"  {col}: empty/null {empty_rate:.1f}%")


# ─── 속성 정규화 (스키마 고정 및 타입 보장) ──────────────────────────────

def _normalize_attrs(attrs: dict) -> dict:
    """
    target_age·signature_keywords가 항상 list가 되도록 보장.
    LLM의 흔한 오타/유사 키워드(target_audience 등) 폴백 처리.
    """
    result = {}
    
    # 1. ip_type
    it = attrs.get("ip_type", attrs.get("category", attrs.get("type", "")))
    result["ip_type"] = str(it) if it else ""

    # 2. target_age (target_audience 폴백)
    age = attrs.get("target_age")
    if age is None:
        age = attrs.get("target_audience")
    
    if isinstance(age, list):
        result["target_age"] = age
    elif isinstance(age, str) and age:
        result["target_age"] = [age]
    else:
        result["target_age"] = []

    # 3. signature_keywords (keywords, keywords_list 폴백)
    kws = attrs.get("signature_keywords")
    if kws is None:
        kws = attrs.get("keywords") or attrs.get("keywords_list") or attrs.get("suggested_keywords")
    
    if isinstance(kws, list):
        result["signature_keywords"] = kws
    elif isinstance(kws, str) and kws:
        # 콤마로 구분된 문자열인 경우 분리
        if "," in kws:
            result["signature_keywords"] = [k.strip() for k in kws.split(",") if k.strip()]
        else:
            result["signature_keywords"] = [kws]
    else:
        result["signature_keywords"] = []
        
    return result


# ─── 재추출 배치 (체크포인트 무시, 별도 파일 저장) ─────────────────────────

def batch_rerun_specific_ips(
    ip_list_file: str,
    output_json: str = RERUN_JSON_PATH,
    output_parquet: str = RERUN_PARQUET_PATH,
    max_retries: int = 2,
) -> None:
    """
    지정 txt 파일의 IP만 강제 재추출 (체크포인트 무시).
    - output_json이 이미 있으면 로드하여 기존 성공 결과를 보존
    - signature_keywords가 비어 있으면 max_retries 횟수만큼 재시도
    - 성공 시마다 즉시 저장 (중단 후 재시작 안전)
    """
    kw_ctx_pairs = _parse_txt_file_with_context(ip_list_file)
    if not kw_ctx_pairs:
        print(f"[오류] {ip_list_file} 에서 키워드를 읽지 못했습니다.")
        return

    print(f"재추출 대상: {len(kw_ctx_pairs)}개 (파일: {ip_list_file})")
    for kw, ctx in kw_ctx_pairs:
        ctx_str = f"  힌트: {ctx}" if ctx else ""
        print(f"  · {kw}{ctx_str}")

    # 기존 결과 로드 → 성공 결과 보존
    existing: dict = {}
    if os.path.exists(output_json):
        with open(output_json, encoding="utf-8") as f:
            for r in json.load(f):
                existing[r["keyword"]] = r
        print(f"기존 재추출 결과 로드: {len(existing)}개")

    file_source = os.path.splitext(os.path.basename(ip_list_file))[0]
    failed = []

    for i, (kw, ctx) in enumerate(kw_ctx_pairs, 1):
        ctx_str = f" (힌트: {ctx})" if ctx else ""
        print(f"[{i}/{len(kw_ctx_pairs)}] {kw}{ctx_str} 처리 중...")
        wiki_text, text_source = collect_enriched_text(kw)

        attrs = {}
        for attempt in range(1, max_retries + 1):
            attrs = _normalize_attrs(extract_ip_attributes(kw, wiki_text, hint=ctx))
            if attrs.get("signature_keywords"):
                break
            if attempt < max_retries:
                print(f"  [재시도 {attempt}/{max_retries}] 키워드 없음 → 재시도...")
                time.sleep(3.0)

        if not attrs.get("signature_keywords"):
            print(f"  ⚠ {kw}: {max_retries}회 시도 후 실패 → 스킵")
            failed.append(kw)
            continue

        record = {
            "keyword": kw,
            "file_source": file_source,
            "wiki_text_snippet": wiki_text[:300] if wiki_text else "",
            "text_source": text_source,
            **attrs,
        }
        existing[kw] = record

        kws_preview = ", ".join(attrs.get("signature_keywords", [])[:5])
        print(f"  → ip_type={attrs.get('ip_type', '?')} | age={attrs.get('target_age', '?')} | keywords=[{kws_preview}]")

        # 성공 즉시 저장 (중단 안전)
        with open(output_json, "w", encoding="utf-8") as f:
            json.dump(list(existing.values()), f, ensure_ascii=False, indent=2)
        time.sleep(1.0)

    records = list(existing.values())
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"\n[완료] JSON 저장: {output_json} ({len(records)}개)")

    df = pd.DataFrame(records)
    df.to_parquet(output_parquet, index=False)
    print(f"[완료] Parquet 저장: {output_parquet} ({len(df)}행)")

    if failed:
        print(f"\n⚠ 최종 실패 ({len(failed)}개): {', '.join(failed)}")


# ─── 키워드 파싱 테스트 ──────────────────────────────────────────────────────

def test_keyword_parsing(folder: str = IP_FOLDER) -> None:
    """
    키워드 파싱 결과를 단계별로 검증:
    1) 파일별 원시 파싱 결과
    2) 중복 키워드 탐지
    3) 최종 scan_ip_folder() 결과 요약
    """
    from collections import Counter

    print("=" * 60)
    print("STEP 1: 파일별 원시 파싱 결과")
    print("=" * 60)

    all_raw: dict[str, list[str]] = {}
    for fname in sorted(os.listdir(folder)):
        if not fname.endswith(".txt"):
            continue
        fpath = os.path.join(folder, fname)
        kws = _parse_txt_file(fpath)
        all_raw[fname] = kws
        print(f"\n[{fname}] → {len(kws)}개")
        for i, kw in enumerate(kws):
            marker = ""
            # 공백 포함 키워드 경고
            if kw != kw.strip() or "  " in kw:
                marker = " ⚠ 공백이상"
            print(f"  {i+1:3d}. '{kw}'{marker}")

    print("\n" + "=" * 60)
    print("STEP 2: 중복 키워드 탐지")
    print("=" * 60)

    all_kws: list[str] = []
    kw_to_files: dict[str, list[str]] = {}
    for fname, kws in all_raw.items():
        all_kws.extend(kws)
        for kw in kws:
            kw_to_files.setdefault(kw, []).append(fname)

    duplicates = {kw: files for kw, files in kw_to_files.items() if len(files) > 1}
    if duplicates:
        print(f"중복 키워드 {len(duplicates)}개 발견 (첫 번째 파일 기준 1회만 처리):")
        for kw, files in duplicates.items():
            print(f"  '{kw}' → {files}")
    else:
        print("중복 키워드 없음 ✓")

    print("\n" + "=" * 60)
    print("STEP 3: scan_ip_folder() 최종 결과")
    print("=" * 60)

    pairs = scan_ip_folder(folder)
    src_counts = Counter(src for _, src in pairs)

    print(f"총 처리 대상: {len(pairs)}개 (중복 제거 후)")
    for src, cnt in sorted(src_counts.items()):
        print(f"  [{src}] {cnt}개")

    # 빈 문자열 키워드 체크
    empty_kws = [kw for kw, _ in pairs if not kw]
    if empty_kws:
        print(f"\n⚠ 빈 키워드 {len(empty_kws)}개 감지됨 — txt 파일 확인 필요")
    else:
        print("\n빈 키워드 없음 ✓")

    print("\n[파싱 테스트 완료]")


# ─── 단독 실행 ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="IP 속성 추출 파이프라인")
    parser.add_argument("--sample", action="store_true", help="샘플 3개 키워드 LLM 테스트")
    parser.add_argument("--keyword", type=str, default=None, help="단일 키워드 전체 파이프라인 테스트")
    parser.add_argument("--list", action="store_true", help="스캔된 키워드 목록만 출력")
    parser.add_argument("--test-keywords", action="store_true", help="키워드 파싱 검증 (파일 읽기·중복 체크)")
    parser.add_argument(
        "--rerun-file", type=str, default=None,
        help="지정 txt 파일의 IP만 강제 재추출 → IP_속성_재추출.json/parquet 저장 (체크포인트 무시)",
    )
    args = parser.parse_args()

    if args.rerun_file:
        batch_rerun_specific_ips(args.rerun_file)

    elif args.test_keywords:
        test_keyword_parsing()

    elif args.keyword:
        print(f"=== 단일 키워드 테스트: {args.keyword} ===")
        text, src = collect_enriched_text(args.keyword)
        print(f"텍스트 출처: {src}, 길이: {len(text)}자")
        print(f"텍스트 미리보기:\n{text[:300]}\n...")
        attrs = extract_ip_attributes(args.keyword, text)
        print(f"추출 속성: {json.dumps(attrs, ensure_ascii=False, indent=2)}")

    elif args.sample:
        samples = ["블랙핑크", "지브리", "오징어게임"]
        for kw in samples:
            print(f"\n=== {kw} ===")
            text, src = collect_enriched_text(kw)
            print(f"텍스트 출처: {src}, 길이: {len(text)}자")
            attrs = extract_ip_attributes(kw, text)
            print(f"속성: {json.dumps(attrs, ensure_ascii=False)}")
            time.sleep(1.0)

    elif args.list:
        pairs = scan_ip_folder(IP_FOLDER)
        from collections import Counter
        src_counts = Counter(src for _, src in pairs)
        print(f"총 {len(pairs)}개 키워드:")
        for src, cnt in sorted(src_counts.items()):
            print(f"  [{src}] {cnt}개")
        for kw, src in pairs:
            print(f"  {kw} ({src})")

    else:
        batch_extract_all_ips()
