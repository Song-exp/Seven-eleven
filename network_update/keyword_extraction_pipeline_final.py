"""
MD 자연어 기획 문장을 네트워크 시작 키워드로 변환하는 파이프라인.

처리 흐름:
1. 입력 문장 정규화
2. 원문 substring match
3. 공백 제거 substring match
4. 행사/시즌 표현 정규화
5. 짧은 키워드 단어 경계 매칭
6. Kiwi 형태소 분석
7. token exact match
8. safe contains
9. 검수된 매핑 사전 적용
10. 감각 표현 정규화
11. 상품 형태 표현 정리
12. fuzzy match
13. 복합 token 내부 매칭
14. 중복 제거 및 매칭 근거 반환

실행 예:
    python keyword_extraction_pipeline.py "빼빼로 데이 시즌에 초콜렛 딸기맛 디저트를 출시합니다."
    python keyword_extraction_pipeline.py --json "KBO 시즌 한정 치킨 상품"
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Iterable

from kiwipiepy import Kiwi


BASE_DIR = Path(__file__).resolve().parent

# 네트워크에 들어가는 전체 키워드 파일
NETWORK_KEYWORD_FILE = BASE_DIR / "network_keyword_dictionary.csv"

# 사람이 검토해 확정한 불용어 사전 파일
STOPWORD_FILE = BASE_DIR / "stopword_dictionary.csv"

# 영어로만 이루어진 token 중 살릴 키워드 목록 파일
ALLOWED_ENGLISH_FILE = BASE_DIR / "english_keyword_allowlist.csv"

# NP_INFO에서 나온 표현을 네트워크 키워드로 연결한 검수 매핑 사전 파일
CURATED_MAPPING_FILE = BASE_DIR / "output" / "curated_keyword_mapping.csv"


KEEP_POS = {"NNG", "NNP", "SL", "SN", "XR"}
CANDIDATE_DIRECT_POS = {"NNP", "SL"}

SAFE_SUFFIXES = {
    "맛",
    "향",
    "풍",
    "식",
    "형",
    "용",
    "류",
    "급",
    "산",
    "넛",
    "칩",
    "바",
    "볼",
    "컵",
    "콘",
    "티",
    "차",
    "빵",
    "번",
    "펄",
    "함",
    "감",
    "와",
    "과",
    "에",
    "의",
    "가",
    "를",
    "을",
    "은",
    "는",
    "이",
    "란",
    "로",
    "으로",
    "도",
}

SUBSTRING_MIN_LEN = 3
COMPOUND_MIN_KEYWORD_LEN = 2
CANDIDATE_MIN_LEN = 2
SIMILAR_KEYWORD_THRESHOLD = 0.92
NO_MATCH_MESSAGE = "입력 문장에서 네트워크 키워드를 찾지 못했습니다."

CANDIDATE_CONTEXT_ANCHORS = {
    "맛",
    "향",
    "풍미",
    "식감",
    "질감",
    "촉감",
    "목넘김",
    "바디감",
    "텍스처",
    "제형",
    "음료",
    "주스",
    "에이드",
    "스무디",
    "커피",
    "라떼",
    "티",
    "차",
    "탄산",
    "제로",
    "디저트",
    "케이크",
    "빵",
    "쿠키",
    "젤리",
    "초코",
    "초콜릿",
    "아이스크림",
    "요거트",
    "샐러드",
    "도시락",
    "김밥",
    "라면",
    "떡볶이",
    "파스타",
    "버거",
    "샌드위치",
    "스낵",
    "과자",
    "간식",
    "치킨",
    "닭",
    "고기",
    "과일",
    "채소",
    "야채",
    "소스",
    "시즈닝",
    "매운",
    "매콤",
    "달콤",
    "어린이날",
    "크리스마스",
    "빼빼로데이",
    "OTT",
    "IP",
    "캐릭터",
    "게임",
    "방송",
    "브랜드",
    "한국",
    "미국",
    "일본",
    "중국",
    "태국",
    "이탈리아",
    "멕시코",
    "멕시칸",
    "프랑스",
    "대만",
    "비건",
    "건강",
}

SENSORY_CONTEXT_ANCHORS = {
    "식감",
    "질감",
    "촉감",
    "풍미",
    "맛",
    "향",
    "향미",
    "목넘김",
    "바디감",
    "씹는맛",
    "텍스처",
    "제형",
}

SENSORY_EXCLUDE_STEMS = {
    "새롭",
    "다르",
    "진",
    "은은",
    "산뜻",
    "같",
    "없",
    "있",
    "좋",
    "나쁘",
    "많",
    "적",
}

SENSORY_CONTEXT_WINDOW = 28

EVENT_NORMALIZATION_RULES = (
    (r"(?<![0-9A-Za-z가-힣])(?:11\s*월\s*11\s*일|11\s*[/.-]\s*11)(?:을|를|은|는|이|가|에|엔|에는|부터|까지)?(?![0-9A-Za-z가-힣])", "빼빼로데이"),
    (r"(?<![0-9A-Za-z가-힣])발렌타인\s*(?:데이|시즌)?(?:을|를|은|는|이|가|에|엔|에는|부터|까지)?(?![0-9A-Za-z가-힣])", "발렌타인"),
    (r"(?<![0-9A-Za-z가-힣])화이트\s*데이\s*(?:시즌)?(?:을|를|은|는|이|가|에|엔|에는|부터|까지)?(?![0-9A-Za-z가-힣])", "화이트데이"),
)

ATTRIBUTE_NORMALIZATION_RULES = (
    (r"(?<![0-9A-Za-z가-힣])미니\s*사이즈(?![0-9A-Za-z가-힣])", "미니"),
)

CONTEXTUAL_ONLY_KEYWORDS = {
    "조리",
    "포장",
}

SUPPRESSED_OUTPUT_KEYWORDS = {
    "포장",
}

CONTEXTUAL_KEEP_CUES = {
    "간편",
    "전용",
    "즉석",
    "렌지업",
    "전자레인지용",
    "조리형",
    "패키지",
}

CONTEXTUAL_CUE_WINDOW = 18

CANDIDATE_PARTICLES = (
    "으로서",
    "으로써",
    "에게서",
    "한테서",
    "에서",
    "에게",
    "한테",
    "부터",
    "까지",
    "처럼",
    "보다",
    "으로",
    "로서",
    "로써",
    "이며",
    "이고",
    "이나",
    "이나마",
    "만큼",
    "와",
    "과",
    "의",
    "을",
    "를",
    "은",
    "는",
    "이",
    "가",
    "도",
    "만",
    "로",
    "에",
)

FOOD_NAME_PARTICLE_EXCEPTIONS = {
    "용과",
}

CANDIDATE_BOUNDARY_TERMS = {
    "맛",
    "향",
    "풍",
    "식",
    "형",
    "용",
    "류",
    "산",
    "칩",
    "바",
    "볼",
    "컵",
    "콘",
    "티",
    "차",
    "빵",
    "번",
    "펄",
}

WHOLE_CANDIDATE_SUFFIXES = (
    "바이오틱",
)

WHOLE_TERMINAL_BOUNDARY_TERMS = {
    "페퍼",
}

FORMULATION_SUFFIX_TERMS = {
    "드레싱",
    "소스",
    "시럽",
    "마요",
}

CANDIDATE_EXCLUDE_TERMS = {
    "기획",
    "구성",
    "준비",
    "강조",
    "시청",
    "상황",
    "겨냥",
    "반영",
    "중심",
    "최신",
    "트렌드",
    "출시",
    "상품",
    "제품",
    "신상품",
    "신규",
    "전용",
    "활용",
    "가능",
    "고객",
    "타겟",
    "타깃",
    "컨셉",
    "콘셉트",
    "협업",
    "콜라보",
    "스타일",
    "타입",
    "사이즈",
    "막대",
    "포장",
    "소포장",
    "개별포장",
    "느낌",
    "개발",
    "운영",
    "판매",
    "제안",
    "합니다",
    "입니다",
    "습니다",
}

CANDIDATE_FUNCTION_SUFFIXES = (
    "하게",
    "스럽게",
    "롭게",
    "답게",
    "되게",
    "같이",
    "처럼",
    "하고",
    "하며",
    "하면서",
    "해서",
    "하여",
    "하기",
    "하도록",
    "한다",
    "하는",
    "된다",
    "되는",
)

METHOD_PRIORITY = {
    "event_normalization": 1,
    "substring_original": 2,
    "substring_no_space": 3,
    "token_exact": 4,
    "short_keyword_exact": 5,
    "safe_contains": 6,
    "curated_mapping": 7,
    "sensory_normalization": 8,
    "attribute_normalization": 9,
    "fuzzy": 10,
    "compound_token": 11,
}


@dataclass(frozen=True)
class MatchEvidence:
    input_term: str
    network_keyword: str
    match_method: str
    confidence: float
    detail: str


@dataclass(frozen=True)
class CandidateEvidence:
    candidate_keyword: str
    source_term: str
    reason: str


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"필수 파일이 없습니다: {path}")

    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f))
    except UnicodeDecodeError as exc:
        raise ValueError(f"CSV 인코딩을 읽을 수 없습니다: {path}") from exc


def normalize_text(text: str) -> str:
    text = "" if text is None else str(text)
    return re.sub(r"\s+", " ", text.strip())


def remove_spaces(text: str) -> str:
    return re.sub(r"\s+", "", text)


def is_english_only(text: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z]+", text or ""))


def has_digit(text: str) -> bool:
    return bool(re.search(r"\d", text or ""))


def is_symbol_only(text: str) -> bool:
    return bool(text) and not bool(re.search(r"[0-9A-Za-z가-힣]", text))


def is_korean_function_candidate(text: str) -> bool:
    """후보 키워드가 아니라 문장 안에서만 기능하는 한국어 활용형/부사형을 거른다."""
    compact_text = remove_spaces(text)
    if not compact_text or not re.fullmatch(r"[가-힣]+", compact_text):
        return False
    return compact_text.endswith(CANDIDATE_FUNCTION_SUFFIXES)


def is_suppressed_descriptor_candidate(text: str) -> bool:
    compact_text = remove_spaces(text)
    if not compact_text or not re.fullmatch(r"[가-힣]+", compact_text):
        return False

    stems = {compact_text}
    for suffix in ("함", "감", "한", "하게", "하고", "하며", "해서", "하여", "하다"):
        if len(compact_text) > len(suffix) and compact_text.endswith(suffix):
            stems.add(compact_text[: -len(suffix)])

    for stem in list(stems):
        if stem.endswith("하") and len(stem) > 1:
            stems.add(stem[:-1])

    return bool(stems & SENSORY_EXCLUDE_STEMS)


def has_final_consonant(text: str) -> bool | None:
    compact_text = remove_spaces(text)
    if not compact_text:
        return None
    char = compact_text[-1]
    code = ord(char)
    if not 0xAC00 <= code <= 0xD7A3:
        return None
    return (code - 0xAC00) % 28 != 0


def can_strip_candidate_particle(stem: str, particle: str) -> bool:
    final_consonant = has_final_consonant(stem)
    if final_consonant is None:
        return True

    consonant_particles = {
        "이",
        "을",
        "은",
        "과",
        "으로",
        "이며",
        "이고",
        "이나",
        "이나마",
        "으로서",
        "으로써",
    }
    vowel_particles = {
        "가",
        "를",
        "는",
        "와",
        "로",
        "로서",
        "로써",
    }
    if particle in consonant_particles:
        return final_consonant
    if particle in vowel_particles:
        return not final_consonant
    return True


def strip_candidate_particle(text: str) -> str:
    stripped = remove_spaces(text)
    changed = True
    while changed:
        if stripped in FOOD_NAME_PARTICLE_EXCEPTIONS:
            break
        changed = False
        for particle in CANDIDATE_PARTICLES:
            if not (len(stripped) > len(particle) and stripped.endswith(particle)):
                continue
            stem = stripped[: -len(particle)]
            if can_strip_candidate_particle(stem, particle):
                stripped = stem
                changed = True
                break
    return stripped


def is_fragment_like_candidate(text: str) -> bool:
    compact_text = remove_spaces(text)
    if not compact_text or not re.fullmatch(r"[가-힣]+", compact_text):
        return False
    if len(compact_text) < 2:
        return True
    if len(compact_text) == 2 and compact_text[1] in {"감", "맛", "향", "식", "층", "길", "수"}:
        return True
    return False


def fuzzy_threshold_for_length(length: int) -> float | None:
    if length <= 3:
        return None
    if length <= 5:
        return 0.80
    if length <= 8:
        return 0.85
    return 0.90


def load_keywords(path: Path) -> list[str]:
    rows = read_csv_rows(path)
    if not rows:
        raise ValueError(f"네트워크 키워드 파일이 비어 있습니다: {path}")

    if "Keyword" in rows[0]:
        column = "Keyword"
    elif "keyword" in rows[0]:
        column = "keyword"
    else:
        raise ValueError(f"네트워크 키워드 파일에 Keyword 또는 keyword 컬럼이 없습니다: {path}")

    keywords: list[str] = []
    seen: set[str] = set()
    for row in rows:
        keyword = normalize_text(row.get(column, ""))
        if keyword and keyword not in seen:
            seen.add(keyword)
            keywords.append(keyword)
    if not keywords:
        raise ValueError(f"네트워크 키워드 파일에서 유효한 키워드를 찾지 못했습니다: {path}")
    return keywords


def load_stopwords(path: Path) -> set[str]:
    rows = read_csv_rows(path)
    if rows and "stopword" not in rows[0]:
        raise ValueError(f"불용어 사전 파일에 stopword 컬럼이 없습니다: {path}")
    return {
        normalize_text(row.get("stopword", ""))
        for row in rows
        if normalize_text(row.get("stopword", ""))
    }


def load_allowed_english(path: Path) -> dict[str, str]:
    rows = read_csv_rows(path)
    if rows and "keyword" not in rows[0]:
        raise ValueError(f"허용 영어 키워드 파일에 keyword 컬럼이 없습니다: {path}")
    allowed: dict[str, str] = {}
    for row in rows:
        keyword = normalize_text(row.get("keyword", ""))
        if keyword and is_english_only(keyword):
            allowed.setdefault(keyword.lower(), keyword)
    return allowed


def load_curated_mapping(path: Path) -> dict[str, str]:
    rows = read_csv_rows(path)
    if rows and not {"NP_INFO_keyword", "network_keyword"}.issubset(rows[0]):
        raise ValueError(
            f"검수 매핑 파일에 NP_INFO_keyword, network_keyword 컬럼이 없습니다: {path}"
        )
    mapping: dict[str, str] = {}
    for row in rows:
        source = normalize_text(row.get("NP_INFO_keyword", ""))
        target = normalize_text(row.get("network_keyword", ""))
        if source and target:
            mapping[source] = target
    return mapping


class MDKeywordPipeline:
    def __init__(
        self,
        network_keyword_file: Path = NETWORK_KEYWORD_FILE,
        stopword_file: Path = STOPWORD_FILE,
        allowed_english_file: Path = ALLOWED_ENGLISH_FILE,
        curated_mapping_file: Path = CURATED_MAPPING_FILE,
    ) -> None:
        self.network_keywords = load_keywords(network_keyword_file)
        self.network_keyword_set = set(self.network_keywords)
        self.stopwords = load_stopwords(stopword_file)
        self.allowed_english = load_allowed_english(allowed_english_file)
        self.allowed_english_values = set(self.allowed_english.values())
        self.curated_mapping = load_curated_mapping(curated_mapping_file)

        # 검수 매핑의 target과 허용 영어 keyword도 출력 가능한 키워드로 본다.
        self.output_keyword_set = (
            self.network_keyword_set
            | set(self.curated_mapping.values())
            | self.allowed_english_values
        )
        self.output_keywords = sorted(self.output_keyword_set, key=lambda x: (-len(remove_spaces(x)), x))
        self.compound_inner_keywords = [
            keyword
            for keyword in self.output_keywords
            if len(remove_spaces(keyword)) >= COMPOUND_MIN_KEYWORD_LEN
        ]
        self.candidate_boundary_terms = sorted(
            {
                remove_spaces(keyword)
                for keyword in self.output_keywords
                if len(remove_spaces(keyword)) >= COMPOUND_MIN_KEYWORD_LEN
            }
            | CANDIDATE_BOUNDARY_TERMS,
            key=lambda x: (-len(x), x),
        )

        self.substring_keywords = [
            kw
            for kw in self.network_keywords
            if len(remove_spaces(kw)) >= SUBSTRING_MIN_LEN
        ]
        self.substring_keywords.sort(key=lambda x: (-len(remove_spaces(x)), x))
        self.short_exact_keywords = [
            kw for kw in self.network_keywords if len(remove_spaces(kw)) == 2
        ]
        self.short_exact_keywords.sort(key=lambda x: (-len(remove_spaces(x)), x))

        self.kiwi = Kiwi()

    def extract(self, text: str) -> dict[str, object]:
        normalized = normalize_text(text)
        no_space_text = remove_spaces(normalized)

        evidence: list[MatchEvidence] = []
        evidence.extend(self._event_expression_matches(normalized))
        evidence.extend(self._substring_original(normalized))
        evidence.extend(self._short_keyword_original(normalized))
        evidence.extend(self._substring_no_space(normalized, no_space_text, evidence))
        evidence = self._filter_covered_substring_evidence(evidence)

        tokens = self._extract_tokens(normalized)
        covered_tokens = self._tokens_covered_by_long_substring_matches(evidence)
        tokens = [token for token in tokens if token not in covered_tokens]
        evidence.extend(self._match_tokens(tokens))
        evidence.extend(self._sensory_expression_matches(normalized))
        evidence.extend(self._attribute_expression_matches(normalized))
        evidence = self._suppress_generic_event_evidence(evidence)
        evidence = self._suppress_contextual_only_evidence(evidence, normalized)
        final_evidence = self._sentence_ordered_evidence(evidence, normalized)
        candidate_evidence = self._candidate_keywords(normalized, final_evidence)

        return {
            "input_text": text,
            "normalized_text": normalized,
            "final_keywords": [ev.network_keyword for ev in final_evidence],
            "candidate_keywords": [
                candidate.candidate_keyword for candidate in candidate_evidence
            ],
            "match_evidence": [ev.__dict__ for ev in final_evidence],
            "candidate_details": [
                candidate.__dict__ for candidate in candidate_evidence
            ],
        }

    def _substring_original(self, text: str) -> list[MatchEvidence]:
        matches: list[MatchEvidence] = []
        for keyword in self.substring_keywords:
            if keyword in text:
                matches.append(
                    MatchEvidence(
                        input_term=keyword,
                        network_keyword=keyword,
                        match_method="substring_original",
                        confidence=1.0,
                        detail="원문에 네트워크 키워드가 그대로 등장",
                    )
                )
        return matches

    def _event_expression_matches(self, text: str) -> list[MatchEvidence]:
        matches: list[MatchEvidence] = []
        for pattern, target in EVENT_NORMALIZATION_RULES:
            if target not in self.output_keyword_set:
                continue
            for match in re.finditer(pattern, text):
                source = normalize_text(match.group(0))
                matches.append(
                    MatchEvidence(
                        input_term=source,
                        network_keyword=target,
                        match_method="event_normalization",
                        confidence=0.96,
                        detail="행사/시즌 표현을 등록 키워드로 정규화",
                    )
                )
        return matches

    def _short_keyword_original(self, text: str) -> list[MatchEvidence]:
        matches: list[MatchEvidence] = []
        for keyword in self.short_exact_keywords:
            pattern = rf"(?<![0-9A-Za-z가-힣]){re.escape(keyword)}(?![0-9A-Za-z가-힣])"
            if re.search(pattern, text):
                matches.append(
                    MatchEvidence(
                        input_term=keyword,
                        network_keyword=keyword,
                        match_method="short_keyword_exact",
                        confidence=0.97,
                        detail="짧은 등록 키워드가 독립 표현으로 등장",
                    )
                )
        return matches

    def _substring_no_space(
        self,
        text: str,
        no_space_text: str,
        previous: Iterable[MatchEvidence],
    ) -> list[MatchEvidence]:
        already_matched = {
            ev.network_keyword
            for ev in previous
            if ev.match_method == "substring_original"
        }
        matches: list[MatchEvidence] = []
        for keyword in self.substring_keywords:
            if keyword in already_matched:
                continue
            compact_keyword = remove_spaces(keyword)
            if compact_keyword and compact_keyword in no_space_text and keyword not in text:
                matches.append(
                    MatchEvidence(
                        input_term=keyword,
                        network_keyword=keyword,
                        match_method="substring_no_space",
                        confidence=0.98,
                        detail="공백 제거 후 네트워크 키워드가 등장",
                    )
                )
        return matches

    def _extract_tokens(self, text: str) -> list[str]:
        tokens: list[str] = []
        for token in self.kiwi.tokenize(text):
            form = token.form.strip()
            if not form or token.tag not in KEEP_POS:
                continue

            normalized = self._normalize_token_for_policy(form)
            if normalized:
                tokens.append(normalized)
        return tokens

    def _filter_covered_substring_evidence(
        self, evidence: list[MatchEvidence]
    ) -> list[MatchEvidence]:
        """긴 substring keyword 안에 들어간 짧은 substring keyword는 보조 조각으로 본다."""
        substring_methods = {"substring_original", "substring_no_space"}
        substring_evidence = [
            ev for ev in evidence if ev.match_method in substring_methods
        ]
        covered_short_keywords: set[str] = set()

        for short_ev in substring_evidence:
            short_keyword = short_ev.network_keyword
            short_compact = remove_spaces(short_keyword)
            if not short_compact:
                continue

            for long_ev in substring_evidence:
                long_keyword = long_ev.network_keyword
                long_compact = remove_spaces(long_keyword)
                if short_keyword == long_keyword:
                    continue
                if len(long_compact) < 4 or len(long_compact) <= len(short_compact):
                    continue
                if short_compact in long_compact:
                    covered_short_keywords.add(short_keyword)
                    break

        if not covered_short_keywords:
            return evidence

        return [
            ev
            for ev in evidence
            if not (
                ev.match_method in substring_methods
                and ev.network_keyword in covered_short_keywords
            )
        ]

    def _tokens_covered_by_long_substring_matches(
        self, evidence: list[MatchEvidence]
    ) -> set[str]:
        """긴 substring keyword가 잡히면, 그 keyword의 Kiwi 조각 token은 보조 후보로 본다."""
        covered: set[str] = set()
        for ev in evidence:
            if ev.match_method not in {"substring_original", "substring_no_space"}:
                continue

            keyword = ev.network_keyword
            if len(remove_spaces(keyword)) < 4:
                continue

            for token in self.kiwi.tokenize(keyword):
                form = token.form.strip()
                if not form or token.tag not in KEEP_POS:
                    continue
                normalized = self._normalize_token_for_policy(form)
                if normalized and normalized != keyword:
                    covered.add(normalized)
        return covered

    def _normalize_token_for_policy(self, token: str) -> str | None:
        token = token.strip()
        if not token or is_symbol_only(token):
            return None

        # 영어-only는 허용 목록에 있는 것만 살린다. 대소문자는 무시하고 출력은 허용 목록 표기를 따른다.
        if is_english_only(token):
            return self.allowed_english.get(token.lower())

        # 이미 네트워크 키워드이거나 검수 매핑 target이면 1글자/숫자 규칙보다 우선 살린다.
        if token in self.output_keyword_set:
            return token

        if len(token) == 1:
            return None

        if has_digit(token):
            return None

        if token in self.stopwords:
            return None

        return token

    def _match_tokens(self, tokens: list[str]) -> list[MatchEvidence]:
        evidence: list[MatchEvidence] = []
        for token in tokens:
            token_matches: list[MatchEvidence] = []

            exact = self._token_exact(token)
            if exact:
                evidence.append(exact)
                continue

            safe = self._safe_contains(token)
            if safe:
                evidence.append(safe)
                continue

            curated = self._curated_mapping(token)
            if curated:
                evidence.append(curated)
                continue

            fuzzy = self._fuzzy(token)
            if fuzzy:
                evidence.append(fuzzy)
                continue

            compound = self._compound_token_inner_matches(token)
            if compound:
                token_matches.extend(compound)

            evidence.extend(token_matches)
        return evidence

    def _token_exact(self, token: str) -> MatchEvidence | None:
        if token in self.output_keyword_set:
            return MatchEvidence(
                input_term=token,
                network_keyword=token,
                match_method="token_exact",
                confidence=1.0,
                detail="Kiwi token이 네트워크 키워드와 완전 일치",
            )
        return None

    def _safe_contains(self, token: str) -> MatchEvidence | None:
        for keyword in self.output_keywords:
            if len(keyword) < 2 or token == keyword:
                continue
            if not token.startswith(keyword):
                continue
            suffix = token[len(keyword) :]
            if suffix in SAFE_SUFFIXES:
                return MatchEvidence(
                    input_term=token,
                    network_keyword=keyword,
                    match_method="safe_contains",
                    confidence=0.95,
                    detail=f"'{token}' = '{keyword}' + 안전 접미어 '{suffix}'",
                )
        return None

    def _curated_mapping(self, token: str) -> MatchEvidence | None:
        target = self.curated_mapping.get(token)
        if not target:
            return None
        return MatchEvidence(
            input_term=token,
            network_keyword=target,
            match_method="curated_mapping",
            confidence=0.93,
            detail="검수된 NP_INFO 키워드 매핑 사전 적용",
        )

    def _fuzzy(self, token: str) -> MatchEvidence | None:
        token_compact = remove_spaces(token)
        threshold = fuzzy_threshold_for_length(len(token_compact))
        if threshold is None or is_english_only(token):
            return None

        best_keyword = ""
        best_score = 0.0
        for keyword in self.output_keywords:
            compact_keyword = remove_spaces(keyword)
            if len(compact_keyword) < 3:
                continue
            if token_compact[0] != compact_keyword[0]:
                continue
            if abs(len(token_compact) - len(compact_keyword)) > 1:
                continue

            score = SequenceMatcher(None, token_compact, compact_keyword).ratio()
            if score > best_score:
                best_keyword = keyword
                best_score = score

        if best_keyword and best_score >= threshold:
            return MatchEvidence(
                input_term=token,
                network_keyword=best_keyword,
                match_method="fuzzy",
                confidence=round(best_score, 4),
                detail="문자열 유사도 기반 오타 보정",
            )
        return None

    def _compound_token_inner_matches(self, token: str) -> list[MatchEvidence]:
        """exact/safe/curated/fuzzy로 잡히지 않은 복합 token을 앞에서부터 분해한다."""
        if len(token) < 4 or is_english_only(token):
            return []

        token_compact = remove_spaces(token)
        matches: list[MatchEvidence] = []
        position = 0

        while position < len(token_compact):
            keyword = self._longest_compound_keyword_at(token_compact, position)
            if not keyword:
                break

            compact_keyword = remove_spaces(keyword)
            matched_term = token_compact[position : position + len(compact_keyword)]
            matches.append(
                MatchEvidence(
                    input_term=matched_term,
                    network_keyword=keyword,
                    match_method="compound_token",
                    confidence=0.88,
                    detail=f"복합 token '{token}' 내부에서 네트워크 키워드 분해",
                )
            )
            position += len(compact_keyword)

        return matches

    def _longest_compound_keyword_at(self, token: str, position: int) -> str | None:
        for keyword in self.compound_inner_keywords:
            compact_keyword = remove_spaces(keyword)
            if not compact_keyword:
                continue
            if token.startswith(compact_keyword, position):
                return keyword
        return None

    def _sensory_expression_matches(self, text: str) -> list[MatchEvidence]:
        """쫀득한/탱글탱글한 같은 감각 표현을 네트워크 키워드형으로 정규화한다."""
        matches: list[MatchEvidence] = []
        for source_term, keyword, is_existing in self._sensory_expression_terms(text):
            if not is_existing:
                continue
            matches.append(
                MatchEvidence(
                    input_term=source_term,
                    network_keyword=keyword,
                    match_method="sensory_normalization",
                    confidence=0.91,
                    detail="감각 표현을 키워드형으로 정규화",
                )
            )
        return matches

    def _sensory_candidate_keywords(
        self, text: str, final_keywords: set[str]
    ) -> list[CandidateEvidence]:
        candidates: list[CandidateEvidence] = []
        for source_term, keyword, is_existing in self._sensory_expression_terms(text):
            if is_existing:
                continue
            candidate = self._normalize_candidate_keyword(keyword, final_keywords)
            if not candidate:
                continue
            candidates.append(
                CandidateEvidence(
                    candidate_keyword=candidate,
                    source_term=source_term,
                    reason="식감/질감 표현을 키워드형으로 정리",
                )
            )
        return candidates

    def _attribute_expression_matches(self, text: str) -> list[MatchEvidence]:
        matches: list[MatchEvidence] = []
        for source_term, keyword, is_existing in self._attribute_expression_terms(text):
            if not is_existing:
                continue
            matches.append(
                MatchEvidence(
                    input_term=source_term,
                    network_keyword=keyword,
                    match_method="attribute_normalization",
                    confidence=0.9,
                    detail="상품 형태 표현을 키워드형으로 정리",
                )
            )
        return matches

    def _attribute_candidate_keywords(
        self, text: str, final_keywords: set[str]
    ) -> list[CandidateEvidence]:
        candidates: list[CandidateEvidence] = []
        for source_term, keyword, is_existing in self._attribute_expression_terms(text):
            if is_existing:
                continue
            candidate = self._normalize_candidate_keyword(keyword, final_keywords)
            if not candidate:
                continue
            candidates.append(
                CandidateEvidence(
                    candidate_keyword=candidate,
                    source_term=source_term,
                    reason="상품 형태 표현을 키워드형으로 정리",
                )
            )
        return candidates

    def _attribute_expression_terms(self, text: str) -> list[tuple[str, str, bool]]:
        terms: list[tuple[str, str, bool]] = []
        for pattern, keyword in ATTRIBUTE_NORMALIZATION_RULES:
            for match in re.finditer(pattern, text):
                source = normalize_text(match.group(0))
                terms.append((source, keyword, keyword in self.output_keyword_set))
        return terms

    def _sensory_expression_terms(self, text: str) -> list[tuple[str, str, bool]]:
        terms: list[tuple[str, str, bool]] = []
        for token in self.kiwi.tokenize(text):
            if not self._is_sensory_source_token(token):
                continue

            forms = self._sensory_keyword_forms(token.form)
            if not forms:
                continue

            existing_keyword = self._first_existing_sensory_keyword(forms)
            source_term = self._token_surface(text, token)
            if existing_keyword:
                terms.append((source_term, existing_keyword, True))
                continue

            if self._has_sensory_context_near(text, token):
                terms.append((source_term, forms[0], False))
        return terms

    def _is_sensory_source_token(self, token: object) -> bool:
        tag = str(getattr(token, "tag", ""))
        return tag.startswith("VA") or tag in {"MAG", "XR"}

    def _token_surface(self, text: str, token: object) -> str:
        start = int(getattr(token, "start", -1))
        length = int(getattr(token, "len", 0))
        if start >= 0 and length > 0:
            surface = text[start : start + length]
            if surface:
                return surface
        return str(getattr(token, "form", "")).strip()

    def _sensory_keyword_forms(self, form: str) -> list[str]:
        stem = remove_spaces(form)
        if not stem or not re.fullmatch(r"[가-힣]+", stem):
            return []
        if stem.endswith("하") and len(stem) > 1:
            stem = stem[:-1]

        bases: list[str] = []
        reduced = self._reduce_reduplicated_stem(stem)
        for base in (reduced, stem):
            if not base or base in bases:
                continue
            if base in SENSORY_EXCLUDE_STEMS or len(base) < 2:
                continue
            bases.append(base)

        forms: list[str] = []
        for base in bases:
            for candidate in self._nominalized_sensory_forms(base):
                if candidate not in forms:
                    forms.append(candidate)
        return forms

    def _reduce_reduplicated_stem(self, stem: str) -> str:
        if len(stem) < 4 or len(stem) % 2:
            return stem
        half = len(stem) // 2
        return stem[:half] if stem[:half] == stem[half:] else stem

    def _nominalized_sensory_forms(self, base: str) -> list[str]:
        forms: list[str] = []
        if base.endswith("럽") and len(base) > 1:
            forms.append(f"{base[:-1]}러움")
        if base.endswith("롭") and len(base) > 1:
            forms.append(f"{base[:-1]}로움")
        forms.extend([f"{base}함", f"{base}감", base])
        return forms

    def _first_existing_sensory_keyword(self, forms: list[str]) -> str:
        for form in forms:
            if form in self.output_keyword_set:
                return form

        compact_to_keyword = {
            remove_spaces(keyword): keyword for keyword in self.output_keywords
        }
        for form in forms:
            keyword = compact_to_keyword.get(remove_spaces(form))
            if keyword:
                return keyword
        return ""

    def _has_sensory_context_near(self, text: str, token: object) -> bool:
        start = int(getattr(token, "start", 0))
        end = int(getattr(token, "end", start + int(getattr(token, "len", 0))))
        window = text[
            max(0, start - SENSORY_CONTEXT_WINDOW) : min(
                len(text), end + SENSORY_CONTEXT_WINDOW
            )
        ]
        if any(anchor in window for anchor in SENSORY_CONTEXT_ANCHORS):
            return True

        sentence_start = max(text.rfind(mark, 0, start) for mark in ".!?。！？")
        sentence_end_candidates = [
            position for mark in ".!?。！？" if (position := text.find(mark, end)) >= 0
        ]
        sentence_end = min(sentence_end_candidates) if sentence_end_candidates else len(text)
        sentence = text[sentence_start + 1 : sentence_end]
        return any(anchor in sentence for anchor in SENSORY_CONTEXT_ANCHORS)

    def _suppress_generic_event_evidence(
        self, evidence: list[MatchEvidence]
    ) -> list[MatchEvidence]:
        event_terms = [
            ev.input_term
            for ev in evidence
            if ev.match_method == "event_normalization"
        ]
        if not any("시즌" in term for term in event_terms):
            return evidence
        return [
            ev
            for ev in evidence
            if not (
                ev.network_keyword == "시즌"
                and ev.match_method in {"token_exact", "substring_original", "short_keyword_exact"}
            )
        ]

    def _suppress_contextual_only_evidence(
        self, evidence: list[MatchEvidence], text: str
    ) -> list[MatchEvidence]:
        return [
            ev
            for ev in evidence
            if not self._is_contextual_only_noise(ev, text)
        ]

    def _is_contextual_only_noise(self, ev: MatchEvidence, text: str) -> bool:
        if ev.network_keyword in SUPPRESSED_OUTPUT_KEYWORDS:
            return True
        if ev.network_keyword not in CONTEXTUAL_ONLY_KEYWORDS:
            return False
        position = self._find_term_position(text, ev.input_term)
        if position < 0:
            position = self._find_term_position(text, ev.network_keyword)
        if position < 0:
            return True
        window = text[
            max(0, position - CONTEXTUAL_CUE_WINDOW) : min(
                len(text), position + len(ev.input_term) + CONTEXTUAL_CUE_WINDOW
            )
        ]
        return not any(cue in window for cue in CONTEXTUAL_KEEP_CUES)

    def _candidate_keywords(
        self, text: str, final_evidence: list[MatchEvidence]
    ) -> list[CandidateEvidence]:
        final_keywords = {ev.network_keyword for ev in final_evidence}
        candidates: list[CandidateEvidence] = []
        candidates.extend(self._attribute_candidate_keywords(text, final_keywords))
        candidates.extend(self._sensory_candidate_keywords(text, final_keywords))

        if not self._has_candidate_context(final_keywords):
            if candidates:
                return self._unique_candidate_evidence(candidates, text)
            return []

        for token in self.kiwi.tokenize(text):
            form = token.form.strip()
            if not form:
                continue
            if token.tag in CANDIDATE_DIRECT_POS:
                allow_direct_candidate = True
            elif token.tag == "NNG":
                allow_direct_candidate = False
            else:
                continue
            if self._maps_to_whole_keyword(form):
                continue

            for term, reason in self._candidate_terms_from_token(
                form, allow_direct_candidate=allow_direct_candidate
            ):
                candidate = self._normalize_candidate_keyword(term, final_keywords)
                if not candidate:
                    continue
                candidates.append(
                    CandidateEvidence(
                        candidate_keyword=candidate,
                        source_term=form,
                        reason=reason,
                    )
                )

        for term, source_term, reason in self._candidate_terms_from_text_chunks(text):
            candidate = self._normalize_candidate_keyword(term, final_keywords)
            if not candidate:
                continue
            candidates.append(
                CandidateEvidence(
                    candidate_keyword=candidate,
                    source_term=source_term,
                    reason=reason,
                )
            )

        return self._unique_candidate_evidence(candidates, text)

    def _has_candidate_context(self, final_keywords: set[str]) -> bool:
        return bool(final_keywords & CANDIDATE_CONTEXT_ANCHORS)

    def _maps_to_network_keyword(self, term: str) -> bool:
        return bool(
            self._token_exact(term)
            or self._safe_contains(term)
            or self._curated_mapping(term)
            or self._maps_to_network_keyword_variant(term)
            or self._fuzzy(term)
        )

    def _maps_to_whole_keyword(self, term: str) -> bool:
        return bool(
            self._token_exact(term)
            or self._curated_mapping(term)
            or self._maps_to_network_keyword_variant(term)
            or self._fuzzy(term)
        )

    def _maps_to_network_keyword_variant(self, term: str) -> bool:
        compact_term = remove_spaces(term)
        variants = {compact_term}
        for suffix in ("함", "감", "한"):
            if len(compact_term) > len(suffix) + 1 and compact_term.endswith(suffix):
                variants.add(compact_term[: -len(suffix)])
        return any(self.curated_mapping.get(variant) for variant in variants)

    def _candidate_terms_from_token(
        self, token: str, allow_direct_candidate: bool
    ) -> list[tuple[str, str]]:
        compact_token = remove_spaces(token)
        if not compact_token:
            return []
        if token in self.stopwords or token in CANDIDATE_EXCLUDE_TERMS:
            return []

        whole_semantic_candidate = self._semantic_whole_candidate(compact_token)
        if whole_semantic_candidate:
            return [
                (
                    whole_semantic_candidate,
                    f"복합 token '{token}' 전체의 전문/기능성 소재 표현",
                )
            ]

        suffix_candidate = self._formulation_suffix_candidate(compact_token)
        if suffix_candidate:
            return [
                (
                    suffix_candidate,
                    f"복합 token '{token}' 제형 접미어 제거 후 핵심 소재",
                )
            ]

        unknown_terms: list[str] = []
        boundary_matches: list[tuple[int, str]] = []
        found_boundary = False
        position = 0
        unknown_start = 0

        while position < len(compact_token):
            boundary = self._longest_candidate_boundary_at(compact_token, position)
            if boundary:
                found_boundary = True
                boundary_matches.append((position, boundary))
                if unknown_start < position:
                    unknown_terms.append(compact_token[unknown_start:position])
                position += len(boundary)
                unknown_start = position
                continue
            position += 1

        if found_boundary:
            if unknown_start < len(compact_token):
                unknown_terms.append(compact_token[unknown_start:])
            whole_candidate = self._whole_compound_candidate(
                compact_token, boundary_matches, unknown_terms
            )
            if whole_candidate:
                return [
                    (
                        whole_candidate,
                        f"복합 token '{token}' 전체의 미등록 의미어",
                    )
                ]
            unknown_terms = [
                term
                for term in unknown_terms
                if not is_fragment_like_candidate(strip_candidate_particle(term))
            ]
            return [
                (term, f"복합 token '{token}' 내부의 미등록 의미어")
                for term in unknown_terms
            ]

        if allow_direct_candidate:
            return [(compact_token, "네트워크 의미 축과 함께 등장한 미등록 고유명사/외국어")]
        return []

    def _whole_compound_candidate(
        self,
        compact_token: str,
        boundary_matches: list[tuple[int, str]],
        unknown_terms: list[str],
    ) -> str:
        terminal_candidate = self._terminal_boundary_whole_candidate(
            compact_token, boundary_matches, unknown_terms
        )
        if terminal_candidate:
            return terminal_candidate

        if len(boundary_matches) != 1 or boundary_matches[0][0] != 0:
            return ""
        if len(unknown_terms) != 1:
            return ""

        tail = strip_candidate_particle(unknown_terms[0])
        if is_fragment_like_candidate(tail):
            return ""
        if self._maps_to_network_keyword(tail):
            return ""

        whole = strip_candidate_particle(compact_token)
        boundary = boundary_matches[0][1]
        if len(whole) <= len(boundary) + 1:
            return ""
        if whole in self.stopwords or whole in CANDIDATE_EXCLUDE_TERMS:
            return ""
        if self._maps_to_whole_keyword(whole):
            return ""
        return whole

    def _semantic_whole_candidate(self, compact_token: str) -> str:
        whole = strip_candidate_particle(compact_token)
        if len(whole) < 4:
            return ""
        if whole in self.stopwords or whole in CANDIDATE_EXCLUDE_TERMS:
            return ""
        if self._maps_to_whole_keyword(whole):
            return ""
        if any(whole.endswith(suffix) for suffix in WHOLE_CANDIDATE_SUFFIXES):
            return whole
        return ""

    def _terminal_boundary_whole_candidate(
        self,
        compact_token: str,
        boundary_matches: list[tuple[int, str]],
        unknown_terms: list[str],
    ) -> str:
        if len(boundary_matches) != 1:
            return ""

        position, boundary = boundary_matches[0]
        if boundary not in WHOLE_TERMINAL_BOUNDARY_TERMS:
            return ""
        if position + len(boundary) != len(compact_token):
            return ""
        if len(unknown_terms) != 1:
            return ""

        prefix = strip_candidate_particle(unknown_terms[0])
        if is_fragment_like_candidate(prefix):
            return ""
        if self._maps_to_whole_keyword(prefix):
            return ""

        whole = strip_candidate_particle(compact_token)
        if whole in self.stopwords or whole in CANDIDATE_EXCLUDE_TERMS:
            return ""
        if self._maps_to_whole_keyword(whole):
            return ""
        return whole

    def _formulation_suffix_candidate(self, compact_token: str) -> str:
        token = strip_candidate_particle(compact_token)
        for suffix in sorted(FORMULATION_SUFFIX_TERMS, key=len, reverse=True):
            if not token.endswith(suffix) or len(token) <= len(suffix) + 1:
                continue
            stem = token[: -len(suffix)]
            if is_fragment_like_candidate(stem):
                continue
            if stem in self.stopwords or stem in CANDIDATE_EXCLUDE_TERMS:
                continue
            if self._maps_to_whole_keyword(stem):
                continue
            return stem
        return ""

    def _candidate_terms_from_text_chunks(
        self, text: str
    ) -> list[tuple[str, str, str]]:
        terms: list[tuple[str, str, str]] = []
        for chunk in re.split(r"\s+", text):
            chunk = re.sub(r"^[^\w가-힣]+|[^\w가-힣]+$", "", chunk)
            if len(remove_spaces(chunk)) < 4:
                continue
            if any(excluded in chunk for excluded in CANDIDATE_EXCLUDE_TERMS):
                continue
            if self._maps_to_whole_keyword(chunk):
                continue
            for term, reason in self._candidate_terms_from_token(
                chunk, allow_direct_candidate=False
            ):
                if not reason.startswith("복합 token"):
                    continue
                terms.append((term, chunk, f"원문 chunk '{chunk}' 내부의 미등록 의미어"))
        return terms

    def _longest_candidate_boundary_at(self, token: str, position: int) -> str | None:
        for boundary in self.candidate_boundary_terms:
            if token.startswith(boundary, position):
                if len(boundary) == 1 and position + len(boundary) != len(token):
                    continue
                return boundary
        return None

    def _normalize_candidate_keyword(
        self, term: str, final_keywords: set[str]
    ) -> str | None:
        candidate = normalize_text(strip_candidate_particle(term))
        compact_candidate = remove_spaces(candidate)
        if len(compact_candidate) < CANDIDATE_MIN_LEN:
            return None
        if is_fragment_like_candidate(candidate):
            return None
        if is_symbol_only(candidate) or has_digit(candidate):
            return None
        if is_korean_function_candidate(candidate):
            return None
        if is_suppressed_descriptor_candidate(candidate):
            return None
        if candidate.endswith(("합니다", "입니다", "습니다")):
            return None
        if is_english_only(candidate):
            if candidate.isupper() or len(candidate) < 3:
                return None
            return candidate
        if candidate in self.stopwords or candidate in CANDIDATE_EXCLUDE_TERMS:
            return None
        if self._maps_to_network_keyword_variant(candidate):
            return None
        if candidate in self.output_keyword_set or candidate in final_keywords:
            return None
        if any(
            compact_candidate != remove_spaces(keyword)
            and compact_candidate in remove_spaces(keyword)
            for keyword in final_keywords
        ):
            return None
        if self._maps_to_whole_keyword(candidate):
            return None
        return candidate

    def _unique_candidate_evidence(
        self, candidates: list[CandidateEvidence], text: str
    ) -> list[CandidateEvidence]:
        best_by_keyword: dict[str, CandidateEvidence] = {}
        for candidate in candidates:
            current = best_by_keyword.get(candidate.candidate_keyword)
            if current and len(current.source_term) <= len(candidate.source_term):
                continue
            best_by_keyword[candidate.candidate_keyword] = candidate

        unique = self._suppress_contained_candidate_evidence(
            list(best_by_keyword.values())
        )
        unique.sort(
            key=lambda candidate: (
                self._candidate_position(candidate, text),
                candidate.candidate_keyword,
            )
        )
        return unique

    def _suppress_contained_candidate_evidence(
        self, candidates: list[CandidateEvidence]
    ) -> list[CandidateEvidence]:
        kept: list[CandidateEvidence] = []
        for candidate in sorted(
            candidates,
            key=lambda item: (
                -len(remove_spaces(item.candidate_keyword)),
                item.candidate_keyword,
            ),
        ):
            compact_candidate = remove_spaces(candidate.candidate_keyword)
            if any(
                compact_candidate != remove_spaces(kept_candidate.candidate_keyword)
                and len(compact_candidate) >= 2
                and compact_candidate in remove_spaces(kept_candidate.candidate_keyword)
                for kept_candidate in kept
            ):
                continue
            kept.append(candidate)
        return kept

    def _candidate_position(self, candidate: CandidateEvidence, text: str) -> int:
        for term in (candidate.candidate_keyword, candidate.source_term):
            position = self._find_term_position(text, term)
            if position >= 0:
                return position

        compact_text = remove_spaces(text)
        compact_candidate = remove_spaces(candidate.candidate_keyword)
        compact_position = compact_text.find(compact_candidate)
        if compact_position >= 0:
            return self._compact_index_to_text_position(text, compact_position)
        return len(text)

    def _unique_evidence(self, evidence: list[MatchEvidence]) -> list[MatchEvidence]:
        best_by_keyword: dict[str, MatchEvidence] = {}
        for ev in evidence:
            current = best_by_keyword.get(ev.network_keyword)
            if current and self._evidence_rank(current) <= self._evidence_rank(ev):
                continue
            best_by_keyword[ev.network_keyword] = ev

        unique = list(best_by_keyword.values())
        unique = self._suppress_similar_keyword_evidence(unique)
        unique.sort(key=self._evidence_rank)
        return unique

    def _sentence_ordered_evidence(
        self, evidence: list[MatchEvidence], text: str
    ) -> list[MatchEvidence]:
        unique = self._unique_evidence(evidence)
        unique.sort(
            key=lambda ev: (
                self._sentence_position(ev, text),
                self._evidence_rank(ev),
            )
        )
        return unique

    def _sentence_position(self, ev: MatchEvidence, text: str) -> int:
        direct_positions = [
            pos
            for pos in (
                self._find_term_position(text, ev.input_term),
                self._find_term_position(text, ev.network_keyword),
            )
            if pos >= 0
        ]
        if direct_positions:
            return min(direct_positions)

        compact_text = remove_spaces(text)
        for term in (ev.input_term, ev.network_keyword):
            compact_term = remove_spaces(term)
            if not compact_term:
                continue
            compact_pos = compact_text.find(compact_term)
            if compact_pos >= 0:
                return self._compact_index_to_text_position(text, compact_pos)

        return len(text) + METHOD_PRIORITY.get(ev.match_method, 99)

    def _find_term_position(self, text: str, term: str) -> int:
        if not term:
            return -1

        if is_english_only(term):
            match = re.search(
                rf"(?<![0-9A-Za-z]){re.escape(term)}(?![0-9A-Za-z])",
                text,
            )
            return match.start() if match else -1

        return text.find(term)

    def _compact_index_to_text_position(self, text: str, compact_index: int) -> int:
        current_compact_index = 0
        for text_index, char in enumerate(text):
            if char.isspace():
                continue
            if current_compact_index == compact_index:
                return text_index
            current_compact_index += 1
        return len(text)

    def _suppress_similar_keyword_evidence(
        self, evidence: list[MatchEvidence]
    ) -> list[MatchEvidence]:
        """최종 출력 직전에 비슷한 키워드가 여러 개면 대표 키워드 하나만 남긴다."""
        kept: list[MatchEvidence] = []
        for ev in sorted(evidence, key=self._representative_rank):
            if any(
                self._are_duplicate_like_keywords(ev.network_keyword, kept_ev.network_keyword)
                for kept_ev in kept
            ):
                continue
            kept.append(ev)
        return kept

    def _are_duplicate_like_keywords(self, left: str, right: str) -> bool:
        left_compact = remove_spaces(left)
        right_compact = remove_spaces(right)
        if not left_compact or not right_compact:
            return False
        if left_compact == right_compact:
            return True

        shorter, longer = sorted(
            (left_compact, right_compact), key=lambda keyword: (len(keyword), keyword)
        )
        if len(shorter) == 1 and len(longer) >= 2 and shorter in longer:
            return True
        if len(shorter) >= 2 and len(longer) >= 4 and shorter in longer:
            return True

        if len(left_compact) < 3 or len(right_compact) < 3:
            return False
        if left_compact[0] != right_compact[0]:
            return False
        if abs(len(left_compact) - len(right_compact)) > 2:
            return False
        return (
            SequenceMatcher(None, left_compact, right_compact).ratio()
            >= SIMILAR_KEYWORD_THRESHOLD
        )

    def _representative_rank(self, ev: MatchEvidence) -> tuple[int, int, float, str, str]:
        return (
            -len(remove_spaces(ev.network_keyword)),
            METHOD_PRIORITY.get(ev.match_method, 99),
            -ev.confidence,
            ev.network_keyword,
            ev.input_term,
        )

    def _evidence_rank(self, ev: MatchEvidence) -> tuple[int, float, int, str, str]:
        return (
            METHOD_PRIORITY.get(ev.match_method, 99),
            -ev.confidence,
            -len(remove_spaces(ev.network_keyword)),
            ev.network_keyword,
            ev.input_term,
        )

    def _unique_keywords(self, evidence: list[MatchEvidence]) -> list[str]:
        unique_evidence = self._unique_evidence(evidence)
        unique_evidence.sort(
            key=lambda ev: (
                METHOD_PRIORITY.get(ev.match_method, 99),
                -ev.confidence,
                ev.network_keyword,
            )
        )

        keywords: list[str] = []
        seen: set[str] = set()
        for ev in unique_evidence:
            if ev.network_keyword in seen:
                continue
            seen.add(ev.network_keyword)
            keywords.append(ev.network_keyword)
        return keywords


def print_text_result(result: dict[str, object]) -> None:
    final_keywords = result["final_keywords"]
    candidate_keywords = result["candidate_keywords"]
    evidence = result["match_evidence"]

    keyword_text = ", ".join(final_keywords) if final_keywords else "매칭 없음"
    candidate_text = ", ".join(candidate_keywords) if candidate_keywords else "없음"
    evidence_by_keyword = {row["network_keyword"]: row for row in evidence}
    ordered_evidence = [
        evidence_by_keyword[keyword]
        for keyword in final_keywords
        if keyword in evidence_by_keyword
    ]
    source_text = (
        ", ".join(
            f"{row['input_term']} -> {row['network_keyword']}" for row in ordered_evidence
        )
        if ordered_evidence
        else NO_MATCH_MESSAGE
    )

    print(f"[키워드]: {keyword_text}")
    print(f"[후보 키워드]: {candidate_text}")
    print(f"[출처]: {source_text}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="MD 자연어 문장을 네트워크 키워드로 변환합니다."
    )
    parser.add_argument("text", nargs="*", help="MD가 작성한 자연어 기획 문장")
    parser.add_argument("--json", action="store_true", help="결과를 JSON으로 출력")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    text = " ".join(args.text).strip()

    try:
        pipeline = MDKeywordPipeline()
    except (FileNotFoundError, ValueError) as exc:
        print(f"[오류]: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    def run_once(input_text: str) -> None:
        result = pipeline.extract(input_text)
        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            print_text_result(result)

    if text:
        run_once(text)
        return

    if not sys.stdin.isatty():
        piped_text = sys.stdin.read().strip()
        if not piped_text:
            raise SystemExit("입력 문장을 전달해주세요.")
        run_once(piped_text)
        return

    print("문장을 입력하세요. 종료하려면 빈 줄에서 Enter를 누르세요.")
    while True:
        input_text = input("> ").strip()
        if not input_text:
            break
        run_once(input_text)


if __name__ == "__main__":
    main()
