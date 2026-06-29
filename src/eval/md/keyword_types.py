"""키워드 4유형 소스 — 초안 CSV(dummy) ↔ 확정정의 핫스왑 단일 지점.

4유형 (사용자 정의 ↔ 코드 tag):
  killer  = 성공 키워드에 잘 붙는 (성공 특이)
  mine    = 실패 키워드에 잘 붙는 (지뢰)
  hub     = 일반화된 키워드 (무색무취 백본)
  neutral = 아무것도 아닌 (보조)

지금(확정 전): `KeywordTypeSource.default(eng)`
  → 초안 `data/processed/hin/keyword_final.csv`(tag 컬럼)를 dummy로 로드.
    파일 없으면 자동장부(build_ledger)로 fallback.
확정 후: **같은 CSV를 재생성/편집**하면 코드 변경 없이 그대로 갈아끼워짐.
  또는 `KeywordTypeSource.from_mapping({이름: tag})` 로 임의 정의 주입.

explain.explain_keyword(types=...) 에 주입. 미지정 시 default(dummy)가 기본.
"""
from __future__ import annotations

from typing import Callable, Dict, Optional
import os

_TYPES = ("killer", "mine", "hub", "neutral")

TYPE_KO = {
    "killer": "성공 특이(잘 붙는)",
    "mine": "실패 특이(지뢰)",
    "hub": "일반 백본",
    "neutral": "보조(무색무취)",
}

DEFAULT_FINAL_CSV = "data/processed/hin/keyword_final.csv"


class KeywordTypeSource:
    """4유형 제공자. `tag_name(keyword)->str` 단일 인터페이스로 소스를 추상화."""

    def __init__(self, tag_fn: Callable[[str], str], source: str = "?"):
        self._tag_fn = tag_fn
        self.source = source

    def tag_name(self, name: str) -> str:
        t = self._tag_fn(name)
        return t if t in _TYPES else "neutral"

    # ---------------------------------------------------------------- 기본(dummy)
    @classmethod
    def default(cls, eng=None, csv_path: str = DEFAULT_FINAL_CSV) -> "KeywordTypeSource":
        """초안 CSV가 있으면 그걸로, 없으면 자동장부로 fallback. 확정 시 CSV 재생성만 하면 됨."""
        if os.path.exists(csv_path):
            return cls.from_csv(csv_path)
        if eng is not None:
            return cls.auto(eng)
        raise FileNotFoundError(f"{csv_path} 없음 + eng 미제공 → 유형 소스 불가")

    # ---------------------------------------------------------------- 확정 스왑
    @classmethod
    def from_csv(cls, path: str, name_col: str = "keyword",
                 tag_col: str = "tag") -> "KeywordTypeSource":
        """keyword_final.csv 스키마(keyword|tag|…) 읽어 적용 — 초안 dummy이자 확정 스왑 대상."""
        import pandas as pd
        df = pd.read_csv(path)
        m = dict(zip(df[name_col].astype(str), df[tag_col].astype(str)))
        return cls(lambda n: m.get(n, "neutral"), source=f"csv:{path}({len(m)})")

    @classmethod
    def from_mapping(cls, mapping: Dict[str, str]) -> "KeywordTypeSource":
        """임의 확정 정의 dict({키워드: tag}) 주입."""
        return cls(lambda n: mapping.get(n, "neutral"), source="mapping")

    # ---------------------------------------------------------------- 장부 fallback
    @classmethod
    def auto(cls, eng, universe: str = "full") -> "KeywordTypeSource":
        """CSV 없을 때 fallback — build_ledger 자동 4분류."""
        lg = eng.ledger.get(universe) or eng.build_ledger(universe)

        def f(name: str) -> str:
            i = eng.seed_to_idx(name)
            return lg.tag(i) if i is not None else "neutral"

        return cls(f, source=f"auto:{universe}")
